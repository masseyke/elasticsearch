/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.coordination.ClusterFormationInfoAction;
import org.elasticsearch.action.admin.cluster.coordination.CoordinationDiagnosticsAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * This service reports the health of master stability.
 * If we have had a master within the last 30 seconds, and that master has not changed more than 3 times in the last 30 minutes, then
 * this will report GREEN.
 * If we have had a master within the last 30 seconds, but that master has changed more than 3 times in the last 30 minutes (and that is
 * confirmed by checking with the last-known master), then this will report YELLOW.
 * If we have not had a master within the last 30 seconds, then this will will report RED with one exception. That exception is when:
 * (1) no node is elected master, (2) this node is not master eligible, (3) some node is master eligible, (4) we ask a master-eligible node
 * to run this service, and (5) it comes back with a result that is not RED.
 * Since this service needs to be able to run when there is no master at all, it does not depend on the dedicated health node (which
 * requires the existence of a master).
 */
public class CoordinationDiagnosticsService implements ClusterStateListener, Coordinator.PeerFinderListener {
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final Coordinator coordinator;
    private final MasterHistoryService masterHistoryService;
    /**
     * This is the amount of time we use to make the initial decision -- have we seen a master node in the very recent past?
     */
    private final TimeValue nodeHasMasterLookupTimeframe;
    /**
     * If the master transitions from a non-null master to a null master at least this many times it starts impacting the health status.
     */
    private final int unacceptableNullTransitions;
    /**
     * If the master transitions from one non-null master to a different non-null master at least this many times it starts impacting the
     * health status.
     */
    private final int unacceptableIdentityChanges;

    /*
     * This is a list of tasks that are periodically reaching out to other master eligible nodes to get their ClusterFormationStates for
     * diagnosis.
     * The field is accessed (reads/writes) from multiple threads, but the reference itself is only ever changed on the cluster change
     * event thread.
     */
    private volatile List<Scheduler.Cancellable> clusterFormationInfoTasks = null;
    /*
     * This field holds the results of the tasks in the clusterFormationInfoTasks field above. The field is accessed (reads/writes) from
     * multiple threads, but the reference itself is only ever changed on the cluster change event thread.
     */
    // Non-private for testing
    volatile ConcurrentMap<DiscoveryNode, ClusterFormationStateOrException> clusterFormationResponses = null;

    private static final Logger logger = LogManager.getLogger(CoordinationDiagnosticsService.class);

    /*
     * This is a reference to the task that is periodically reaching out to a master eligible node to get its CoordinationDiagnosticsResult
     * for diagnosis. It is null when no polling is occurring.
     * The field is accessed (reads/writes) from multiple threads, and is also reassigned on multiple threads.
     */
    private volatile AtomicReference<Scheduler.Cancellable> remoteStableMasterHealthIndicatorTask = null;
    /*
     * This field holds the result of the task in the remoteStableMasterHealthIndicatorTask field above. The field is accessed
     * (reads/writes) from multiple threads, and is also reassigned on multiple threads.
     */
    // Non-private for testing
    volatile AtomicReference<RemoteMasterHealthResult> remoteCoordinationDiagnosisResult = new AtomicReference<>();

    /**
     * This field has a reference to an AtomicBoolean indicating whether the most recent attempt at polling for remote coordination
     * diagnostics ought to still be running. There is one AtomicBoolean per remote coordination diagnostics poll. We keep track of the
     * current (or most recent anyway) one here so that the cancelPollingRemoteStableMasterHealthIndicatorService can cancel it. All
     * older copies will have already been cancelled, which will help any still-running polls to know that they have been cancelled (only
     * one needs to be running at any given time).
     */
    private volatile AtomicBoolean remoteCoordinationDiagnosticsCancelled = new AtomicBoolean(true);

    /*
     * The previous three variables (remoteStableMasterHealthIndicatorTask, remoteCoordinationDiagnosisResult, and
     * remoteCoordinationDiagnosticsCancelled) are reassigned on multiple threads. This mutex is used to protect those reassignments.
     */
    private final Object remoteDiagnosticsMutex = new Object();

    /**
     * This is the default amount of time we look back to see if we have had a master at all, before moving on with other checks
     */
    public static final Setting<TimeValue> NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING = Setting.timeSetting(
        "health.master_history.has_master_lookup_timeframe",
        new TimeValue(30, TimeUnit.SECONDS),
        new TimeValue(1, TimeUnit.SECONDS),
        Setting.Property.NodeScope
    );

    /**
     * This is the number of times that it is not OK to have a master go null. This many transitions or more will be reported as a problem.
     */
    public static final Setting<Integer> NO_MASTER_TRANSITIONS_THRESHOLD_SETTING = Setting.intSetting(
        "health.master_history.no_master_transitions_threshold",
        4,
        0,
        Setting.Property.NodeScope
    );

    /**
     * This is the number of times that it is not OK to have a master change identity. This many changes or more will be reported as a
     * problem.
     */
    public static final Setting<Integer> IDENTITY_CHANGES_THRESHOLD_SETTING = Setting.intSetting(
        "health.master_history.identity_changes_threshold",
        4,
        0,
        Setting.Property.NodeScope
    );

    public CoordinationDiagnosticsService(
        ClusterService clusterService,
        TransportService transportService,
        Coordinator coordinator,
        MasterHistoryService masterHistoryService
    ) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.coordinator = coordinator;
        this.masterHistoryService = masterHistoryService;
        this.nodeHasMasterLookupTimeframe = NODE_HAS_MASTER_LOOKUP_TIMEFRAME_SETTING.get(clusterService.getSettings());
        this.unacceptableNullTransitions = NO_MASTER_TRANSITIONS_THRESHOLD_SETTING.get(clusterService.getSettings());
        this.unacceptableIdentityChanges = IDENTITY_CHANGES_THRESHOLD_SETTING.get(clusterService.getSettings());
        clusterService.addListener(this);
        coordinator.addPeerFinderListener(this);
    }

    /**
     * This method calculates the master stability as seen from this node.
     * @param explain If true, the result will contain a non-empty CoordinationDiagnosticsDetails if the resulting status is non-GREEN
     * @return Information about the current stability of the master node, as seen from this node
     */
    public CoordinationDiagnosticsResult diagnoseMasterStability(boolean explain) {
        MasterHistory localMasterHistory = masterHistoryService.getLocalMasterHistory();
        if (hasSeenMasterInHasMasterLookupTimeframe()) {
            return diagnoseOnHaveSeenMasterRecently(localMasterHistory, explain);
        } else {
            return diagnoseOnHaveNotSeenMasterRecently(localMasterHistory, explain);
        }
    }

    /**
     * Returns the health result for the case when we have seen a master recently (at some point in the last 30 seconds).
     * @param localMasterHistory The master history as seen from the local machine
     * @param explain Whether to calculate and include the details and user actions in the result
     * @return The CoordinationDiagnosticsResult for the given localMasterHistory
     */
    private CoordinationDiagnosticsResult diagnoseOnHaveSeenMasterRecently(MasterHistory localMasterHistory, boolean explain) {
        int masterChanges = MasterHistory.getNumberOfMasterIdentityChanges(localMasterHistory.getNodes());
        logger.trace(
            "Have seen a master in the last {}): {}",
            nodeHasMasterLookupTimeframe,
            localMasterHistory.getMostRecentNonNullMaster()
        );
        final CoordinationDiagnosticsResult result;
        if (masterChanges >= unacceptableIdentityChanges) {
            result = diagnoseOnMasterHasChangedIdentity(localMasterHistory, masterChanges, explain);
        } else if (localMasterHistory.hasMasterGoneNullAtLeastNTimes(unacceptableNullTransitions)) {
            result = diagnoseOnMasterHasFlappedNull(localMasterHistory, explain);
        } else {
            result = getMasterIsStableResult(explain, localMasterHistory);
        }
        return result;
    }

    /**
     * Returns the health result when we have detected locally that the master has changed identity repeatedly (by default more than 3
     * times in the last 30 minutes)
     * @param localMasterHistory The master history as seen from the local machine
     * @param masterChanges The number of times that the local machine has seen the master identity change in the last 30 minutes
     * @param explain Whether to calculate and include the details in the result
     * @return The CoordinationDiagnosticsResult for the given localMasterHistory
     */
    private CoordinationDiagnosticsResult diagnoseOnMasterHasChangedIdentity(
        MasterHistory localMasterHistory,
        int masterChanges,
        boolean explain
    ) {
        logger.trace("Have seen {} master changes in the last {}", masterChanges, localMasterHistory.getMaxHistoryAge());
        CoordinationDiagnosticsStatus coordinationDiagnosticsStatus = CoordinationDiagnosticsStatus.YELLOW;
        String summary = String.format(
            Locale.ROOT,
            "The elected master node has changed %d times in the last %s",
            masterChanges,
            localMasterHistory.getMaxHistoryAge()
        );
        CoordinationDiagnosticsDetails details = getDetails(explain, localMasterHistory, null, null);
        return new CoordinationDiagnosticsResult(coordinationDiagnosticsStatus, summary, details);
    }

    /**
     * This returns CoordinationDiagnosticsDetails.EMPTY if explain is false, otherwise a CoordinationDiagnosticsDetails object
     * containing only a "current_master" object and a "recent_masters" array. The "current_master" object will have "node_id" and "name"
     * fields for the master node. Both will be null if the last-seen master was null. The "recent_masters" array will contain
     * "recent_master" objects. Each "recent_master" object will have "node_id" and "name" fields for the master node. These fields will
     * never be null because null masters are not written to this array.
     * @param explain If true, the CoordinationDiagnosticsDetails will contain "current_master" and "recent_masters". Otherwise it will
     *                be empty.
     * @param localMasterHistory The MasterHistory object to pull current and recent master info from
     * @return An empty CoordinationDiagnosticsDetails if explain is false, otherwise a CoordinationDiagnosticsDetails containing only
     * "current_master" and "recent_masters"
     */
    private static CoordinationDiagnosticsDetails getDetails(
        boolean explain,
        MasterHistory localMasterHistory,
        @Nullable Exception remoteException,
        @Nullable Map<String, String> clusterFormationMessages
    ) {
        if (explain == false) {
            return CoordinationDiagnosticsDetails.EMPTY;
        }
        DiscoveryNode masterNode = localMasterHistory.getMostRecentMaster();
        List<DiscoveryNode> recentNonNullMasters = localMasterHistory.getNodes().stream().filter(Objects::nonNull).toList();
        return new CoordinationDiagnosticsDetails(masterNode, recentNonNullMasters, remoteException, clusterFormationMessages);
    }

    /**
     * Returns the health result when we have detected locally that the master has changed to null repeatedly (by default more than 3 times
     * in the last 30 minutes). This method attemtps to use the master history from a remote node to confirm what we are seeing locally.
     * If the information from the remote node confirms that the master history has been unstable, a YELLOW status is returned. If the
     * information from the remote node shows that the master history has been stable, then we assume that the problem is with this node
     * and a GREEN status is returned (the problems with this node will be covered in a separate health indicator). If there had been
     * problems fetching the remote master history, the exception seen will be included in the details of the result.
     * @param localMasterHistory The master history as seen from the local machine
     * @param explain Whether to calculate and include the details in the result
     * @return The CoordinationDiagnosticsResult for the given localMasterHistory
     */
    private CoordinationDiagnosticsResult diagnoseOnMasterHasFlappedNull(MasterHistory localMasterHistory, boolean explain) {
        DiscoveryNode master = localMasterHistory.getMostRecentNonNullMaster();
        boolean localNodeIsMaster = clusterService.localNode().equals(master);
        List<DiscoveryNode> remoteHistory;
        Exception remoteHistoryException = null;
        if (localNodeIsMaster) {
            remoteHistory = null; // We don't need to fetch the remote master's history if we are that remote master
        } else {
            try {
                remoteHistory = masterHistoryService.getRemoteMasterHistory();
            } catch (Exception e) {
                remoteHistory = null;
                remoteHistoryException = e;
            }
        }
        /*
         * If the local node is master, then we have a confirmed problem (since we now know that from this node's point of view the
         * master is unstable).
         * If the local node is not master but the remote history is null then we have a problem (since from this node's point of view the
         * master is unstable, and we were unable to get the master's own view of its history). It could just be a short-lived problem
         * though if the remote history has not arrived yet.
         * If the local node is not master and the master history from the master itself reports that the master has gone null repeatedly
         * or changed identity repeatedly, then we have a problem (the master has confirmed what the local node saw).
         */
        boolean masterConfirmedUnstable = localNodeIsMaster
            || remoteHistoryException != null
            || (remoteHistory != null
                && (MasterHistory.hasMasterGoneNullAtLeastNTimes(remoteHistory, unacceptableNullTransitions)
                    || MasterHistory.getNumberOfMasterIdentityChanges(remoteHistory) >= unacceptableIdentityChanges));
        if (masterConfirmedUnstable) {
            logger.trace("The master node {} thinks it is unstable", master);
            String summary = String.format(
                Locale.ROOT,
                "The cluster's master has alternated between %s and no master multiple times in the last %s",
                localMasterHistory.getNodes().stream().filter(Objects::nonNull).collect(Collectors.toSet()),
                localMasterHistory.getMaxHistoryAge()
            );
            final CoordinationDiagnosticsDetails details = getDetails(explain, localMasterHistory, remoteHistoryException, null);
            return new CoordinationDiagnosticsResult(CoordinationDiagnosticsStatus.YELLOW, summary, details);
        } else {
            logger.trace("This node thinks the master is unstable, but the master node {} thinks it is stable", master);
            return getMasterIsStableResult(explain, localMasterHistory);
        }
    }

    /**
     * Returns a CoordinationDiagnosticsResult for the case when the master is seen as stable
     * @return A CoordinationDiagnosticsResult for the case when the master is seen as stable (GREEN status, no impacts or details)
     */
    private CoordinationDiagnosticsResult getMasterIsStableResult(boolean explain, MasterHistory localMasterHistory) {
        String summary = "The cluster has a stable master node";
        logger.trace("The cluster has a stable master node");
        CoordinationDiagnosticsDetails details = getDetails(explain, localMasterHistory, null, null);
        return new CoordinationDiagnosticsResult(CoordinationDiagnosticsStatus.GREEN, summary, details);
    }

    /**
     * Returns the health result for the case when we have NOT seen a master recently (at some point in the last 30 seconds).
     * @param localMasterHistory The master history as seen from the local machine
     * @param explain Whether to calculate and include the details in the result
     * @return The CoordinationDiagnosticsResult for the given localMasterHistory
     */
    private CoordinationDiagnosticsResult diagnoseOnHaveNotSeenMasterRecently(MasterHistory localMasterHistory, boolean explain) {
        Collection<DiscoveryNode> masterEligibleNodes = getMasterEligibleNodes();
        final CoordinationDiagnosticsResult result;
        boolean clusterHasLeader = coordinator.getPeerFinder().getLeader().isPresent();
        boolean noLeaderAndNoMasters = clusterHasLeader == false && masterEligibleNodes.isEmpty();
        boolean isLocalNodeMasterEligible = clusterService.localNode().isMasterNode();
        if (noLeaderAndNoMasters) {
            result = getResultOnNoMasterEligibleNodes(localMasterHistory, explain);
        } else if (clusterHasLeader) {
            DiscoveryNode currentMaster = coordinator.getPeerFinder().getLeader().get();
            result = getResultOnCannotJoinLeader(localMasterHistory, currentMaster, explain);
        } else if (isLocalNodeMasterEligible == false) { // none is elected master and we aren't master eligible
            result = diagnoseOnHaveNotSeenMasterRecentlyAndWeAreNotMasterEligible(
                coordinator,
                nodeHasMasterLookupTimeframe,
                remoteCoordinationDiagnosisResult,
                explain
            );
        } else { // none is elected master and we are master eligible
            result = diagnoseOnHaveNotSeenMasterRecentlyAndWeAreMasterEligible(
                localMasterHistory,
                masterEligibleNodes,
                coordinator,
                clusterFormationResponses,
                nodeHasMasterLookupTimeframe,
                explain
            );
        }
        return result;
    }

    /**
     * This method handles the case when we have not had an elected master node recently, and we are on a node that is not
     * master-eligible. In this case we reach out to some master-eligible node in order to see what it knows about master stability.
     * @param coordinator The Coordinator for this node
     * @param nodeHasMasterLookupTimeframe The value of health.master_history.has_master_lookup_timeframe
     * @param remoteCoordinationDiagnosisResult
     * @param explain If true, details are returned
     * @return A CoordinationDiagnosticsResult that will be determined by the CoordinationDiagnosticsResult returned by the remote
     * master-eligible node
     */
    static CoordinationDiagnosticsResult diagnoseOnHaveNotSeenMasterRecentlyAndWeAreNotMasterEligible(
        Coordinator coordinator,
        TimeValue nodeHasMasterLookupTimeframe,
        AtomicReference<RemoteMasterHealthResult> remoteCoordinationDiagnosisResult,
        boolean explain
    ) {
        RemoteMasterHealthResult remoteResultOrException = remoteCoordinationDiagnosisResult.get();
        final CoordinationDiagnosticsStatus status;
        final String summary;
        final CoordinationDiagnosticsDetails details;
        if (remoteResultOrException == null) {
            status = CoordinationDiagnosticsStatus.RED;
            summary = String.format(
                Locale.ROOT,
                "No master node observed in the last %s, and this node is not master eligible. Reaching out to a master-eligible node"
                    + " for more information, but no result yet.",
                nodeHasMasterLookupTimeframe
            );
            if (explain) {
                details = CoordinationDiagnosticsDetails.EMPTY; // TODO
            } else {
                details = CoordinationDiagnosticsDetails.EMPTY;
            }
        } else {
            DiscoveryNode remoteNode = remoteResultOrException.node;
            CoordinationDiagnosticsResult remoteResult = remoteResultOrException.result;
            Exception exception = remoteResultOrException.remoteException;
            if (remoteResult != null) {
                if (remoteResult.status().equals(CoordinationDiagnosticsStatus.GREEN) == false) {
                    status = remoteResult.status();
                    summary = remoteResult.summary();
                } else {
                    status = CoordinationDiagnosticsStatus.RED;
                    summary = String.format(
                        Locale.ROOT,
                        "No master node observed in the last %s from this node, but %s reports that the status is GREEN. This "
                            + "indicates that there is a discovery problem on %s",
                        nodeHasMasterLookupTimeframe,
                        remoteNode.getName(),
                        coordinator.getLocalNode().getName()
                    );
                }
                if (explain) {
                    details = remoteResult.details();
                } else {
                    details = CoordinationDiagnosticsDetails.EMPTY;
                }
            } else if (exception != null) {
                status = CoordinationDiagnosticsStatus.RED;
                summary = String.format(
                    Locale.ROOT,
                    "No master node observed in the last %s from this node, and received an exception while reaching out to %s for "
                        + "diagnosis",
                    nodeHasMasterLookupTimeframe,
                    remoteNode.getName()
                );
                if (explain) {
                    details = CoordinationDiagnosticsDetails.EMPTY; // TODO: once we merge in #88020
                } else {
                    details = CoordinationDiagnosticsDetails.EMPTY;
                }
            } else {
                // It should not be possible to get here
                status = CoordinationDiagnosticsStatus.RED;
                summary = String.format(
                    Locale.ROOT,
                    "No master node observed in the last %s from this node, and received an unexpected response from %s when "
                        + "reaching out for diagnosis",
                    nodeHasMasterLookupTimeframe,
                    remoteNode.getName()
                );
                details = CoordinationDiagnosticsDetails.EMPTY;
            }
        }
        return new CoordinationDiagnosticsResult(status, summary, details);
    }

    /**
     * This method handles the case when we have not had an elected master node recently, and we are on a master-eligible node. In this
     * case we look at the cluster formation information from all master-eligible nodes, trying to understand if we have a discovery
     * problem, a problem forming a quorum, or something else.
     * @param localMasterHistory The master history, as seen from this node
     * @param masterEligibleNodes The known master eligible nodes in the cluster
     * @param coordinator The Coordinator for this node
     * @param clusterFormationResponses A map that contains the cluster formation information (or exception encountered while requesting
     *                                  it) from each master eligible node in the cluster
     * @param nodeHasMasterLookupTimeframe The value of health.master_history.has_master_lookup_timeframe
     * @param explain If true, details are returned
     * @return A CoordinationDiagnosticsResult with a RED status
     */
    static CoordinationDiagnosticsResult diagnoseOnHaveNotSeenMasterRecentlyAndWeAreMasterEligible(
        MasterHistory localMasterHistory,
        Collection<DiscoveryNode> masterEligibleNodes,
        Coordinator coordinator,
        ConcurrentMap<DiscoveryNode, ClusterFormationStateOrException> clusterFormationResponses,
        TimeValue nodeHasMasterLookupTimeframe,
        boolean explain

    ) {
        final CoordinationDiagnosticsResult result;
        /*
         * We want to make sure that the same elements are in this set every time we loop through it. We don't care if values are added
         * while we're copying it, which is why this is not synchronized. We only care that once we have a copy it is not changed.
         */
        final Map<DiscoveryNode, ClusterFormationStateOrException> nodeToClusterFormationResponses = Map.copyOf(clusterFormationResponses);
        for (Map.Entry<DiscoveryNode, ClusterFormationStateOrException> entry : nodeToClusterFormationResponses.entrySet()) {
            Exception remoteException = entry.getValue().exception();
            if (remoteException != null) {
                return new CoordinationDiagnosticsResult(
                    CoordinationDiagnosticsStatus.RED,
                    String.format(
                        Locale.ROOT,
                        "No master node observed in the last %s, and an exception occurred while reaching out " + "to %s for diagnosis",
                        nodeHasMasterLookupTimeframe,
                        entry.getKey().getName()
                    ),
                    getDetails(
                        explain,
                        localMasterHistory,
                        remoteException,
                        Map.of(coordinator.getLocalNode().getId(), coordinator.getClusterFormationState().getDescription())
                    )
                );
            }
        }
        Map<DiscoveryNode, ClusterFormationFailureHelper.ClusterFormationState> nodeClusterFormationStateMap =
            nodeToClusterFormationResponses.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().clusterFormationState()));
        Map<String, String> nodeIdToClusterFormationDescription = nodeClusterFormationStateMap.entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getId(), entry -> entry.getValue().getDescription()));
        if (anyNodeInClusterReportsDiscoveryProblems(masterEligibleNodes, nodeClusterFormationStateMap)) {
            result = new CoordinationDiagnosticsResult(
                CoordinationDiagnosticsStatus.RED,
                String.format(
                    Locale.ROOT,
                    "No master node observed in the last %s, and some master eligible nodes are unable to discover other master "
                        + "eligible nodes",
                    nodeHasMasterLookupTimeframe
                ),
                getDetails(explain, localMasterHistory, null, nodeIdToClusterFormationDescription)
            );
        } else {
            if (anyNodeInClusterReportsQuorumProblems(nodeClusterFormationStateMap)) {
                result = new CoordinationDiagnosticsResult(
                    CoordinationDiagnosticsStatus.RED,
                    String.format(
                        Locale.ROOT,
                        "No master node observed in the last %s, and the master eligible nodes are unable to form a quorum",
                        nodeHasMasterLookupTimeframe
                    ),
                    getDetails(explain, localMasterHistory, null, nodeIdToClusterFormationDescription)
                );
            } else {
                result = new CoordinationDiagnosticsResult(
                    CoordinationDiagnosticsStatus.RED,
                    String.format(
                        Locale.ROOT,
                        "No master node observed in the last %s, and the cause has not been determined.",
                        nodeHasMasterLookupTimeframe
                    ),
                    getDetails(explain, localMasterHistory, null, nodeIdToClusterFormationDescription)
                );
            }
        }
        return result;
    }

    /**
     * This method checks whether each master eligible node has discovered each of the other master eligible nodes. For the sake of this
     * method, a discovery problem is when the foundPeers of any ClusterFormationState on any node we have that information for does not
     * contain all of the nodes in the local coordinator.getFoundPeers().
     * @param masterEligibleNodes The collection of all master eligible nodes
     * @param nodeToClusterFormationStateMap A map of each master node to its ClusterFormationState
     * @return true if there are discovery problems, false otherwise
     */
    static boolean anyNodeInClusterReportsDiscoveryProblems(
        Collection<DiscoveryNode> masterEligibleNodes,
        Map<DiscoveryNode, ClusterFormationFailureHelper.ClusterFormationState> nodeToClusterFormationStateMap
    ) {
        Map<DiscoveryNode, Collection<DiscoveryNode>> nodesNotDiscoveredMap = new HashMap<>();
        for (Map.Entry<DiscoveryNode, ClusterFormationFailureHelper.ClusterFormationState> entry : nodeToClusterFormationStateMap
            .entrySet()) {
            Set<DiscoveryNode> foundPeersOnNode = new HashSet<>(entry.getValue().foundPeers());
            if (foundPeersOnNode.containsAll(masterEligibleNodes) == false) {
                Collection<DiscoveryNode> nodesNotDiscovered = masterEligibleNodes.stream()
                    .filter(node -> foundPeersOnNode.contains(node) == false)
                    .toList();
                nodesNotDiscoveredMap.put(entry.getKey(), nodesNotDiscovered);
            }
        }
        if (nodesNotDiscoveredMap.isEmpty()) {
            return false;
        } else {
            String nodeDiscoveryProblemsMessage = nodesNotDiscoveredMap.entrySet()
                .stream()
                .map(
                    entry -> String.format(
                        Locale.ROOT,
                        "%s cannot discover [%s]",
                        entry.getKey().getName(),
                        entry.getValue().stream().map(DiscoveryNode::getName).collect(Collectors.joining(", "))
                    )
                )
                .collect(Collectors.joining("; "));
            logger.debug("The following nodes report discovery problems: {}", nodeDiscoveryProblemsMessage);
            return true;
        }
    }

    /**
     * This method checks that each master eligible node in the quorum thinks that it can form a quorum. If there are nodes that report a
     * problem forming a quorum, this method returns true. This method determines whether a node thinks that a quorum can be formed by
     * checking the value of that node's ClusterFormationState.hasDiscoveredQuorum field.
     * @param nodeToClusterFormationStateMap A map of each master node to its ClusterFormationState
     * @return True if any nodes in nodeToClusterFormationStateMap report a problem forming a quorum, false otherwise.
     */
    static boolean anyNodeInClusterReportsQuorumProblems(
        Map<DiscoveryNode, ClusterFormationFailureHelper.ClusterFormationState> nodeToClusterFormationStateMap
    ) {
        Map<DiscoveryNode, String> quorumProblems = new HashMap<>();
        for (Map.Entry<DiscoveryNode, ClusterFormationFailureHelper.ClusterFormationState> entry : nodeToClusterFormationStateMap
            .entrySet()) {
            ClusterFormationFailureHelper.ClusterFormationState clusterFormationState = entry.getValue();
            if (clusterFormationState.hasDiscoveredQuorum() == false) {
                quorumProblems.put(entry.getKey(), clusterFormationState.getDescription());
            }
        }
        if (quorumProblems.isEmpty()) {
            return false;
        } else {
            String quorumProblemsMessage = quorumProblems.entrySet()
                .stream()
                .map(
                    entry -> String.format(
                        Locale.ROOT,
                        "%s reports that a quorum " + "cannot be formed: [%s]",
                        entry.getKey().getName(),
                        entry.getValue()
                    )
                )
                .collect(Collectors.joining("; "));
            logger.debug("Some master eligible nodes report that a quorum cannot be formed: {}", quorumProblemsMessage);
            return true;
        }
    }

    /**
     * Creates a CoordinationDiagnosticsResult in the case that there has been no master in the last few seconds, there is no elected
     * master known, and there are no master eligible nodes. The status will be RED, and the details (if explain is true) will contain
     * the list of any masters seen previously and a description of known problems from this node's Coordinator.
     * @param localMasterHistory Used to pull recent master nodes for the details if explain is true
     * @param explain If true, details are returned
     * @return A CoordinationDiagnosticsResult with a RED status
     */
    private CoordinationDiagnosticsResult getResultOnNoMasterEligibleNodes(MasterHistory localMasterHistory, boolean explain) {
        String summary = "No master eligible nodes found in the cluster";
        CoordinationDiagnosticsDetails details = getDetails(
            explain,
            localMasterHistory,
            null,
            Map.of(coordinator.getLocalNode().getId(), coordinator.getClusterFormationState().getDescription())
        );
        return new CoordinationDiagnosticsResult(CoordinationDiagnosticsStatus.RED, summary, details);
    }

    /**
     * Creates a CoordinationDiagnosticsResult in the case that there has been no master in the last few seconds in this node's cluster
     * state, but PeerFinder reports that there is an elected master. The assumption is that this node is having a problem joining the
     * elected master. The status will be RED, and the details (if explain is true) will contain the list of any masters seen previously
     * and a description of known problems from this node's Coordinator.
     * @param localMasterHistory Used to pull recent master nodes for the details if explain is true
     * @param currentMaster The node that PeerFinder reports as the elected master
     * @param explain If true, details are returned
     * @return A CoordinationDiagnosticsResult with a RED status
     */
    private CoordinationDiagnosticsResult getResultOnCannotJoinLeader(
        MasterHistory localMasterHistory,
        DiscoveryNode currentMaster,
        boolean explain
    ) {
        String summary = String.format(
            Locale.ROOT,
            "%s has been elected master, but the node being queried, %s, is unable to join it",
            currentMaster,
            clusterService.localNode()
        );
        CoordinationDiagnosticsDetails details = getDetails(
            explain,
            localMasterHistory,
            null,
            Map.of(coordinator.getLocalNode().getId(), coordinator.getClusterFormationState().getDescription())
        );
        return new CoordinationDiagnosticsResult(CoordinationDiagnosticsStatus.RED, summary, details);
    }

    /**
     * Returns the master eligible nodes as found in this node's Coordinator, plus the local node if it is master eligible.
     * @return All known master eligible nodes in this cluster
     */
    private Collection<DiscoveryNode> getMasterEligibleNodes() {
        Set<DiscoveryNode> masterEligibleNodes = new HashSet<>();
        coordinator.getFoundPeers().forEach(node -> {
            if (node.isMasterNode()) {
                masterEligibleNodes.add(node);
            }
        });
        // Coordinator does not report the local node, so add it:
        if (clusterService.localNode().isMasterNode()) {
            masterEligibleNodes.add(clusterService.localNode());
        }
        return masterEligibleNodes;
    }

    /**
     * This returns true if this node has seen a master node within the last few seconds
     * @return true if this node has seen a master node within the last few seconds, false otherwise
     */
    private boolean hasSeenMasterInHasMasterLookupTimeframe() {
        return masterHistoryService.getLocalMasterHistory().hasSeenMasterInLastNSeconds((int) nodeHasMasterLookupTimeframe.seconds());
    }

    /*
     * If we detect that the master has gone null 3 or more times (by default), we ask the MasterHistoryService to fetch the master
     * history as seen from the most recent master node so that it is ready in case a health API request comes in. The request to the
     * MasterHistoryService is made asynchronously, and populates the value that MasterHistoryService.getRemoteMasterHistory() will return.
     * The remote master history is ordinarily returned very quickly if it is going to be returned, so the odds are very good it will be
     * in place by the time a request for it comes in. If not, this service's status will briefly switch to yellow.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
        DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
        if (currentMaster == null && previousMaster != null) {
            if (masterHistoryService.getLocalMasterHistory().hasMasterGoneNullAtLeastNTimes(unacceptableNullTransitions)) {
                DiscoveryNode master = masterHistoryService.getLocalMasterHistory().getMostRecentNonNullMaster();
                /*
                 * If the most recent master was this box, there is no point in making a transport request -- we already know what this
                 * box's view of the master history is
                 */
                if (master != null && clusterService.localNode().equals(master) == false) {
                    masterHistoryService.refreshRemoteMasterHistory(master);
                }
            }
        }
        if (currentMaster == null && clusterService.localNode().isMasterNode()) {
            /*
             * This begins polling all master-eligible nodes for cluster formation information. However there's a 10-second delay before it
             * starts, so in the normal situation where during a master transition it flips from master1 -> null -> master2, it the
             * polling tasks will be canceled before any requests are actually made.
             */
            beginPollingClusterFormationInfo();
        } else {
            cancelPollingClusterFormationInfo();
        }
    }

    /**
     * This method begins polling all known master-eligible nodes for cluster formation information. After a 10-second initial delay, it
     * polls each node every 10 seconds until cancelPollingClusterFormationInfo() is called.
     */
    private void beginPollingClusterFormationInfo() {
        assert ThreadPool.assertCurrentThreadPool(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME);
        cancelPollingClusterFormationInfo();
        ConcurrentMap<DiscoveryNode, ClusterFormationStateOrException> responses = new ConcurrentHashMap<>();
        List<Scheduler.Cancellable> cancellables = new CopyOnWriteArrayList<>();
        beginPollingClusterFormationInfo(getMasterEligibleNodes(), responses::put, cancellables::add);
        clusterFormationResponses = responses;
        clusterFormationInfoTasks = cancellables;
    }

    /**
     * This method returns quickly, but in the background schedules to query the remote node's cluster formation state in 10 seconds, and
     * repeats doing that until cancel() is called on all of the Cancellable that this method inserts into cancellables. This method
     * exists (rather than being just part of the beginPollingClusterFormationInfo() above) in order to facilitate unit testing.
     * @param nodeResponseConsumer A consumer for any results produced for a node by this method
     * @param cancellableConsumer A consumer for any Cancellable tasks produced by this method
     */
    // Non-private for testing
    void beginPollingClusterFormationInfo(
        Collection<DiscoveryNode> masterEligibleNodes,
        BiConsumer<DiscoveryNode, ClusterFormationStateOrException> nodeResponseConsumer,
        Consumer<Scheduler.Cancellable> cancellableConsumer
    ) {
        masterEligibleNodes.forEach(masterEligibleNode -> {
            Consumer<ClusterFormationStateOrException> responseConsumer = result -> nodeResponseConsumer.accept(masterEligibleNode, result);
            cancellableConsumer.accept(
                fetchClusterFormationInfo(
                    masterEligibleNode,
                    responseConsumer.andThen(
                        rescheduleClusterFormationFetchConsumer(masterEligibleNode, responseConsumer, cancellableConsumer)
                    )
                )
            );
        });
    }

    /**
     * This wraps the responseConsumer in a Consumer that will run rescheduleClusterFormationFetchConsumer() after responseConsumer has
     * completed, adding the resulting Cancellable to cancellableConsumer.
     * @param masterEligibleNode The node being polled
     * @param responseConsumer The response consumer to be wrapped
     * @param cancellableConsumer The list of Cancellables
     * @return
     */
    private Consumer<CoordinationDiagnosticsService.ClusterFormationStateOrException> rescheduleClusterFormationFetchConsumer(
        DiscoveryNode masterEligibleNode,
        Consumer<CoordinationDiagnosticsService.ClusterFormationStateOrException> responseConsumer,
        Consumer<Scheduler.Cancellable> cancellableConsumer
    ) {
        return response -> {
            cancellableConsumer.accept(
                fetchClusterFormationInfo(
                    masterEligibleNode,
                    responseConsumer.andThen(
                        rescheduleClusterFormationFetchConsumer(masterEligibleNode, responseConsumer, cancellableConsumer)
                    )
                )
            );
        };
    }

    private void cancelPollingClusterFormationInfo() {
        assert ThreadPool.assertCurrentThreadPool(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME);
        if (clusterFormationResponses != null) {
            clusterFormationInfoTasks.forEach(Scheduler.Cancellable::cancel);
            clusterFormationResponses = null;
            clusterFormationInfoTasks = null;
        }
    }

    /**
     * This method returns quickly, but in the background schedules to query the remote node's cluster formation state in 10 seconds
     * unless cancel() is called on the Cancellable that this method returns.
     * @param node The node to poll for cluster formation information
     * @param responseConsumer The consumer of the cluster formation info for the node, or the exception encountered while contacting it
     * @return A Cancellable for the task that is scheduled to fetch cluster formation information
     */
    private Scheduler.Cancellable fetchClusterFormationInfo(
        DiscoveryNode node,
        Consumer<ClusterFormationStateOrException> responseConsumer
    ) {
        StepListener<Releasable> connectionListener = new StepListener<>();
        StepListener<ClusterFormationInfoAction.Response> fetchClusterInfoListener = new StepListener<>();
        long startTime = System.nanoTime();
        connectionListener.whenComplete(releasable -> {
            logger.trace("Opened connection to {}, making cluster coordination info request", node);
            // If we don't get a response in 10 seconds that is a failure worth capturing on its own:
            final TimeValue transportTimeout = TimeValue.timeValueSeconds(10);
            transportService.sendRequest(
                node,
                ClusterFormationInfoAction.NAME,
                new ClusterFormationInfoAction.Request(),
                TransportRequestOptions.timeout(transportTimeout),
                new ActionListenerResponseHandler<>(
                    ActionListener.runBefore(fetchClusterInfoListener, () -> Releasables.close(releasable)),
                    ClusterFormationInfoAction.Response::new
                )
            );
        }, e -> {
            logger.warn("Exception connecting to master node", e);
            responseConsumer.accept(new ClusterFormationStateOrException(e));
        });

        fetchClusterInfoListener.whenComplete(response -> {
            long endTime = System.nanoTime();
            logger.trace("Received cluster coordination info from {} in {}", node, TimeValue.timeValueNanos(endTime - startTime));
            responseConsumer.accept(new ClusterFormationStateOrException(response.getClusterFormationState()));
        }, e -> {
            logger.warn("Exception in cluster coordination info request to master node", e);
            responseConsumer.accept(new ClusterFormationStateOrException(e));
        });

        return transportService.getThreadPool().schedule(() -> {
            Version minSupportedVersion = Version.V_8_4_0;
            if (node.getVersion().onOrAfter(minSupportedVersion) == false) { // This was introduced in 8.4.0
                logger.trace(
                    "Cannot get cluster coordination info for {} because it is at version {} and {} is required",
                    node,
                    node.getVersion(),
                    minSupportedVersion
                );
            } else {
                transportService.connectToNode(
                    // Note: This connection must be explicitly closed in the connectionListener
                    node,
                    ConnectionProfile.buildDefaultConnectionProfile(clusterService.getSettings()),
                    connectionListener
                );
            }
        }, new TimeValue(10, TimeUnit.SECONDS), ThreadPool.Names.SAME);
    }

    private void beginPollingRemoteStableMasterHealthIndicatorService(Collection<DiscoveryNode> masterEligibleNodes) {
        synchronized (remoteDiagnosticsMutex) {
            if (remoteCoordinationDiagnosticsCancelled.get()) { // Don't start a 2nd one if the last hasn't been cancelled
                AtomicReference<Scheduler.Cancellable> cancellableReference = new AtomicReference<>();
                AtomicReference<RemoteMasterHealthResult> resultReference = new AtomicReference<>();
                AtomicBoolean isCancelled = new AtomicBoolean(false);
                beginPollingRemoteStableMasterHealthIndicatorService(masterEligibleNodes, resultReference::set, task -> {
                    synchronized (remoteDiagnosticsMutex) {
                        if (isCancelled.get()) {
                            /*
                             * cancelPollingRemoteStableMasterHealthIndicatorService() has been called already and if we were to put
                             * this task into this cancellableReference it would be lost (and never cancelled).
                             */
                            task.cancel();
                            logger.trace("A Cancellable came in for a cancelled remote coordination diagnostics task");
                        } else {
                            cancellableReference.set(task);
                        }
                    }
                }, isCancelled);
                remoteStableMasterHealthIndicatorTask = cancellableReference;
                remoteCoordinationDiagnosisResult = resultReference;
                // We're leaving the old one forever false in case something is still running and checks it in the future
                remoteCoordinationDiagnosticsCancelled = isCancelled;
            }
        }
    }

    /**
     * This method returns quickly, but in the background schedules to query the remote node's cluster diagnostics in 10 seconds, and
     * repeats doing that until cancel() is called on all of the Cancellable that this method sends to the cancellableConsumer. This method
     * exists (rather than being just part of the beginPollingRemoteStableMasterHealthIndicatorService() above) in order to facilitate
     * unit testing.
     * masterEligibleNodes A collection of all master eligible nodes that may be polled
     * @param responseConsumer A consumer for any results produced for a node by this method
     * @param cancellableConsumer A consumer for any Cancellable tasks produced by this method
     * @param isCancelled If true, this task has been cancelled in another thread and does not need to do any more work
     */
    // Non-private for testing
    void beginPollingRemoteStableMasterHealthIndicatorService(
        Collection<DiscoveryNode> masterEligibleNodes,
        Consumer<RemoteMasterHealthResult> responseConsumer,
        Consumer<Scheduler.Cancellable> cancellableConsumer,
        AtomicBoolean isCancelled
    ) {
        masterEligibleNodes.stream().findAny().ifPresentOrElse(masterEligibleNode -> {
            cancellableConsumer.accept(
                fetchCoordinationDiagnostics(
                    masterEligibleNode,
                    responseConsumer.andThen(
                        rescheduleDiagnosticsFetchConsumer(masterEligibleNode, responseConsumer, cancellableConsumer, isCancelled)
                    ),
                    isCancelled
                )
            );
        }, () -> logger.trace("No master eligible node found"));
    }

    /**
     * This wraps the responseConsumer in a Consumer that will run rescheduleDiagnosticsFetchConsumer() after responseConsumer has
     * completed, adding the resulting Cancellable to cancellableConsumer.
     * @param masterEligibleNode The node being polled
     * @param responseConsumer The response consumer to be wrapped
     * @param cancellableConsumer The list of Cancellables
     * @param isCancelled If true, this task has been cancelled in another thread and does not need to do any more work
     * @return A wrapped Consumer that will run fetchCoordinationDiagnostics()
     */
    private Consumer<RemoteMasterHealthResult> rescheduleDiagnosticsFetchConsumer(
        DiscoveryNode masterEligibleNode,
        Consumer<RemoteMasterHealthResult> responseConsumer,
        Consumer<Scheduler.Cancellable> cancellableConsumer,
        AtomicBoolean isCancelled
    ) {
        return response -> {
            if (isCancelled.get()) {
                logger.trace("An attempt to reschedule a cancelled remote coordination diagnostics task is being ignored");
            } else {
                /*
                 * We make sure that the task hasn't been cancelled. If it has, we don't reschedule the task. Since this block is not
                 * synchronized it is possible that the task will be cancelled after the check and before the following is run. In that case
                 * the cancellableConsumer will immediately cancel the task.
                 */
                cancellableConsumer.accept(
                    fetchCoordinationDiagnostics(
                        masterEligibleNode,
                        responseConsumer.andThen(
                            rescheduleDiagnosticsFetchConsumer(masterEligibleNode, responseConsumer, cancellableConsumer, isCancelled)
                        ),
                        isCancelled
                    )
                );
            }
        };
    }

    /**
     * This method returns quickly, but in the background schedules to query the remote node's cluster diagnostics in 10 seconds
     * unless cancel() is called on the Cancellable that this method returns.
     * @param node The node to poll for cluster diagnostics
     * @param responseConsumer The consumer of the cluster diagnostics for the node, or the exception encountered while contacting it
     * @param isCancelled If true, this task has been cancelled in another thread and does not need to do any more work
     * @return A Cancellable for the task that is scheduled to fetch cluster diagnostics
     */
    private Scheduler.Cancellable fetchCoordinationDiagnostics(
        DiscoveryNode node,
        Consumer<RemoteMasterHealthResult> responseConsumer,
        AtomicBoolean isCancelled
    ) {
        StepListener<Releasable> connectionListener = new StepListener<>();
        StepListener<CoordinationDiagnosticsAction.Response> fetchCoordinationDiagnosticsListener = new StepListener<>();
        long startTime = System.nanoTime();
        connectionListener.whenComplete(releasable -> {
            if (isCancelled.get()) {
                IOUtils.close(releasable);
                logger.trace(
                    "Opened connection to {} for a remote coordination diagnostics request, but the task was cancelled and the "
                        + "trasport request will not be made",
                    node
                );
            } else {
                /*
                 * Since this block is not synchronized it is possible that this task is cancelled between the check above and when the
                 * code below is run, but this is harmless and not worth the additional synchronization in the normal case. The result
                 * will just be ignored.
                 */
                logger.trace("Opened connection to {}, making master stability request", node);
                // If we don't get a response in 10 seconds that is a failure worth capturing on its own:
                final TimeValue transportTimeout = TimeValue.timeValueSeconds(10);
                transportService.sendRequest(
                    node,
                    CoordinationDiagnosticsAction.NAME,
                    new CoordinationDiagnosticsAction.Request(true),
                    TransportRequestOptions.timeout(transportTimeout),
                    new ActionListenerResponseHandler<>(
                        ActionListener.runBefore(fetchCoordinationDiagnosticsListener, () -> Releasables.close(releasable)),
                        CoordinationDiagnosticsAction.Response::new
                    )
                );
            }
        }, e -> {
            logger.warn("Exception connecting to master node", e);
            responseConsumer.accept(new RemoteMasterHealthResult(node, null, e));
        });

        fetchCoordinationDiagnosticsListener.whenComplete(response -> {
            long endTime = System.nanoTime();
            logger.trace("Received master stability result from {} in {}", node, TimeValue.timeValueNanos(endTime - startTime));
            /*
             * It is possible that the task has been cancelled at this point, but it does not really matter. In that case this result
             * will be ignored and soon garbage collected.
             */
            responseConsumer.accept(new RemoteMasterHealthResult(node, response.getCoordinationDiagnosticsResult(), null));
        }, e -> {
            logger.warn("Exception in master stability request to master node", e);
            responseConsumer.accept(new RemoteMasterHealthResult(node, null, e));
        });

        return transportService.getThreadPool().schedule(() -> {
            Version minSupportedVersion = Version.V_8_4_0;
            if (node.getVersion().onOrAfter(minSupportedVersion) == false) { // This was introduced in 8.4.0
                logger.trace(
                    "Cannot get cluster coordination info for {} because it is at version {} and {} is required",
                    node,
                    node.getVersion(),
                    minSupportedVersion
                );
            } else if (isCancelled.get()) {
                logger.trace("The remote coordination diagnostics task has been cancelled, so not opening a remote connection");
            } else {
                /*
                 * Since this block is not synchronized it is possible that this task is cancelled between the check above and when the
                 * code below is run, but this is harmless and not worth the additional synchronization in the normal case. In that case
                 * the connection will just be closed and the transport request will not be made.
                 */
                transportService.connectToNode(
                    // Note: This connection must be explicitly closed in the connectionListener
                    node,
                    ConnectionProfile.buildDefaultConnectionProfile(clusterService.getSettings()),
                    connectionListener
                );
            }
        }, new TimeValue(10, TimeUnit.SECONDS), ThreadPool.Names.SAME);
    }

    private void cancelPollingRemoteStableMasterHealthIndicatorService() {
        synchronized (remoteDiagnosticsMutex) {
            if (remoteStableMasterHealthIndicatorTask != null && remoteCoordinationDiagnosticsCancelled.getAndSet(true) == false) {
                remoteStableMasterHealthIndicatorTask.get().cancel();
                remoteCoordinationDiagnosisResult = new AtomicReference<>();
            }
        }
    }

    @Override
    public void onFoundPeersUpdated() {
        /*
         * If we are on a non-master-eligible node, and the list of peers in PeerFinder is non-empty, that implies that there is
         * currently no master node elected. At the time that clusterChanged is called notifying us that there is no master, the list of
         * peers is empty (it is before this method is called). That is why this logic is in here rather than in clusterChanged.
         * This begins polling a random master-eligible node for its result from this service. However there's a 10-second delay before it
         * starts, so in the normal situation where during a master transition it flips from master1 -> null -> master2, it the
         * polling tasks will be canceled before any requests are actually made.
         * Note that this method can be called from multiple threads.
         */
        if (clusterService.localNode().isMasterNode() == false) {
            /*
             * Note that PeerFinder (the source of master eligible nodes) could be updating the master eligible nodes on a different
             * thread, so making a copy here so that it doesn't change for the short duration of this method.
             */
            List<DiscoveryNode> masterEligibleNodes = new ArrayList<>(getMasterEligibleNodes());
            if (masterEligibleNodes.isEmpty()) {
                cancelPollingRemoteStableMasterHealthIndicatorService();
            } else {
                beginPollingRemoteStableMasterHealthIndicatorService(masterEligibleNodes);
            }
        }
    }

    // Non-private for testing
    record ClusterFormationStateOrException(
        ClusterFormationFailureHelper.ClusterFormationState clusterFormationState,
        Exception exception
    ) {
        ClusterFormationStateOrException {
            if (clusterFormationState != null && exception != null) {
                throw new IllegalArgumentException("Cluster formation state and exception cannot both be non-null");
            }
        }

        ClusterFormationStateOrException(ClusterFormationFailureHelper.ClusterFormationState clusterFormationState) {
            this(clusterFormationState, null);
        }

        ClusterFormationStateOrException(Exception exception) {
            this(null, exception);
        }
    }

    public record CoordinationDiagnosticsResult(
        CoordinationDiagnosticsStatus status,
        String summary,
        CoordinationDiagnosticsDetails details
    ) implements Writeable {

        public CoordinationDiagnosticsResult(StreamInput in) throws IOException {
            this(CoordinationDiagnosticsStatus.fromStreamInput(in), in.readString(), new CoordinationDiagnosticsDetails(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            status.writeTo(out);
            out.writeString(summary);
            details.writeTo(out);
        }
    }

    public enum CoordinationDiagnosticsStatus implements Writeable {
        GREEN,
        UNKNOWN,
        YELLOW,
        RED;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public static CoordinationDiagnosticsStatus fromStreamInput(StreamInput in) throws IOException {
            return in.readEnum(CoordinationDiagnosticsStatus.class);
        }
    }

    public record CoordinationDiagnosticsDetails(
        DiscoveryNode currentMaster,
        List<DiscoveryNode> recentMasters,
        @Nullable String remoteExceptionMessage,
        @Nullable String remoteExceptionStackTrace,
        @Nullable Map<String, String> nodeToClusterFormationDescriptionMap
    ) implements Writeable {
        public CoordinationDiagnosticsDetails(
            DiscoveryNode currentMaster,
            List<DiscoveryNode> recentMasters,
            Exception remoteException,
            Map<String, String> nodeToClusterFormationDescriptionMap
        ) {
            this(
                currentMaster,
                recentMasters,
                remoteException == null ? null : remoteException.getMessage(),
                getStackTrace(remoteException),
                nodeToClusterFormationDescriptionMap
            );
        }

        public CoordinationDiagnosticsDetails(StreamInput in) throws IOException {
            this(
                readCurrentMaster(in),
                readRecentMasters(in),
                in.readOptionalString(),
                in.readOptionalString(),
                readClusterFormationStates(in)
            );
        }

        private static DiscoveryNode readCurrentMaster(StreamInput in) throws IOException {
            boolean hasCurrentMaster = in.readBoolean();
            DiscoveryNode currentMaster;
            if (hasCurrentMaster) {
                currentMaster = new DiscoveryNode(in);
            } else {
                currentMaster = null;
            }
            return currentMaster;
        }

        private static List<DiscoveryNode> readRecentMasters(StreamInput in) throws IOException {
            boolean hasRecentMasters = in.readBoolean();
            List<DiscoveryNode> recentMasters;
            if (hasRecentMasters) {
                recentMasters = in.readImmutableList(DiscoveryNode::new);
            } else {
                recentMasters = null;
            }
            return recentMasters;
        }

        private static Map<String, String> readClusterFormationStates(StreamInput in) throws IOException {
            if (in.readBoolean()) {
                return in.readMap(StreamInput::readString, StreamInput::readString);
            } else {
                return Map.of();
            }
        }

        private static String getStackTrace(Exception e) {
            if (e == null) {
                return null;
            }
            StringWriter stringWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stringWriter));
            return stringWriter.toString();
        }

        public static final CoordinationDiagnosticsDetails EMPTY = new CoordinationDiagnosticsDetails(null, null, null, null, null);

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (currentMaster == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                currentMaster.writeTo(out);
            }
            if (recentMasters == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeList(recentMasters);
            }
            out.writeOptionalString(remoteExceptionMessage);
            out.writeOptionalString(remoteExceptionStackTrace);
            if (nodeToClusterFormationDescriptionMap == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeMap(nodeToClusterFormationDescriptionMap, StreamOutput::writeString, StreamOutput::writeString);
            }
        }

    }

    // Non-private for testing:
    record RemoteMasterHealthResult(DiscoveryNode node, CoordinationDiagnosticsResult result, Exception remoteException) {}
}
