/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DiskHealthIndicatorServiceTests extends ESTestCase {
    public void testServiceBasics() {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(false, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        {
            HealthStatus expectedStatus = HealthStatus.GREEN;
            HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
        }
        {
            HealthStatus expectedStatus = HealthStatus.YELLOW;
            HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
        }
        {
            HealthStatus expectedStatus = HealthStatus.RED;
            HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
        }
    }

    public void testGreen() throws IOException {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(false, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.GREEN;
        HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(result.symptom(), equalTo("The cluster has enough available disk space."));
        assertThat(result.impacts().size(), equalTo(0));
        assertThat(result.diagnosisList().size(), equalTo(0));
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get("green_nodes"), equalTo(discoveryNodes.size()));
        assertThat(details.get("unknown_nodes"), equalTo(0));
        assertThat(details.get("yellow_nodes"), equalTo(0));
        assertThat(details.get("red_nodes"), equalTo(0));
        assertThat(details.get("blocked_indices"), equalTo(0));
    }

    public void testRedNoBlocksNoIndices() throws IOException {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(false, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.RED;
        HealthInfo healthInfo = createHealthInfo(expectedStatus, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(result.symptom(), equalTo("1 data node is out of disk space."));
        assertThat(result.impacts().size(), equalTo(1));
        HealthIndicatorImpact impact = result.impacts().get(0);
        assertNotNull(impact);
        List<ImpactArea> impactAreas = impact.impactAreas();
        assertThat(impactAreas.size(), equalTo(1));
        assertThat(impactAreas.get(0), equalTo(ImpactArea.INGEST));
        assertThat(impact.severity(), equalTo(1));
        assertThat(impact.impactDescription(), equalTo("Cannot insert or update documents in the affected indices."));
        assertThat(result.diagnosisList().size(), equalTo(1));
        Diagnosis diagnosis = result.diagnosisList().get(0);
        List<String> affectedResources = diagnosis.affectedResources();
        assertThat(affectedResources.size(), equalTo(1));
        String expectedRedNodeId = healthInfo.diskInfoByNode()
            .entrySet()
            .stream()
            .filter(entry -> expectedStatus.equals(entry.getValue().healthStatus()))
            .map(Map.Entry::getKey)
            .findAny()
            .orElseThrow();
        assertThat(affectedResources.get(0), equalTo(expectedRedNodeId));
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get("green_nodes"), equalTo(discoveryNodes.size() - 1));
        assertThat(details.get("unknown_nodes"), equalTo(0));
        assertThat(details.get("yellow_nodes"), equalTo(0));
        assertThat(details.get("red_nodes"), equalTo(1));
        assertThat(details.get("blocked_indices"), equalTo(0));
    }

    public void testRedNoBlocksWithIndices() throws IOException {
        /*
         * This method tests that we get the expected behavior when there are nodes with indices that report RED status and there are no
         * blocks in the cluster state.
         */
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        HealthStatus expectedStatus = HealthStatus.RED;
        int numberOfRedNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(expectedStatus, numberOfRedNodes, discoveryNodes);
        Set<String> redNodeIds = healthInfo.diskInfoByNode()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().healthStatus().equals(expectedStatus))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        Set<String> nonRedNodeIds = healthInfo.diskInfoByNode()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().healthStatus().equals(expectedStatus) == false)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        Map<String, Set<String>> indexNameToNodeIdsMap = new HashMap<>();
        int numberOfIndices = randomIntBetween(1, 1000);
        int numberOfRedIndices = randomIntBetween(1, numberOfIndices);
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(20);
            /*
             * The following is artificial but useful for making sure the test has the right counts. The first numberOfRedIndices indices
             *  are always placed on all of the red nodes. All other indices are placed on all of the non red nodes.
             */
            if (i < numberOfRedIndices) {
                indexNameToNodeIdsMap.put(indexName, redNodeIds);
            } else {
                indexNameToNodeIdsMap.put(indexName, nonRedNodeIds);
            }
        }
        ClusterService clusterService = createClusterService(Set.of(), discoveryNodes, indexNameToNodeIdsMap);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(
            result.symptom(),
            equalTo(
                numberOfRedIndices
                    + " ind"
                    + (numberOfRedIndices == 1 ? "ex is" : "ices are")
                    + " blocked and cannot be updated because "
                    + numberOfRedNodes
                    + " node"
                    + (numberOfRedNodes == 1 ? " is" : "s are")
                    + " out of disk space."
            )
        );
        assertThat(result.impacts().size(), equalTo(1));
        HealthIndicatorImpact impact = result.impacts().get(0);
        assertNotNull(impact);
        List<ImpactArea> impactAreas = impact.impactAreas();
        assertThat(impactAreas.size(), equalTo(1));
        assertThat(impactAreas.get(0), equalTo(ImpactArea.INGEST));
        assertThat(impact.severity(), equalTo(1));
        assertThat(impact.impactDescription(), equalTo("Cannot insert or update documents in the affected indices."));
        assertThat(result.diagnosisList().size(), equalTo(1));
        Diagnosis diagnosis = result.diagnosisList().get(0);
        List<String> affectedResources = diagnosis.affectedResources();
        assertThat(affectedResources.size(), equalTo(numberOfRedNodes));
        assertTrue(affectedResources.containsAll(redNodeIds));
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get("green_nodes"), equalTo(discoveryNodes.size() - numberOfRedNodes));
        assertThat(details.get("unknown_nodes"), equalTo(0));
        assertThat(details.get("yellow_nodes"), equalTo(0));
        assertThat(details.get("red_nodes"), equalTo(numberOfRedNodes));
        assertThat(details.get("blocked_indices"), equalTo(0));
    }

    public void testHasBlockButOtherwiseGreen() {
        /*
         * Tests when there is an index that has a block on it but the nodes report green (so the lock is probably about to be released).
         */
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(true, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        {
            HealthStatus expectedStatus = HealthStatus.RED;
            HealthInfo healthInfo = createHealthInfo(HealthStatus.GREEN, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(expectedStatus));
            assertThat(result.symptom(), equalTo("1 index is blocked and cannot be updated but 0 nodes are currently out of space."));
        }
    }

    public void testHasBlockButOtherwiseYellow() {
        /*
         * Tests when there is an index that has a block on it but the nodes report yellow.
         */
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        ClusterService clusterService = createClusterService(true, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = HealthStatus.RED;
        int numberOfYellowNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(HealthStatus.YELLOW, numberOfYellowNodes, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(
            result.symptom(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "1 index is blocked and cannot be updated because %s running low on disk space.",
                    numberOfYellowNodes == 1 ? "1 node is" : numberOfYellowNodes + " nodes are"
                )
            )
        );
    }

    public void testHasBlockButOtherwiseRed() throws IOException {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        HealthStatus expectedStatus = HealthStatus.RED;
        int numberOfRedNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(HealthStatus.RED, numberOfRedNodes, discoveryNodes);
        Set<String> redNodeIds = healthInfo.diskInfoByNode()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().healthStatus().equals(expectedStatus))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        Set<String> nonRedNodeIds = healthInfo.diskInfoByNode()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().healthStatus().equals(expectedStatus) == false)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        Map<String, Set<String>> indexNameToNodeIdsMap = new HashMap<>();
        int numberOfIndices = randomIntBetween(1, 1000);
        Set<String> blockedIndices = new HashSet<>();
        int numberOfRedIndices = randomIntBetween(1, numberOfIndices);
        Set<String> redIndices = new HashSet<>();
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(20);
            /*
             * The following is artificial but useful for making sure the test has the right counts. The first numberOfRedIndices indices
             *  are always placed on all of the red nodes. All other indices are placed on all of the non red nodes.
             */
            if (i < numberOfRedIndices) {
                indexNameToNodeIdsMap.put(indexName, redNodeIds);
                redIndices.add(indexName);
            } else {
                indexNameToNodeIdsMap.put(indexName, nonRedNodeIds);
            }
            if (randomBoolean()) {
                blockedIndices.add(indexName);
            }
        }
        ClusterService clusterService = createClusterService(blockedIndices, discoveryNodes, indexNameToNodeIdsMap);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        int numberOfProblemIndices = Stream.concat(redIndices.stream(), blockedIndices.stream()).collect(Collectors.toSet()).size();
        assertThat(
            result.symptom(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "%s blocked and cannot be updated because %s out of disk space.",
                    numberOfProblemIndices == 1 ? "1 index is" : numberOfProblemIndices + " indices are",
                    numberOfRedNodes == 1 ? "1 node is" : numberOfRedNodes + " nodes are"
                )
            )
        );
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(details.get("green_nodes"), equalTo(discoveryNodes.size() - numberOfRedNodes));
        assertThat(details.get("unknown_nodes"), equalTo(0));
        assertThat(details.get("yellow_nodes"), equalTo(0));
        assertThat(details.get("red_nodes"), equalTo(numberOfRedNodes));
        assertThat(details.get("blocked_indices"), equalTo(blockedIndices.size()));
    }

    public void testMissingHealthInfo() {
        Set<DiscoveryNode> discoveryNodes = createNodesWithAllRoles();
        Set<DiscoveryNode> discoveryNodesInClusterState = new HashSet<>(discoveryNodes);
        discoveryNodesInClusterState.add(
            new DiscoveryNode(
                randomAlphaOfLength(30),
                UUID.randomUUID().toString(),
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                DiscoveryNodeRole.roles(),
                Version.CURRENT
            )
        );
        ClusterService clusterService = createClusterService(false, discoveryNodesInClusterState);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        {
            HealthInfo healthInfo = HealthInfo.EMPTY_HEALTH_INFO;
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.UNKNOWN));
        }
        {
            HealthInfo healthInfo = createHealthInfo(HealthStatus.GREEN, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.GREEN));
        }
        {
            HealthInfo healthInfo = createHealthInfo(HealthStatus.YELLOW, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.YELLOW));
        }
        {
            HealthInfo healthInfo = createHealthInfo(HealthStatus.RED, discoveryNodes);
            HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
            assertThat(result.status(), equalTo(HealthStatus.RED));
        }
    }

    public void testMasterNodeProblems() {
        Set<DiscoveryNode> discoveryNodes = createNodes(
            Set.of(
                DiscoveryNodeRole.MASTER_ROLE,
                randomFrom(
                    DiscoveryNodeRole.ML_ROLE,
                    DiscoveryNodeRole.INGEST_ROLE,
                    DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE,
                    DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE,
                    DiscoveryNodeRole.TRANSFORM_ROLE
                )
            )
        );
        ClusterService clusterService = createClusterService(false, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = randomFrom(HealthStatus.RED, HealthStatus.YELLOW);
        int numberOfProblemNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(expectedStatus, numberOfProblemNodes, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(
            result.symptom(),
            equalTo(
                numberOfProblemNodes
                    + " node"
                    + (numberOfProblemNodes == 1 ? "" : "s")
                    + " with role master "
                    + (numberOfProblemNodes == 1 ? "is" : "are")
                    + " out of disk space. As a result "
                    + (numberOfProblemNodes == 1 ? "its" : "their")
                    + " functions might be impaired."
            )
        );
        List<HealthIndicatorImpact> impacts = result.impacts();
        assertThat(impacts.size(), equalTo(1));
        assertThat(impacts.get(0).impactDescription(), equalTo("Cluster stability might be impaired."));
        assertThat(impacts.get(0).severity(), equalTo(2));
        assertThat(impacts.get(0).impactAreas(), equalTo(List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)));
        List<Diagnosis> diagnosisList = result.diagnosisList();
        assertThat(diagnosisList.size(), equalTo(1));
        Diagnosis diagnosis = diagnosisList.get(0);
        List<String> affectedResults = diagnosis.affectedResources();
        assertThat(affectedResults.size(), equalTo(numberOfProblemNodes));
        Diagnosis.Definition diagnosisDefinition = diagnosis.definition();
        assertThat(diagnosisDefinition.cause(), equalTo("Disk is almost full."));
        assertThat(
            diagnosisDefinition.action(),
            equalTo("Please add capacity to the current nodes, or replace them with ones with higher capacity.")
        );
    }

    public void testNonDataNonMasterNodeProblems() {
        Set<DiscoveryNodeRole> nonMasterNonDataRoles = Set.of(
            DiscoveryNodeRole.ML_ROLE,
            DiscoveryNodeRole.INGEST_ROLE,
            DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE,
            DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE,
            DiscoveryNodeRole.TRANSFORM_ROLE
        );
        Set<DiscoveryNodeRole> roles = new HashSet<>(
            randomSubsetOf(randomIntBetween(1, nonMasterNonDataRoles.size()), nonMasterNonDataRoles)
        );
        Set<DiscoveryNode> discoveryNodes = createNodes(roles);
        ClusterService clusterService = createClusterService(false, discoveryNodes);
        DiskHealthIndicatorService diskHealthIndicatorService = new DiskHealthIndicatorService(clusterService);
        HealthStatus expectedStatus = randomFrom(HealthStatus.RED, HealthStatus.YELLOW);
        int numberOfProblemNodes = randomIntBetween(1, discoveryNodes.size());
        HealthInfo healthInfo = createHealthInfo(expectedStatus, numberOfProblemNodes, discoveryNodes);
        HealthIndicatorResult result = diskHealthIndicatorService.calculate(true, healthInfo);
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(
            result.symptom(),
            equalTo(
                numberOfProblemNodes
                    + " node"
                    + (numberOfProblemNodes == 1 ? "" : "s")
                    + " with roles ["
                    + roles.stream().map(DiscoveryNodeRole::roleName).sorted().collect(Collectors.joining(", "))
                    + "] "
                    + (numberOfProblemNodes == 1 ? "is" : "are")
                    + " out of disk space. As a result "
                    + (numberOfProblemNodes == 1 ? "its" : "their")
                    + " functions might be impaired."
            )
        );
        List<HealthIndicatorImpact> impacts = result.impacts();
        assertThat(impacts.size(), equalTo(1));
        assertThat(impacts.get(0).impactDescription(), equalTo("Some cluster functionality might be unavailable."));
        assertThat(impacts.get(0).severity(), equalTo(2));
        assertThat(impacts.get(0).impactAreas(), equalTo(List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)));
        List<Diagnosis> diagnosisList = result.diagnosisList();
        assertThat(diagnosisList.size(), equalTo(1));
        Diagnosis diagnosis = diagnosisList.get(0);
        List<String> affectedResults = diagnosis.affectedResources();
        assertThat(affectedResults.size(), equalTo(numberOfProblemNodes));
        Diagnosis.Definition diagnosisDefinition = diagnosis.definition();
        assertThat(diagnosisDefinition.cause(), equalTo("Disk is almost full."));
        assertThat(
            diagnosisDefinition.action(),
            equalTo("Please add capacity to the current nodes, or replace them with ones with higher capacity.")
        );
    }

    private Set<DiscoveryNode> createNodesWithAllRoles() {
        return createNodes(DiscoveryNodeRole.roles());
    }

    private Set<DiscoveryNode> createNodes(Set<DiscoveryNodeRole> roles) {
        int numberOfNodes = randomIntBetween(1, 200);
        Set<DiscoveryNode> discoveryNodes = new HashSet<>();
        for (int i = 0; i < numberOfNodes; i++) {
            discoveryNodes.add(
                new DiscoveryNode(
                    randomAlphaOfLength(30),
                    UUID.randomUUID().toString(),
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    roles,
                    Version.CURRENT
                )
            );
        }
        return discoveryNodes;
    }

    private HealthInfo createHealthInfo(HealthStatus expectedStatus, Set<DiscoveryNode> nodes) {
        return createHealthInfo(expectedStatus, 1, nodes);
    }

    private HealthInfo createHealthInfo(HealthStatus expectedStatus, int numberOfNodesWithExpectedStatus, Set<DiscoveryNode> nodes) {
        assert numberOfNodesWithExpectedStatus <= nodes.size();
        Map<String, DiskHealthInfo> diskInfoByNode = new HashMap<>(nodes.size());
        int numberWithNonGreenStatus = 0;
        for (DiscoveryNode node : nodes) {
            final DiskHealthInfo diskHealthInfo;
            if (numberWithNonGreenStatus < numberOfNodesWithExpectedStatus) {
                diskHealthInfo = randomBoolean()
                    ? new DiskHealthInfo(expectedStatus)
                    : new DiskHealthInfo(expectedStatus, randomFrom(DiskHealthInfo.Cause.values()));
                numberWithNonGreenStatus++;
            } else {
                diskHealthInfo = randomBoolean()
                    ? new DiskHealthInfo(HealthStatus.GREEN)
                    : new DiskHealthInfo(HealthStatus.GREEN, randomFrom(DiskHealthInfo.Cause.values()));
            }
            diskInfoByNode.put(node.getId(), diskHealthInfo);
        }
        return new HealthInfo(diskInfoByNode);
    }

    private static ClusterService createClusterService(boolean blockIndex, Set<DiscoveryNode> nodes) {
        return createClusterService(1, blockIndex ? 1 : 0, nodes);
    }

    private static ClusterService createClusterService(int numberOfIndices, int numberOfIndicesToBlock, Set<DiscoveryNode> nodes) {
        Map<String, Set<String>> indexNameToNodeIdsMap = new HashMap<>();
        Set<String> blockedIndices = new HashSet<>(numberOfIndicesToBlock);
        for (int i = 0; i < numberOfIndices; i++) {
            String indexName = randomAlphaOfLength(20);
            /*
             * The following effectively makes it so that the index does not exist on any node. That's not realistic, but works out for
             * tests where we want for there to be no indices on red/yellow nodes
             */
            indexNameToNodeIdsMap.put(indexName, Set.of());
            if (i < numberOfIndicesToBlock) {
                blockedIndices.add(indexName);
            }
        }
        return createClusterService(blockedIndices, nodes, indexNameToNodeIdsMap);
    }

    private static ClusterService createClusterService(
        Set<String> blockedIndices,
        Set<DiscoveryNode> nodes,
        Map<String, Set<String>> indexNameToNodeIdsMap
    ) {
        RoutingTable routingTable = mock(RoutingTable.class);
        List<ShardRouting> shardRoutings = new ArrayList<>();
        when(routingTable.allShards()).thenReturn(shardRoutings);

        ClusterBlocks.Builder clusterBlocksBuilder = new ClusterBlocks.Builder();
        Map<String, IndexMetadata> indexMetadataMap = new HashMap<>();
        List<ClusterBlocks> clusterBlocksList = new ArrayList<>();
        for (String indexName : indexNameToNodeIdsMap.keySet()) {
            boolean blockIndex = blockedIndices.contains(indexName);
            IndexMetadata indexMetadata = new IndexMetadata.Builder(indexName).settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), blockIndex)
                    .build()
            ).build();
            indexMetadataMap.put(indexMetadata.getIndex().getName(), indexMetadata);
            if (blockIndex) {
                ClusterBlocks clusterBlocks = clusterBlocksBuilder.addBlocks(indexMetadata).build();
                clusterBlocksList.add(clusterBlocks);
            }
            for (String nodeId : indexNameToNodeIdsMap.get(indexName)) {
                ShardRouting shardRouting = TestShardRouting.newShardRouting(
                    indexMetadata.getIndex().getName(),
                    randomIntBetween(1, 5),
                    nodeId,
                    randomBoolean(),
                    ShardRoutingState.STARTED
                );
                shardRoutings.add(shardRouting);
            }
        }
        Metadata.Builder metadataBuilder = Metadata.builder();
        metadataBuilder.indices(indexMetadataMap);
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        for (DiscoveryNode node : nodes) {
            nodesBuilder.add(node);
        }
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(routingTable)
            .metadata(metadataBuilder.build())
            .nodes(nodesBuilder);
        for (ClusterBlocks clusterBlocks : clusterBlocksList) {
            clusterStateBuilder.blocks(clusterBlocks);
        }
        clusterStateBuilder.nodes(nodesBuilder);
        ClusterState clusterState = clusterStateBuilder.build();
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return clusterService;
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        return parser.map();
    }
}
