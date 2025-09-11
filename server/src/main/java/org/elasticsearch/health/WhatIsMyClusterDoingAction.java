/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequestParameters;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class WhatIsMyClusterDoingAction extends ActionType<WhatIsMyClusterDoingAction.Response> {

    public static final WhatIsMyClusterDoingAction INSTANCE = new WhatIsMyClusterDoingAction();
    public static final String NAME = "cluster:monitor/what_is_my_cluster_doing";
    // private static final String diagLocation = "/Users/keithmassey/sdh/8676/api-diagnostics-20241231-112131";
    // private static final String diagLocation = "/Users/keithmassey/sdh/8689/api-diagnostics-20250108-163913";
    private static final String[] diagLocations = { "/Users/keithmassey/sdh/9271/api-diagnostics-20250910-183143" };
    // "/Users/keithmassey/sdh/8924/20240414_2000/api-diagnostics-20250414-130554" };

    private WhatIsMyClusterDoingAction() {
        super(NAME);
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final Map<String, List<LocalAction.DistilledHotThread>> nodesToDistilledHotThreads;
        private final Map<String, Map<String, LocalAction.PipelineDetails>> nodeToPipelineInfoMap;
        private final Request.Mode mode;

        public Response(
            Map<String, List<LocalAction.DistilledHotThread>> nodesToDistilledHotThreads,
            Map<String, Map<String, LocalAction.PipelineDetails>> nodeToPipelineInfoMap,
            Request.Mode mode
        ) {
            this.nodesToDistilledHotThreads = nodesToDistilledHotThreads;
            this.nodeToPipelineInfoMap = nodeToPipelineInfoMap;
            this.mode = mode;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Response response = (Response) o;
            return true;// TODO
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return switch (mode) {
                case SUPER_SUMMARY -> superSummaryToXContent(builder, params);
                case SUMMARY -> summaryToXContent(builder, params);
                case STANDARD, VERBOSE -> standardToXContent(builder, params);
            };
        }

        public XContentBuilder superSummaryToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("current_activity");
            Map<String, Double> classificationToPercentMap = new HashMap<>();
            for (Map.Entry<String, List<LocalAction.DistilledHotThread>> entry : nodesToDistilledHotThreads.entrySet()) {
                List<LocalAction.DistilledHotThread> threads = entry.getValue();
                String nodeName = entry.getKey();
                if (threads.isEmpty() && (nodeToPipelineInfoMap.get(nodeName) == null || nodeToPipelineInfoMap.get(nodeName).isEmpty())) {
                    continue;
                }
                for (LocalAction.DistilledHotThread thread : threads) {
                    double cpuPercent = thread.percent;
                    String classification = thread.classification;
                    if (classificationToPercentMap.containsKey(classification)) {
                        classificationToPercentMap.put(classification, classificationToPercentMap.get(classification) + cpuPercent);
                    } else {
                        classificationToPercentMap.put(classification, cpuPercent);
                    }
                }
            }
            for (Map.Entry<String, Double> classificationEntry : classificationToPercentMap.entrySet()
                .stream()
                .sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue()))
                .toList()) {
                int cpusInUse = (int) (classificationEntry.getValue() / 100d);
                if (cpusInUse > 0) {
                    builder.startObject();
                    builder.field("classification", classificationEntry.getKey());
                    builder.field("cpus_in_use", cpusInUse);
                    builder.endObject();
                }
            }
            builder.endArray();

            builder.startArray("pipelines_history");
            Map<String, Long> pipelineToRuntimeMap = new HashMap<>();
            for (Map.Entry<String, Map<String, LocalAction.PipelineDetails>> nodePipelinesEntry : nodeToPipelineInfoMap.entrySet()) {
                Map<String, LocalAction.PipelineDetails> nodePipelineDetails = nodePipelinesEntry.getValue();
                for (LocalAction.PipelineDetails pipelineDetails : nodePipelineDetails.values()) {
                    if (pipelineToRuntimeMap.containsKey(pipelineDetails.name)) {
                        pipelineToRuntimeMap.put(
                            pipelineDetails.name,
                            pipelineToRuntimeMap.get(pipelineDetails.name) + pipelineDetails.runtime
                        );
                    } else {
                        pipelineToRuntimeMap.put(pipelineDetails.name, pipelineDetails.runtime);
                    }
                }
            }
            for (Map.Entry<String, Long> pipelineEntry : pipelineToRuntimeMap.entrySet()
                .stream()
                .sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue()))
                .toList()) {
                builder.startObject();
                builder.field("pipeline", pipelineEntry.getKey());
                builder.field("total_runtime", TimeValue.timeValueMillis(pipelineEntry.getValue()).toHumanReadableString(0));
                builder.endObject();
            }
            builder.endArray();

            builder.endObject();
            return builder;
        }

        public XContentBuilder summaryToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("nodes");
            for (Map.Entry<String, List<LocalAction.DistilledHotThread>> entry : nodesToDistilledHotThreads.entrySet()) {
                List<LocalAction.DistilledHotThread> threads = entry.getValue();
                String nodeName = entry.getKey();
                List<LocalAction.DistilledHotThread> filteredThreads = threads.stream().filter(thread -> thread.percent > 50).toList();
                Map<String, Map<String, LocalAction.PipelineDetails>> filteredNodeToPipelineInfoMap = nodeToPipelineInfoMap.entrySet()
                    .stream()
                    .filter(e -> e.getValue().isEmpty() == false)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                if (filteredThreads.isEmpty()
                    && (filteredNodeToPipelineInfoMap.get(nodeName) == null || filteredNodeToPipelineInfoMap.get(nodeName).isEmpty())) {
                    continue;
                }
                builder.startObject();
                builder.field("name", nodeName);
                Map<String, Double> classificationToPercentMap = new HashMap<>();
                for (LocalAction.DistilledHotThread thread : filteredThreads) {
                    double cpuPercent = thread.percent;
                    String classification = thread.classification;
                    if (classificationToPercentMap.containsKey(classification)) {
                        classificationToPercentMap.put(classification, classificationToPercentMap.get(classification) + cpuPercent);
                    } else {
                        classificationToPercentMap.put(classification, cpuPercent);
                    }
                }
                builder.startArray("current_activity");
                for (Map.Entry<String, Double> classificationEntry : classificationToPercentMap.entrySet()
                    .stream()
                    .sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue()))
                    .toList()) {
                    double cpusInUse = classificationEntry.getValue() / 100d;
                    if (cpusInUse > 0.1) {
                        builder.startObject();
                        builder.field("classification", classificationEntry.getKey());
                        builder.field("cpus_in_use", cpusInUse);
                        builder.endObject();
                    }
                }
                builder.endArray();

                builder.startArray("pipelines_history");
                Map<String, LocalAction.PipelineDetails> nodePipelineDetails = nodeToPipelineInfoMap.get(nodeName);
                for (LocalAction.PipelineDetails pipelineDetails : nodePipelineDetails.values()
                    .stream()
                    .sorted((o1, o2) -> Long.compare(o2.runtime, o1.runtime))
                    .toList()) {
                    builder.startObject();
                    builder.field("pipeline", pipelineDetails.name);
                    builder.field("total_runtime", TimeValue.timeValueMillis(pipelineDetails.runtime).toHumanReadableString(1));
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        public XContentBuilder standardToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("nodes");
            for (Map.Entry<String, List<LocalAction.DistilledHotThread>> entry : nodesToDistilledHotThreads.entrySet()) {
                List<LocalAction.DistilledHotThread> threads = entry.getValue();
                String nodeName = entry.getKey();
                List<LocalAction.DistilledHotThread> filteredThreads = threads.stream().filter(thread -> thread.percent > 50).toList();
                Map<String, Map<String, LocalAction.PipelineDetails>> filteredNodeToPipelineInfoMap = nodeToPipelineInfoMap.entrySet()
                    .stream()
                    .filter(e -> e.getValue().isEmpty() == false)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                if (filteredThreads.isEmpty()
                    && (filteredNodeToPipelineInfoMap.get(nodeName) == null || filteredNodeToPipelineInfoMap.get(nodeName).isEmpty())) {
                    continue;
                }
                builder.startObject();
                builder.field("name", nodeName);
                builder.startArray("current_activity");
                for (LocalAction.DistilledHotThread thread : filteredThreads) {
                    builder.startObject();
                    builder.field("summary", thread.percent + "% " + String.join(", ", thread.activities));
                    builder.field("classification", thread.classification);
                    builder.endObject();
                }
                builder.endArray();
                builder.startArray("pipelines_history");
                Map<String, LocalAction.PipelineDetails> pipelinesForNode = nodeToPipelineInfoMap.get(nodeName);
                if (pipelinesForNode != null) {
                    for (Map.Entry<String, LocalAction.PipelineDetails> pipelineEntry : pipelinesForNode.entrySet()) {
                        builder.startObject();
                        builder.field("name", pipelineEntry.getKey());
                        String pipelineMessage = "Takes "
                            + TimeValue.timeValueMillis(pipelineEntry.getValue().runtime).toHumanReadableString(1)
                            + " out of a total of "
                            + TimeValue.timeValueMillis(pipelineEntry.getValue().totalNodeRuntime).toHumanReadableString(1)
                            + " on the node";
                        builder.field("message", pipelineMessage);
                        builder.startArray("processors");
                        long pipelineTime = pipelineEntry.getValue().runtime;
                        for (LocalAction.ProcessorDetail processorDetail : pipelineEntry.getValue().processorDetails) {
                            builder.startObject();
                            builder.field("offset", processorDetail.index);
                            builder.field("type", processorDetail.type);
                            builder.field("name", processorDetail.name);
                            String message = "Takes "
                                + TimeValue.timeValueMillis(processorDetail.runtime).toHumanReadableString(1)
                                + " out of a total of "
                                + TimeValue.timeValueMillis(pipelineTime).toHumanReadableString(1);
                            builder.field("message", message);
                            if (mode.equals(Request.Mode.VERBOSE) && processorDetail.detail != null) {
                                builder.startObject("detail");
                                for (Map.Entry<String, Object> detailEntry : processorDetail.detail.entrySet()) {
                                    builder.field(detailEntry.getKey(), detailEntry.getValue().toString());
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endArray();
                        builder.endObject();
                    }
                }
                builder.endArray();
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

    }

    public static class Request extends ActionRequest {
        public enum Mode {
            SUPER_SUMMARY,
            SUMMARY,
            STANDARD,
            VERBOSE
        }

        private final Mode mode;
        private final String node;
        private final boolean demoMode;
        private final int demoOffset;

        public Request(Mode mode, @Nullable String node, boolean demoMode, int demoOffset) {
            this.mode = mode;
            this.node = node;
            this.demoMode = demoMode;
            this.demoOffset = demoOffset;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }
    }

    public static class LocalAction extends TransportAction<Request, Response> {

        private final ClusterService clusterService;
        private final NodeClient client;
        private final List<Map<String, PipelineConfiguration>> demoPipelines = new ArrayList<>(diagLocations.length);
        private final ProjectResolver projectResolver;

        @Inject
        public LocalAction(
            ActionFilters actionFilters,
            TransportService transportService,
            ClusterService clusterService,
            NodeClient client,
            ProjectResolver projectResolver
        ) {
            super(NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
            this.clusterService = clusterService;
            this.client = client;
            this.projectResolver = projectResolver;
            try {
                for (int i = 0; i < diagLocations.length; i++) {
                    demoPipelines.add(getPipelineConfigurationMapFromDisk(i));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> responseListener) {
            AtomicReference<Map<String, List<DistilledHotThread>>> nodesHotThreadsMap = new AtomicReference<>();
            final int demoOffset = request.demoOffset;
            SubscribableListener.newForked(
                request.demoMode
                    ? (CheckedConsumer<ActionListener<NodesHotThreadsResponse>, Exception>) actionListener -> fetchHotThreadsFromDisk(
                        actionListener,
                        demoOffset
                    )
                    : this::fetchHotThreads
            ).<NodesStatsResponse>andThen((l, hotThreadsResponse) -> {
                nodesHotThreadsMap.set(distillHotThreads(hotThreadsResponse.getNodesMap(), request.node));
                if (request.demoMode) {
                    fetchNodesStatsFromDisk(l, demoOffset);
                } else {
                    fetchNodesStats(l);
                }
            })
                .andThenApply(
                    nodesStatsResponse -> createResponse(
                        nodesHotThreadsMap.get(),
                        getNodeToPipelineMap(nodesStatsResponse.getNodesMap(), request.node, request.demoMode, demoOffset),
                        request.mode
                    )
                )
                .addListener(responseListener);
        }

        private void fetchHotThreads(ActionListener<NodesHotThreadsResponse> listener) {
            String[] nodesIds = new String[0];
            NodesHotThreadsRequest nodesHotThreadsRequest = new NodesHotThreadsRequest(
                nodesIds,
                new HotThreads.RequestOptions(
                    HotThreads.RequestOptions.DEFAULT.threads(),
                    HotThreads.RequestOptions.DEFAULT.reportType(),
                    HotThreads.RequestOptions.DEFAULT.sortOrder(),
                    HotThreads.RequestOptions.DEFAULT.interval(),
                    HotThreads.RequestOptions.DEFAULT.snapshots(),
                    HotThreads.RequestOptions.DEFAULT.ignoreIdleThreads()
                )
            );
            client.execute(TransportNodesHotThreadsAction.TYPE, nodesHotThreadsRequest, listener);
        }

        private void fetchHotThreadsFromDisk(ActionListener<NodesHotThreadsResponse> listener, int offset) {
            Path filePath = Path.of(diagLocations[offset], "nodes_hot_threads.txt");
            List<String> lines = null;
            try {
                lines = Files.readAllLines(filePath);
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }

            String currentNode = null;
            String currentNodeId = null;
            List<NodeHotThreads> nodes = new ArrayList<>();
            StringBuilder linesForNode = new StringBuilder();
            for (String line : lines) {
                if (line.startsWith(":::")) {
                    if (currentNode != null) {
                        NodeHotThreads nodeHotThreads = new NodeHotThreads(
                            new DiscoveryNode(
                                currentNode,
                                currentNodeId,
                                new TransportAddress(new InetSocketAddress("localhost", 9200)),
                                Map.of(),
                                Set.of(),
                                VersionInformation.CURRENT
                            ),
                            ReleasableBytesReference.wrap(new BytesArray(linesForNode.toString()))
                        );
                        nodes.add(nodeHotThreads);
                    }
                    currentNode = line.substring(line.indexOf("{") + 1, line.indexOf("}"));
                    currentNodeId = currentNode; // TODO
                    linesForNode = new StringBuilder();
                } else {
                    linesForNode.append(line);
                    linesForNode.append("\n");
                }

            }
            NodeHotThreads nodeHotThreads = new NodeHotThreads(
                new DiscoveryNode(
                    currentNode,
                    currentNodeId,
                    new TransportAddress(new InetSocketAddress("localhost", 9200)),
                    Map.of(),
                    Set.of(),
                    VersionInformation.CURRENT
                ),
                ReleasableBytesReference.wrap(new BytesArray(linesForNode.toString()))
            );
            nodes.add(nodeHotThreads);
            ClusterName clusterName = new ClusterName("cluster");
            List<FailedNodeException> failedNodeExceptions = List.of();
            NodesHotThreadsResponse hotThreadsResponse = new NodesHotThreadsResponse(clusterName, nodes, failedNodeExceptions);
            hotThreadsResponse.decRef();
            listener.onResponse(hotThreadsResponse);
        }

        private void fetchNodesStats(ActionListener<NodesStatsResponse> listener) {
            NodesStatsRequestParameters params = new NodesStatsRequestParameters();
            params.requestedMetrics().add(NodesStatsRequestParameters.Metric.INGEST);
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(params);
            client.execute(TransportNodesStatsAction.TYPE, nodesStatsRequest, listener);
        }

        @SuppressWarnings("unchecked")
        private void fetchNodesStatsFromDisk(ActionListener<NodesStatsResponse> listener, int offset) {
            Path filePath = Path.of(diagLocations[offset], "nodes_stats.json");
            String jsonString = null;
            try {
                jsonString = Files.readString(filePath);
            } catch (IOException e) {
                listener.onFailure(e);
            }
            Map<String, Object> nodesStatsMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, jsonString, false);
            Map<String, Map<String, Object>> nodesMaps = (Map<String, Map<String, Object>>) nodesStatsMap.get("nodes");
            List<NodeStats> nodes = new ArrayList<>();
            for (String node : nodesMaps.keySet()) {
                Map<String, Object> nodeMap = nodesMaps.get(node);
                DiscoveryNode discoveryNode = new DiscoveryNode(
                    nodeMap.get("name").toString(),
                    node,
                    new TransportAddress(new InetSocketAddress("localhost", 9200)),
                    Map.of(),
                    Set.of(),
                    VersionInformation.CURRENT
                );

                Map<String, Object> ingestMap = (Map<String, Object>) nodeMap.get("ingest");

                Map<String, Object> totalMap = (Map<String, Object>) ingestMap.get("total");
                long totalTimeInMillis = ((Number) totalMap.get("time_in_millis")).longValue();
                long ingestCount = ((Number) totalMap.get("count")).longValue();
                long ingestCurrent = ((Number) totalMap.get("current")).longValue();
                long ingestFailedCount = ((Number) totalMap.get("failed")).longValue();
                IngestStats.Stats totalStats = new IngestStats.Stats(ingestCount, totalTimeInMillis, ingestCurrent, ingestFailedCount);

                List<IngestStats.PipelineStat> pipelineStats = new ArrayList<>();
                Map<String, List<IngestStats.ProcessorStat>> processorStats = new HashMap<>();
                Map<String, Object> pipelinesMap = (Map<String, Object>) ingestMap.get("pipelines");
                for (String pipelineName : pipelinesMap.keySet()) {
                    Map<String, Object> pipelineMap = (Map<String, Object>) pipelinesMap.get(pipelineName);
                    long pipelineTimeInMillis = ((Number) pipelineMap.get("time_in_millis")).longValue();
                    long pipelineIngestCount = ((Number) pipelineMap.get("count")).longValue();
                    long pipelineIngestCurrent = ((Number) pipelineMap.get("current")).longValue();
                    long pipelineIngestFailedCount = ((Number) pipelineMap.get("failed")).longValue();
                    IngestStats.Stats stats = new IngestStats.Stats(
                        pipelineIngestCount,
                        pipelineTimeInMillis,
                        pipelineIngestCurrent,
                        pipelineIngestFailedCount
                    );
                    Number ingestedBytes = (Number) pipelineMap.get("ingested_as_first_pipeline_in_bytes");
                    long bytesIngested;
                    if (ingestedBytes == null) {
                        bytesIngested = 0;
                    } else {
                        bytesIngested = ingestedBytes.longValue();
                    }
                    Number producedBytes = (Number) pipelineMap.get("produced_as_first_pipeline_in_bytes");
                    long bytesProduced;
                    if (producedBytes == null) {
                        bytesProduced = 0;
                    } else {
                        bytesProduced = producedBytes.longValue();
                    }
                    IngestStats.ByteStats byteStats = new IngestStats.ByteStats(bytesIngested, bytesProduced);
                    IngestStats.PipelineStat pipelineStat = new IngestStats.PipelineStat(ProjectId.DEFAULT, pipelineName, stats, byteStats);
                    pipelineStats.add(pipelineStat);

                    List<Map<String, Map<String, Object>>> processors = (List<Map<String, Map<String, Object>>>) pipelineMap.get(
                        "processors"
                    );
                    List<IngestStats.ProcessorStat> processorsList = new ArrayList<>();
                    processorStats.put(pipelineName, processorsList);
                    for (Map<String, Map<String, Object>> processorEntry : processors) {
                        assert processorEntry.keySet().size() == 1;
                        String processorName = processorEntry.keySet().iterator().next();
                        Map<String, Object> processor = processorEntry.get(processorName);
                        Map<String, Object> procStats = (Map<String, Object>) processor.get("stats");
                        long processorTimeInMillis = ((Number) procStats.get("time_in_millis")).longValue();
                        long processorIngestCount = ((Number) procStats.get("count")).longValue();
                        long processorIngestCurrent = ((Number) procStats.get("current")).longValue();
                        long processorIngestFailedCount = ((Number) procStats.get("failed")).longValue();
                        IngestStats.Stats procsesorStats = new IngestStats.Stats(
                            processorIngestCount,
                            processorTimeInMillis,
                            processorIngestCurrent,
                            processorIngestFailedCount
                        );
                        IngestStats.ProcessorStat processorStat = new IngestStats.ProcessorStat(
                            processorName,
                            processor.get("type").toString(),
                            procsesorStats
                        );
                        processorsList.add(processorStat);
                    }
                }

                IngestStats ingestStats = new IngestStats(totalStats, pipelineStats, Map.of(ProjectId.DEFAULT, processorStats));
                NodeStats nodeStats = new NodeStats(
                    discoveryNode,
                    0,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    ingestStats,
                    null,
                    null,
                    null,
                    null,
                    null
                );
                nodes.add(nodeStats);
            }

            ClusterName clusterName = new ClusterName("cluster");
            List<FailedNodeException> failedNodeExceptions = List.of();
            listener.onResponse(new NodesStatsResponse(clusterName, nodes, failedNodeExceptions));
        }

        public static Map<String, List<DistilledHotThread>> distillHotThreads(
            Map<String, NodeHotThreads> hotThreadsMap,
            @Nullable String node
        ) {
            Map<String, List<DistilledHotThread>> nodesToDistilledHotThreads = new HashMap<>();
            for (Map.Entry<String, NodeHotThreads> entry : hotThreadsMap.entrySet()) {
                if (node == null || node.equals(entry.getKey())) {
                    NodeHotThreads hotThreads = entry.getValue();
                    String hotThreadsForNode = hotThreads.getHotThreads();
                    List<DistilledHotThread> distilledHotThreads = distillHotThreadsForSingleNode(false, hotThreadsForNode);
                    nodesToDistilledHotThreads.put(hotThreads.getNode().getName(), distilledHotThreads);
                }
            }
            return nodesToDistilledHotThreads;
        }

        private Response createResponse(
            Map<String, List<DistilledHotThread>> nodesToDistilledHotThreads,
            Map<String, Map<String, PipelineDetails>> nodeToPipelineInfoMap,
            Request.Mode mode
        ) {
            return new Response(nodesToDistilledHotThreads, nodeToPipelineInfoMap, mode);
        }

        @SuppressWarnings("unchecked")
        private Map<String, Map<String, PipelineDetails>> getNodeToPipelineMap(
            Map<String, NodeStats> nodesMap,
            String node,
            boolean demoMode,
            int demoOffset
        ) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
            // maps node name to list of pipelines tuple, which is pipeline name and list of processor messages (yes, it needs a class)
            Map<String, Map<String, PipelineDetails>> nodeToPipelineInfoMap = new HashMap<>();
            for (Map.Entry<String, NodeStats> nodesMapEntry : nodesMap.entrySet()) {
                String nodeName = nodesMapEntry.getValue().getNode().getName();
                if (node == null || node.equals(nodeName)) {
                    nodeToPipelineInfoMap.put(nodeName, new HashMap<>());
                    IngestStats ingestStats = nodesMapEntry.getValue().getIngestStats();
                    if (ingestStats != null) {
                        long totalTimeInMillis = ingestStats.totalStats().ingestTimeInMillis();
                        Map<String, Long> pipelinesToTimes = new HashMap<>();
                        List<IngestStats.PipelineStat> pipelineStats = ingestStats.pipelineStats();
                        for (IngestStats.PipelineStat pipelineStat : pipelineStats) {
                            long pipelineTimeInMillis = pipelineStat.stats().ingestTimeInMillis();
                            if (pipelineTimeInMillis > 0) {
                                pipelinesToTimes.put(pipelineStat.pipelineId(), pipelineTimeInMillis);
                            }
                        }
                        double mean = calculateMean(pipelinesToTimes.values());
                        double stdDev = calculateStdDev(pipelinesToTimes.values(), mean);
                        long allPipelineTime = 0;
                        for (Map.Entry<String, Long> entry : pipelinesToTimes.entrySet()) {
                            long pipelineTime = entry.getValue();
                            allPipelineTime += pipelineTime;
                            String pipelineName = entry.getKey();
                            if ((pipelineTime - mean) > 2 * stdDev || pipelineTime > (0.5 * totalTimeInMillis)) {
                                List<ProcessorDetail> processorMessages = new ArrayList<>();
                                nodeToPipelineInfoMap.get(nodeName)
                                    .put(
                                        pipelineName,
                                        new PipelineDetails(pipelineName, pipelineTime, totalTimeInMillis, processorMessages)
                                    );
                                List<IngestStats.ProcessorStat> processors = ingestStats.processorStats()
                                    .get(ProjectId.DEFAULT)
                                    .get(pipelineName);
                                Collection<Long> nonZeroProcessorTimes = processors.stream()
                                    .map(stat -> stat.stats().ingestTimeInMillis())
                                    .filter(stat -> stat > 0)
                                    .toList();
                                double processorMean = calculateMean(nonZeroProcessorTimes);
                                double processorStdDev = calculateStdDev(nonZeroProcessorTimes, mean);
                                Map<String, PipelineConfiguration> pipelineConfigurationMap = demoMode
                                    ? getPipelineConfigurationMapFromDisk(demoOffset)
                                    : getPipelineConfigurationMap();
                                PipelineConfiguration pipelineConfiguration = pipelineConfigurationMap.get(pipelineName);
                                List<Map<String, Map<String, Object>>> processorConfigs = pipelineConfiguration == null
                                    ? null
                                    : (List<Map<String, Map<String, Object>>>) pipelineConfiguration.getConfig().get("processors");
                                for (int i = 0; i < processors.size(); i++) {
                                    IngestStats.ProcessorStat processorStat = processors.get(i);
                                    long processorIngestTime = processorStat.stats().ingestTimeInMillis();
                                    if ((processorIngestTime - processorMean) > 2 * processorStdDev
                                        || processorIngestTime > 0.3 * pipelineTime) {
                                        Map<String, Object> detail;
                                        if (processorConfigs == null) {
                                            detail = null;
                                        } else {
                                            Map<String, Map<String, Object>> processorConfig = processorConfigs.get(i);
                                            detail = processorConfig.values().iterator().next();
                                        }
                                        processorMessages.add(
                                            new ProcessorDetail(i, processorStat.type(), processorStat.name(), processorIngestTime, detail)
                                        );
                                    }
                                }
                            }
                        }
                        if (allPipelineTime < 0.2 * totalTimeInMillis) {
                            List<ProcessorDetail> processorMessages = new ArrayList<>();
                            nodeToPipelineInfoMap.get(nodeName)
                                .put(
                                    "indexing",
                                    new PipelineDetails(
                                        "indexing",
                                        totalTimeInMillis - allPipelineTime,
                                        totalTimeInMillis,
                                        processorMessages
                                    )
                                );
                        }
                        // }
                    }
                }
            }
            return nodeToPipelineInfoMap;
        }

        private Map<String, PipelineConfiguration> getPipelineConfigurationMap() {
            return ((IngestMetadata) clusterService.state().metadata().getProject(projectResolver.getProjectId()).custom("ingest"))
                .getPipelines();
        }

        @SuppressWarnings("unchecked")
        private Map<String, PipelineConfiguration> getPipelineConfigurationMapFromDisk(int offset) throws IOException {
            if (demoPipelines.size() > offset) {
                return demoPipelines.get(offset);
            }
            Path filePath = Path.of(diagLocations[offset], "cluster_state.json");
            Map<String, PipelineConfiguration> pipelineConfigurationMap = new HashMap<>();
            List<Map<String, Object>> pipelinesList;
            {
                Map<String, Object> clusterStateMap;
                {
                    String jsonString = Files.readString(filePath);
                    clusterStateMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, jsonString, false);
                }
                pipelinesList = (List<Map<String, Object>>) ((Map<String, Map<String, Object>>) clusterStateMap.get("metadata")).get(
                    "ingest"
                ).get("pipeline");
            }
            for (Map<String, Object> pipeline : pipelinesList) {
                String pipelineName = pipeline.get("id").toString();
                Map<String, Object> config = (Map<String, Object>) pipeline.get("config");
                PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(pipelineName, config);
                pipelineConfigurationMap.put(pipelineName, pipelineConfiguration);
            }
            return pipelineConfigurationMap;
        }

        record DistilledHotThread(double percent, List<String> activities, String classification) {}

        record PipelineDetails(String name, long runtime, long totalNodeRuntime, List<ProcessorDetail> processorDetails) {}

        record ProcessorDetail(int index, String type, String name, long runtime, Map<String, Object> detail) {}

        private static double calculateMean(Collection<Long> data) {
            return data.stream().mapToDouble(Long::doubleValue).average().orElse(0);
        }

        private static double calculateStdDev(Collection<Long> data, double mean) {
            double sumSquaredDiff = 0;
            for (double num : data) {
                sumSquaredDiff += Math.pow(num - mean, 2);
            }
            double variance = sumSquaredDiff / data.size();
            return Math.sqrt(variance);
        }

        public static List<DistilledHotThread> distillHotThreadsForSingleNode(boolean summarize, String threadDump) {
            String[] lines = threadDump.split("\n");
            String firstLine = null;
            List<String> elasticStack = new ArrayList<>();
            double percent = -1;
            List<DistilledHotThread> threadsSummaries = new ArrayList<>();
            for (String line : lines) {
                if (line.isBlank()) {
                    if (firstLine != null && elasticStack.size() > 3) {
                        Tuple<List<String>, String> summary = getSummaryOfElasticStackForOneThread(elasticStack);
                        DistilledHotThread distilledHotThread = new DistilledHotThread(percent, summary.v1(), summary.v2());
                        threadsSummaries.add(distilledHotThread);
                    }
                    firstLine = null;
                    elasticStack = new ArrayList<>();
                    percent = -1;
                } else if (line.contains("Hot threads at ")) {
                    // skip
                } else if (firstLine == null) {
                    firstLine = line;
                    percent = Double.valueOf(firstLine.substring(0, firstLine.indexOf("%")));
                    elasticStack.add(firstLine);
                } else {
                    if (line.contains("elastic") || line.contains("netty") || line.contains("lucene")) {
                        elasticStack.add(line);
                    }
                }
            }
            if (elasticStack.isEmpty() == false) {
                Tuple<List<String>, String> summary = getSummaryOfElasticStackForOneThread(elasticStack);
                DistilledHotThread distilledHotThread = new DistilledHotThread(percent, summary.v1(), summary.v2());
                threadsSummaries.add(distilledHotThread);
            }
            return threadsSummaries;
        }
    }

    enum Classification {
        LOGGING,
        AUTH,
        PIPELINES,
        INDEXING,
        SEARCH,
        ENRICH,
        ML,
        SYSTEM_BACKGROUND_TASKS,
        UNKNOWN
    }

    private static Tuple<List<String>, String> getSummaryOfElasticStackForOneThread(List<String> elasticStack) {
        Set<String> reasons = new LinkedHashSet<>();
        Set<String> products = new LinkedHashSet<>();
        Set<Classification> potentialClassifications = new HashSet<>();
        for (String stackElement : elasticStack.reversed()) {
            if (stackElement.contains("[transport_worker]")) {
                reasons.add("TRANSPORT");
                potentialClassifications.add(Classification.SYSTEM_BACKGROUND_TASKS);
            }
            if (stackElement.contains("org.elasticsearch.ingest.IngestService")) {
                reasons.add("ingesting data through pipelines");
                potentialClassifications.add(Classification.PIPELINES);
            } else if (stackElement.contains("org.elasticsearch.action.bulk.TransportShardBulkAction")) {
                reasons.add("indexing data into indices");
                potentialClassifications.add(Classification.INDEXING);
            } else if (stackElement.contains("RestBulkAction")) {
                reasons.add("reading rest request to index data into indices");
                potentialClassifications.add(Classification.INDEXING);
            } else if (stackElement.contains("org.elasticsearch.xpack.enrich.EnrichProcessorFactory")) {
                reasons.add("enriching data");
                potentialClassifications.add(Classification.ENRICH);
            } else if (stackElement.contains("org.elasticsearch.index.engine.ElasticsearchConcurrentMergeScheduler.doMerge")
                || stackElement.contains("SegmentMerger")
                || stackElement.contains("IndexWriterMergeSource.merge")) {
                    reasons.add("maintenance -- merging segments");
                    potentialClassifications.add(Classification.SYSTEM_BACKGROUND_TASKS);
                } else if (stackElement.contains("org.elasticsearch.search.SearchService")
                    || stackElement.contains("org.elasticsearch.search.internal.ContextIndexSearcher.search")
                    || stackElement.contains("TransportSearchAction")
                    || stackElement.contains("org.elasticsearch.action.search")) {
                        reasons.add("executing search requests");
                        potentialClassifications.add(Classification.SEARCH);
                    } else if (stackElement.contains("searchable_snapshots_cache_prewarming")
                        || stackElement.contains("searchable_snapshots_cache_fetch_async")) {
                            reasons.add("maintenance -- prewarming the cache from searchable snapshots");
                            potentialClassifications.add(Classification.SYSTEM_BACKGROUND_TASKS);
                        } else if (stackElement.contains("org.elasticsearch.repositories.blobstore.ShardSnapshotTaskRunner")
                            || stackElement.contains("org.elasticsearch.repositories.blobstore.BlobStoreRepository.snapshotFile")) {
                                reasons.add("maintenance -- snapshotting data for backup");
                                potentialClassifications.add(Classification.SYSTEM_BACKGROUND_TASKS);
                            } else if (stackElement.contains("org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail")) {
                                reasons.add("writing audit logs of user activity");
                                potentialClassifications.add(Classification.LOGGING);
                            } else if (stackElement.contains("[flush]")) {
                                reasons.add("maintenance -- flushing data to disk");
                                potentialClassifications.add(Classification.SYSTEM_BACKGROUND_TASKS);
                            } else if (stackElement.contains(
                                "org.elasticsearch.rest.RestController$EncodedLengthTrackingChunkedRestResponseBodyPart.encodeChunk"
                            )) {
                                reasons.add("reading potentially large data from request");
                                potentialClassifications.add(Classification.UNKNOWN);
                            } else if (stackElement.contains("org.elasticsearch.xpack.monitoring.exporter.Exporters.export")) {
                                reasons.add("exporting data for Monitoring");
                                potentialClassifications.add(Classification.SYSTEM_BACKGROUND_TASKS);
                            } else if (reasons.isEmpty() && stackElement.contains("[write]")) {
                                reasons.add("writing data possibly through an ingest pipeline");
                                potentialClassifications.add(Classification.PIPELINES);
                            } else if (stackElement.contains(
                                "org.elasticsearch.repositories.blobstore.BlobStoreRepository.deleteFromContainer"
                            )) {
                                reasons.add("deleting item from snapshot repository");
                                potentialClassifications.add(Classification.SYSTEM_BACKGROUND_TASKS);
                            } else if (stackElement.contains("org.elasticsearch.indices.recovery.PeerRecoveryTargetService")
                                || stackElement.contains("org.elasticsearch.indices.recovery")) {
                                    reasons.add("maintenance -- peer recovery");
                                    potentialClassifications.add(Classification.SYSTEM_BACKGROUND_TASKS);
                                } else if (stackElement.contains("maybeRefresh")) {
                                    reasons.add("maintenance -- refreshing indices");
                                    potentialClassifications.add(Classification.SYSTEM_BACKGROUND_TASKS);
                                } else if (stackElement.contains("admin.cluster.stats")) {
                                    reasons.add("gathering cluster stats");
                                    potentialClassifications.add(Classification.SYSTEM_BACKGROUND_TASKS);
                                } else if (stackElement.contains("org.elasticsearch.cluster.service.MasterService")) {
                                    reasons.add("maintenance -- master node");
                                    potentialClassifications.add(Classification.SYSTEM_BACKGROUND_TASKS);
                                } else if (stackElement.contains("ShardsAllocator")) {
                                    reasons.add("maintenance -- balancing shards");
                                    potentialClassifications.add(Classification.SYSTEM_BACKGROUND_TASKS);
                                } else if (stackElement.contains("Processor") && stackElement.contains("CompoundProcessor") == false) {
                                    String firstHalfOfLine = stackElement.substring(0, stackElement.indexOf("Processor"));
                                    String processorName = firstHalfOfLine.substring(firstHalfOfLine.lastIndexOf(".") + 1).toLowerCase();
                                    if (processorName.contains("enrich") == false
                                        && processorName.contains("abstractstring") == false
                                        && processorName.contains("continuouscomputation$") == false
                                        && processorName.contains("asyncio") == false) {
                                        reasons.add("running " + processorName + " processor");
                                        potentialClassifications.add(Classification.PIPELINES);
                                    }
                                } else if (stackElement.contains("io.netty.handler.ssl.SslHandler.flush")) {
                                    reasons.add("returning potentially large response");
                                    potentialClassifications.add(Classification.UNKNOWN);
                                } else if (stackElement.contains("org.elasticsearch.xpack.ml")) {
                                    reasons.add("machine learning");
                                    potentialClassifications.add(Classification.ML);
                                } else if (stackElement.contains("RBACEngine")) {
                                    reasons.add("authentication");
                                    potentialClassifications.add(Classification.AUTH);
                                } else if (stackElement.contains("ReadinessService")) {
                                    reasons.add("readiness service");
                                    potentialClassifications.add(Classification.SYSTEM_BACKGROUND_TASKS);
                                } else if (stackElement.contains("org.apache.lucene.search.TaskExecutor")) {
                                    reasons.add("executing search requests");
                                    potentialClassifications.add(Classification.SEARCH);
                                } else {
                                    reasons.add("unknown");
                                    potentialClassifications.add(Classification.UNKNOWN);
                                }
            if (stackElement.contains("elastic") && stackElement.contains("%") == false) {
                products.add("elastic");
            } else if (stackElement.contains("netty")) {
                products.add("netty (network)");
            } else if (stackElement.contains("lucene")) {
                products.add("lucene");
            }
        }
        String classification = potentialClassifications.stream()
            .sorted()
            .findFirst()
            .orElse(Classification.UNKNOWN)
            .toString()
            .toLowerCase();
        if (Classification.UNKNOWN.name().toLowerCase().equals(classification)
            && elasticStack.isEmpty() == false
            && elasticStack.get(0).contains("[search")) {
            classification = Classification.SEARCH.toString().toLowerCase();
        }
        if (reasons.isEmpty()) {
            return Tuple.tuple(List.of("unknown " + products.stream().collect(Collectors.joining(", ")) + " thread"), classification);
        } else {
            return Tuple.tuple(reasons.stream().toList(), classification);
        }
    }
}
