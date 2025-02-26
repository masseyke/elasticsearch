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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.health.stats.HealthApiStats;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.IngestService;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class WhatIsMyClusterDoingAction extends ActionType<WhatIsMyClusterDoingAction.Response> {

    public static final WhatIsMyClusterDoingAction INSTANCE = new WhatIsMyClusterDoingAction();
    public static final String NAME = "cluster:monitor/what_is_my_cluster_doing";
    private static final String diagLocation = "/Users/keithmassey/sdh/8739/api-diagnostics-20250131-172326";

    private WhatIsMyClusterDoingAction() {
        super(NAME);
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final Map<String, List<String>> nodesToDistilledHotThreads;
        private final Map<String, Map<String, Tuple<String, List<LocalAction.ProcessorDetail>>>> nodeToPipelineInfoMap;
        private final Request.Mode mode;

        public Response(
            Map<String, List<String>> nodesToDistilledHotThreads,
            Map<String, Map<String, Tuple<String, List<LocalAction.ProcessorDetail>>>> nodeToPipelineInfoMap,
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
            builder.startObject();
            builder.startArray("nodes");
            for (Map.Entry<String, List<String>> entry : nodesToDistilledHotThreads.entrySet()) {
                String nodeName = entry.getKey();
                if (mode.equals(Request.Mode.VERBOSE) == false
                    && entry.getValue().isEmpty()
                    && (nodeToPipelineInfoMap.get(nodeName) == null || nodeToPipelineInfoMap.get(nodeName).isEmpty())) {
                    continue;
                }
                builder.startObject();
                builder.field("name", nodeName);
                List<String> threadDescriptions = entry.getValue();
                builder.startArray("threads");
                for (String threadDescription : threadDescriptions) {
                    builder.startObject();
                    builder.field("summary", threadDescription);
                    builder.endObject();
                }
                builder.endArray();
                builder.startArray("pipelines");
                Map<String, Tuple<String, List<LocalAction.ProcessorDetail>>> pipelinesForNode = nodeToPipelineInfoMap.get(nodeName);
                if (pipelinesForNode != null) {
                    for (Map.Entry<String, Tuple<String, List<LocalAction.ProcessorDetail>>> pipelineEntry : pipelinesForNode.entrySet()) {
                        builder.startObject();
                        builder.field("name", pipelineEntry.getKey());
                        builder.field("message", pipelineEntry.getValue().v1());
                        builder.startArray("processors");
                        for (LocalAction.ProcessorDetail processorDetail : pipelineEntry.getValue().v2()) {
                            builder.startObject();
                            builder.field("index", processorDetail.index);
                            builder.field("name", processorDetail.name);
                            builder.field("message", processorDetail.message);
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
            SUMMARY,
            STANDARD,
            VERBOSE
        }

        private final boolean demoMode;
        private final Mode mode;

        public Request(Mode mode, boolean demoMode) {
            this.demoMode = demoMode;
            this.mode = mode;
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
        private final IngestService ingestService;
        private final Map<String, PipelineConfiguration> demoPipelines;

        @Inject
        public LocalAction(
            ActionFilters actionFilters,
            TransportService transportService,
            ClusterService clusterService,
            IngestService ingestService,
            HealthService healthService,
            NodeClient client,
            HealthApiStats healthApiStats
        ) {
            super(NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
            this.clusterService = clusterService;
            this.client = client;
            this.ingestService = ingestService;
            try {
                demoPipelines = getPipelineConfigurationMapFromDisk();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> responseListener) {
            AtomicReference<Map<String, List<String>>> nodesHotThreadsMap = new AtomicReference<>();
            SubscribableListener.newForked(request.demoMode ? this::fetchHotThreadsFromDisk : this::fetchHotThreads)
                .<NodesStatsResponse>andThen((l, hotThreadsResponse) -> {
                    nodesHotThreadsMap.set(distillHotThreads(hotThreadsResponse.getNodesMap()));
                    if (request.demoMode) {
                        fetchNodesStatsFromDisk(l);
                    } else {
                        fetchNodesStats(l);
                    }
                })
                .andThenApply(
                    nodesStatsResponse -> createResponse(
                        nodesHotThreadsMap.get(),
                        getNodeToPipelineMap(nodesStatsResponse.getNodesMap(), request.demoMode),
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

        private void fetchHotThreadsFromDisk(ActionListener<NodesHotThreadsResponse> listener) {
            System.out.println("Loading " + diagLocation);
            Path filePath = Path.of(diagLocation, "nodes_hot_threads.txt");
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
            listener.onResponse(new NodesHotThreadsResponse(clusterName, nodes, failedNodeExceptions));
        }

        private void fetchNodesStats(ActionListener<NodesStatsResponse> listener) {
            NodesStatsRequestParameters params = new NodesStatsRequestParameters();
            params.requestedMetrics().add(NodesStatsRequestParameters.Metric.INGEST);
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(params);
            client.execute(TransportNodesStatsAction.TYPE, nodesStatsRequest, listener);
        }

        @SuppressWarnings("unchecked")
        private void fetchNodesStatsFromDisk(ActionListener<NodesStatsResponse> listener) {
            Path filePath = Path.of(diagLocation, "nodes_stats.json");
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
                    IngestStats.PipelineStat pipelineStat = new IngestStats.PipelineStat(pipelineName, stats, byteStats);
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

                IngestStats ingestStats = new IngestStats(totalStats, pipelineStats, processorStats);
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

        public static Map<String, List<String>> distillHotThreads(Map<String, NodeHotThreads> hotThreadsMap) {
            Map<String, List<String>> nodesToDistilledHotThreads = new HashMap<>();
            for (Map.Entry<String, NodeHotThreads> entry : hotThreadsMap.entrySet()) {
                NodeHotThreads hotThreads = entry.getValue();
                String hotThreadsForNode = hotThreads.getHotThreads();
                List<String> distilledHotThreads = distillSingleHotThread(entry.getKey(), hotThreadsForNode);
                nodesToDistilledHotThreads.put(hotThreads.getNode().getName(), distilledHotThreads);
            }
            return nodesToDistilledHotThreads;
        }

        private Response createResponse(
            Map<String, List<String>> nodesToDistilledHotThreads,
            Map<String, Map<String, Tuple<String, List<ProcessorDetail>>>> nodeToPipelineInfoMap,
            Request.Mode mode
        ) {
            return new Response(nodesToDistilledHotThreads, nodeToPipelineInfoMap, mode);
        }

        @SuppressWarnings("unchecked")
        private Map<String, Map<String, Tuple<String, List<ProcessorDetail>>>> getNodeToPipelineMap(
            Map<String, NodeStats> nodesMap,
            boolean demoMode
        ) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
            // maps node name to list of pipelines tuple, which is pipeline name and list of processor messages (yes, it needs a class)
            Map<String, Map<String, Tuple<String, List<ProcessorDetail>>>> nodeToPipelineInfoMap = new HashMap<>();
            for (Map.Entry<String, NodeStats> nodesMapEntry : nodesMap.entrySet()) {
                String nodeName = nodesMapEntry.getValue().getNode().getName();
                nodeToPipelineInfoMap.put(nodeName, new HashMap<>());
                IngestStats ingestStats = nodesMapEntry.getValue().getIngestStats();
                if (ingestStats != null) {
                    long totalTimeInMillis = ingestStats.totalStats().ingestTimeInMillis();
                    Map<String, Long> pipelinesToTimes = new HashMap<>();
                    List<IngestStats.PipelineStat> pipelineStats = ingestStats.pipelineStats();
                    for (IngestStats.PipelineStat pipelineStat : pipelineStats) {
                        long pipelineTimeInMillis = pipelineStat.stats().ingestTimeInMillis();
                        pipelinesToTimes.put(pipelineStat.pipelineId(), pipelineTimeInMillis);
                    }
                    double mean = calculateMean(pipelinesToTimes.values());
                    double stdDev = calculateStdDev(pipelinesToTimes.values(), mean);
                    for (Map.Entry<String, Long> entry : pipelinesToTimes.entrySet()) {
                        long pipelineTime = entry.getValue();
                        String pipelineName = entry.getKey();
                        if ((pipelineTime - mean) > 2 * stdDev || pipelineTime > (0.5 * totalTimeInMillis)) {
                            String pipelineMessage = "Takes "
                                + TimeValue.timeValueMillis(entry.getValue()).toHumanReadableString(1)
                                + " out of a total of "
                                + TimeValue.timeValueMillis(totalTimeInMillis).toHumanReadableString(1)
                                + " on the node";
                            List<ProcessorDetail> processorMessages = new ArrayList<>();
                            nodeToPipelineInfoMap.get(nodeName).put(pipelineName, Tuple.tuple(pipelineMessage, processorMessages));
                            List<IngestStats.ProcessorStat> processors = ingestStats.processorStats().get(pipelineName);
                            List<Tuple<String, Long>> processorRuntimes = new ArrayList<>();
                            for (IngestStats.ProcessorStat processorEntry : processors) {
                                long processorTime = processorEntry.stats().ingestTimeInMillis();
                                processorRuntimes.add(Tuple.tuple(processorEntry.name(), processorTime));
                            }
                            Collection<Long> processorTimes = processorRuntimes.stream().map(Tuple::v2).toList();
                            double processorMean = calculateMean(processorTimes);
                            double processorStdDev = calculateStdDev(processorTimes, mean);
                            Map<String, PipelineConfiguration> pipelineConfigurationMap = demoMode
                                ? getPipelineConfigurationMapFromDisk()
                                : getPipelineConfigurationMap();
                            PipelineConfiguration pipelineConfiguration = pipelineConfigurationMap.get(pipelineName);
                            List<Map<String, Map<String, Object>>> processorConfigs = pipelineConfiguration == null
                                ? null
                                : (List<Map<String, Map<String, Object>>>) pipelineConfiguration.getConfig().get("processors");
                            for (int i = 0; i < processorRuntimes.size(); i++) {
                                Tuple<String, Long> processorRuntime = processorRuntimes.get(i);
                                if ((processorRuntime.v2() - processorMean) > 2 * processorStdDev
                                    || processorRuntime.v2() > 0.5 * pipelineTime) {
                                    Map<String, Object> detail;
                                    if (processorConfigs == null) {
                                        detail = null;
                                    } else {
                                        Map<String, Map<String, Object>> processorConfig = processorConfigs.get(i);
                                        detail = processorConfig.values().iterator().next();
                                    }
                                    processorMessages.add(
                                        new ProcessorDetail(
                                            i,
                                            processorRuntime.v1(),
                                            "Takes "
                                                + TimeValue.timeValueMillis(processorRuntime.v2()).toHumanReadableString(1)
                                                + " out of a total of "
                                                + TimeValue.timeValueMillis(pipelineTime).toHumanReadableString(1),
                                            detail
                                        )
                                    );
                                }
                            }
                        }
                    }
                    // }
                }
            }
            return nodeToPipelineInfoMap;
        }

        private Map<String, PipelineConfiguration> getPipelineConfigurationMap() {
            return ((IngestMetadata) clusterService.state().metadata().custom("ingest")).getPipelines();
        }

        @SuppressWarnings("unchecked")
        private Map<String, PipelineConfiguration> getPipelineConfigurationMapFromDisk() throws IOException {
            if (demoPipelines != null) {
                return demoPipelines;
            }
            Path filePath = Path.of(diagLocation, "cluster_state.json");
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
                // List<Map<String, Map<String, Object>>> processors = (List<Map<String, Map<String, Object>>>) config.get("processors");
                // for (Map<String, Map<String, Object>> processor : processors) {
                // String processorType = processor.keySet().iterator().next();
                // Map<String, Object> details = processor.values().iterator().next();
                //
                // }
                PipelineConfiguration pipelineConfiguration = new PipelineConfiguration(pipelineName, config);
                pipelineConfigurationMap.put(pipelineName, pipelineConfiguration);
            }
            return pipelineConfigurationMap;
        }

        record ProcessorDetail(int index, String name, String message, Map<String, Object> detail) {}

        private static double calculateMean(Collection<Long> data) {
            double sum = 0;
            for (double num : data) {
                sum += num;
            }
            return sum / data.size();
        }

        private static double calculateStdDev(Collection<Long> data, double mean) {
            double sumSquaredDiff = 0;
            for (double num : data) {
                sumSquaredDiff += Math.pow(num - mean, 2);
            }
            double variance = sumSquaredDiff / data.size();
            return Math.sqrt(variance);
        }

        public static List<String> distillSingleHotThread(String node, String threadDump) {
            String[] lines = threadDump.split("\n");
            String firstLine = null;
            boolean watchIt = false;
            List<String> elasticStack = new ArrayList<>();
            double percent = -1;
            List<String> threadSummaries = new ArrayList<>();
            for (String line : lines) {
                if (line.isBlank()) {
                    if (firstLine != null && elasticStack.size() > 3) {
                        threadSummaries.add(percent + "% " + getSummaryOfElasticStack(elasticStack));
                    }
                    firstLine = null;
                    elasticStack = new ArrayList<>();
                } else if (line.contains("Hot threads at ")) {
                    // skip
                } else if (firstLine == null) {
                    firstLine = line;
                    percent = Double.valueOf(firstLine.substring(0, firstLine.indexOf("%")));
                    if (percent > 50) {
                        watchIt = true;
                        elasticStack.add(firstLine);
                    } else {
                        watchIt = false;
                    }
                } else {
                    if (watchIt && (line.contains("elastic") || line.contains("netty") || line.contains("lucene"))) {
                        elasticStack.add(line);
                    }
                }
            }
            return threadSummaries;
        }
    }

    private static String getSummaryOfElasticStack(List<String> elasticStack) {
        Set<String> reasons = new LinkedHashSet<>();
        Set<String> products = new LinkedHashSet<>();
        for (String stackElement : elasticStack.reversed()) {
            if (stackElement.contains("[transport_worker]")) {
                reasons.add("TRANSPORT");
            }
            if (stackElement.contains("org.elasticsearch.ingest.IngestService")) {
                reasons.add("ingesting data through pipelines");
            } else if (stackElement.contains("org.elasticsearch.action.bulk.TransportShardBulkAction")) {
                reasons.add("indexing data into indices");
            } else if (stackElement.contains("RestBulkAction")) {
                reasons.add("reading rest request to index data into indices");
            } else if (stackElement.contains("org.elasticsearch.xpack.enrich.EnrichProcessorFactory")) {
                reasons.add("enriching data");
            } else if (stackElement.contains("org.elasticsearch.index.engine.ElasticsearchConcurrentMergeScheduler.doMerge")
                || stackElement.contains("SegmentMerger")) {
                    reasons.add("maintenance -- merging segments");
                } else if (stackElement.contains("org.elasticsearch.search.SearchService")
                    || stackElement.contains("org.elasticsearch.search.internal.ContextIndexSearcher.search")
                    || stackElement.contains("TransportSearchAction")
                    || stackElement.contains("org.elasticsearch.action.search")) {
                        reasons.add("executing search requests");
                    } else if (stackElement.contains("searchable_snapshots_cache_prewarming")
                        || stackElement.contains("searchable_snapshots_cache_fetch_async")) {
                            reasons.add("maintenance -- prewarming the cache from searchable snapshots");
                        } else if (stackElement.contains("org.elasticsearch.repositories.blobstore.ShardSnapshotTaskRunner")) {
                            reasons.add("maintenance -- snapshotting data for backup");
                        } else if (stackElement.contains("org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail")) {
                            reasons.add("writing audit logs of user activity");
                        } else if (stackElement.contains("[flush]")) {
                            reasons.add("maintenance -- flushing data to disk");
                        } else if (stackElement.contains(
                            "org.elasticsearch.rest.RestController$EncodedLengthTrackingChunkedRestResponseBodyPart.encodeChunk"
                        )) {
                            reasons.add("reading potentially large data from request");
                        } else if (stackElement.contains("org.elasticsearch.xpack.monitoring.exporter.Exporters.export")) {
                            reasons.add("exporting data for Monitoring");
                        } else if (reasons.isEmpty() && stackElement.contains("[write]")) {
                            reasons.add("writing data possibly through an ingest pipeline");
                        } else if (stackElement.contains(
                            "org.elasticsearch.repositories.blobstore.BlobStoreRepository.deleteFromContainer"
                        )) {
                            reasons.add("deleting item from snapshot repository");
                        } else if (stackElement.contains("org.elasticsearch.indices.recovery.PeerRecoveryTargetService")
                            || stackElement.contains("org.elasticsearch.indices.recovery")) {
                                reasons.add("maintenance -- peer recovery");
                            } else if (stackElement.contains("maybeRefresh")) {
                                reasons.add("maintenance -- refreshing indices");
                            } else if (stackElement.contains("admin.cluster.stats")) {
                                reasons.add("gathering cluster stats");
                            } else if (stackElement.contains("org.elasticsearch.cluster.service.MasterService")) {
                                reasons.add("maintenance -- master node");
                            } else if (stackElement.contains("ShardsAllocator")) {
                                reasons.add("maintenance -- balancing shards");
                            } else if (stackElement.contains("Processor") && stackElement.contains("CompoundProcessor") == false) {
                                String firstHalfOfLine = stackElement.substring(0, stackElement.indexOf("Processor"));
                                String processorName = firstHalfOfLine.substring(firstHalfOfLine.lastIndexOf(".") + 1).toLowerCase();
                                if (processorName.contains("enrich") == false
                                    && processorName.contains("abstractstring") == false
                                    && processorName.contains("continuouscomputation$") == false
                                    && processorName.contains("asyncio") == false) {
                                    reasons.add("running " + processorName + " processor");
                                }
                            } else if (stackElement.contains("io.netty.handler.ssl.SslHandler.flush")) {
                                reasons.add("returning potentially large response");
                            } else if (stackElement.contains("org.elasticsearch.xpack.ml")) {
                                reasons.add("machine learning");
                            } else if (stackElement.contains("RBACEngine")) {
                                reasons.add("authentication");
                            }
            if (stackElement.contains("elastic") && stackElement.contains("%") == false) {
                products.add("elastic");
            } else if (stackElement.contains("netty")) {
                products.add("netty (network)");
            } else if (stackElement.contains("lucene")) {
                products.add("lucene");
            }
        }
        if (reasons.isEmpty()) {
            return "unknown " + products.stream().collect(Collectors.joining(", ")) + " thread";
        } else {
            return reasons.stream().collect(Collectors.joining(", "));
        }
    }
}
