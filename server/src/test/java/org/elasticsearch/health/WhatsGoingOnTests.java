/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class WhatsGoingOnTests extends ESTestCase {
    public void testFromDiagBundle() throws IOException {
        // List<String> diagLocations = List.of("/Users/keithmassey/sdh/8811/remote-diagnostics-20250211-131529",
        // "/Users/keithmassey/sdh/8811/remote-diagnostics-20250217-082037",
        // "/Users/keithmassey/sdh/8811/remote-diagnostics-20250217-082318",
        // "/Users/keithmassey/sdh/8811/remote-diagnostics-20250217-083708");
        List<String> diagLocations = List.of(
            "/Users/keithmassey/sdh/8689/api-diagnostics-20250108-163913", // audit logs
            "/Users/keithmassey/sdh/8707/remote-diagnostics-20250122-083338", // segment merging
            "/Users/keithmassey/sdh/8739/api-diagnostics-20250131-172326", // enrich, script slowness
            "/Users/keithmassey/sdh/8763/api-diagnostics-20250127-171211", // ml processor slowness
            "/Users/keithmassey/sdh/8676/api-diagnostics-20241231-112131", // horrible grok
            "/Users/keithmassey/sdh/8732/api-diagnostics-20250122-155818", // terrible script processor
            "/Users/keithmassey/sdh/8806/elastic/elasticsearch/elasticsearch"
        );
        for (String diagLocation : diagLocations) {
            // System.out.println("Loading " + diagLocation);
            // Path filePath = Path.of(diagLocation, "nodes_hot_threads.txt");
            // List<String> lines = Files.readAllLines(filePath);
            //
            // String currentNode = null;
            // String currentNodeId = null;
            // Map<String, String> nodeToThreads = new HashMap<>();
            // StringBuilder currentThreads = new StringBuilder();
            // List<NodeHotThreads> nodes = new ArrayList<>();
            // StringBuilder linesForNode = new StringBuilder();
            // for (String line : lines) {
            // if (line.startsWith(":::")) {
            // if (currentNode != null) {
            // nodeToThreads.put(currentNode, currentThreads.toString());
            //
            // NodeHotThreads nodeHotThreads = new NodeHotThreads(new DiscoveryNode(currentNode, currentNodeId, new TransportAddress(new
            // InetSocketAddress("localhost", 9200)), Map.of(), Set.of(), VersionInformation.CURRENT), ReleasableBytesReference.wrap(new
            // BytesArray(linesForNode.toString())));
            // nodes.add(nodeHotThreads);
            //
            // currentThreads = new StringBuilder();
            // }
            // currentNode = line.substring(line.indexOf("{") + 1, line.indexOf("}"));
            // currentNodeId = currentNode; //TODO
            // linesForNode = new StringBuilder();
            // } else {
            // currentThreads.append(line);
            // currentThreads.append("\n");
            // linesForNode.append(line);
            // linesForNode.append("\n");
            // }
            //
            // }
            // NodeHotThreads nodeHotThreads = new NodeHotThreads(new DiscoveryNode(currentNode, currentNodeId, new TransportAddress(new
            // InetSocketAddress("localhost", 9200)), Map.of(), Set.of(), VersionInformation.CURRENT), ReleasableBytesReference.wrap(new
            // BytesArray(linesForNode.toString())));
            // nodes.add(nodeHotThreads);
            //// for (Map.Entry<String, String> entry : nodeToThreads.entrySet()) {
            //// System.out.println(
            //// WhatIsMyClusterDoingAction.LocalAction.distillSingleHotThread(entry.getKey(), entry.getValue())
            //// .stream()
            //// .collect(Collectors.joining("\n"))
            //// );
            //// }
            //
            //// printNodesStats(diagLocation);
            // ClusterName clusterName = new ClusterName("cluster");
            // List<FailedNodeException> failedNodeExceptions = List.of();
            NodesHotThreadsResponse nodesHotThreadsResponse = getNodesHotThreadsResponse(diagLocation);
            Map<String, List<WhatIsMyClusterDoingAction.LocalAction.DistilledHotThread>> nodesToResults = WhatIsMyClusterDoingAction.LocalAction.distillHotThreads(
                nodesHotThreadsResponse.getNodesMap(),
                null
            );
            nodesHotThreadsResponse.decRef();
            for (Map.Entry<String, List<WhatIsMyClusterDoingAction.LocalAction.DistilledHotThread>> entry : nodesToResults.entrySet()) {
                System.out.println("**** " + entry.getKey());
                System.out.println(entry.getValue().stream().map(t -> t.percent() + "% " + t.classification() + " " + t.activities()).collect(Collectors.joining("\n")));
            }
            // printNodesStats(diagLocation);
        }
    }

    private NodesHotThreadsResponse getNodesHotThreadsResponse(String diagLocation) throws IOException {
        // String diagLocation = "/Users/keithmassey/sdh/8689/api-diagnostics-20250108-163913";
        System.out.println("Loading " + diagLocation);
        Path filePath = Path.of(diagLocation, "nodes_hot_threads.txt");
        List<String> lines = Files.readAllLines(filePath);

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
        return new NodesHotThreadsResponse(clusterName, nodes, failedNodeExceptions);
    }

    @SuppressWarnings("unchecked")
    private void printNodesStats(String diagLocation) throws IOException {
        Path filePath = Path.of(diagLocation, "nodes_stats.json");
        // System.out.println("Loading " + filePath);
        // Path filePath = Path.of(diagLocation, "formatted_nodes_stats");
        String jsonString = Files.readString(filePath);
        String currentNode = null;
        Map<String, String> nodeToThreads = new HashMap<>();
        Map<String, Object> nodesStatsMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, jsonString, false);
        Map<String, Map<String, Object>> nodesMaps = (Map<String, Map<String, Object>>) nodesStatsMap.get("nodes");
        for (String node : nodesMaps.keySet()) {
            Map<String, Object> nodeMap = nodesMaps.get(node);
            Map<String, Object> ingestMap = (Map<String, Object>) nodeMap.get("ingest");
            Map<String, Object> totalMap = (Map<String, Object>) ingestMap.get("total");
            long totalTimeInMillis = ((Number) totalMap.get("time_in_millis")).longValue();
            if (totalTimeInMillis > TimeValue.timeValueMinutes(10).millis()) {
                Map<String, Long> pipelinesToTimes = new HashMap<>();
                System.out.println("\n****** " + nodeMap.get("name"));
                Map<String, Object> pipelinesMap = (Map<String, Object>) ingestMap.get("pipelines");
                for (String pipelineName : pipelinesMap.keySet()) {
                    Map<String, Object> pipelineMap = (Map<String, Object>) pipelinesMap.get(pipelineName);
                    long pipelineTimeInMillis = ((Number) pipelineMap.get("time_in_millis")).longValue();
                    pipelinesToTimes.put(pipelineName, pipelineTimeInMillis);
                }
                double mean = calculateMean(pipelinesToTimes.values());
                double stdDev = calculateStdDev(pipelinesToTimes.values(), mean);
                for (Map.Entry<String, Long> entry : pipelinesToTimes.entrySet()) {
                    long pipelineTime = entry.getValue();
                    String pipelineName = entry.getKey();
                    if ((pipelineTime - mean) > 2 * stdDev || pipelineTime > (0.5 * totalTimeInMillis)) {
                        System.out.println(
                            pipelineName
                                + " is an outlier and takes "
                                + TimeValue.timeValueMillis(entry.getValue()).toHumanReadableString(1)
                                + " out of a total of "
                                + TimeValue.timeValueMillis(totalTimeInMillis).toHumanReadableString(1)
                                + " on the node"
                        );
                        Map<String, Object> things = (Map<String, Object>) pipelinesMap.get(pipelineName);
                        List<Map<String, Map<String, Object>>> processors = (List<Map<String, Map<String, Object>>>) things.get(
                            "processors"
                        );
                        List<Tuple<String, Long>> processorRuntimes = new ArrayList<>();
                        for (Map<String, Map<String, Object>> processorEntry : processors) {
                            assert processorEntry.keySet().size() == 1;
                            String processorType = processorEntry.keySet().iterator().next();
                            Map<String, Object> procoessor = processorEntry.get(processorType);
                            Map<String, Object> stats = (Map<String, Object>) procoessor.get("stats");
                            long processorTime = ((Number) stats.get("time_in_millis")).longValue();
                            processorRuntimes.add(Tuple.tuple(processorType, processorTime));
                        }
                        Collection<Long> processorTimes = processorRuntimes.stream().map(Tuple::v2).toList();
                        double processorMean = calculateMean(processorTimes);
                        double processorStdDev = calculateStdDev(processorTimes, mean);
                        for (int i = 0; i < processorRuntimes.size(); i++) {
                            Tuple<String, Long> processorRuntime = processorRuntimes.get(i);
                            if ((processorRuntime.v2() - processorMean) > 2 * processorStdDev
                                || processorRuntime.v2() > 0.5 * pipelineTime) {
                                System.out.println(
                                    "\tProcessor "
                                        + i
                                        + ", \""
                                        + processorRuntime.v1()
                                        + "\", takes "
                                        + TimeValue.timeValueMillis(processorRuntime.v2()).toHumanReadableString(1)
                                        + " out of a total of "
                                        + TimeValue.timeValueMillis(pipelineTime).toHumanReadableString(1)
                                        + " for the pipeline on the node"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    public void xtestNodesStatsFromDiagBundle() throws IOException {
        // List<String> diagLocations = List.of("/Users/keithmassey/sdh/8689/api-diagnostics-20250108-163913");
        List<String> diagLocations = List.of(
            "/Users/keithmassey/sdh/8689/api-diagnostics-20250108-163913",
            "/Users/keithmassey/sdh/8707/remote-diagnostics-20250122-083338",
            "/Users/keithmassey/sdh/8739/api-diagnostics-20250131-172326",
            "/Users/keithmassey/sdh/8763/api-diagnostics-20250127-171211",
            "/Users/keithmassey/sdh/8676/api-diagnostics-20241231-112131",
            "/Users/keithmassey/sdh/8732/api-diagnostics-20250122-155818"
        );
        for (String diagLocation : diagLocations) {
            printNodesStats(diagLocation);
        }
    }

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
}
