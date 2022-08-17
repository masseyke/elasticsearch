/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.node.selection.HealthNode;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps track of several health statuses per node that can be used in health.
 */
public class HealthInfoCache implements ClusterStateListener {

    private volatile ConcurrentHashMap<Class<? extends HealthNodeInfo>, Map<String, ? extends HealthNodeInfo>> healthInfoByTypeAndNode =
        new ConcurrentHashMap<>();

    private HealthInfoCache() {}

    public static HealthInfoCache create(ClusterService clusterService) {
        HealthInfoCache healthInfoCache = new HealthInfoCache();
        clusterService.addListener(healthInfoCache);
        return healthInfoCache;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T extends HealthNodeInfo> void updateNodeHealth(String nodeId, T healthInfo) {
        if (healthInfoByTypeAndNode.containsKey(healthInfo.getClass()) == false) {
            healthInfoByTypeAndNode.put(healthInfo.getClass(), new HashMap<>());
        }
        // HealthNodeInfo x = (HealthNodeInfo) healthInfo;
        Map mup = healthInfoByTypeAndNode.get(healthInfo.getClass());
        mup.put(nodeId, healthInfo);
        // healthInfoByTypeAndNode.get(healthInfo.getClass()).put(nodeId, x);
        // healthInfoByTypeAndNode.merge(healthInfo.getClass(), Map.of(nodeId, healthInfo),
        // new BiFunction<Map<String, ? extends HealthNodeInfo>, Map<String, ? extends HealthNodeInfo>, Map<String, ? extends
        // HealthNodeInfo>>() {
        // @Override
        // public Map<String, ? extends HealthNodeInfo> apply(Map<String, ? extends HealthNodeInfo> stringMap, Map<String, ? extends
        // HealthNodeInfo> stringMap2) {
        // return stringMap.put(nodeId, healthInfo);
        // }
        // });
    }

    public static void main(String[] args) {
        Map<String, Integer> wordCounts = new LinkedHashMap<String, Integer>();

        String s = "Lorem ipsum dolor sit amet consetetur iam nonumy sadipscing "
            + "elitr, sed diam nonumy eirmod tempor invidunt ut erat sed "
            + "labore et dolore magna dolor sit amet aliquyam erat sed diam";

        // wordCounts.put("sed", 0);
        // wordCounts.put("erat", 0);

        for (String t : s.split(" ")) {
            wordCounts.computeIfPresent(t, (k, v) -> v + 1);
            wordCounts.computeIfAbsent(t, v -> 1);
        }
        System.out.println(wordCounts);

        wordCounts = new LinkedHashMap<String, Integer>();
        for (String t : s.split(" ")) {
            wordCounts.merge(t, 1, (k, v) -> k + v);
            // wordCounts.computeIfAbsent(t, v -> 1);
        }
        System.out.println(wordCounts);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentHealthNode = HealthNode.findHealthNode(event.state());
        DiscoveryNode localNode = event.state().nodes().getLocalNode();
        if (localNode.equals(currentHealthNode)) {
            if (event.nodesRemoved()) {
                for (DiscoveryNode removedNode : event.nodesDelta().removedNodes()) {
                    for (Map<String, ? extends HealthNodeInfo> healthInfoByNode : healthInfoByTypeAndNode.values()) {
                        healthInfoByNode.remove(removedNode.getId());
                    }
                }
            }
            // Resetting the cache is not synchronized for efficiency and simplicity.
            // Processing a delayed update after the cache has been emptied because
            // the node is not the health node anymore has small impact since it will
            // be reset in the next round again.
        } else if (healthInfoByTypeAndNode.isEmpty() == false) {
            healthInfoByTypeAndNode = new ConcurrentHashMap<>();
        }
    }

    // A shallow copy is enough because the inner data is immutable.
    @SuppressWarnings({ "unchecked" })
    public Map<String, DiskHealthInfo> getDiskHealthInfo() {
        return (Map<String, DiskHealthInfo>) Map.copyOf(healthInfoByTypeAndNode.get(DiskHealthInfo.class));
    }
}
