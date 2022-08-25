/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DiskHealthIndicatorService implements HealthIndicatorService {
    public static final String NAME = "disk_health";

    private final ClusterService clusterService;

    private static final String DISK_PROBLEMS_INGEST_IMPACT = "The cluster cannot create, delete, or rebalance indices, and cannot "
        + "insert or update documents.";
    private static final String DISK_PROBLEMS_DEPLOYMENT_MANAGEMENT_IMPACT = "Scheduled tasks such as Watcher, ILM, and SLM might not "
        + "work.";
    private static final String DISK_PROBLEMS_BACKUP_IMPACT = "Snapshot and restore might not work.";

    private static final List<HealthIndicatorImpact> DISK_PROBLEMS_IMPACTS = List.of(
        new HealthIndicatorImpact(1, DISK_PROBLEMS_INGEST_IMPACT, List.of(ImpactArea.INGEST)),
        new HealthIndicatorImpact(1, DISK_PROBLEMS_DEPLOYMENT_MANAGEMENT_IMPACT, List.of(ImpactArea.DEPLOYMENT_MANAGEMENT)),
        new HealthIndicatorImpact(3, DISK_PROBLEMS_BACKUP_IMPACT, List.of(ImpactArea.BACKUP))
    );

    public DiskHealthIndicatorService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return NAME;
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public HealthIndicatorResult calculate(boolean explain, HealthInfo healthInfo) {
        Map<String, DiskHealthInfo> diskHealthInfoMap = healthInfo.diskInfoByNode();
        if (diskHealthInfoMap == null || diskHealthInfoMap.isEmpty()) {
            return createIndicator(
                HealthStatus.UNKNOWN,
                "No disk usage data",
                HealthIndicatorDetails.EMPTY,
                Collections.emptyList(),
                Collections.emptyList()
            );
        }
        // TODO: Do we need to make sure that we have a value for each node that's in the cluster state?
        Set<String> indicesWithBlock = clusterService.state()
            .blocks()
            .indices()
            .entrySet()
            .stream()
            .filter(entry -> entry.getValue().contains(IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        boolean hasIndexReadOnlyAllowDeleteBlock = indicesWithBlock.isEmpty() == false;
        final HealthStatus healthStatus;
        final String symptom;
        HealthIndicatorDetails details = getDetails(explain, diskHealthInfoMap);
        final List<HealthIndicatorImpact> impacts;
        final List<Diagnosis> diagnosisList;
        if (hasIndexReadOnlyAllowDeleteBlock) {
            healthStatus = HealthStatus.RED;
            symptom = String.format(
                Locale.ROOT,
                "%d %s a read only / allow deletes block",
                indicesWithBlock.size(),
                indicesWithBlock.size() > 1 ? "indices have" : "index has"
            );
            impacts = DISK_PROBLEMS_IMPACTS;
            diagnosisList = getIndexReadOnlyBlockDiagnosis(explain);
        } else {
            healthStatus = HealthStatus.merge(diskHealthInfoMap.values().stream().map(DiskHealthInfo::healthStatus));
            if (HealthStatus.GREEN.equals(healthStatus)) {
                symptom = "Disk usage is within configured thresholds";
                impacts = List.of();
                diagnosisList = List.of();
            } else {
                if (HealthStatus.RED.equals(healthStatus)) {
                    final int nodesLimit = 5;
                    Set<String> redNodes = diskHealthInfoMap.entrySet()
                        .stream()
                        .filter(entry -> HealthStatus.RED.equals(entry.getValue().healthStatus()))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet());
                    String redNodesString = redNodes.stream().limit(nodesLimit).collect(Collectors.joining(", "));
                    boolean hadToTruncate = redNodes.size() > nodesLimit;
                    int numberTruncated = hadToTruncate ? redNodes.size() - nodesLimit : 0;
                    symptom = String.format(
                        Locale.ROOT,
                        "Node%s %s%s %s out of disk space",
                        redNodes.size() > 1 ? "s" : "",
                        redNodesString,
                        hadToTruncate ? String.format(Locale.ROOT, ", and %d more", numberTruncated) : "",
                        redNodes.size() > 1 ? "are" : "is"
                    );
                } else {
                    final int nodesLimit = 5;
                    Set<String> yellowNodes = diskHealthInfoMap.entrySet()
                        .stream()
                        .filter(entry -> HealthStatus.YELLOW.equals(entry.getValue().healthStatus()))
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toSet());
                    String yellowNodesString = yellowNodes.stream().limit(nodesLimit).collect(Collectors.joining(", "));
                    boolean hadToTruncate = yellowNodes.size() > nodesLimit;
                    int numberTruncated = hadToTruncate ? yellowNodes.size() - nodesLimit : 0;
                    symptom = String.format(
                        Locale.ROOT,
                        "Node%s %s%s ha%s increased disk usage",
                        yellowNodes.size() > 1 ? "s" : "",
                        yellowNodesString,
                        hadToTruncate ? String.format(Locale.ROOT, ", and %d more", numberTruncated) : "",
                        yellowNodes.size() > 1 ? "ve" : "s"
                    );
                }
                impacts = DISK_PROBLEMS_IMPACTS;
                diagnosisList = getDiskProblemsDiagnosis(explain);
            }
        }
        return createIndicator(healthStatus, symptom, details, impacts, diagnosisList);
    }

    private HealthIndicatorDetails getDetails(boolean explain, Map<String, DiskHealthInfo> diskHealthInfoMap) {
        if (explain == false) {
            return HealthIndicatorDetails.EMPTY;
        }
        return (builder, params) -> {
            builder.startObject();
            builder.array("disk_health_by_node", arrayXContentBuilder -> {
                for (Map.Entry<String, DiskHealthInfo> entry : diskHealthInfoMap.entrySet()) {
                    builder.startObject();
                    builder.field("node_id", entry.getKey());
                    builder.field("status", entry.getValue().healthStatus());
                    DiskHealthInfo.Cause cause = entry.getValue().cause();
                    if (cause != null) {
                        builder.field("cause", entry.getValue().cause());
                    }
                    builder.endObject();
                }
            });
            return builder.endObject();
        };
    }

    private List<Diagnosis> getDiskProblemsDiagnosis(boolean explain) {
        if (explain == false) {
            return List.of();
        }
        return List.of(
            new Diagnosis(
                new Diagnosis.Definition(
                    "free-disk-space",
                    "Disk thresholds have been exceeded",
                    "Free up disk space",
                    "https://ela.st/free-disk-space"
                ),
                null
            ),
            new Diagnosis(
                new Diagnosis.Definition(
                    "add-disk-capacity",
                    "Disk thresholds have been exceeded",
                    "Add disk capacity",
                    "https://ela.st/increase-disk-capacity"
                ),
                null
            )
        );
    }

    private List<Diagnosis> getIndexReadOnlyBlockDiagnosis(boolean explain) {
        if (explain == false) {
            return List.of();
        }
        return List.of(
            new Diagnosis(
                new Diagnosis.Definition(
                    "free-disk-space",
                    "Disk thresholds have been exceeded",
                    "Free up disk space",
                    "https://ela.st/free-disk-space"
                ),
                null
            ),
            new Diagnosis(
                new Diagnosis.Definition(
                    "add-disk-capacity",
                    "Disk thresholds have been exceeded",
                    "Add disk capacity",
                    "https://ela.st/increase-disk-capacity"
                ),
                null
            )
        );
    }
}
