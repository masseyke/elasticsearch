/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;

import java.util.List;
import java.util.Map;

public class DiskHealthIndicatorService implements HealthIndicatorService {
    public static final String NAME = "disk_health";

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

    public DiskHealthIndicatorService() {}

    @Override
    public String name() {
        return NAME;
    }

    public Class<DiskHealthInfo> getHealthNodeDataTypeRequired() {
        return DiskHealthInfo.class;
    }

    @Override
    public HealthIndicatorResult calculate(boolean explain) {
        throw new RuntimeException("Wrong one");
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public HealthIndicatorResult calculate(boolean explain, Map<String, ? extends HealthNodeInfo> healthInfoMap) {
        Map<String, DiskHealthInfo> diskHealthInfoMap = (Map<String, DiskHealthInfo>) healthInfoMap;
        HealthStatus healthStatus = HealthStatus.merge(diskHealthInfoMap.values().stream().map(DiskHealthInfo::healthStatus));
        String symptom;
        HealthIndicatorDetails details = getDetails(explain, diskHealthInfoMap);
        ;
        List<HealthIndicatorImpact> impacts;
        List<Diagnosis> diagnosisList;
        if (HealthStatus.GREEN.equals(healthStatus)) {
            symptom = "Disk usage is within configured thresholds";
            impacts = List.of();
            diagnosisList = List.of();
        } else {
            symptom = "Disk usage exceeds limits";
            impacts = DISK_PROBLEMS_IMPACTS;
            diagnosisList = getDiskProblemsDiagnosis(explain);
        }
        return new HealthIndicatorResult(NAME, healthStatus, symptom, details, impacts, diagnosisList);
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
}
