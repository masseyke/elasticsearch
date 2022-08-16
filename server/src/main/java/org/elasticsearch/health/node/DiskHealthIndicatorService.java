/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.health.HealthStatus;

import java.util.List;
import java.util.Map;

public class DiskHealthIndicatorService implements HealthIndicatorService {
    public static final String NAME = "disk_health";

    public DiskHealthIndicatorService(ClusterService clusterService) {

    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public HealthIndicatorResult calculate(boolean explain) {
        throw new RuntimeException("Wrong one");
    }

    @Override
    public HealthIndicatorResult calculate(boolean explain, Map<String, DiskHealthInfo> diskHealthInfoMap) {
        HealthStatus healthStatus = HealthStatus.merge(diskHealthInfoMap.values().stream().map(DiskHealthInfo::healthStatus));
        HealthIndicatorResult result = new HealthIndicatorResult(
            NAME,
            healthStatus,
            "symtom",
            HealthIndicatorDetails.EMPTY,
            List.of(),
            List.of()
        );
        return result;
    }
}
