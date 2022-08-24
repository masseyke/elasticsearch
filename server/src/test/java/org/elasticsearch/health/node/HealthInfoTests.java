/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.HashMap;
import java.util.Map;

public class HealthInfoTests extends ESTestCase {
    public void testResponseSerialization() {
        int numberOfNodes = randomIntBetween(0, 200);
        Map<String, DiskHealthInfo> diskInfoByNode = new HashMap<>(numberOfNodes);
        for (int i = 0; i < numberOfNodes; i++) {
            DiskHealthInfo diskHealthInfo = randomBoolean()
                ? new DiskHealthInfo(randomFrom(HealthStatus.values()))
                : new DiskHealthInfo(randomFrom(HealthStatus.values()), randomFrom(DiskHealthInfo.Cause.values()));
            diskInfoByNode.put(randomAlphaOfLengthBetween(10, 100), diskHealthInfo);
        }
        HealthInfo healthInfo = new HealthInfo(diskInfoByNode);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            healthInfo,
            healthInfoWritable -> copyWriteable(healthInfoWritable, writableRegistry(), HealthInfo::new),
            this::mutateHealthInfo
        );
    }

    private HealthInfo mutateHealthInfo(HealthInfo originalHealthInfo) {
        Map<String, DiskHealthInfo> diskHealthInfoMap = originalHealthInfo.diskInfoByNode();
        Map<String, DiskHealthInfo> diskHealthInfoMapCopy = new HashMap<>(diskHealthInfoMap);
        switch (randomIntBetween(1, 3)) {
            case 1 -> {
                diskHealthInfoMapCopy.put(
                    randomAlphaOfLength(10),
                    new DiskHealthInfo(randomFrom(HealthStatus.values()), randomFrom(DiskHealthInfo.Cause.values()))
                );
            }
            case 2 -> {
                if (diskHealthInfoMap.size() > 0) {
                    String someNode = diskHealthInfoMap.keySet().iterator().next();
                    diskHealthInfoMapCopy.put(
                        someNode,
                        new DiskHealthInfo(
                            randomValueOtherThan(diskHealthInfoMap.get(someNode).healthStatus(), () -> randomFrom(HealthStatus.values())),
                            randomFrom(DiskHealthInfo.Cause.values())
                        )
                    );
                } else {
                    diskHealthInfoMapCopy.put(
                        randomAlphaOfLength(10),
                        new DiskHealthInfo(randomFrom(HealthStatus.values()), randomFrom(DiskHealthInfo.Cause.values()))
                    );
                }
            }
            case 3 -> {
                if (diskHealthInfoMap.size() > 0) {
                    diskHealthInfoMapCopy.remove(randomFrom(diskHealthInfoMapCopy.keySet()));
                    return new HealthInfo(diskHealthInfoMapCopy);
                } else {
                    diskHealthInfoMapCopy.put(
                        randomAlphaOfLength(10),
                        new DiskHealthInfo(randomFrom(HealthStatus.values()), randomFrom(DiskHealthInfo.Cause.values()))
                    );
                }
            }
            default -> throw new IllegalStateException();
        }
        return new HealthInfo(diskHealthInfoMapCopy);

    }
}
