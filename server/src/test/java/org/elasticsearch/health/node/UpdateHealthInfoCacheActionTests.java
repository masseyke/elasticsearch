/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;

public class UpdateHealthInfoCacheActionTests extends ESTestCase {

    private record TestHealthNodeInfo(String someString) implements HealthNodeInfo {

        TestHealthNodeInfo(StreamInput input) throws IOException {
            this(input.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(someString);
        }
    }

    public void testRequestSerialization() {
        UpdateHealthInfoCacheAction.Request request = new UpdateHealthInfoCacheAction.Request(
            randomAlphaOfLength(10),
            new DiskHealthInfo(randomFrom(HealthStatus.values()), randomFrom(DiskHealthInfo.Cause.values())),
            new TestHealthNodeInfo(randomAlphaOfLength(100))
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            request,
            requestWritable -> copyWriteable(requestWritable, writableRegistry(), UpdateHealthInfoCacheAction.Request::new),
            this::mutateRequest
        );
    }

    private UpdateHealthInfoCacheAction.Request mutateRequest(UpdateHealthInfoCacheAction.Request originalRequest) {
        return new UpdateHealthInfoCacheAction.Request(originalRequest.getNodeId(), originalRequest.getDiskHealthInfo());
    }
}
