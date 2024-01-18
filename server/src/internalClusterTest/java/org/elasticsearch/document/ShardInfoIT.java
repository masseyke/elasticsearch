/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.document;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ShardInfoIT extends ESIntegTestCase {
    private int numCopies;
    private int numNodes;

    public void testIndexAndDelete() throws Exception {
        prepareIndex(1);
        IndexRequestBuilder indexRequestBuilder = prepareIndex("idx").setSource("{}", XContentType.JSON);
        DocWriteResponse indexResponse = indexRequestBuilder.get();
        indexRequestBuilder.request().decRef();
        assertShardInfo(indexResponse);
        DeleteResponse deleteResponse = client().prepareDelete("idx", indexResponse.getId()).get();
        assertShardInfo(deleteResponse);
    }

    public void testUpdate() throws Exception {
        prepareIndex(1);
        UpdateRequestBuilder updateRequestBuilder = client().prepareUpdate("idx", "1").setDoc("{}", XContentType.JSON).setDocAsUpsert(true);
        UpdateResponse updateResponse = updateRequestBuilder.get();
        updateRequestBuilder.request().decRef();
        assertShardInfo(updateResponse);
    }

    public void testBulkWithIndexAndDeleteItems() throws Exception {
        prepareIndex(1);
        try (BulkRequestBuilder indexBulkRequestBuilder = client().prepareBulk()) {
            for (int i = 0; i < 10; i++) {
                IndexRequestBuilder indexRequestBuilder = prepareIndex("idx").setSource("{}", XContentType.JSON);
                indexBulkRequestBuilder.add(indexRequestBuilder);
                indexRequestBuilder.request().decRef();
            }

            BulkResponse bulkResponse = indexBulkRequestBuilder.get();
            try (BulkRequestBuilder deleteBulkRequestBuilder = client().prepareBulk()) {
                for (BulkItemResponse item : bulkResponse) {
                    assertThat(item.isFailed(), equalTo(false));
                    assertShardInfo(item.getResponse());
                    deleteBulkRequestBuilder.add(client().prepareDelete("idx", item.getId()));
                }

                bulkResponse = deleteBulkRequestBuilder.get();
                for (BulkItemResponse item : bulkResponse) {
                    assertThat(item.isFailed(), equalTo(false));
                    assertShardInfo(item.getResponse());
                }
            }
        }
    }

    public void testBulkWithUpdateItems() throws Exception {
        prepareIndex(1);
        try (BulkRequestBuilder bulkRequestBuilder = client().prepareBulk()) {
            for (int i = 0; i < 10; i++) {
                UpdateRequestBuilder updateRequestBuilder = client().prepareUpdate("idx", Integer.toString(i))
                    .setDoc("{}", XContentType.JSON)
                    .setDocAsUpsert(true);
                bulkRequestBuilder.add(updateRequestBuilder);
                updateRequestBuilder.request().decRef();
            }

            BulkResponse bulkResponse = bulkRequestBuilder.get();
            for (BulkItemResponse item : bulkResponse) {
                assertThat(item.getFailure(), nullValue());
                assertThat(item.isFailed(), equalTo(false));
                assertShardInfo(item.getResponse());
            }
        }
    }

    private void prepareIndex(int numberOfPrimaryShards) throws Exception {
        prepareIndex(numberOfPrimaryShards, false);
    }

    private void prepareIndex(int numberOfPrimaryShards, boolean routingRequired) throws Exception {
        numNodes = cluster().numDataNodes();
        logger.info("Number of nodes: {}", numNodes);
        int maxNumberOfCopies = (numNodes * 2) - 1;
        numCopies = randomIntBetween(numNodes, maxNumberOfCopies);
        logger.info("Number of copies: {}", numCopies);

        assertAcked(
            prepareCreate("idx").setSettings(indexSettings(numberOfPrimaryShards, numCopies - 1))
                .setMapping("_routing", "required=" + routingRequired)
        );
        for (int i = 0; i < numberOfPrimaryShards; i++) {
            ensureActiveShardCopies(i, numNodes);
        }
    }

    private void assertShardInfo(ReplicationResponse response) {
        assertShardInfo(response, numCopies, numNodes);
    }

    private void assertShardInfo(ReplicationResponse response, int expectedTotal, int expectedSuccessful) {
        assertThat(response.getShardInfo().getTotal(), greaterThanOrEqualTo(expectedTotal));
        assertThat(response.getShardInfo().getSuccessful(), greaterThanOrEqualTo(expectedSuccessful));
    }

    private void ensureActiveShardCopies(final int shardId, final int copyCount) throws Exception {
        assertBusy(() -> {
            ClusterState state = clusterAdmin().prepareState().get().getState();
            assertThat(state.routingTable().index("idx"), not(nullValue()));
            assertThat(state.routingTable().index("idx").shard(shardId), not(nullValue()));
            assertThat(state.routingTable().index("idx").shard(shardId).activeShards().size(), equalTo(copyCount));

            ClusterHealthResponse healthResponse = clusterAdmin().prepareHealth("idx").setWaitForNoRelocatingShards(true).get();
            assertThat(healthResponse.isTimedOut(), equalTo(false));

            RecoveryResponse recoveryResponse = indicesAdmin().prepareRecoveries("idx").setActiveOnly(true).get();
            assertThat(recoveryResponse.shardRecoveryStates().get("idx").size(), equalTo(0));
        });
    }
}
