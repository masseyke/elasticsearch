/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xcontent.XContentType;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class BulkProcessorClusterSettingsIT extends ESIntegTestCase {
    public void testBulkProcessorAutoCreateRestrictions() {
        // See issue #8125
        Settings settings = Settings.builder().put("action.auto_create_index", false).build();

        internalCluster().startNode(settings);

        createIndex("willwork");
        clusterAdmin().prepareHealth("willwork").setWaitForGreenStatus().get();

        try (BulkRequestBuilder bulkRequestBuilder = client().prepareBulk()) {
            IndexRequestBuilder indexRequestBuilder1 = prepareIndex("willwork").setId("1").setSource("{\"foo\":1}", XContentType.JSON);
            IndexRequestBuilder indexRequestBuilder2 = prepareIndex("wontwork").setId("2").setSource("{\"foo\":2}", XContentType.JSON);
            IndexRequestBuilder indexRequestBuilder3 = prepareIndex("willwork").setId("3").setSource("{\"foo\":3}", XContentType.JSON);
            bulkRequestBuilder.add(indexRequestBuilder1);
            bulkRequestBuilder.add(indexRequestBuilder2);
            bulkRequestBuilder.add(indexRequestBuilder3);
            BulkResponse br = bulkRequestBuilder.get();
            indexRequestBuilder1.request().decRef();
            indexRequestBuilder2.request().decRef();
            indexRequestBuilder3.request().decRef();
            BulkItemResponse[] responses = br.getItems();
            assertEquals(3, responses.length);
            assertFalse("Operation on existing index should succeed", responses[0].isFailed());
            assertTrue("Missing index should have been flagged", responses[1].isFailed());
            assertThat(
                responses[1].getFailureMessage(),
                equalTo(
                    "[wontwork] org.elasticsearch.index.IndexNotFoundException: no such index [wontwork]"
                        + " and [action.auto_create_index] is [false]"
                )
            );
            assertFalse("Operation on existing index should succeed", responses[2].isFailed());
        }
    }

    public void testIndexWithDisabledAutoCreateIndex() {
        updateClusterSettings(Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), randomFrom("-*", "+.*")));
        try (BulkRequestBuilder bulkRequestBuilder = client().prepareBulk()) {
            IndexRequestBuilder indexRequestBuilder = prepareIndex("test-index").setSource("foo", "bar");
            final BulkItemResponse itemResponse = bulkRequestBuilder.add(indexRequestBuilder).get().getItems()[0];
            indexRequestBuilder.request().decRef();
            assertThat(itemResponse.getFailure().getCause(), instanceOf(IndexNotFoundException.class));
        }
    }
}
