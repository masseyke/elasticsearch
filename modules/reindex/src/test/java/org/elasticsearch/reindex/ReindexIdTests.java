/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.reindex.AbstractAsyncBulkByScrollActionTestCase;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Reindex tests for picking ids.
 */
public class ReindexIdTests extends AbstractAsyncBulkByScrollActionTestCase<ReindexRequest, BulkByScrollResponse> {
    public void testEmptyStateCopiesId() throws Exception {
        testReindexId(ClusterState.EMPTY_STATE, false);
    }

    public void testStandardIndexCopiesId() throws Exception {
        testReindexId(stateWithIndex(standardSettings()), false);
    }

    public void testTsdbIndexClearsId() throws Exception {
        testReindexId(stateWithIndex(tsdbSettings()), true);
    }

    public void testMissingIndexWithStandardTemplateCopiesId() throws Exception {
        testReindexId(stateWithTemplate(standardSettings()), false);
    }

    public void testMissingIndexWithTsdbTemplateClearsId() throws Exception {
        testReindexId(stateWithTemplate(tsdbSettings()), true);
    }

    private ClusterState stateWithTemplate(Settings.Builder settings) {
        Metadata.Builder metadata = Metadata.builder();
        Template template = new Template(settings.build(), null, null);
        if (randomBoolean()) {
            metadata.put("c", new ComponentTemplate(template, null, null));
            metadata.put(
                "c",
                ComposableIndexTemplate.builder().indexPatterns(List.of("dest_index")).componentTemplates(List.of("c")).build()
            );
        } else {
            metadata.put("c", ComposableIndexTemplate.builder().indexPatterns(List.of("dest_index")).template(template).build());
        }
        return ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();
    }

    private ClusterState stateWithIndex(Settings.Builder settings) {
        ReindexRequest reindexRequest = request();
        IndexMetadata.Builder meta = IndexMetadata.builder(reindexRequest.getDestination().index())
            .settings(settings.put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfReplicas(0)
            .numberOfShards(1);
        reindexRequest.decRef();
        return ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder(Metadata.EMPTY_METADATA).put(meta)).build();
    }

    private Settings.Builder standardSettings() {
        if (randomBoolean()) {
            return Settings.builder();
        }
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.STANDARD);
    }

    private Settings.Builder tsdbSettings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "foo");
    }

    private ScrollableHitSource.BasicHit doc() {
        return new ScrollableHitSource.BasicHit("index", "id", -1).setSource(new BytesArray("{}"), XContentType.JSON);
    }

    @Override
    protected ReindexRequest request() {
        return new ReindexRequest().setDestIndex("dest_index");
    }

    private Reindexer.AsyncIndexBySearchAction action(ClusterState state) {
        return new Reindexer.AsyncIndexBySearchAction(task, logger, null, null, threadPool, null, state, null, request(), listener());
    }

    private void testReindexId(ClusterState state, boolean nullValueExpected) {
        Reindexer.AsyncIndexBySearchAction action = action(state);
        try {
            AbstractAsyncBulkByScrollAction.RequestWrapper<IndexRequest> indexRequest = action.buildRequest(doc());
            try {
                String id = indexRequest.getId();
                if (nullValueExpected) {
                    assertThat(id, nullValue());
                } else {
                    assertThat(id, equalTo(doc().getId()));
                }
            } finally {
                indexRequest.self().decRef();
            }
        } finally {
            action.mainRequest.decRef();
        }
    }
}
