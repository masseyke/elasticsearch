/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.ingest;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.ingest.PipelineProcessor;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

/**
 * The purpose of this test is to verify that when a processor executes an operation asynchronously that
 * the expected result is the same as if the same operation happens synchronously.
 *
 * In this test two test processor are defined that basically do the same operation, but a single processor
 * executes asynchronously. The result of the operation should be the same and also the order in which the
 * bulk responses are returned should be the same as how the corresponding index requests were defined.
 */
public class AsyncIngestProcessorIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(TestPlugin.class);
    }

    public void testAsyncProcessorImplementation() throws IOException {
        String innerInnerPipeline = """
            {
                "processors": [
                    {
                        "test-async3": {
                            "description": "test-async3-in-innerInner"
                        }
                    }
                ]
            }
            """;
        BytesReference innerInnerPipelineReference = new BytesArray(innerInnerPipeline);
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("innerInnerPipeline", innerInnerPipelineReference, XContentType.JSON))
            .actionGet();
        String innerPipeline = """
            {
                "processors": [
                    {
                        "test-async3": {
                            "description": "test-async3-in-inner"
                        },
                        "pipeline": {
                            "name": "innerInnerPipeline",
                            "description": "innerInnerPipeline-in-inner"
                        }
                    }
                ]
            }
            """;
        BytesReference innerPipelineReference = new BytesArray(innerPipeline);
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("innerPipeline", innerPipelineReference, XContentType.JSON))
            .actionGet();
        String outerPipeline = """
            {
                "processors": [
                    {
                        "pipeline": {
                            "name": "innerPipeline",
                            "description": "innerPipeline-in-outer"
                        }
                    },
                    {
                        "test-async3": {
                            "description": "test-async3-in-outer"
                        },
                        "pipeline": {
                            "name": "innerInnerPipeline",
                            "description": "innerInnerPipeline-in-outer"
                        }
                    }
                ]
            }
            """;
        BytesReference outerPipelineReference = new BytesArray(outerPipeline);
        client().admin()
            .cluster()
            .putPipeline(new PutPipelineRequest("outerPipeline", outerPipelineReference, XContentType.JSON))
            .actionGet();

        BulkRequest bulkRequest = new BulkRequest();
        int numDocs = randomIntBetween(100, 100);
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(
                new IndexRequest("foobar").id(Integer.toString(i)).source("{}", XContentType.JSON).setPipeline("outerPipeline")
            );
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            assertThat(bulkResponse.getItems()[i].getId(), equalTo(id));
        }
        NodesStatsResponse nodesStatsResponse = client().admin()
            .cluster()
            .nodesStats(new NodesStatsRequest().addMetric("ingest"))
            .actionGet();
        IngestStats ingestStats = nodesStatsResponse.getNodes().get(0).getIngestStats();
        Map<String, Object> ingestStatsMap = xContentToMap(ingestStats);
        assertNotNull(ingestStats);
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.yamlBuilder().prettyPrint();
        builder.startObject();
        xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        XContentParser parser = XContentType.YAML.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        System.out.println(new String(((ByteArrayOutputStream) builder.getOutputStream()).toByteArray(), "UTF-8"));
        return parser.map();
    }

    public static class TestPlugin extends Plugin implements IngestPlugin {

        private ThreadPool threadPool;

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver expressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier,
            Tracer tracer,
            AllocationDeciders allocationDeciders
        ) {
            this.threadPool = threadPool;
            return List.of();
        }

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> procMap = new HashMap<>();
            for (int i = 1; i < 8; i++) {
                final int finalI = i;
                procMap.put("test-async" + i, (factories, tag, description, config) -> new AbstractProcessor(tag, description) {

                    @Override
                    public void execute(IngestDocument ingestDocument, String context, BiConsumer<IngestDocument, Exception> handler) {
                        threadPool.generic().execute(() -> {
                            String id = (String) ingestDocument.getSourceAndMetadata().get("_id");
                            if (usually()) {
                                try {
                                    Thread.sleep(10);
                                } catch (InterruptedException e) {
                                    // ignore
                                }
                            }
                            ingestDocument.setFieldValue(randomAlphaOfLength(5), "bar-" + id);
                            handler.accept(ingestDocument, null);
                        });
                    }

                    @Override
                    public String getType() {
                        return "test-async" + finalI;
                    }

                    @Override
                    public boolean isAsync() {
                        return true;
                    }

                });
                procMap.put("test" + i, (processorFactories, tag, description, config) -> new AbstractProcessor(tag, description) {
                    @Override
                    public IngestDocument execute(IngestDocument ingestDocument, String context) throws Exception {
                        String id = (String) ingestDocument.getSourceAndMetadata().get("_id");
                        ingestDocument.setFieldValue(randomAlphaOfLength(5), "baz-" + id);
                        return ingestDocument;
                    }

                    @Override
                    public String getType() {
                        return "test" + finalI;
                    }
                });
            }
            Processor.Factory pipelineFactory1 = new PipelineProcessor.Factory(parameters.ingestService);
            procMap.put("pipeline", pipelineFactory1);
            return procMap;
        }
    }
}
