/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for the hot thread classification logic in {@link WhatIsMyClusterDoingAction}.
 */
public class WhatIsMyClusterDoingActionClassificationTests extends ESTestCase {

    // Helper method to create a mock hot thread dump with a given stack trace
    private String createHotThreadDump(double cpuPercent, String threadName, String... stackLines) {
        StringBuilder sb = new StringBuilder();
        sb.append(cpuPercent)
            .append("% [cpu=")
            .append(cpuPercent)
            .append("%, other=0.0%] cpu usage by thread '")
            .append(threadName)
            .append("'\n");
        for (String line : stackLines) {
            sb.append("       ").append(line).append("\n");
        }
        sb.append("\n");
        return sb.toString();
    }

    private WhatIsMyClusterDoingAction.LocalAction.DistilledHotThread getFirstThread(String threadDump) {
        List<WhatIsMyClusterDoingAction.LocalAction.DistilledHotThread> threads =
            WhatIsMyClusterDoingAction.LocalAction.distillHotThreadsForSingleNode(false, threadDump);
        assertThat(threads, hasSize(1));
        return threads.get(0);
    }

    // ========== LOGGING Classification Tests ==========

    public void testLoggingClassification_AuditTrail() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][write][T#1]",
            "org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail.log(LoggingAuditTrail.java:100)",
            "org.elasticsearch.action.bulk.TransportShardBulkAction.execute(TransportShardBulkAction.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("logging"));
        assertThat(thread.activities().toString(), containsString("writing audit logs"));
    }

    // ========== AUTH Classification Tests ==========

    public void testAuthClassification_RBACEngine() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][transport_worker][T#1]",
            "org.elasticsearch.xpack.security.authz.RBACEngine.authorize(RBACEngine.java:100)",
            "org.elasticsearch.transport.TransportService.handleRequest(TransportService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("auth"));
        assertThat(thread.activities().toString(), containsString("authentication"));
    }

    // ========== PIPELINES Classification Tests ==========

    public void testPipelinesClassification_IngestService() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][write][T#1]",
            "org.elasticsearch.ingest.IngestService.executePipeline(IngestService.java:100)",
            "org.elasticsearch.action.bulk.TransportBulkAction.execute(TransportBulkAction.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("pipelines"));
        assertThat(thread.activities().toString(), containsString("ingesting data through pipelines"));
    }

    public void testPipelinesClassification_GrokProcessor() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][write][T#1]",
            "org.elasticsearch.ingest.common.GrokProcessor.execute(GrokProcessor.java:100)",
            "org.elasticsearch.ingest.IngestService.executePipeline(IngestService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("pipelines"));
        assertThat(thread.activities().toString(), containsString("running grok processor"));
    }

    public void testPipelinesClassification_ScriptProcessor() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][write][T#1]",
            "org.elasticsearch.ingest.common.ScriptProcessor.execute(ScriptProcessor.java:100)",
            "org.elasticsearch.ingest.IngestService.executePipeline(IngestService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("pipelines"));
        assertThat(thread.activities().toString(), containsString("running script processor"));
    }

    // Note: The [write] thread classification is a fallback that only triggers when
    // reasons is empty and the stack element contains "[write]". This is difficult to
    // test in isolation because any elastic/lucene/netty stack frame would need to be
    // processed first, and unmatched frames add "unknown" to reasons.
    // In practice, [write] threads usually have IngestService, TransportShardBulkAction,
    // or other identifiable patterns in their stack.

    // ========== INDEXING Classification Tests ==========

    public void testIndexingClassification_TransportShardBulkAction() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][write][T#1]",
            "org.elasticsearch.action.bulk.TransportShardBulkAction.performOnPrimary(TransportShardBulkAction.java:100)",
            "org.elasticsearch.action.support.replication.TransportReplicationAction.execute(TransportReplicationAction.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("indexing"));
        assertThat(thread.activities().toString(), containsString("indexing data into indices"));
    }

    public void testIndexingClassification_RestBulkAction() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][http_worker][T#1]",
            "org.elasticsearch.rest.action.document.RestBulkAction.prepareRequest(RestBulkAction.java:100)",
            "org.elasticsearch.rest.RestController.dispatchRequest(RestController.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("indexing"));
        assertThat(thread.activities().toString(), containsString("reading rest request to index data"));
    }

    // ========== SEARCH Classification Tests ==========

    public void testSearchClassification_SearchService() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][search][T#1]",
            "org.elasticsearch.search.SearchService.executeQueryPhase(SearchService.java:100)",
            "org.elasticsearch.action.search.SearchTransportService.handleRequest(SearchTransportService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("search"));
        assertThat(thread.activities().toString(), containsString("executing search requests"));
    }

    public void testSearchClassification_ContextIndexSearcher() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][search][T#1]",
            "org.elasticsearch.search.internal.ContextIndexSearcher.search(ContextIndexSearcher.java:100)",
            "org.elasticsearch.search.SearchService.executeQueryPhase(SearchService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("search"));
        assertThat(thread.activities().toString(), containsString("executing search requests"));
    }

    public void testSearchClassification_TransportSearchAction() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][search][T#1]",
            "org.elasticsearch.action.search.TransportSearchAction.execute(TransportSearchAction.java:100)",
            "org.elasticsearch.transport.TransportService.handleRequest(TransportService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("search"));
        assertThat(thread.activities().toString(), containsString("executing search requests"));
    }

    public void testSearchClassification_LuceneTaskExecutor() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][search][T#1]",
            "org.apache.lucene.search.TaskExecutor.execute(TaskExecutor.java:100)",
            "org.elasticsearch.search.SearchService.executeQueryPhase(SearchService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("search"));
        assertThat(thread.activities().toString(), containsString("executing search requests"));
    }

    public void testSearchClassification_ESQL_ComputeOperator() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][esql_worker][T#1]",
            "org.elasticsearch.compute.operator.Driver.run(Driver.java:100)",
            "org.elasticsearch.compute.operator.DriverScheduler.execute(DriverScheduler.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("search"));
        assertThat(thread.activities().toString(), containsString("executing ES|QL query"));
    }

    public void testSearchClassification_ESQL_LuceneOperator() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][esql_worker][T#1]",
            "org.elasticsearch.compute.lucene.LuceneSourceOperator.getOutput(LuceneSourceOperator.java:100)",
            "org.elasticsearch.compute.operator.Driver.run(Driver.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("search"));
        assertThat(thread.activities().toString(), containsString("executing ES|QL query"));
    }

    public void testSearchClassification_SearchThreadName() {
        // Tests the fallback that classifies based on thread name containing "[search"
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][search][T#1]",
            "org.elasticsearch.common.util.concurrent.AbstractRunnable.run(AbstractRunnable.java:27)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("search"));
    }

    // ========== ENRICH Classification Tests ==========

    public void testEnrichClassification_EnrichProcessorFactory() {
        // Note: The enrich classification is checked early in the if-else chain
        // so we need a stack that matches EnrichProcessorFactory but not earlier patterns
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][generic][T#1]",
            "org.elasticsearch.xpack.enrich.EnrichProcessorFactory.create(EnrichProcessorFactory.java:100)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("enrich"));
        assertThat(thread.activities().toString(), containsString("enriching data"));
    }

    // ========== ML Classification Tests ==========

    public void testMLClassification_MachineLearning() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][ml][T#1]",
            "org.elasticsearch.xpack.ml.inference.InferenceRunner.run(InferenceRunner.java:100)",
            "org.elasticsearch.common.util.concurrent.AbstractRunnable.run(AbstractRunnable.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("ml"));
        assertThat(thread.activities().toString(), containsString("machine learning"));
    }

    public void testMLClassification_Inference() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][inference_utility][T#1]",
            "org.elasticsearch.xpack.inference.external.http.sender.RequestExecutorService.handleTasks(RequestExecutorService.java:100)",
            "org.elasticsearch.common.util.concurrent.AbstractRunnable.run(AbstractRunnable.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("ml"));
        assertThat(thread.activities().toString(), containsString("inference"));
    }

    public void testMLClassification_Transforms() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][transform][T#1]",
            "org.elasticsearch.xpack.transform.transforms.TransformIndexer.run(TransformIndexer.java:100)",
            "org.elasticsearch.common.util.concurrent.AbstractRunnable.run(AbstractRunnable.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("ml"));
        assertThat(thread.activities().toString(), containsString("transforms"));
    }

    // ========== WATCHER Classification Tests ==========

    public void testWatcherClassification_XpackWatcher() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][watcher][T#1]",
            "org.elasticsearch.xpack.watcher.execution.WatchExecutionService.execute(WatchExecutionService.java:100)",
            "org.elasticsearch.common.util.concurrent.AbstractRunnable.run(AbstractRunnable.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("watcher"));
        assertThat(thread.activities().toString(), containsString("watcher"));
    }

    public void testWatcherClassification_TickerScheduleTriggerEngine() {
        String threadDump = createHotThreadDump(
            50.0,
            "ticker-schedule-trigger-engine",
            "org.elasticsearch.xpack.watcher.trigger.schedule.engine.TickerScheduleTriggerEngine$Ticker.run(Ticker.java:100)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("watcher"));
        assertThat(thread.activities().toString(), containsString("watcher"));
    }

    // ========== SEARCHABLE_SNAPSHOTS Classification Tests ==========

    public void testSearchableSnapshotsClassification_BlobCache() {
        // Don't include SearchService in the stack as SEARCH has higher priority
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][generic][T#1]",
            "org.elasticsearch.blobcache.shared.SharedBlobCacheService.read(SharedBlobCacheService.java:100)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("searchable_snapshots"));
        assertThat(thread.activities().toString(), containsString("reading from searchable snapshot"));
    }

    public void testSearchableSnapshotsClassification_Stateless() {
        // Don't include SearchService in the stack as SEARCH has higher priority
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][generic][T#1]",
            "co.elastic.elasticsearch.stateless.cache.reader.CacheFileReader.read(CacheFileReader.java:100)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("searchable_snapshots"));
        assertThat(thread.activities().toString(), containsString("reading from searchable snapshot"));
    }

    public void testSearchableSnapshotsClassification_S3RetryingInputStream() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][generic][T#1]",
            "org.elasticsearch.repositories.s3.S3RetryingInputStream.read(S3RetryingInputStream.java:100)",
            "org.elasticsearch.blobcache.shared.SharedBytes.copyToCacheFileAligned(SharedBytes.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("searchable_snapshots"));
        assertThat(thread.activities().toString(), containsString("reading from searchable snapshot"));
    }

    public void testSearchableSnapshotsClassification_S3BlobContainer() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][generic][T#1]",
            "org.elasticsearch.repositories.s3.S3BlobContainer.readBlob(S3BlobContainer.java:100)",
            "org.elasticsearch.blobcache.shared.SharedBlobCacheService.read(SharedBlobCacheService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("searchable_snapshots"));
        assertThat(thread.activities().toString(), containsString("reading from searchable snapshot"));
    }

    // ========== SYSTEM_BACKGROUND_TASKS Classification Tests ==========

    public void testSystemBackgroundTasksClassification_SegmentMerging() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][lucene_merge][T#1]",
            "org.elasticsearch.index.engine.ElasticsearchConcurrentMergeScheduler.doMerge(ElasticsearchConcurrentMergeScheduler.java:100)",
            "org.apache.lucene.index.ConcurrentMergeScheduler$MergeThread.run(ConcurrentMergeScheduler.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- merging segments"));
    }

    public void testSystemBackgroundTasksClassification_SegmentMerger() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][lucene_merge][T#1]",
            "org.apache.lucene.index.SegmentMerger.merge(SegmentMerger.java:100)",
            "org.elasticsearch.index.engine.ElasticsearchConcurrentMergeScheduler.doMerge(ElasticsearchConcurrentMergeScheduler.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- merging segments"));
    }

    public void testSystemBackgroundTasksClassification_Flush() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][flush][T#1]",
            "org.elasticsearch.index.engine.InternalEngine.flush(InternalEngine.java:100)",
            "org.elasticsearch.index.shard.IndexShard.flush(IndexShard.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- flushing data to disk"));
    }

    public void testSystemBackgroundTasksClassification_Refresh() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][refresh][T#1]",
            "org.elasticsearch.index.engine.InternalEngine.maybeRefresh(InternalEngine.java:100)",
            "org.elasticsearch.index.shard.IndexShard.refresh(IndexShard.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- refreshing indices"));
    }

    public void testSystemBackgroundTasksClassification_PeerRecovery() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][generic][T#1]",
            "org.elasticsearch.indices.recovery.PeerRecoveryTargetService.startRecovery(PeerRecoveryTargetService.java:100)",
            "org.elasticsearch.index.shard.IndexShard.startRecovery(IndexShard.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- peer recovery"));
    }

    public void testSystemBackgroundTasksClassification_Snapshotting() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][snapshot][T#1]",
            "org.elasticsearch.repositories.blobstore.ShardSnapshotTaskRunner.run(ShardSnapshotTaskRunner.java:100)",
            "org.elasticsearch.snapshots.SnapshotsService.snapshot(SnapshotsService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- snapshotting data for backup"));
    }

    public void testSystemBackgroundTasksClassification_BlobStoreRepositorySnapshotFile() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][snapshot][T#1]",
            "org.elasticsearch.repositories.blobstore.BlobStoreRepository.snapshotFile(BlobStoreRepository.java:100)",
            "org.elasticsearch.snapshots.SnapshotsService.snapshot(SnapshotsService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- snapshotting data for backup"));
    }

    public void testSystemBackgroundTasksClassification_SearchableSnapshotsCachePrewarming() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][searchable_snapshots_cache_prewarming][T#1]",
            "org.elasticsearch.xpack.searchablesnapshots.cache.CachePrewarmingService.prewarm(CachePrewarmingService.java:100)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- prewarming the cache"));
    }

    public void testSystemBackgroundTasksClassification_SearchableSnapshotsCacheFetchAsync() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][searchable_snapshots_cache_fetch_async][T#1]",
            "org.elasticsearch.xpack.searchablesnapshots.cache.CacheFetchService.fetch(CacheFetchService.java:100)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- prewarming the cache"));
    }

    public void testSystemBackgroundTasksClassification_MasterService() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][masterService][T#1]",
            "org.elasticsearch.cluster.service.MasterService.runTasks(MasterService.java:100)",
            "org.elasticsearch.cluster.service.MasterService.execute(MasterService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- master node"));
    }

    public void testSystemBackgroundTasksClassification_ShardsAllocator() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][masterService][T#1]",
            "org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator.allocate(BalancedShardsAllocator.java:100)",
            "org.elasticsearch.cluster.routing.allocation.AllocationService.reroute(AllocationService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- balancing shards"));
    }

    public void testSystemBackgroundTasksClassification_MonitoringExporter() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][monitoring][T#1]",
            "org.elasticsearch.xpack.monitoring.exporter.Exporters.export(Exporters.java:100)",
            "org.elasticsearch.xpack.monitoring.MonitoringService.doRun(MonitoringService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("exporting data for Monitoring"));
    }

    public void testSystemBackgroundTasksClassification_ClusterStats() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][management][T#1]",
            "org.elasticsearch.action.admin.cluster.stats.TransportClusterStatsAction.nodeOperation(TransportClusterStatsAction.java:100)",
            "org.elasticsearch.action.support.nodes.TransportNodesAction.handleNodeRequest(TransportNodesAction.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("gathering cluster stats"));
    }

    public void testSystemBackgroundTasksClassification_ReadinessService() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][generic][T#1]",
            "org.elasticsearch.readiness.ReadinessService.checkReady(ReadinessService.java:100)",
            "org.elasticsearch.common.util.concurrent.AbstractRunnable.run(AbstractRunnable.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("readiness service"));
    }

    public void testSystemBackgroundTasksClassification_TransportWorker() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][transport_worker][T#1]",
            "io.netty.channel.nio.NioEventLoop.processSelectedKeys(NioEventLoop.java:100)",
            "io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("TRANSPORT"));
    }

    public void testSystemBackgroundTasksClassification_DeleteFromSnapshotRepository() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][snapshot][T#1]",
            "org.elasticsearch.repositories.blobstore.BlobStoreRepository.deleteFromContainer(BlobStoreRepository.java:100)",
            "org.elasticsearch.snapshots.SnapshotsService.deleteSnapshot(SnapshotsService.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("deleting item from snapshot repository"));
    }

    public void testSystemBackgroundTasksClassification_GlobalCheckpointSyncAction() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][write][T#1]",
            "org.elasticsearch.index.seqno.GlobalCheckpointSyncAction.shardOperationOnPrimary(GlobalCheckpointSyncAction.java:100)",
            "org.elasticsearch.action.support.replication.TransportReplicationAction.execute(TransportReplicationAction.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- syncing translog for durability"));
    }

    public void testSystemBackgroundTasksClassification_TranslogSync() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][flush][T#1]",
            "org.elasticsearch.index.translog.Translog.sync(Translog.java:100)",
            "org.elasticsearch.index.engine.InternalEngine.flush(InternalEngine.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- syncing translog for durability"));
    }

    public void testSystemBackgroundTasksClassification_TranslogEnsureSynced() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][write][T#1]",
            "org.elasticsearch.index.translog.Translog.ensureSynced(Translog.java:100)",
            "org.elasticsearch.index.engine.InternalEngine.syncTranslog(InternalEngine.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- syncing translog for durability"));
    }

    public void testSystemBackgroundTasksClassification_SyncGlobalCheckpoint() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][write][T#1]",
            "org.elasticsearch.index.shard.IndexShard.syncGlobalCheckpoint(IndexShard.java:100)",
            "org.elasticsearch.index.seqno.GlobalCheckpointSyncAction.execute(GlobalCheckpointSyncAction.java:200)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("system_background_tasks"));
        assertThat(thread.activities().toString(), containsString("maintenance -- syncing translog for durability"));
    }

    // ========== UNKNOWN Classification Tests ==========

    public void testUnknownClassification_GenericThread() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][generic][T#1]",
            "org.elasticsearch.common.util.concurrent.AbstractRunnable.run(AbstractRunnable.java:27)",
            "java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)"
        );
        var thread = getFirstThread(threadDump);
        assertThat(thread.classification(), equalTo("unknown"));
    }

    public void testUnknownClassification_SslHandlerFlush() {
        String threadDump = createHotThreadDump(50.0, "elasticsearch[node1][transport_worker][T#1]",
            "io.netty.handler.ssl.SslHandler.flush(SslHandler.java:100)",
            "io.netty.channel.AbstractChannelHandlerContext.flush(AbstractChannelHandlerContext.java:200)"
        );
        var thread = getFirstThread(threadDump);
        // SslHandler.flush leads to UNKNOWN classification (explicitly marked as such)
        assertThat(thread.activities().toString(), containsString("returning potentially large response"));
    }
}

