/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class IngestStats implements Writeable, ToXContentFragment {
    private final Stats totalStats;
    private final List<PipelineStat> pipelineStats;
    private final Map<String, List<ProcessorStat>> processorStats;

    /**
     * @param totalStats - The total stats for Ingest. This is the logically the sum of all pipeline stats,
     *                   and pipeline stats are logically the sum of the processor stats.
     * @param pipelineStats - The stats for a given ingest pipeline.
     * @param processorStats - The per-processor stats for a given pipeline. A map keyed by the pipeline identifier.
     */
    public IngestStats(Stats totalStats, List<PipelineStat> pipelineStats, Map<String, List<ProcessorStat>> processorStats) {
        this.totalStats = totalStats;
        this.pipelineStats = pipelineStats.stream().sorted((p1, p2) -> {
            final IngestStats.Stats p2Stats = p2.stats;
            final IngestStats.Stats p1Stats = p1.stats;
            final int ingestTimeCompare = Long.compare(p2Stats.ingestTimeInMillis, p1Stats.ingestTimeInMillis);
            if (ingestTimeCompare == 0) {
                return Long.compare(p2Stats.ingestCount, p1Stats.ingestCount);
            } else {
                return ingestTimeCompare;
            }
        }).toList();
        this.processorStats = processorStats;

    }

    /**
     * Read from a stream.
     */
    public IngestStats(StreamInput in) throws IOException {
        this.totalStats = new Stats(in);
        int size = in.readVInt();
        this.pipelineStats = new ArrayList<>(size);
        this.processorStats = Maps.newMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            String pipelineId = in.readString();
            Stats pipelineStat = new Stats(in);
            this.pipelineStats.add(new PipelineStat(pipelineId, pipelineStat));
            int processorsSize = in.readVInt();
            List<ProcessorStat> processorStatsPerPipeline = new ArrayList<>(processorsSize);
            for (int j = 0; j < processorsSize; j++) {
                String processorName = in.readString();
                String processorType = in.readString();
                Stats processorStat = new Stats(in);
                processorStatsPerPipeline.add(new ProcessorStat(processorName, processorType, processorStat));
            }
            this.processorStats.put(pipelineId, processorStatsPerPipeline);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        out.writeVInt(pipelineStats.size());
        for (PipelineStat pipelineStat : pipelineStats) {
            out.writeString(pipelineStat.getPipelineId());
            pipelineStat.getStats().writeTo(out);
            List<ProcessorStat> processorStatsForPipeline = processorStats.get(pipelineStat.getPipelineId());
            if (processorStatsForPipeline == null) {
                out.writeVInt(0);
            } else {
                out.writeCollection(processorStatsForPipeline, (o, processorStat) -> {
                    o.writeString(processorStat.getName());
                    o.writeString(processorStat.getType());
                    processorStat.getStats().writeTo(o);
                });
            }
        }
    }

    public XContentBuilder toXContentUnnested(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("ingest");
        builder.startObject("total");
        totalStats.toXContent(builder, params);
        builder.endObject();
        builder.startObject("pipelines");
        for (PipelineStat pipelineStat : pipelineStats) {
            builder.startObject(pipelineStat.getPipelineId());
            pipelineStat.getStats().toXContent(builder, params);
            List<ProcessorStat> processorStatsForPipeline = processorStats.get(pipelineStat.getPipelineId());
            builder.startArray("processors");
            if (processorStatsForPipeline != null) {
                for (ProcessorStat processorStat : processorStatsForPipeline) {
                    builder.startObject();
                    builder.startObject(processorStat.getName());
                    builder.field("type", processorStat.getType());
                    builder.startObject("stats");
                    processorStat.getStats().toXContent(builder, params);
                    builder.endObject();
                    builder.endObject();
                    builder.endObject();
                }
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (builder.getRestApiVersion().matches(RestApiVersion.onOrAfter(RestApiVersion.V_8))) {
            builder.startObject("ingest");
            builder.startObject("total");
            totalStats.toXContent(builder, params);
            builder.endObject();
            builder.startObject("pipelines");
            String pipelineProcessorPrefix = PipelineProcessor.TYPE + ":";
            Set<String> topLevelPipelineNames = findTopLevelPipelineNames(processorStats, pipelineProcessorPrefix);
            Map<String, PipelineStat> pipelineStatMap = pipelineStats.stream()
                .collect(
                    Collectors.toMap(pipelineStat -> pipelineProcessorPrefix + pipelineStat.getPipelineId(), pipelineStat -> pipelineStat)
                );
            for (PipelineStat pipelineStat : pipelineStats) {
                try {
                    builder.startObject(pipelineStat.getPipelineId());
                    pipelineStat.getStats().toXContent(builder, params);
                    if (topLevelPipelineNames.contains(pipelineStat.getPipelineId())) {
                        processorsToXContent(pipelineStat.getPipelineId(), pipelineStat, processorStats, pipelineStatMap, builder, params);
                    }
                    builder.endObject();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            builder.endObject();
            builder.endObject();
            return builder;
        } else {
            return toXContentUnnested(builder, params);
        }
    }

    private Set<String> findTopLevelPipelineNames(Map<String, List<ProcessorStat>> processorStats, String pipelineProcessorPrefix) {
        Set<String> allPipelines = processorStats.keySet();
        Set<String> topLevelPipelineNames = new HashSet<>(allPipelines);
        Set<String> nestedPipelines = processorStats.values()
            .stream()
            .flatMap(Collection::stream)
            .filter(processorStat -> processorStat.name != null && processorStat.name.startsWith(pipelineProcessorPrefix))
            .map(processorStat -> processorStat.name.substring(pipelineProcessorPrefix.length()))
            .collect(Collectors.toSet());
        topLevelPipelineNames.removeAll(nestedPipelines);
        return topLevelPipelineNames;
    }

    private static void processorsToXContent(
        String context,
        PipelineStat pipelineStat,
        Map<String, List<ProcessorStat>> processorStats,
        Map<String, PipelineStat> pipelineStatMap,
        XContentBuilder builder,
        Params params
    ) throws IOException {

        List<ProcessorStat> processorStatsForPipeline = processorStats.get(context);
        builder.startArray("processors");
        if (processorStatsForPipeline != null) {
            for (ProcessorStat processorStat : processorStatsForPipeline) {
                builder.startObject();
                builder.startObject(processorStat.getName());
                builder.field("type", processorStat.getType());
                builder.startObject("stats");
                processorStat.getStats().toXContent(builder, params);
                if (processorStat.getType().equals(PipelineProcessor.TYPE)) {
                    processorsToXContent(
                        context + ":" + processorStat.getName().substring(PipelineProcessor.TYPE.length() + 1),
                        pipelineStatMap.get(processorStat.getName()),
                        processorStats,
                        pipelineStatMap,
                        builder,
                        params
                    );
                }
                builder.endObject();
                builder.endObject();
                builder.endObject();
            }
        }
        builder.endArray();
    }

    public Stats getTotalStats() {
        return totalStats;
    }

    public List<PipelineStat> getPipelineStats() {
        return pipelineStats;
    }

    public Map<String, List<ProcessorStat>> getProcessorStats() {
        return processorStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IngestStats that = (IngestStats) o;
        return Objects.equals(totalStats, that.totalStats)
            && Objects.equals(pipelineStats, that.pipelineStats)
            && Objects.equals(processorStats, that.processorStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalStats, pipelineStats, processorStats);
    }

    public static class Stats implements Writeable, ToXContentFragment {

        private final long ingestCount;
        private final long ingestTimeInMillis;
        private final long ingestCurrent;
        private final long ingestFailedCount;

        public Stats(long ingestCount, long ingestTimeInMillis, long ingestCurrent, long ingestFailedCount) {
            this.ingestCount = ingestCount;
            this.ingestTimeInMillis = ingestTimeInMillis;
            this.ingestCurrent = ingestCurrent;
            this.ingestFailedCount = ingestFailedCount;
        }

        /**
         * Read from a stream.
         */
        public Stats(StreamInput in) throws IOException {
            ingestCount = in.readVLong();
            ingestTimeInMillis = in.readVLong();
            ingestCurrent = in.readVLong();
            ingestFailedCount = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(ingestCount);
            out.writeVLong(ingestTimeInMillis);
            out.writeVLong(ingestCurrent);
            out.writeVLong(ingestFailedCount);
        }

        /**
         * @return The total number of executed ingest preprocessing operations.
         */
        public long getIngestCount() {
            return ingestCount;
        }

        /**
         * @return The total time spent of ingest preprocessing in millis.
         */
        public long getIngestTimeInMillis() {
            return ingestTimeInMillis;
        }

        /**
         * @return The total number of ingest preprocessing operations currently executing.
         */
        public long getIngestCurrent() {
            return ingestCurrent;
        }

        /**
         * @return The total number of ingest preprocessing operations that have failed.
         */
        public long getIngestFailedCount() {
            return ingestFailedCount;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("count", ingestCount);
            builder.humanReadableField("time_in_millis", "time", new TimeValue(ingestTimeInMillis, TimeUnit.MILLISECONDS));
            builder.field("current", ingestCurrent);
            builder.field("failed", ingestFailedCount);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IngestStats.Stats that = (IngestStats.Stats) o;
            return Objects.equals(ingestCount, that.ingestCount)
                && Objects.equals(ingestTimeInMillis, that.ingestTimeInMillis)
                && Objects.equals(ingestFailedCount, that.ingestFailedCount)
                && Objects.equals(ingestCurrent, that.ingestCurrent);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ingestCount, ingestTimeInMillis, ingestFailedCount, ingestCurrent);
        }
    }

    /**
     * Easy conversion from scoped {@link IngestMetric} objects to a serializable Stats objects
     */
    static class Builder {
        private Stats totalStats;
        private List<PipelineStat> pipelineStats = new ArrayList<>();
        private Map<String, List<ProcessorStat>> processorStats = new HashMap<>();

        Builder addTotalMetrics(IngestMetric totalMetric) {
            this.totalStats = totalMetric.createStats();
            return this;
        }

        Builder addPipelineMetrics(String pipelineId, IngestMetric pipelineMetric) {
            this.pipelineStats.add(new PipelineStat(pipelineId, pipelineMetric.createStats()));
            return this;
        }

        Builder addProcessorMetrics(String pipelineId, String processorName, String processorType, IngestMetric metric) {
            this.processorStats.computeIfAbsent(pipelineId, k -> new ArrayList<>())
                .add(new ProcessorStat(processorName, processorType, metric.createStats()));
            return this;
        }

        IngestStats build() {
            return new IngestStats(totalStats, Collections.unmodifiableList(pipelineStats), Collections.unmodifiableMap(processorStats));
        }
    }

    /**
     * Container for pipeline stats.
     */
    public static class PipelineStat {
        private final String pipelineId;
        private final Stats stats;

        public PipelineStat(String pipelineId, Stats stats) {
            this.pipelineId = pipelineId;
            this.stats = stats;
        }

        public String getPipelineId() {
            return pipelineId;
        }

        public Stats getStats() {
            return stats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IngestStats.PipelineStat that = (IngestStats.PipelineStat) o;
            return Objects.equals(pipelineId, that.pipelineId) && Objects.equals(stats, that.stats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pipelineId, stats);
        }
    }

    /**
     * Container for processor stats.
     */
    public static class ProcessorStat {
        private final String name;
        private final String type;
        private final Stats stats;

        public ProcessorStat(String name, String type, Stats stats) {
            this.name = name;
            this.type = type;
            this.stats = stats;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public Stats getStats() {
            return stats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IngestStats.ProcessorStat that = (IngestStats.ProcessorStat) o;
            return Objects.equals(name, that.name) && Objects.equals(type, that.type) && Objects.equals(stats, that.stats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type, stats);
        }
    }
}
