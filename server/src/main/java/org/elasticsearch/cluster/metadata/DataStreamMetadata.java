/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Custom {@link Metadata} implementation for storing a map of {@link DataStream}s and their names.
 */
public class DataStreamMetadata implements Metadata.Custom {
    private static final Logger logger = LogManager.getLogger(DataStreamMetadata.class);

    public static final String TYPE = "data_stream";

    public static final DataStreamMetadata EMPTY = new DataStreamMetadata(ImmutableOpenMap.of(), ImmutableOpenMap.of());
    private static final ParseField DATA_STREAM = new ParseField("data_stream");
    private static final ParseField DATA_STREAM_ALIASES = new ParseField("data_stream_aliases");
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStreamMetadata, Void> PARSER = new ConstructingObjectParser<>(TYPE, false, args -> {
        ImmutableOpenMap<String, DataStream> dataStreams = (ImmutableOpenMap<String, DataStream>) args[0];
        ImmutableOpenMap<String, DataStreamAlias> dataStreamAliases = (ImmutableOpenMap<String, DataStreamAlias>) args[1];
        if (dataStreamAliases == null) {
            dataStreamAliases = ImmutableOpenMap.of();
        }
        return new DataStreamMetadata(dataStreams, dataStreamAliases);
    });

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            ImmutableOpenMap.Builder<String, DataStream> dataStreams = ImmutableOpenMap.builder();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                String name = p.currentName();
                dataStreams.put(name, DataStream.fromXContent(p));
            }
            return dataStreams.build();
        }, DATA_STREAM);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            ImmutableOpenMap.Builder<String, DataStreamAlias> dataStreams = ImmutableOpenMap.builder();
            while (p.nextToken() != XContentParser.Token.END_OBJECT) {
                DataStreamAlias alias = DataStreamAlias.fromXContent(p);
                dataStreams.put(alias.getName(), alias);
            }
            return dataStreams.build();
        }, DATA_STREAM_ALIASES);
    }

    private final ImmutableOpenMap<String, DataStream> dataStreams;
    private final ImmutableOpenMap<String, DataStreamAlias> dataStreamAliases;

    public DataStreamMetadata(
        ImmutableOpenMap<String, DataStream> dataStreams,
        ImmutableOpenMap<String, DataStreamAlias> dataStreamAliases
    ) {
        this.dataStreams = dataStreams;
        this.dataStreamAliases = dataStreamAliases;
    }

    public DataStreamMetadata(StreamInput in) throws IOException {
        this(
            in.readImmutableOpenMap(StreamInput::readString, DataStream::new),
            in.readImmutableOpenMap(StreamInput::readString, DataStreamAlias::new)
        );
    }

    public DataStreamMetadata withAddedDatastream(DataStream datastream) {
        final String name = datastream.getName();
        final DataStream existing = dataStreams.get(name);
        if (datastream.equals(existing)) {
            return this;
        }
        return new DataStreamMetadata(ImmutableOpenMap.builder(dataStreams).fPut(name, datastream).build(), dataStreamAliases);
    }

    public DataStreamMetadata withAlias(String aliasName, String dataStreamName, Boolean isWriteDataStream, String filter) {
        if (dataStreams.containsKey(dataStreamName) == false) {
            throw new IllegalArgumentException("alias [" + aliasName + "] refers to a non existing data stream [" + dataStreamName + "]");
        }

        Map<String, Object> filterAsMap;
        if (filter != null) {
            filterAsMap = XContentHelper.convertToMap(XContentFactory.xContent(filter), filter, true);
        } else {
            filterAsMap = null;
        }

        // First, update the alias with the data stream:
        DataStreamAlias alias = dataStreamAliases.get(aliasName);
        /*
         * Note that we put the filterAsMap into this DataStreamAlias even though we're putting it in the top-level aliases map, and the
         * filter is only meaningful within the context of a DataStream (since this DataStreamAlias might point to many DataStreams, each
         * with its own filter). We could use a different DataStreamAlias object for this top-level map and for the alias added to the
         * DataStream below. But we set the filter here on this top-level DataStreamAlias as a way to let the system know that the
         * aliases within the cluster state have changed without having to loop through all DataStreams. See Metadata#equalsAliases.
         */
        if (alias == null) {
            String writeDataStream = isWriteDataStream != null && isWriteDataStream ? dataStreamName : null;
            alias = new DataStreamAlias(aliasName, List.of(dataStreamName), writeDataStream, filterAsMap);
        } else {
            DataStreamAlias copy = alias.update(dataStreamName, isWriteDataStream, filterAsMap);
            if (copy == alias) {
                return this;
            }
            alias = copy;
        }
        ImmutableOpenMap<String, DataStreamAlias> updatedDataStreamAliases = ImmutableOpenMap.builder(dataStreamAliases)
            .fPut(aliasName, alias)
            .build();

        // Next, update the data stream with the alias:
        DataStream originalDataStream = dataStreams.get(dataStreamName);
        DataStream newDataStream = originalDataStream.addAlias(alias);
        ImmutableOpenMap<String, DataStream> updatedDataStreams = new ImmutableOpenMap.Builder<String, DataStream>().putAllFromMap(
            dataStreams
        ).fPut(dataStreamName, newDataStream).build();

        return new DataStreamMetadata(updatedDataStreams, updatedDataStreamAliases);
    }

    public DataStreamMetadata withRemovedDataStream(String name) {
        ImmutableOpenMap.Builder<String, DataStream> existingDataStreams = ImmutableOpenMap.builder(dataStreams);
        ImmutableOpenMap.Builder<String, DataStreamAlias> existingDataStreamAliases = ImmutableOpenMap.builder(dataStreamAliases);
        existingDataStreams.remove(name);

        Set<String> aliasesToDelete = new HashSet<>();
        List<DataStreamAlias> aliasesToUpdate = new ArrayList<>();
        for (var alias : dataStreamAliases.values()) {
            DataStreamAlias copy = alias.removeDataStream(name);
            if (copy != null) {
                if (copy == alias) {
                    continue;
                }
                aliasesToUpdate.add(copy);
            } else {
                aliasesToDelete.add(alias.getName());
            }
        }
        for (DataStreamAlias alias : aliasesToUpdate) {
            existingDataStreamAliases.put(alias.getName(), alias);
        }
        for (String aliasToDelete : aliasesToDelete) {
            existingDataStreamAliases.remove(aliasToDelete);
        }
        return new DataStreamMetadata(existingDataStreams.build(), existingDataStreamAliases.build());
    }

    public DataStreamMetadata withRemovedAlias(String aliasName, String dataStreamName, boolean mustExist) {
        ImmutableOpenMap.Builder<String, DataStreamAlias> dataStreamAliases = ImmutableOpenMap.builder(this.dataStreamAliases);

        DataStreamAlias existing = dataStreamAliases.get(aliasName);
        if (mustExist && existing == null) {
            throw new ResourceNotFoundException("alias [" + aliasName + "] doesn't exist");
        } else if (existing == null) {
            return this;
        }
        DataStreamAlias copy = existing.removeDataStream(dataStreamName);
        if (copy == existing) {
            return this;
        }
        if (copy != null) {
            dataStreamAliases.put(aliasName, copy);
        } else {
            dataStreamAliases.remove(aliasName);
        }
        return new DataStreamMetadata(dataStreams, dataStreamAliases.build());
    }

    public Map<String, DataStream> dataStreams() {
        return this.dataStreams;
    }

    public Map<String, DataStreamAlias> getDataStreamAliases() {
        return dataStreamAliases;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new DataStreamMetadata.DataStreamMetadataDiff((DataStreamMetadata) before, this);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new DataStreamMetadata.DataStreamMetadataDiff(in);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public boolean isRestorable() {
        // this metadata is written to the snapshot, however it uses custom logic for restoring
        return false;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_7_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.dataStreams, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
        out.writeMap(this.dataStreamAliases, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
    }

    public static DataStreamMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return Iterators.concat(
            ChunkedToXContentHelper.xContentValuesMap(DATA_STREAM.getPreferredName(), dataStreams),
            ChunkedToXContentHelper.startObject(DATA_STREAM_ALIASES.getPreferredName()),
            dataStreamAliases.values().iterator(),
            ChunkedToXContentHelper.endObject()
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.dataStreams, dataStreamAliases);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        DataStreamMetadata other = (DataStreamMetadata) obj;
        return Objects.equals(this.dataStreams, other.dataStreams) && Objects.equals(this.dataStreamAliases, other.dataStreamAliases);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    static class DataStreamMetadataDiff implements NamedDiff<Metadata.Custom> {

        private static final DiffableUtils.DiffableValueReader<String, DataStream> DS_DIFF_READER = new DiffableUtils.DiffableValueReader<>(
            DataStream::new,
            DataStream::readDiffFrom
        );

        private static final DiffableUtils.DiffableValueReader<String, DataStreamAlias> ALIAS_DIFF_READER =
            new DiffableUtils.DiffableValueReader<>(DataStreamAlias::new, DataStreamAlias::readDiffFrom);

        final DiffableUtils.MapDiff<String, DataStream, ImmutableOpenMap<String, DataStream>> dataStreamDiff;
        final DiffableUtils.MapDiff<String, DataStreamAlias, ImmutableOpenMap<String, DataStreamAlias>> dataStreamAliasDiff;

        DataStreamMetadataDiff(DataStreamMetadata before, DataStreamMetadata after) {
            this.dataStreamDiff = DiffableUtils.diff(before.dataStreams, after.dataStreams, DiffableUtils.getStringKeySerializer());
            this.dataStreamAliasDiff = DiffableUtils.diff(
                before.dataStreamAliases,
                after.dataStreamAliases,
                DiffableUtils.getStringKeySerializer()
            );
        }

        DataStreamMetadataDiff(StreamInput in) throws IOException {
            this.dataStreamDiff = DiffableUtils.readImmutableOpenMapDiff(in, DiffableUtils.getStringKeySerializer(), DS_DIFF_READER);
            this.dataStreamAliasDiff = DiffableUtils.readImmutableOpenMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                ALIAS_DIFF_READER
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new DataStreamMetadata(
                dataStreamDiff.apply(((DataStreamMetadata) part).dataStreams),
                dataStreamAliasDiff.apply(((DataStreamMetadata) part).dataStreamAliases)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            dataStreamDiff.writeTo(out);
            dataStreamAliasDiff.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_7_7_0;
        }
    }
}
