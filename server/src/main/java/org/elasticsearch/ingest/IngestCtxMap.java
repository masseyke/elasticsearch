/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.CtxMap;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Map containing ingest source and metadata.
 * <p>
 * The Metadata values in {@link IngestDocument.Metadata} are validated when put in the map:
 * <ul>
 *   <li>{@code _index}, {@code _id} and {@code _routing} must be a String or null</li>
 *   <li>{@code _version_type} must be a lower case VersionType or null</li>
 *   <li>{@code _version} must be representable as a long without loss of precision or null</li>
 *   <li>{@code _dynamic_templates} must be a map</li>
 *   <li>{@code _if_seq_no} must be a long or null</li>
 *   <li>{@code _if_primary_term} must be a long or null</li>
 * </ul>
 * <p>
 * The map is expected to be used by processors, server code should the typed getter and setters where possible.
 */
final class IngestCtxMap extends CtxMap<IngestDocMetadata> {

    /**
     * Create an IngestCtxMap with the given metadata, source and default validators
     * <p>
     * The passed-in source map is used directly (that is, it's neither shallowly nor deeply copied). mutation-like methods (e.g. setters,
     * put, etc.) may rely on the map being mutable, and will fail if the passed-in map isn't mutable.
     */
    IngestCtxMap(
        String index,
        String id,
        long version,
        String routing,
        VersionType versionType,
        ZonedDateTime timestamp,
        Map<String, Object> source
    ) {
        super(source, new IngestDocMetadata(index, id, version, routing, versionType, timestamp));
    }

    /**
     * Create IngestCtxMap from a source and metadata
     *
     * @param source the source document map
     * @param metadata the metadata map
     */
    IngestCtxMap(Map<String, Object> source, IngestDocMetadata metadata) {
        super(source, metadata);
    }

    // Running total of the estimated bytes written into this document's fields, across its whole ingest lifecycle (declarative
    // processors via IngestDocument.setFieldValue, and Painless scripts that mutate this map directly). Checked against
    // IngestDocument.MAX_CUMULATIVE_FIELD_VALUE_BYTES.
    private long cumulativeFieldValueBytes = 0L;

    /**
     * When true (set only while a script processor is executing its Painless script), direct mutations of this map are charged
     * against the cumulative field-value size budget. Painless scripts mutate this map directly, bypassing
     * {@link IngestDocument#setFieldValue(String, Object)}, so without this they would evade the guard.
     */
    private boolean trackMutations = false;

    void setTrackMutations(boolean trackMutations) {
        this.trackMutations = trackMutations;
    }

    @Override
    public Object put(String key, Object value) {
        if (trackMutations) {
            // Charge script-driven writes against the same cumulative budget as declarative processors. Throws before the value
            // is stored if the limit is exceeded, so a runaway script can't grow the document without bound.
            trackFieldValueSize(key, value);
        }
        return super.put(key, value);
    }

    /**
     * Accumulates the estimated size of {@code value} onto this document's running total, and throws if the document has now had
     * more than {@link IngestDocument#MAX_CUMULATIVE_FIELD_VALUE_BYTES} written into it over its ingest lifecycle. This guards
     * against pipelines that duplicate an already-large field into many other fields (e.g. many processors' worth of
     * {@code copy_from}) -- no single write is large, but the cumulative effect can make the document dangerously large once it
     * is fully serialized elsewhere.
     */
    void trackFieldValueSize(String path, Object value) {
        // seenContainers is allocated lazily -- by far the most common case is a scalar (String, number, etc.) being set or
        // appended, which can never recurse or cycle, so it should never pay for an IdentityHashMap allocation.
        cumulativeFieldValueBytes += estimateSizeInBytes(value, null);
        if (cumulativeFieldValueBytes > IngestDocument.MAX_CUMULATIVE_FIELD_VALUE_BYTES) {
            throw new IllegalArgumentException(
                "failed to set field ["
                    + path
                    + "]: this document's ingest pipeline(s) have written more than ["
                    + IngestDocument.MAX_CUMULATIVE_FIELD_VALUE_BYTES
                    + "] bytes of field values into it, exceeding the limit"
            );
        }
    }

    /**
     * A rough, conservative estimate (in bytes) of the in-memory size of an arbitrary field value, recursing into the same
     * container types that {@link IngestDocument#deepCopy(Object)} handles. Precision doesn't matter much here -- this only needs
     * to catch orders-of-magnitude blowups, not track exact heap usage.
     * <p>
     * {@code seenContainers} guards against self-referencing structures (e.g. a list that contains itself). It's passed in as
     * {@code null} and only allocated on first use, since the overwhelming majority of calls are for a scalar value (a plain
     * String, number, etc.) that can never contain a cycle and so should never pay for the allocation.
     */
    private static long estimateSizeInBytes(Object value, Set<Object> seenContainers) {
        if (value instanceof Map<?, ?> mapValue) {
            if (seenContainers == null) {
                seenContainers = Collections.newSetFromMap(new IdentityHashMap<>());
            }
            if (seenContainers.add(mapValue) == false) {
                return 0L;
            }
            long size = 0;
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                size += estimateSizeInBytes(entry.getKey(), seenContainers);
                size += estimateSizeInBytes(entry.getValue(), seenContainers);
            }
            return size;
        } else if (value instanceof List<?> listValue) {
            if (seenContainers == null) {
                seenContainers = Collections.newSetFromMap(new IdentityHashMap<>());
            }
            if (seenContainers.add(listValue) == false) {
                return 0L;
            }
            long size = 0;
            for (Object itemValue : listValue) {
                size += estimateSizeInBytes(itemValue, seenContainers);
            }
            return size;
        } else if (value instanceof Set<?> setValue) {
            if (seenContainers == null) {
                seenContainers = Collections.newSetFromMap(new IdentityHashMap<>());
            }
            if (seenContainers.add(setValue) == false) {
                return 0L;
            }
            long size = 0;
            for (Object itemValue : setValue) {
                size += estimateSizeInBytes(itemValue, seenContainers);
            }
            return size;
        } else if (value instanceof byte[] bytes) {
            return bytes.length;
        } else if (value instanceof double[][] doubles) {
            long size = 0;
            for (double[] row : doubles) {
                size += (long) row.length * Double.BYTES;
            }
            return size;
        } else if (value instanceof double[] doubles) {
            return (long) doubles.length * Double.BYTES;
        } else if (value instanceof String string) {
            return (long) string.length() * Character.BYTES; // rough estimate; ignores compact-string encoding
        } else {
            // null, boxed numbers, Boolean, ZonedDateTime, Date, or anything else -- treat as a small fixed cost
            return 16L;
        }
    }

    /**
     * In ingest, all non-metadata keys are source keys, so the {@link #source} map is accessed directly from ctx.
     */
    @Override
    protected boolean directSourceAccess() {
        return true;
    }

    /**
     * Fetch the timestamp from the ingestMetadata, if it exists
     * @return the timestamp for the document or null
     */
    public static ZonedDateTime getTimestamp(Map<String, Object> ingestMetadata) {
        if (ingestMetadata == null) {
            return null;
        }
        Object ts = ingestMetadata.get(IngestDocument.TIMESTAMP);
        if (ts instanceof ZonedDateTime timestamp) {
            return timestamp;
        } else if (ts instanceof String str) {
            return ZonedDateTime.parse(str);
        }
        return null;
    }
}
