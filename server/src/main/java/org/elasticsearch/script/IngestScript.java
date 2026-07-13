
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.Map;

/**
 * A script used by the Ingest Script Processor.
 */
public abstract class IngestScript extends WriteScript {

    public static final String[] PARAMETERS = {};

    /**
     * Default cap on the heap a single ingest script execution may allocate. Ingest scripts are default-on for allocation
     * tracking (unlike most script contexts) because they run with only 'write' privileges yet can allocate unbounded memory
     * and OOM the node. Operators can raise, lower, or disable this via
     * {@code script.painless.max_allocation_bytes.context.ingest.limit}.
     */
    public static final long DEFAULT_MAX_ALLOCATION_BYTES = ByteSizeValue.ofMb(50).getBytes();

    /** The context used to compile {@link IngestScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>(
        "ingest",
        Factory.class,
        200,
        TimeValue.timeValueMillis(0),
        false,
        true,
        DEFAULT_MAX_ALLOCATION_BYTES
    );

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    public IngestScript(Map<String, Object> params, CtxMap<?> ctxMap) {
        super(ctxMap);
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public abstract void execute();

    public interface Factory {
        IngestScript newInstance(Map<String, Object> params, CtxMap<?> ctx);
    }
}
