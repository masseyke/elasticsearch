/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestSettings;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public class ScriptProcessorTests extends ESTestCase {

    private ScriptService scriptService;
    private Script script;
    private IngestScript.Factory ingestScriptFactory;

    private static final String DOS_SCRIPT_NAME = "dos_script";
    private static final int DOS_FIELD_LENGTH = 50_000;
    // Derived from the actual configured default limit (rather than a hardcoded guess) so this test keeps working if that
    // default is ever retuned. IngestDocument estimates a String's size as length * 2 bytes, hence the (length * 2) divisor.
    private static final int DOS_ITERATIONS = Math.toIntExact(
        IngestSettings.MAX_CUMULATIVE_FIELD_VALUE_BYTES.getDefault(Settings.EMPTY).getBytes() / (DOS_FIELD_LENGTH * 2L)
    ) + 10;

    @Before
    public void setupScripting() {
        String scriptName = "script";
        scriptService = new ScriptService(
            Settings.builder().build(),
            Map.of(Script.DEFAULT_SCRIPT_LANG, new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Map.of(scriptName, ctx -> {
                Integer bytesIn = (Integer) ctx.get("bytes_in");
                Integer bytesOut = (Integer) ctx.get("bytes_out");
                ctx.put("bytes_total", bytesIn + bytesOut);
                ctx.put("_dynamic_templates", Map.of("foo", "bar"));
                return null;
            }, DOS_SCRIPT_NAME, ctx -> {
                // Copy the same already-large value into many top-level fields. The cumulative size guard should
                // trip before all writes complete.
                String largeValue = (String) ctx.get("foo");
                for (int i = 0; i < DOS_ITERATIONS; i++) {
                    ctx.put("bar" + i, largeValue);
                }
                return null;
            }), Map.of())),
            new HashMap<>(ScriptModule.CORE_CONTEXTS),
            () -> 1L,
            TestProjectResolvers.singleProject(randomProjectIdOrDefault())
        );
        script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Map.of());
        ingestScriptFactory = scriptService.compile(script, IngestScript.CONTEXT);
    }

    public void testScriptingWithoutPrecompiledScriptFactory() throws Exception {
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, null, scriptService);
        IngestDocument ingestDocument = randomDocument();
        processor.execute(ingestDocument);
        assertIngestDocument(ingestDocument);
    }

    public void testScriptingWithPrecompiledIngestScript() {
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, script, ingestScriptFactory, scriptService);
        IngestDocument ingestDocument = randomDocument();
        processor.execute(ingestDocument);
        assertIngestDocument(ingestDocument);
    }

    public void testScriptGrowingCtxTripsCumulativeSizeLimit() {
        // A script that copies an already-large field into many top-level fields mutates the ctx map directly, bypassing
        // IngestDocument.setFieldValue. Verify those mutations are still charged against the cumulative field-value size guard
        // so the document can't grow without bound.
        Script dosScript = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, DOS_SCRIPT_NAME, Map.of());
        ScriptProcessor processor = new ScriptProcessor(randomAlphaOfLength(10), null, dosScript, null, scriptService);
        Map<String, Object> document = new HashMap<>();
        document.put("foo", randomAlphaOfLength(DOS_FIELD_LENGTH));
        IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), document);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), containsString("bytes of field values"));
    }

    private IngestDocument randomDocument() {
        Map<String, Object> document = new HashMap<>();
        document.put("bytes_in", randomInt());
        document.put("bytes_out", randomInt());
        return RandomDocumentPicks.randomIngestDocument(random(), document);
    }

    private void assertIngestDocument(IngestDocument ingestDocument) {
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_in"));
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_out"));
        assertThat(ingestDocument.getSourceAndMetadata(), hasKey("bytes_total"));
        int bytesTotal = ingestDocument.getFieldValue("bytes_in", Integer.class) + ingestDocument.getFieldValue("bytes_out", Integer.class);
        assertThat(ingestDocument.getSourceAndMetadata().get("bytes_total"), is(bytesTotal));
        assertThat(ingestDocument.getSourceAndMetadata().get("_dynamic_templates"), equalTo(Map.of("foo", "bar")));
    }
}
