/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.geoip.stats.GeoIpStatsAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, maxNumDataNodes = 1)
public class GeoIpDownloaderStatsIT extends AbstractGeoIpIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, IngestGeoIpPlugin.class, GeoIpProcessorNonIngestNodeIT.IngestGeoIpSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (getEndpoint() != null) {
            settings.put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), getEndpoint());
        }
        return settings.build();
    }

    @After
    public void disableDownloader() {
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), (String) null));
    }

    public void testStats() throws Exception {
        /*
         * Testing without the geoip endpoint fixture falls back to https://storage.googleapis.com/, which can cause this test to run too
         * slowly to pass.
         */
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        GeoIpStatsAction.Request req = new GeoIpStatsAction.Request();
        GeoIpStatsAction.Response response = client().execute(GeoIpStatsAction.INSTANCE, req).actionGet();
        XContentTestUtils.JsonMapView jsonMapView = new XContentTestUtils.JsonMapView(convertToMap(response));
        assertThat(jsonMapView.get("stats.successful_downloads"), equalTo(0));
        assertThat(jsonMapView.get("stats.failed_downloads"), equalTo(0));
        assertThat(jsonMapView.get("stats.skipped_updates"), equalTo(0));
        assertThat(jsonMapView.get("stats.databases_count"), equalTo(0));
        assertThat(jsonMapView.get("stats.total_download_time"), equalTo(0));
        assertEquals(0, jsonMapView.<List<Object>>get("stats.downloader_attempts").size());
        assertEquals(0, jsonMapView.<Map<String, Object>>get("nodes").size());
        putPipeline();
        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));

        assertBusy(() -> {
            GeoIpStatsAction.Response res = client().execute(GeoIpStatsAction.INSTANCE, req).actionGet();
            XContentTestUtils.JsonMapView view = new XContentTestUtils.JsonMapView(convertToMap(res));
            assertThat(view.get("stats.successful_downloads"), equalTo(4));
            assertThat(view.get("stats.failed_downloads"), equalTo(0));
            assertThat(view.get("stats.skipped_updates"), equalTo(0));
            assertThat(view.get("stats.databases_count"), equalTo(4));
            assertThat(view.get("stats.total_download_time"), greaterThan(0));
            assertEquals(4, view.<List<Object>>get("stats.downloader_attempts").size());
            List<Map<String, Object>> downloaderAttempts = view.get("stats.downloader_attempts");
            assertThat(
                downloaderAttempts.stream().map(m -> m.get("name")).collect(Collectors.toSet()),
                containsInAnyOrder("GeoLite2-City.mmdb", "GeoLite2-ASN.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb")
            );
            for (Map<String, Object> downloaderAttempt : downloaderAttempts) {
                assertThat(downloaderAttempt.keySet().size(), equalTo(2));
                @SuppressWarnings("unchecked")
                Map<String, Object> lastSuccess = (Map<String, Object>) downloaderAttempt.get("last_success");
                assertThat(
                    lastSuccess.keySet(),
                    containsInAnyOrder(
                        "archive_md5",
                        "download_date_in_millis",
                        "download_time_in_millis",
                        "source",
                        "build_date_in_millis"
                    )
                );
            }
            Map<String, Map<String, List<Map<String, Object>>>> nodes = view.get("nodes");
            assertThat(nodes.values(), hasSize(greaterThan(0)));
            for (Map<String, List<Map<String, Object>>> value : nodes.values()) {
                assertThat(value, hasKey("databases"));
                assertThat(
                    value.get("databases").stream().map(m -> m.get("name")).collect(Collectors.toSet()),
                    containsInAnyOrder("GeoLite2-City.mmdb", "GeoLite2-ASN.mmdb", "GeoLite2-Country.mmdb", "MyCustomGeoLite2-City.mmdb")
                );
                for (Map<String, Object> database : value.get("databases")) {
                    assertThat(
                        database.keySet(),
                        containsInAnyOrder("name", "source", "archive_md5", "md5", "build_date_in_millis", "type")
                    );
                }
            }
        }, 2, TimeUnit.MINUTES);
    }

    private void putPipeline() throws IOException {
        BytesReference bytes;
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.startArray("processors");
                {
                    builder.startObject();
                    {
                        builder.startObject("geoip");
                        {
                            builder.field("field", "ip");
                            builder.field("target_field", "ip-city");
                            builder.field("database_file", "GeoLite2-City.mmdb");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();
            bytes = BytesReference.bytes(builder);
        }
        assertAcked(clusterAdmin().preparePutPipeline("_id", bytes, XContentType.JSON).get());
    }

    public static Map<String, Object> convertToMap(ToXContent part) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        part.toXContent(builder, EMPTY_PARAMS);
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }
}
