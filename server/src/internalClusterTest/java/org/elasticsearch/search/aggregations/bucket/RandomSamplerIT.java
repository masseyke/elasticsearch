/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.sampler.random.InternalRandomSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.random.RandomSamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.SuiteScopeTestCase
public class RandomSamplerIT extends ESIntegTestCase {

    private static final String NUMERIC_VALUE = "number";
    private static final String MONOTONIC_VALUE = "monotonic";
    private static final String KEYWORD_VALUE = "kind";
    private static final String LOWER_KEYWORD = "lower";
    private static final String UPPER_KEYWORD = "upper";
    private static final double PROBABILITY = 0.25;
    private static int numDocs;
    private static double varMonotonic = 0.0;
    private static double varNumeric = 0.0;

    private static final int NUM_SAMPLE_RUNS = 25;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        numDocs = randomIntBetween(5000, 10000);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        double avgMonotonic = 0.0;
        double avgNumeric = 0.0;
        for (int i = 0; i < numDocs; i++) {
            final String keywordValue;
            final double numericValue;
            final double monotonicValue = randomDouble() + i;
            if (i % 2 == 0) {
                keywordValue = LOWER_KEYWORD;
                numericValue = randomDoubleBetween(0.0, 3.0, false);
            } else {
                keywordValue = UPPER_KEYWORD;
                numericValue = randomDoubleBetween(5.0, 9.0, false);
            }
            builders.add(
                client().prepareIndex("idx")
                    .setSource(
                        jsonBuilder().startObject()
                            .field(KEYWORD_VALUE, keywordValue)
                            .field(MONOTONIC_VALUE, monotonicValue)
                            .field(NUMERIC_VALUE, numericValue)
                            .endObject()
                    )
            );

            final double oldAvgMonotonic = avgMonotonic;
            final double oldAvgNumeric = avgNumeric;
            avgMonotonic = (i * avgMonotonic + monotonicValue) / (i + 1);
            avgNumeric = (i * avgNumeric + numericValue) / (i + 1);
            final double avgMonotonicDiff = avgMonotonic - oldAvgMonotonic;
            final double avgNumericDiff = avgNumeric - oldAvgNumeric;
            final double resMonotonic = monotonicValue - avgMonotonic;
            final double resNumeric = numericValue - avgNumeric;
            varMonotonic = (i * (varMonotonic + Math.pow(avgMonotonicDiff, 2.0)) + Math.pow(resMonotonic, 2.0)) / (i + 1);
            varNumeric = (i * (varNumeric + Math.pow(avgNumericDiff, 2.0)) + Math.pow(resNumeric, 2.0)) / (i + 1);
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/89721")
    public void testRandomSampler() {
        double sampleMonotonicValue = 0.0;
        double sampleNumericValue = 0.0;
        double sampledDocCount = 0.0;

        SearchRequest sampledRequest = client().prepareSearch("idx")
            .addAggregation(
                new RandomSamplerAggregationBuilder("sampler").setProbability(PROBABILITY)
                    .subAggregation(avg("mean_monotonic").field(MONOTONIC_VALUE))
                    .subAggregation(avg("mean_numeric").field(NUMERIC_VALUE))
            )
            .request();
        for (int i = 0; i < NUM_SAMPLE_RUNS; i++) {
            InternalRandomSampler sampler = client().search(sampledRequest).actionGet().getAggregations().get("sampler");
            sampleMonotonicValue += ((Avg) sampler.getAggregations().get("mean_monotonic")).getValue();
            sampleNumericValue += ((Avg) sampler.getAggregations().get("mean_numeric")).getValue();
            sampledDocCount += sampler.getDocCount();
        }
        sampledDocCount /= NUM_SAMPLE_RUNS;
        sampleMonotonicValue /= NUM_SAMPLE_RUNS;
        sampleNumericValue /= NUM_SAMPLE_RUNS;
        double expectedDocCount = PROBABILITY * numDocs;
        // We're taking the mean of NUM_SAMPLE_RUNS for which each run has standard deviation
        // sqrt(PROBABILITY * numDocs) so the 6 sigma error, for which we expect 1 failure in
        // 500M runs, is 6 * sqrt(PROBABILITY * numDocs / NUM_SAMPLE_RUNS).
        double maxCountError = 6.0 * Math.sqrt(PROBABILITY * numDocs / NUM_SAMPLE_RUNS);
        assertThat(Math.abs(sampledDocCount - expectedDocCount), lessThan(maxCountError));

        SearchResponse trueValueResponse = client().prepareSearch("idx")
            .addAggregation(avg("mean_monotonic").field(MONOTONIC_VALUE))
            .addAggregation(avg("mean_numeric").field(NUMERIC_VALUE))
            .get();
        double trueMonotonic = ((Avg) trueValueResponse.getAggregations().get("mean_monotonic")).getValue();
        double trueNumeric = ((Avg) trueValueResponse.getAggregations().get("mean_numeric")).getValue();
        double maxMonotonicError = 6.0 * Math.sqrt(varMonotonic / (numDocs * PROBABILITY * NUM_SAMPLE_RUNS));
        double maxNumericError = 6.0 * Math.sqrt(varNumeric / (numDocs * PROBABILITY * NUM_SAMPLE_RUNS));
        assertThat(Math.abs(sampleMonotonicValue - trueMonotonic), lessThan(maxMonotonicError));
        assertThat(Math.abs(sampleNumericValue - trueNumeric), lessThan(maxNumericError));
    }

    public void testRandomSamplerHistogram() {
        Map<String, Double> sampleMonotonicValue = new HashMap<>();
        Map<String, Double> sampleNumericValue = new HashMap<>();
        Map<String, Double> sampledDocCount = new HashMap<>();

        SearchRequest sampledRequest = client().prepareSearch("idx")
            .addAggregation(
                new RandomSamplerAggregationBuilder("sampler").setProbability(0.5)
                    .subAggregation(
                        histogram("histo").field(NUMERIC_VALUE)
                            .interval(5.0)
                            .subAggregation(avg("mean_monotonic").field(MONOTONIC_VALUE))
                            .subAggregation(avg("mean_numeric").field(NUMERIC_VALUE))
                    )
            )
            .request();
        for (int i = 0; i < NUM_SAMPLE_RUNS; i++) {
            InternalRandomSampler sampler = client().search(sampledRequest).actionGet().getAggregations().get("sampler");
            Histogram histo = sampler.getAggregations().get("histo");
            for (Histogram.Bucket bucket : histo.getBuckets()) {
                sampleMonotonicValue.compute(
                    bucket.getKeyAsString(),
                    (k, v) -> ((Avg) bucket.getAggregations().get("mean_monotonic")).getValue() + (v == null ? 0 : v)
                );
                sampleNumericValue.compute(
                    bucket.getKeyAsString(),
                    (k, v) -> ((Avg) bucket.getAggregations().get("mean_numeric")).getValue() + (v == null ? 0 : v)
                );
                sampledDocCount.compute(bucket.getKeyAsString(), (k, v) -> bucket.getDocCount() + (v == null ? 0 : v));
            }
        }
        for (String key : sampledDocCount.keySet()) {
            sampledDocCount.put(key, sampledDocCount.get(key) / NUM_SAMPLE_RUNS);
            sampleNumericValue.put(key, sampleNumericValue.get(key) / NUM_SAMPLE_RUNS);
            sampleMonotonicValue.put(key, sampleMonotonicValue.get(key) / NUM_SAMPLE_RUNS);
        }

        SearchResponse trueValueResponse = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(NUMERIC_VALUE)
                    .interval(5.0)
                    .subAggregation(avg("mean_monotonic").field(MONOTONIC_VALUE))
                    .subAggregation(avg("mean_numeric").field(NUMERIC_VALUE))
            )
            .get();
        Histogram histogram = trueValueResponse.getAggregations().get("histo");
        for (Histogram.Bucket bucket : histogram.getBuckets()) {
            long numDocs = bucket.getDocCount();
            // Note the true count is estimated by dividing the bucket sample doc count by PROBABILITY.
            double maxCountError = 6.0 * Math.sqrt(numDocs / NUM_SAMPLE_RUNS / (0.5 * PROBABILITY));
            assertThat(Math.abs(sampledDocCount.get(bucket.getKeyAsString()) - numDocs), lessThan(maxCountError));
            double trueMonotonic = ((Avg) bucket.getAggregations().get("mean_monotonic")).getValue();
            double trueNumeric = ((Avg) bucket.getAggregations().get("mean_numeric")).getValue();
            double maxMonotonicError = 6.0 * Math.sqrt(varMonotonic / (numDocs * 0.5 * PROBABILITY * NUM_SAMPLE_RUNS));
            double maxNumericError = 6.0 * Math.sqrt(varNumeric / (numDocs * 0.5 * PROBABILITY * NUM_SAMPLE_RUNS));
            assertThat(Math.abs(sampleMonotonicValue.get(bucket.getKeyAsString()) - trueMonotonic), lessThan(maxMonotonicError));
            assertThat(Math.abs(sampleNumericValue.get(bucket.getKeyAsString()) - trueNumeric), lessThan(maxNumericError));
        }
    }

}
