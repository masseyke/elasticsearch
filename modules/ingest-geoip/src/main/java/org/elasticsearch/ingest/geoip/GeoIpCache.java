/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.ingest.geoip;

import com.maxmind.db.NodeCache;
import com.maxmind.geoip2.model.AbstractResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * The in-memory cache for the geoip data. There should only be 1 instance of this class.
 * This cache differs from the maxmind's {@link NodeCache} such that this cache stores the deserialized Json objects to avoid the
 * cost of deserialization for each lookup (cached or not). This comes at slight expense of higher memory usage, but significant
 * reduction of CPU usage.
 */
final class GeoIpCache {
    private static final Logger logger = LogManager.getLogger(GeoIpCache.class);

    /**
     * Internal-only sentinel object for recording that a result from the geoip database was null (i.e. there was no result). By caching
     * this no-result we can distinguish between something not being in the cache because we haven't searched for that data yet, versus
     * something not being in the cache because the data doesn't exist in the database.
     */
    // visible for testing
    static final AbstractResponse NO_RESULT = new AbstractResponse() {
        @Override
        public String toString() {
            return "AbstractResponse[NO_RESULT]";
        }
    };

    private final Cache<CacheKey, AbstractResponse> cache;
    private final AtomicLong queryCount = new AtomicLong(0);
    private final AtomicLong totalTime = new AtomicLong(0);
    private final AtomicLong totalCacheRequestTime = new AtomicLong(0);
    private final AtomicLong totalCacheHitRequestTime = new AtomicLong(0);
    private final AtomicLong totalCacheMissRequestTime = new AtomicLong(0);
    private final AtomicLong totalDatabaseRequests = new AtomicLong(0);
    private final AtomicLong totalDatabaseRequestTime = new AtomicLong(0);

    // package private for testing
    GeoIpCache(long maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("geoip max cache size must be 0 or greater");
        }
        this.cache = CacheBuilder.<CacheKey, AbstractResponse>builder().setMaximumWeight(maxSize).build();
    }

    @SuppressWarnings("unchecked")
    <T extends AbstractResponse> T putIfAbsent(
        InetAddress ip,
        String databasePath,
        Function<InetAddress, AbstractResponse> retrieveFunction
    ) {
        long currentCount = queryCount.incrementAndGet();
        // can't use cache.computeIfAbsent due to the elevated permissions for the jackson (run via the cache loader)
        CacheKey cacheKey = new CacheKey(ip, databasePath);
        // intentionally non-locking for simplicity...it's OK if we re-put the same key/value in the cache during a race condition.
        long cacheStart = System.nanoTime();
        AbstractResponse response = cache.get(cacheKey);
        long cacheRequestTime = System.nanoTime() - cacheStart;
        totalCacheRequestTime.addAndGet(cacheRequestTime);
        totalTime.addAndGet(cacheRequestTime);

        // populate the cache for this key, if necessary
        if (response == null) {
            long start = System.nanoTime();
            response = retrieveFunction.apply(ip);
            long databaseRequestTime = System.nanoTime() - start;
            totalDatabaseRequestTime.addAndGet(databaseRequestTime);
            totalCacheMissRequestTime.addAndGet(cacheRequestTime);
            totalCacheMissRequestTime.addAndGet(databaseRequestTime);
            totalTime.addAndGet(databaseRequestTime);
            totalDatabaseRequests.incrementAndGet();
            // if the response from the database was null, then use the no-result sentinel value
            if (response == null) {
                response = NO_RESULT;
            }
            // store the result or no-result in the cache
            cache.put(cacheKey, response);
        } else {
            totalCacheHitRequestTime.addAndGet(cacheRequestTime);
        }
        if (currentCount % 100000 == 0) {
            logger.info("******** GeoIp cache stats **********");
            logger.info("Total requests to cache: " + currentCount);
            logger.info("Total amount of request time including database requests (ns): " + totalTime.get());
            logger.info("Total amount of cache request time (ns): " + totalCacheRequestTime.get());
            logger.info("Total amount of cache request time for cache hits (ns): " + totalCacheHitRequestTime.get());
            logger.info("Total amount of request time for cache misses (ns): " + totalCacheMissRequestTime.get());
            logger.info("Total number of database requests: " + totalDatabaseRequests.get());
            logger.info("Total amount of database request time (ns): " + totalDatabaseRequestTime.get());
            Cache.CacheStats cacheStats = cache.stats();
            logger.info("Cache hits: " + cacheStats.getHits());
            logger.info("Cache misses: " + cacheStats.getMisses());
            logger.info("Cache evictions: " + cacheStats.getEvictions());
            logger.info("******** End GeoIp cache stats **********");
        }
        if (true) throw new RuntimeException("argh");
        if (response == NO_RESULT) {
            return null; // the no-result sentinel is an internal detail, don't expose it
        } else {
            return (T) response;
        }
    }

    // only useful for testing
    AbstractResponse get(InetAddress ip, String databasePath) {
        CacheKey cacheKey = new CacheKey(ip, databasePath);
        return cache.get(cacheKey);
    }

    public int purgeCacheEntriesForDatabase(Path databaseFile) {
        String databasePath = databaseFile.toString();
        int counter = 0;
        for (CacheKey key : cache.keys()) {
            if (key.databasePath.equals(databasePath)) {
                cache.invalidate(key);
                counter++;
            }
        }
        return counter;
    }

    public int count() {
        return cache.count();
    }

    /**
     * The key to use for the cache. Since this cache can span multiple geoip processors that all use different databases, the database
     * path is needed to be included in the cache key. For example, if we only used the IP address as the key the City and ASN the same
     * IP may be in both with different values and we need to cache both.
     */
    private record CacheKey(InetAddress ip, String databasePath) {}
}
