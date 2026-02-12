/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 * Patent Pending: Certain architectural patterns and implementations described in
 * this module may be subject to patent applications.
 */
package com.kuber.server.factory;

/**
 * Factory interface for creating cache instances.
 * 
 * <p>This factory abstraction allows different cache implementations to be
 * plugged in without changing the consuming code. Implementations can create
 * caches backed by Caffeine, Guava Cache, EhCache, or other providers.
 * 
 * <p>Example usage:
 * <pre>{@code
 * CacheFactory factory = new CaffeineCacheFactory();
 * CacheProxy<String, MyObject> cache = factory.createCache("myCache", 
 *     CacheConfig.builder()
 *         .maximumSize(10000)
 *         .expireAfterWrite(Duration.ofHours(1))
 *         .build());
 * }</pre>
 * 
 * @version 2.3.0
 * @since 1.5.0
 */
public interface CacheFactory {
    
    /**
     * Create a new cache instance with the given configuration.
     * 
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of mapped values
     * @param name the name of the cache (for identification/metrics)
     * @param config the cache configuration
     * @return a new cache proxy instance
     */
    <K, V> CacheProxy<K, V> createCache(String name, CacheConfig config);
    
    /**
     * Create a new cache instance with default configuration.
     * 
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of mapped values
     * @param name the name of the cache
     * @return a new cache proxy instance with default settings
     */
    default <K, V> CacheProxy<K, V> createCache(String name) {
        return createCache(name, CacheConfig.defaults());
    }
    
    /**
     * Get the type of cache this factory creates.
     * 
     * @return the cache type (e.g., "CAFFEINE", "GUAVA", "EHCACHE")
     */
    String getType();
    
    /**
     * Cache implementation type enumeration.
     */
    enum CacheType {
        /** Caffeine cache - high performance, JDK 8+ */
        CAFFEINE,
        /** Guava cache - Google's caching library */
        GUAVA,
        /** EhCache - enterprise caching */
        EHCACHE,
        /** Simple concurrent hash map based cache */
        SIMPLE
    }
}
