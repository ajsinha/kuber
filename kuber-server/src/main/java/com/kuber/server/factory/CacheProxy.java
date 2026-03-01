/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.server.factory;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Proxy interface for cache operations.
 * 
 * <p>This abstraction allows the underlying cache implementation to be swapped
 * without changing the code that uses it. Currently supports Caffeine, but can
 * be extended to support other implementations like Guava Cache, EhCache, etc.
 * 
 * <p>The proxy pattern provides:
 * <ul>
 *   <li>Abstraction from specific cache implementation</li>
 *   <li>Consistent API across different cache providers</li>
 *   <li>Ability to add cross-cutting concerns (logging, metrics)</li>
 *   <li>Easy migration between cache implementations</li>
 * </ul>
 * 
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 * 
 * @version 2.6.3
 * @since 1.5.0
 */
public interface CacheProxy<K, V> {
    
    /**
     * Get a value from the cache.
     * 
     * @param key the key whose associated value is to be returned
     * @return the value, or null if not present
     */
    V get(K key);
    
    /**
     * Get a value from the cache, computing it if absent.
     * 
     * @param key the key whose associated value is to be returned
     * @param mappingFunction the function to compute a value
     * @return the current (existing or computed) value associated with the key
     */
    V get(K key, Function<? super K, ? extends V> mappingFunction);
    
    /**
     * Put a value into the cache.
     * 
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     */
    void put(K key, V value);
    
    /**
     * Put all entries from the map into the cache.
     * 
     * @param map mappings to be stored in this cache
     */
    void putAll(Map<? extends K, ? extends V> map);
    
    /**
     * Invalidate (remove) a single entry from the cache.
     * 
     * @param key the key to invalidate
     */
    void invalidate(K key);
    
    /**
     * Invalidate (remove) multiple entries from the cache.
     * 
     * @param keys the keys to invalidate
     */
    void invalidateAll(Iterable<? extends K> keys);
    
    /**
     * Invalidate all entries from the cache.
     */
    void invalidateAll();
    
    /**
     * Get the approximate number of entries in this cache.
     * 
     * @return the estimated size of the cache
     */
    long estimatedSize();
    
    /**
     * Perform any pending maintenance operations.
     * This may trigger eviction of expired entries.
     */
    void cleanUp();
    
    /**
     * Get a view of the entries stored in this cache as a thread-safe map.
     * 
     * @return a thread-safe view of the cache entries
     */
    Map<K, V> asMap();
    
    /**
     * Get all keys currently in the cache.
     * 
     * @return set of keys
     */
    Set<K> keys();
    
    /**
     * Check if the cache contains a key.
     * 
     * @param key the key to check
     * @return true if present
     */
    boolean containsKey(K key);
    
    /**
     * Get cache statistics if available.
     * 
     * @return statistics snapshot, or null if not available
     */
    CacheStats stats();
    
    /**
     * Get the name of this cache.
     * 
     * @return the cache name
     */
    String getName();
    
    /**
     * Get the type of the underlying cache implementation.
     * 
     * @return the implementation type (e.g., "CAFFEINE", "GUAVA", "EHCACHE")
     */
    String getType();
    
    /**
     * Cache statistics snapshot.
     */
    interface CacheStats {
        long hitCount();
        long missCount();
        long loadSuccessCount();
        long loadFailureCount();
        long evictionCount();
        double hitRate();
        double missRate();
    }
}
