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

import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Caffeine-based implementation of the CacheProxy interface.
 * 
 * <p>This proxy wraps a Caffeine cache instance and provides a consistent
 * API that can be swapped with other implementations if needed.
 * 
 * <p>Caffeine is a high-performance caching library for Java 8+, providing:
 * <ul>
 *   <li>Near-optimal hit rate</li>
 *   <li>Excellent performance</li>
 *   <li>Size-based eviction</li>
 *   <li>Time-based expiration</li>
 *   <li>Removal listeners</li>
 * </ul>
 * 
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values
 * 
 * @version 2.6.0
 * @since 1.5.0
 */
@Slf4j
public class CaffeineCacheProxy<K, V> implements CacheProxy<K, V> {
    
    private static final String TYPE = "CAFFEINE";
    
    private final String name;
    private final Cache<K, V> delegate;
    
    /**
     * Create a new Caffeine cache proxy.
     * 
     * @param name the cache name
     * @param delegate the underlying Caffeine cache
     */
    public CaffeineCacheProxy(String name, Cache<K, V> delegate) {
        this.name = name;
        this.delegate = delegate;
        log.debug("Created CaffeineCacheProxy '{}'", name);
    }
    
    @Override
    public V get(K key) {
        return delegate.getIfPresent(key);
    }
    
    @Override
    public V get(K key, Function<? super K, ? extends V> mappingFunction) {
        return delegate.get(key, mappingFunction);
    }
    
    @Override
    public void put(K key, V value) {
        delegate.put(key, value);
    }
    
    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        delegate.putAll(map);
    }
    
    @Override
    public void invalidate(K key) {
        delegate.invalidate(key);
    }
    
    @Override
    public void invalidateAll(Iterable<? extends K> keys) {
        delegate.invalidateAll(keys);
    }
    
    @Override
    public void invalidateAll() {
        delegate.invalidateAll();
    }
    
    @Override
    public long estimatedSize() {
        return delegate.estimatedSize();
    }
    
    @Override
    public void cleanUp() {
        delegate.cleanUp();
    }
    
    @Override
    public Map<K, V> asMap() {
        return delegate.asMap();
    }
    
    @Override
    public Set<K> keys() {
        return delegate.asMap().keySet();
    }
    
    @Override
    public boolean containsKey(K key) {
        return delegate.getIfPresent(key) != null;
    }
    
    @Override
    public CacheStats stats() {
        try {
            com.github.benmanes.caffeine.cache.stats.CacheStats caffeineStats = delegate.stats();
            return new CaffeineCacheStats(caffeineStats);
        } catch (Exception e) {
            log.debug("Stats not available for cache '{}': {}", name, e.getMessage());
            return null;
        }
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public String getType() {
        return TYPE;
    }
    
    /**
     * Get the underlying Caffeine cache.
     * Use with caution - breaks abstraction.
     * 
     * @return the underlying Caffeine cache
     */
    public Cache<K, V> getDelegate() {
        return delegate;
    }
    
    /**
     * Caffeine-specific cache statistics wrapper.
     */
    private static class CaffeineCacheStats implements CacheStats {
        private final com.github.benmanes.caffeine.cache.stats.CacheStats stats;
        
        CaffeineCacheStats(com.github.benmanes.caffeine.cache.stats.CacheStats stats) {
            this.stats = stats;
        }
        
        @Override
        public long hitCount() {
            return stats.hitCount();
        }
        
        @Override
        public long missCount() {
            return stats.missCount();
        }
        
        @Override
        public long loadSuccessCount() {
            return stats.loadSuccessCount();
        }
        
        @Override
        public long loadFailureCount() {
            return stats.loadFailureCount();
        }
        
        @Override
        public long evictionCount() {
            return stats.evictionCount();
        }
        
        @Override
        public double hitRate() {
            return stats.hitRate();
        }
        
        @Override
        public double missRate() {
            return stats.missRate();
        }
    }
    
    @Override
    public String toString() {
        return "CaffeineCacheProxy{name='" + name + "', size=" + estimatedSize() + "}";
    }
}
