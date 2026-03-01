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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Factory for creating Caffeine-backed cache instances.
 * 
 * <p>Caffeine is a high-performance, near optimal caching library based on
 * Java 8. This factory creates {@link CaffeineCacheProxy} instances that
 * wrap Caffeine caches.
 * 
 * <p>Configuration options supported:
 * <ul>
 *   <li>Maximum size (entry count)</li>
 *   <li>Expire after write duration</li>
 *   <li>Expire after access duration</li>
 *   <li>Statistics recording</li>
 *   <li>Removal listeners</li>
 * </ul>
 * 
 * @version 2.6.4
 * @since 1.5.0
 */
@Slf4j
@Component
public class CaffeineCacheFactory implements CacheFactory {
    
    private static final String TYPE = "CAFFEINE";
    
    @Override
    @SuppressWarnings("unchecked")
    public <K, V> CacheProxy<K, V> createCache(String name, CacheConfig config) {
        log.debug("Creating Caffeine cache '{}' with maxSize={}", name, config.getMaximumSize());
        
        Caffeine<Object, Object> builder = Caffeine.newBuilder();
        
        // Set maximum size
        if (config.getMaximumSize() > 0) {
            builder.maximumSize(config.getMaximumSize());
        }
        
        // Set initial capacity
        if (config.getInitialCapacity() > 0) {
            builder.initialCapacity(config.getInitialCapacity());
        }
        
        // Set expire after write
        if (config.getExpireAfterWrite() != null) {
            builder.expireAfterWrite(config.getExpireAfterWrite().toMillis(), TimeUnit.MILLISECONDS);
        }
        
        // Set expire after access
        if (config.getExpireAfterAccess() != null) {
            builder.expireAfterAccess(config.getExpireAfterAccess().toMillis(), TimeUnit.MILLISECONDS);
        }
        
        // Set synchronous execution if requested (avoids ForkJoinPool)
        if (config.isSynchronousExecution()) {
            builder.executor(Runnable::run);
        }
        
        // Set up removal listener
        if (config.getRemovalListenerWithCause() != null) {
            builder.removalListener((key, value, cause) -> {
                CacheConfig.RemovalCause proxyCause = mapRemovalCause(cause);
                config.getRemovalListenerWithCause().onRemoval(key, value, proxyCause);
            });
        } else if (config.getRemovalListener() != null) {
            builder.removalListener((key, value, cause) -> 
                config.getRemovalListener().accept(key, value));
        }
        
        // Record stats if requested
        if (config.isRecordStats()) {
            builder.recordStats();
        }
        
        Cache<K, V> cache = (Cache<K, V>) builder.build();
        
        log.info("Created Caffeine cache '{}': maxSize={}, expireAfterWrite={}, stats={}", 
                name, config.getMaximumSize(), config.getExpireAfterWrite(), config.isRecordStats());
        
        return new CaffeineCacheProxy<>(name, cache);
    }
    
    @Override
    public String getType() {
        return TYPE;
    }
    
    /**
     * Map Caffeine removal cause to our abstraction.
     */
    private CacheConfig.RemovalCause mapRemovalCause(RemovalCause cause) {
        return switch (cause) {
            case EXPLICIT -> CacheConfig.RemovalCause.EXPLICIT;
            case REPLACED -> CacheConfig.RemovalCause.REPLACED;
            case COLLECTED -> CacheConfig.RemovalCause.COLLECTED;
            case EXPIRED -> CacheConfig.RemovalCause.EXPIRED;
            case SIZE -> CacheConfig.RemovalCause.SIZE;
        };
    }
}
