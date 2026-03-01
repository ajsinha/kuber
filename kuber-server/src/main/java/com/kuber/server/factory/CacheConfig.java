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

import lombok.Builder;
import lombok.Data;

import java.time.Duration;
import java.util.function.BiConsumer;

/**
 * Configuration for cache creation.
 * 
 * <p>Used by {@link CacheFactory} to configure cache instances with
 * consistent settings across different implementations.
 * 
 * @version 2.6.4
 * @since 1.5.0
 */
@Data
@Builder
public class CacheConfig {
    
    /**
     * Maximum number of entries in the cache.
     * When exceeded, entries are evicted based on the eviction policy.
     */
    @Builder.Default
    private long maximumSize = 10000;
    
    /**
     * Duration after which entries expire after being written.
     * Null means no expiration based on write time.
     */
    private Duration expireAfterWrite;
    
    /**
     * Duration after which entries expire after being accessed.
     * Null means no expiration based on access time.
     */
    private Duration expireAfterAccess;
    
    /**
     * Whether to record cache statistics.
     */
    @Builder.Default
    private boolean recordStats = true;
    
    /**
     * Whether to use synchronous executor (avoiding ForkJoinPool).
     * Recommended during shutdown scenarios.
     */
    @Builder.Default
    private boolean synchronousExecution = true;
    
    /**
     * Initial capacity hint for the cache.
     */
    @Builder.Default
    private int initialCapacity = 16;
    
    /**
     * Removal listener to be notified when entries are removed.
     * The BiConsumer receives (key, value) pairs.
     */
    private BiConsumer<Object, Object> removalListener;
    
    /**
     * Removal cause listener with full cause information.
     */
    private RemovalListener removalListenerWithCause;
    
    /**
     * Listener interface for removal events with cause.
     */
    @FunctionalInterface
    public interface RemovalListener {
        void onRemoval(Object key, Object value, RemovalCause cause);
    }
    
    /**
     * Cause of entry removal.
     */
    public enum RemovalCause {
        /** Entry was explicitly invalidated. */
        EXPLICIT,
        /** Entry was replaced with a new value. */
        REPLACED,
        /** Entry was evicted due to garbage collection. */
        COLLECTED,
        /** Entry expired. */
        EXPIRED,
        /** Entry was evicted due to size constraints. */
        SIZE
    }
    
    /**
     * Create a default configuration with reasonable defaults.
     */
    public static CacheConfig defaults() {
        return CacheConfig.builder().build();
    }
    
    /**
     * Create a configuration with specified max size.
     */
    public static CacheConfig withMaxSize(long maxSize) {
        return CacheConfig.builder()
                .maximumSize(maxSize)
                .build();
    }
}
