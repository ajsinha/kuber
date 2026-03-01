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

/**
 * Configuration for collection creation.
 * 
 * <p>Used by {@link CollectionsFactory} to configure collection instances with
 * consistent settings across different implementations.
 * 
 * @version 2.6.3
 * @since 1.5.0
 */
@Data
@Builder
public class CollectionsConfig {
    
    /**
     * Initial capacity of the collection.
     * Higher values reduce resize operations but use more memory.
     */
    @Builder.Default
    private int initialCapacity = 16;
    
    /**
     * Load factor for hash-based collections (Map, Set).
     * Controls when the collection is resized.
     */
    @Builder.Default
    private float loadFactor = 0.75f;
    
    /**
     * Concurrency level for concurrent collections.
     * Estimated number of concurrent updating threads.
     */
    @Builder.Default
    private int concurrencyLevel = 16;
    
    /**
     * Whether the collection should be thread-safe.
     * Default is true for server-side use.
     */
    @Builder.Default
    private boolean threadSafe = true;
    
    /**
     * Whether the collection should maintain insertion order.
     * Applicable to Map and Set.
     */
    @Builder.Default
    private boolean ordered = false;
    
    /**
     * Whether the collection should be sorted.
     * Applicable to Map and Set.
     */
    @Builder.Default
    private boolean sorted = false;
    
    /**
     * Whether to use weak references for keys.
     * Applicable to Map.
     */
    @Builder.Default
    private boolean weakKeys = false;
    
    /**
     * Whether to use weak references for values.
     * Applicable to Map.
     */
    @Builder.Default
    private boolean weakValues = false;
    
    /**
     * Create a default configuration with reasonable defaults.
     */
    public static CollectionsConfig defaults() {
        return CollectionsConfig.builder().build();
    }
    
    /**
     * Create a configuration with specified initial capacity.
     */
    public static CollectionsConfig withInitialCapacity(int initialCapacity) {
        return CollectionsConfig.builder()
                .initialCapacity(initialCapacity)
                .build();
    }
    
    /**
     * Create a configuration optimized for high concurrency.
     */
    public static CollectionsConfig highConcurrency() {
        return CollectionsConfig.builder()
                .initialCapacity(256)
                .concurrencyLevel(32)
                .threadSafe(true)
                .build();
    }
    
    /**
     * Create a configuration for ordered collections.
     */
    public static CollectionsConfig ordered() {
        return CollectionsConfig.builder()
                .ordered(true)
                .build();
    }
    
    /**
     * Create a configuration for sorted collections.
     */
    public static CollectionsConfig sorted() {
        return CollectionsConfig.builder()
                .sorted(true)
                .build();
    }
    
    /**
     * Create a non-thread-safe configuration (for single-threaded use).
     */
    public static CollectionsConfig nonThreadSafe() {
        return CollectionsConfig.builder()
                .threadSafe(false)
                .build();
    }
}
