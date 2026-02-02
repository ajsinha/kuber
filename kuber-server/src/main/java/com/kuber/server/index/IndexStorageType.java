/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.index;

/**
 * Storage type for secondary indexes.
 * 
 * @version 1.8.2
 * @since 1.8.2
 */
public enum IndexStorageType {
    /**
     * On-heap storage using JVM heap memory.
     * Fastest access but contributes to GC pressure.
     * Best for: Small to medium indexes (<10M entries)
     */
    HEAP,
    
    /**
     * Off-heap storage using direct memory (ByteBuffers).
     * Slower access but zero GC pressure.
     * Best for: Large indexes (>10M entries), memory-constrained environments
     */
    OFFHEAP,
    
    /**
     * Use the default storage from configuration.
     */
    DEFAULT
}
