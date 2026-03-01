/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.index;

/**
 * Storage type for secondary indexes.
 * 
 * <p>Kuber supports multiple storage backends for secondary indexes, allowing
 * you to balance between query speed and memory usage:
 * 
 * <ul>
 *   <li><b>HEAP</b> - Fastest, but uses JVM heap memory</li>
 *   <li><b>OFFHEAP</b> - Fast, uses direct memory (no GC pressure)</li>
 *   <li><b>DISK</b> - Slowest, but minimal RAM usage and survives restarts</li>
 * </ul>
 * 
 * <p>The disk-based storage uses RocksDB by default, but can be configured
 * to use LMDB or SQLite via {@code kuber.index.disk-backend} property.
 * 
 * @version 2.6.3
 * @since 1.8.0
 */
public enum IndexStorageType {
    /**
     * On-heap storage using JVM heap memory.
     * <p>
     * Fastest access but contributes to GC pressure.
     * <p>
     * Best for: Small to medium indexes (&lt;10M entries)
     */
    HEAP,
    
    /**
     * Off-heap storage using direct memory (Chronicle Map).
     * <p>
     * Slower access than HEAP but zero GC pressure.
     * <p>
     * Best for: Large indexes (&gt;10M entries), memory-constrained environments
     */
    OFFHEAP,
    
    /**
     * Disk-based storage using embedded database.
     * <p>
     * Slowest access but minimal RAM usage. Index data survives restarts
     * (no rebuild required). The backend is configurable:
     * <ul>
     *   <li>RocksDB (default) - Best write performance</li>
     *   <li>LMDB - Best read performance</li>
     *   <li>SQLite - Most portable</li>
     * </ul>
     * <p>
     * Best for: Very large indexes, memory-constrained environments,
     * fast startup (no rebuild)
     * 
     * @since 1.9.0
     */
    DISK,
    
    /**
     * Use the default storage type from configuration.
     * <p>
     * Resolves to the value of {@code kuber.index.default-storage-type}
     */
    DEFAULT
}
