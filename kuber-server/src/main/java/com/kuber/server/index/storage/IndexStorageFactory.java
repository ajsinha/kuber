/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.index.storage;

import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Factory for creating IndexStorageProvider instances based on configuration.
 * 
 * <p>The storage backend for secondary indexes is configurable via:
 * <pre>
 * kuber.indexing.default-storage=DISK
 * kuber.indexing.disk-backend=rocksdb  # rocksdb, lmdb, sqlite
 * </pre>
 * 
 * <p>This is independent of the main data persistence backend. You can use:
 * <ul>
 *   <li>PostgreSQL for data + RocksDB for indexes</li>
 *   <li>MongoDB for data + SQLite for indexes</li>
 *   <li>RocksDB for data + Memory for indexes (fastest)</li>
 * </ul>
 * 
 * @version 2.5.0
 * @since 1.9.0
 */
@Slf4j
@Component
public class IndexStorageFactory {
    
    private final KuberProperties properties;
    
    public IndexStorageFactory(KuberProperties properties) {
        this.properties = properties;
    }
    
    /**
     * Create an IndexStorageProvider based on the configured storage type.
     * 
     * @param storageType The storage type (HEAP, OFFHEAP, DISK, or DEFAULT)
     * @return Initialized storage provider
     */
    public IndexStorageProvider createStorage(String storageType) {
        // Resolve DEFAULT to actual type
        String resolved = resolveStorageType(storageType);
        
        IndexStorageProvider provider = switch (resolved.toUpperCase()) {
            case "MEMORY", "HEAP" -> new MemoryIndexStorage();
            case "OFFHEAP" -> new MemoryIndexStorage(); // TODO: Add OffHeapIndexStorage
            case "DISK" -> createDiskStorage();
            case "ROCKSDB" -> new RocksDbIndexStorage(properties);
            case "LMDB" -> new LmdbIndexStorage(properties);
            case "SQLITE" -> new SqliteIndexStorage(properties);
            default -> {
                log.warn("Unknown storage type '{}', defaulting to MEMORY", storageType);
                yield new MemoryIndexStorage();
            }
        };
        
        provider.initialize();
        return provider;
    }
    
    /**
     * Create the default storage provider based on configuration.
     * 
     * @return Initialized storage provider
     */
    public IndexStorageProvider createDefaultStorage() {
        return createStorage(properties.getIndexing().getDefaultStorage());
    }
    
    /**
     * Create disk-based storage using the configured backend.
     * 
     * @return Initialized disk storage provider
     */
    private IndexStorageProvider createDiskStorage() {
        String backend = properties.getIndexing().getDiskBackend();
        
        return switch (backend.toLowerCase()) {
            case "rocksdb" -> new RocksDbIndexStorage(properties);
            case "lmdb" -> new LmdbIndexStorage(properties);
            case "sqlite" -> new SqliteIndexStorage(properties);
            default -> {
                log.warn("Unknown disk backend '{}', defaulting to RocksDB", backend);
                yield new RocksDbIndexStorage(properties);
            }
        };
    }
    
    /**
     * Resolve DEFAULT storage type to the configured default.
     */
    private String resolveStorageType(String storageType) {
        if (storageType == null || storageType.equalsIgnoreCase("DEFAULT")) {
            return properties.getIndexing().getDefaultStorage();
        }
        return storageType;
    }
    
    /**
     * Get a description of the storage configuration.
     */
    public String getStorageDescription() {
        String defaultStorage = properties.getIndexing().getDefaultStorage();
        
        if ("DISK".equalsIgnoreCase(defaultStorage)) {
            return "DISK (" + properties.getIndexing().getDiskBackend().toUpperCase() + ")";
        }
        
        return defaultStorage.toUpperCase();
    }
}
