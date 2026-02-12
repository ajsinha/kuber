/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.index.storage;

import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RocksDB-based index storage for disk-persistent secondary indexes.
 * 
 * <p>This storage backend persists index data to disk using RocksDB,
 * providing minimal RAM usage while maintaining good performance.
 * 
 * <p><b>Data Layout:</b>
 * <pre>
 * Key:   indexName:fieldValue
 * Value: cacheKey1\0cacheKey2\0cacheKey3...  (null-separated list)
 * </pre>
 * 
 * <p><b>Characteristics:</b>
 * <ul>
 *   <li>Minimal RAM usage (only block cache)</li>
 *   <li>Data persists across restarts (no rebuild needed)</li>
 *   <li>Best write performance among disk backends</li>
 *   <li>Supports range queries via sorted storage</li>
 *   <li>Configurable bloom filters for faster lookups</li>
 * </ul>
 * 
 * @version 2.3.0
 * @since 1.9.0
 */
@Slf4j
public class RocksDbIndexStorage implements IndexStorageProvider {
    
    private final KuberProperties properties;
    private final String dataDirectory;
    
    private RocksDB db;
    private Options options;
    private WriteOptions writeOptions;
    private ReadOptions readOptions;
    private BlockBasedTableConfig tableConfig;
    
    // Cache for frequently accessed index metadata
    private final ConcurrentHashMap<String, Long> indexSizeCache = new ConcurrentHashMap<>();
    
    public RocksDbIndexStorage(KuberProperties properties) {
        this.properties = properties;
        this.dataDirectory = properties.getIndexing().getDiskDirectory();
    }
    
    @Override
    public void initialize() {
        try {
            RocksDB.loadLibrary();
            
            // Create base index directory if needed
            File baseDir = new File(dataDirectory);
            if (!baseDir.exists()) {
                if (baseDir.mkdirs()) {
                    log.info("Created index directory: {}", baseDir.getAbsolutePath());
                }
            }
            
            // Create rocksdb subfolder inside index directory
            File rocksdbDir = new File(baseDir, "rocksdb");
            if (!rocksdbDir.exists()) {
                if (rocksdbDir.mkdirs()) {
                    log.info("Created RocksDB index subfolder: {}", rocksdbDir.getAbsolutePath());
                }
            }
            
            // Configure block-based table with bloom filter
            tableConfig = new BlockBasedTableConfig();
            
            if (properties.getIndexing().isDiskBloomFilterEnabled()) {
                tableConfig.setFilterPolicy(new BloomFilter(10, false));
            }
            
            // Set block cache size
            long cacheSize = properties.getIndexing().getDiskCacheSizeMb() * 1024L * 1024L;
            tableConfig.setBlockCache(new LRUCache(cacheSize));
            tableConfig.setBlockSize(16 * 1024); // 16KB blocks
            
            // Configure options
            options = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig)
                    .setMaxOpenFiles(256)
                    .setWriteBufferSize(64 * 1024 * 1024) // 64MB write buffer
                    .setMaxWriteBufferNumber(3)
                    .setTargetFileSizeBase(64 * 1024 * 1024) // 64MB SST files
                    .setLevelCompactionDynamicLevelBytes(true)
                    .setCompactionStyle(CompactionStyle.LEVEL);
            
            // Configure write options
            writeOptions = new WriteOptions();
            if (properties.getIndexing().isDiskSyncWrites()) {
                writeOptions.setSync(true);
            }
            
            // Configure read options
            readOptions = new ReadOptions()
                    .setVerifyChecksums(false) // Faster reads
                    .setFillCache(true);
            
            // Open database in rocksdb subfolder
            String dbPath = rocksdbDir.getAbsolutePath();
            db = RocksDB.open(options, dbPath);
            
            log.info("╔══════════════════════════════════════════════════════════════╗");
            log.info("║         ROCKSDB INDEX STORAGE INITIALIZED                     ║");
            log.info("╠══════════════════════════════════════════════════════════════╣");
            log.info("║  Path:       {}",  String.format("%-46s║", dbPath));
            log.info("║  Cache:      {} MB", String.format("%-43d║", properties.getIndexing().getDiskCacheSizeMb()));
            log.info("║  Bloom:      {}", String.format("%-46s║", properties.getIndexing().isDiskBloomFilterEnabled() ? "ENABLED" : "DISABLED"));
            log.info("║  Sync:       {}", String.format("%-46s║", properties.getIndexing().isDiskSyncWrites() ? "ENABLED" : "DISABLED"));
            log.info("╚══════════════════════════════════════════════════════════════╝");
            
        } catch (RocksDBException e) {
            log.error("Failed to initialize RocksDB index storage: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize RocksDB index storage", e);
        }
    }
    
    @Override
    public void addToIndex(String indexName, Object fieldValue, String cacheKey) {
        if (db == null || fieldValue == null || cacheKey == null) return;
        
        String key = buildKey(indexName, fieldValue);
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        
        try {
            synchronized (getLock(key)) {
                // Read existing keys
                byte[] existingBytes = db.get(readOptions, keyBytes);
                Set<String> keys = decodeKeys(existingBytes);
                
                // Add new key
                if (keys.add(cacheKey)) {
                    // Write back
                    byte[] newBytes = encodeKeys(keys);
                    db.put(writeOptions, keyBytes, newBytes);
                    
                    // Invalidate size cache
                    indexSizeCache.remove(indexName);
                }
            }
        } catch (RocksDBException e) {
            log.error("Failed to add to index {}: {}", indexName, e.getMessage());
        }
    }
    
    @Override
    public void removeFromIndex(String indexName, Object fieldValue, String cacheKey) {
        if (db == null || fieldValue == null || cacheKey == null) return;
        
        String key = buildKey(indexName, fieldValue);
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        
        try {
            synchronized (getLock(key)) {
                byte[] existingBytes = db.get(readOptions, keyBytes);
                if (existingBytes == null) return;
                
                Set<String> keys = decodeKeys(existingBytes);
                if (keys.remove(cacheKey)) {
                    if (keys.isEmpty()) {
                        db.delete(writeOptions, keyBytes);
                    } else {
                        db.put(writeOptions, keyBytes, encodeKeys(keys));
                    }
                    indexSizeCache.remove(indexName);
                }
            }
        } catch (RocksDBException e) {
            log.error("Failed to remove from index {}: {}", indexName, e.getMessage());
        }
    }
    
    @Override
    public Set<String> getExact(String indexName, Object fieldValue) {
        if (db == null || fieldValue == null) return Collections.emptySet();
        
        String key = buildKey(indexName, fieldValue);
        
        try {
            byte[] bytes = db.get(readOptions, key.getBytes(StandardCharsets.UTF_8));
            return decodeKeys(bytes);
        } catch (RocksDBException e) {
            log.error("Failed to get from index {}: {}", indexName, e.getMessage());
            return Collections.emptySet();
        }
    }
    
    @Override
    public Set<String> getRange(String indexName, Object fromValue, Object toValue) {
        if (db == null) return Collections.emptySet();
        
        Set<String> results = new HashSet<>();
        String prefix = indexName + ":";
        
        try (RocksIterator iterator = db.newIterator(readOptions)) {
            // Seek to start position
            if (fromValue != null) {
                iterator.seek((prefix + fromValue.toString()).getBytes(StandardCharsets.UTF_8));
            } else {
                iterator.seek(prefix.getBytes(StandardCharsets.UTF_8));
            }
            
            String toKey = toValue != null ? prefix + toValue.toString() : null;
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                
                // Check if still in this index
                if (!key.startsWith(prefix)) break;
                
                // Check upper bound
                if (toKey != null && key.compareTo(toKey) > 0) break;
                
                // Decode and add keys
                results.addAll(decodeKeys(iterator.value()));
                
                iterator.next();
            }
        }
        
        return results;
    }
    
    @Override
    public Set<String> getByPrefix(String indexName, String prefix) {
        if (db == null || prefix == null) return Collections.emptySet();
        
        Set<String> results = new HashSet<>();
        String searchPrefix = indexName + ":" + prefix;
        
        try (RocksIterator iterator = db.newIterator(readOptions)) {
            iterator.seek(searchPrefix.getBytes(StandardCharsets.UTF_8));
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                
                if (!key.startsWith(searchPrefix)) break;
                
                results.addAll(decodeKeys(iterator.value()));
                iterator.next();
            }
        }
        
        return results;
    }
    
    @Override
    public void clearIndex(String indexName) {
        if (db == null) return;
        
        String prefix = indexName + ":";
        
        try (RocksIterator iterator = db.newIterator(readOptions);
             WriteBatch batch = new WriteBatch()) {
            
            iterator.seek(prefix.getBytes(StandardCharsets.UTF_8));
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(prefix)) break;
                
                batch.delete(iterator.key());
                iterator.next();
            }
            
            db.write(writeOptions, batch);
            indexSizeCache.remove(indexName);
            
            log.debug("Cleared index: {}", indexName);
            
        } catch (RocksDBException e) {
            log.error("Failed to clear index {}: {}", indexName, e.getMessage());
        }
    }
    
    @Override
    public void clearRegion(String region) {
        if (db == null) return;
        
        String prefix = region + ":";
        
        try (RocksIterator iterator = db.newIterator(readOptions);
             WriteBatch batch = new WriteBatch()) {
            
            iterator.seek(prefix.getBytes(StandardCharsets.UTF_8));
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(prefix)) break;
                
                batch.delete(iterator.key());
                iterator.next();
            }
            
            db.write(writeOptions, batch);
            indexSizeCache.keySet().removeIf(k -> k.startsWith(prefix));
            
            log.debug("Cleared all indexes for region: {}", region);
            
        } catch (RocksDBException e) {
            log.error("Failed to clear region {}: {}", region, e.getMessage());
        }
    }
    
    @Override
    public long getIndexSize(String indexName) {
        return indexSizeCache.computeIfAbsent(indexName, this::countIndexEntries);
    }
    
    private long countIndexEntries(String indexName) {
        if (db == null) return 0;
        
        long count = 0;
        String prefix = indexName + ":";
        
        try (RocksIterator iterator = db.newIterator(readOptions)) {
            iterator.seek(prefix.getBytes(StandardCharsets.UTF_8));
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(prefix)) break;
                
                count++;
                iterator.next();
            }
        }
        
        return count;
    }
    
    @Override
    public long getTotalMappings(String indexName) {
        if (db == null) return 0;
        
        long count = 0;
        String prefix = indexName + ":";
        
        try (RocksIterator iterator = db.newIterator(readOptions)) {
            iterator.seek(prefix.getBytes(StandardCharsets.UTF_8));
            
            while (iterator.isValid()) {
                String key = new String(iterator.key(), StandardCharsets.UTF_8);
                if (!key.startsWith(prefix)) break;
                
                count += decodeKeys(iterator.value()).size();
                iterator.next();
            }
        }
        
        return count;
    }
    
    @Override
    public boolean supportsRangeQueries() {
        return true;
    }
    
    @Override
    public boolean supportsPrefixQueries() {
        return true;
    }
    
    @Override
    public boolean isPersistent() {
        return true;
    }
    
    @Override
    public long getMemoryUsage() {
        // Return block cache size as approximation
        return properties.getIndexing().getDiskCacheSizeMb() * 1024L * 1024L;
    }
    
    @Override
    public StorageType getStorageType() {
        return StorageType.ROCKSDB;
    }
    
    @Override
    public void flush() {
        if (db == null) return;
        
        try {
            db.flush(new FlushOptions().setWaitForFlush(true));
            log.debug("Flushed RocksDB index storage");
        } catch (RocksDBException e) {
            log.error("Failed to flush RocksDB: {}", e.getMessage());
        }
    }
    
    @Override
    public void compact() {
        if (db == null) return;
        
        try {
            db.compactRange();
            log.info("Compacted RocksDB index storage");
        } catch (RocksDBException e) {
            log.error("Failed to compact RocksDB: {}", e.getMessage());
        }
    }
    
    @Override
    public void shutdown() {
        if (db != null) {
            flush();
            db.close();
            db = null;
        }
        if (options != null) options.close();
        if (writeOptions != null) writeOptions.close();
        if (readOptions != null) readOptions.close();
        
        log.info("Shutdown RocksDB index storage");
    }
    
    // ==================== Helper Methods ====================
    
    private String buildKey(String indexName, Object fieldValue) {
        return indexName + ":" + fieldValue.toString();
    }
    
    private Set<String> decodeKeys(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return new HashSet<>();
        }
        
        // Decode bytes as UTF-8 string, then split by null character
        String data = new String(bytes, StandardCharsets.UTF_8);
        Set<String> keys = new HashSet<>();
        
        for (String key : data.split("\0")) {
            if (!key.isEmpty()) {
                keys.add(key);
            }
        }
        
        return keys;
    }
    
    private byte[] encodeKeys(Set<String> keys) {
        if (keys.isEmpty()) return new byte[0];
        
        String data = String.join("\0", keys);
        return data.getBytes(StandardCharsets.UTF_8);
    }
    
    // Simple striped locking for concurrent access to same key
    private final Object[] locks = new Object[64];
    {
        for (int i = 0; i < locks.length; i++) {
            locks[i] = new Object();
        }
    }
    
    private Object getLock(String key) {
        return locks[Math.abs(key.hashCode() % locks.length)];
    }
}
