/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.index.storage;

import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * LMDB-based index storage for disk-persistent secondary indexes.
 * 
 * <p>LMDB (Lightning Memory-Mapped Database) provides excellent read performance
 * through memory-mapped files, making it ideal for read-heavy index workloads.
 * 
 * <p><b>Characteristics:</b>
 * <ul>
 *   <li>Excellent read performance (memory-mapped)</li>
 *   <li>MVCC - readers don't block writers</li>
 *   <li>Data persists across restarts</li>
 *   <li>Ordered keys (supports range queries)</li>
 *   <li>Single-writer model (writes are serialized)</li>
 * </ul>
 * 
 * @version 2.5.0
 * @since 1.9.0
 */
@Slf4j
public class LmdbIndexStorage implements IndexStorageProvider {
    
    private static final String INDEX_DB_NAME = "kuber_indexes";
    private static final int MAX_KEY_SIZE = 511; // LMDB limit
    
    private final KuberProperties properties;
    private final String dataDirectory;
    
    private Env<ByteBuffer> env;
    private Dbi<ByteBuffer> dbi;
    
    public LmdbIndexStorage(KuberProperties properties) {
        this.properties = properties;
        this.dataDirectory = properties.getIndexing().getDiskDirectory();
    }
    
    @Override
    public void initialize() {
        try {
            // Create base index directory if needed
            File baseDir = new File(dataDirectory);
            if (!baseDir.exists()) {
                if (baseDir.mkdirs()) {
                    log.info("Created index directory: {}", baseDir.getAbsolutePath());
                }
            }
            
            // Create lmdb subfolder inside index directory
            File lmdbDir = new File(baseDir, "lmdb");
            if (!lmdbDir.exists()) {
                if (lmdbDir.mkdirs()) {
                    log.info("Created LMDB index subfolder: {}", lmdbDir.getAbsolutePath());
                }
            }
            
            // Configure environment
            long mapSize = 10L * 1024L * 1024L * 1024L; // 10GB default
            
            env = Env.create()
                    .setMapSize(mapSize)
                    .setMaxDbs(1)
                    .setMaxReaders(126)
                    .open(lmdbDir, EnvFlags.MDB_WRITEMAP, EnvFlags.MDB_MAPASYNC);
            
            // Open database
            dbi = env.openDbi(INDEX_DB_NAME, DbiFlags.MDB_CREATE);
            
            log.info("╔══════════════════════════════════════════════════════════════╗");
            log.info("║         LMDB INDEX STORAGE INITIALIZED                        ║");
            log.info("╠══════════════════════════════════════════════════════════════╣");
            log.info("║  Path:       {}",  String.format("%-46s║", lmdbDir.getAbsolutePath()));
            log.info("║  Map Size:   {} GB", String.format("%-43d║", mapSize / (1024L * 1024L * 1024L)));
            log.info("╚══════════════════════════════════════════════════════════════╝");
            
        } catch (Exception e) {
            log.error("Failed to initialize LMDB index storage: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize LMDB index storage", e);
        }
    }
    
    @Override
    public void addToIndex(String indexName, Object fieldValue, String cacheKey) {
        if (env == null || fieldValue == null || cacheKey == null) return;
        
        String key = buildKey(indexName, fieldValue);
        if (key.length() > MAX_KEY_SIZE) {
            log.warn("Index key too long, truncating: {}", key.substring(0, 50));
            key = key.substring(0, MAX_KEY_SIZE);
        }
        
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer keyBuf = toBuffer(key);
            ByteBuffer existingBuf = dbi.get(txn, keyBuf);
            
            Set<String> keys = decodeKeys(existingBuf);
            if (keys.add(cacheKey)) {
                dbi.put(txn, keyBuf, encodeKeys(keys));
            }
            
            txn.commit();
        }
    }
    
    @Override
    public void removeFromIndex(String indexName, Object fieldValue, String cacheKey) {
        if (env == null || fieldValue == null || cacheKey == null) return;
        
        String key = buildKey(indexName, fieldValue);
        
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer keyBuf = toBuffer(key);
            ByteBuffer existingBuf = dbi.get(txn, keyBuf);
            
            if (existingBuf != null) {
                Set<String> keys = decodeKeys(existingBuf);
                if (keys.remove(cacheKey)) {
                    if (keys.isEmpty()) {
                        dbi.delete(txn, keyBuf);
                    } else {
                        dbi.put(txn, keyBuf, encodeKeys(keys));
                    }
                }
            }
            
            txn.commit();
        }
    }
    
    @Override
    public Set<String> getExact(String indexName, Object fieldValue) {
        if (env == null || fieldValue == null) return Collections.emptySet();
        
        String key = buildKey(indexName, fieldValue);
        
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer buf = dbi.get(txn, toBuffer(key));
            return decodeKeys(buf);
        }
    }
    
    @Override
    public Set<String> getRange(String indexName, Object fromValue, Object toValue) {
        if (env == null) return Collections.emptySet();
        
        Set<String> results = new HashSet<>();
        String prefix = indexName + ":";
        String startKey = fromValue != null ? prefix + fromValue.toString() : prefix;
        String endKey = toValue != null ? prefix + toValue.toString() : null;
        
        try (Txn<ByteBuffer> txn = env.txnRead();
             CursorIterable<ByteBuffer> cursor = dbi.iterate(txn, KeyRange.atLeast(toBuffer(startKey)))) {
            
            for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                String key = fromBuffer(kv.key());
                
                if (!key.startsWith(prefix)) break;
                if (endKey != null && key.compareTo(endKey) > 0) break;
                
                results.addAll(decodeKeys(kv.val()));
            }
        }
        
        return results;
    }
    
    @Override
    public Set<String> getByPrefix(String indexName, String prefix) {
        if (env == null || prefix == null) return Collections.emptySet();
        
        Set<String> results = new HashSet<>();
        String searchPrefix = indexName + ":" + prefix;
        
        try (Txn<ByteBuffer> txn = env.txnRead();
             CursorIterable<ByteBuffer> cursor = dbi.iterate(txn, KeyRange.atLeast(toBuffer(searchPrefix)))) {
            
            for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                String key = fromBuffer(kv.key());
                
                if (!key.startsWith(searchPrefix)) break;
                
                results.addAll(decodeKeys(kv.val()));
            }
        }
        
        return results;
    }
    
    @Override
    public void clearIndex(String indexName) {
        if (env == null) return;
        
        String prefix = indexName + ":";
        List<ByteBuffer> keysToDelete = new ArrayList<>();
        
        try (Txn<ByteBuffer> txn = env.txnRead();
             CursorIterable<ByteBuffer> cursor = dbi.iterate(txn, KeyRange.atLeast(toBuffer(prefix)))) {
            
            for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                String key = fromBuffer(kv.key());
                if (!key.startsWith(prefix)) break;
                
                // Copy buffer for deletion
                ByteBuffer copy = ByteBuffer.allocateDirect(kv.key().remaining());
                copy.put(kv.key().duplicate());
                copy.flip();
                keysToDelete.add(copy);
            }
        }
        
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            for (ByteBuffer key : keysToDelete) {
                dbi.delete(txn, key);
            }
            txn.commit();
        }
        
        log.debug("Cleared index: {} ({} entries)", indexName, keysToDelete.size());
    }
    
    @Override
    public void clearRegion(String region) {
        if (env == null) return;
        
        String prefix = region + ":";
        List<ByteBuffer> keysToDelete = new ArrayList<>();
        
        try (Txn<ByteBuffer> txn = env.txnRead();
             CursorIterable<ByteBuffer> cursor = dbi.iterate(txn, KeyRange.atLeast(toBuffer(prefix)))) {
            
            for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                String key = fromBuffer(kv.key());
                if (!key.startsWith(prefix)) break;
                
                ByteBuffer copy = ByteBuffer.allocateDirect(kv.key().remaining());
                copy.put(kv.key().duplicate());
                copy.flip();
                keysToDelete.add(copy);
            }
        }
        
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            for (ByteBuffer key : keysToDelete) {
                dbi.delete(txn, key);
            }
            txn.commit();
        }
        
        log.debug("Cleared all indexes for region: {} ({} entries)", region, keysToDelete.size());
    }
    
    @Override
    public long getIndexSize(String indexName) {
        if (env == null) return 0;
        
        long count = 0;
        String prefix = indexName + ":";
        
        try (Txn<ByteBuffer> txn = env.txnRead();
             CursorIterable<ByteBuffer> cursor = dbi.iterate(txn, KeyRange.atLeast(toBuffer(prefix)))) {
            
            for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                String key = fromBuffer(kv.key());
                if (!key.startsWith(prefix)) break;
                count++;
            }
        }
        
        return count;
    }
    
    @Override
    public long getTotalMappings(String indexName) {
        if (env == null) return 0;
        
        long count = 0;
        String prefix = indexName + ":";
        
        try (Txn<ByteBuffer> txn = env.txnRead();
             CursorIterable<ByteBuffer> cursor = dbi.iterate(txn, KeyRange.atLeast(toBuffer(prefix)))) {
            
            for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                String key = fromBuffer(kv.key());
                if (!key.startsWith(prefix)) break;
                count += decodeKeys(kv.val()).size();
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
        // LMDB uses memory-mapped files, RAM usage varies
        return -1;
    }
    
    @Override
    public StorageType getStorageType() {
        return StorageType.LMDB;
    }
    
    @Override
    public void flush() {
        if (env != null) {
            env.sync(true);
            log.debug("Flushed LMDB index storage");
        }
    }
    
    @Override
    public void shutdown() {
        if (dbi != null) dbi.close();
        if (env != null) env.close();
        
        log.info("Shutdown LMDB index storage");
    }
    
    // ==================== Helper Methods ====================
    
    private String buildKey(String indexName, Object fieldValue) {
        return indexName + ":" + fieldValue.toString();
    }
    
    private ByteBuffer toBuffer(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocateDirect(bytes.length);
        buf.put(bytes).flip();
        return buf;
    }
    
    private String fromBuffer(ByteBuffer buf) {
        if (buf == null) return "";
        byte[] bytes = new byte[buf.remaining()];
        buf.duplicate().get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
    
    private Set<String> decodeKeys(ByteBuffer buf) {
        if (buf == null || buf.remaining() == 0) {
            return new HashSet<>();
        }
        
        String data = fromBuffer(buf);
        Set<String> keys = new HashSet<>();
        
        for (String key : data.split("\0")) {
            if (!key.isEmpty()) {
                keys.add(key);
            }
        }
        
        return keys;
    }
    
    private ByteBuffer encodeKeys(Set<String> keys) {
        if (keys.isEmpty()) {
            return ByteBuffer.allocateDirect(0);
        }
        
        String data = String.join("\0", keys);
        return toBuffer(data);
    }
}
