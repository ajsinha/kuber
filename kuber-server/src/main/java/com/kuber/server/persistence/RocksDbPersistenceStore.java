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
package com.kuber.server.persistence;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheRegion;
import com.kuber.core.util.JsonUtils;
import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RocksDB implementation of PersistenceStore.
 * High-performance embedded key-value store.
 */
@Slf4j
public class RocksDbPersistenceStore extends AbstractPersistenceStore {
    
    private static final String REGIONS_CF = "regions";
    private static final String ENTRIES_PREFIX = "entries_";
    
    private final KuberProperties properties;
    private final String dbPath;
    private RocksDB db;
    private DBOptions dbOptions;
    private final Map<String, ColumnFamilyHandle> columnFamilyHandles = new ConcurrentHashMap<>();
    private ColumnFamilyHandle defaultHandle;
    private ColumnFamilyHandle regionsHandle;
    
    static {
        RocksDB.loadLibrary();
    }
    
    public RocksDbPersistenceStore(KuberProperties properties) {
        this.properties = properties;
        this.dbPath = properties.getPersistence().getRocksdb().getPath();
    }
    
    @Override
    public PersistenceType getType() {
        return PersistenceType.ROCKSDB;
    }
    
    @Override
    public void initialize() {
        log.info("Initializing RocksDB persistence store at: {}", dbPath);
        
        try {
            // Ensure directory exists
            File dbDir = new File(dbPath);
            if (!dbDir.exists()) {
                dbDir.mkdirs();
            }
            
            // Configure RocksDB options
            dbOptions = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .setMaxBackgroundJobs(4)
                    .setMaxOpenFiles(-1);
            
            // Get existing column families
            List<byte[]> existingCfs;
            try {
                existingCfs = RocksDB.listColumnFamilies(new Options().setCreateIfMissing(true), dbPath);
            } catch (RocksDBException e) {
                existingCfs = new ArrayList<>();
            }
            
            // Prepare column family descriptors
            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                    .setCompressionType(CompressionType.LZ4_COMPRESSION);
            
            // Always include default column family
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
            
            // Add regions column family
            cfDescriptors.add(new ColumnFamilyDescriptor(REGIONS_CF.getBytes(StandardCharsets.UTF_8), cfOptions));
            
            // Add existing column families
            for (byte[] cf : existingCfs) {
                String cfName = new String(cf, StandardCharsets.UTF_8);
                if (!cfName.equals(new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8)) 
                    && !cfName.equals(REGIONS_CF)) {
                    cfDescriptors.add(new ColumnFamilyDescriptor(cf, cfOptions));
                }
            }
            
            // Open database with column families
            List<ColumnFamilyHandle> handles = new ArrayList<>();
            db = RocksDB.open(dbOptions, dbPath, cfDescriptors, handles);
            
            // Store handles
            for (int i = 0; i < cfDescriptors.size(); i++) {
                String cfName = new String(cfDescriptors.get(i).getName(), StandardCharsets.UTF_8);
                ColumnFamilyHandle handle = handles.get(i);
                
                if (cfName.equals(new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8))) {
                    defaultHandle = handle;
                } else if (cfName.equals(REGIONS_CF)) {
                    regionsHandle = handle;
                } else {
                    columnFamilyHandles.put(cfName, handle);
                }
            }
            
            available = true;
            log.info("RocksDB persistence store initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize RocksDB persistence store: {}", e.getMessage(), e);
            available = false;
        }
    }
    
    @Override
    public void shutdown() {
        log.info("Shutting down RocksDB persistence store...");
        available = false;
        
        try {
            // Close column family handles
            for (ColumnFamilyHandle handle : columnFamilyHandles.values()) {
                handle.close();
            }
            if (regionsHandle != null) {
                regionsHandle.close();
            }
            if (defaultHandle != null) {
                defaultHandle.close();
            }
            
            // Close database
            if (db != null) {
                db.close();
            }
            
            // Close options
            if (dbOptions != null) {
                dbOptions.close();
            }
        } catch (Exception e) {
            log.warn("Error during RocksDB shutdown: {}", e.getMessage());
        }
    }
    
    private ColumnFamilyHandle getOrCreateColumnFamily(String region) {
        String cfName = ENTRIES_PREFIX + region;
        return columnFamilyHandles.computeIfAbsent(cfName, name -> {
            try {
                ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                        .setCompressionType(CompressionType.LZ4_COMPRESSION);
                ColumnFamilyHandle handle = db.createColumnFamily(
                        new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8), cfOptions));
                log.info("Created column family for region: {}", region);
                return handle;
            } catch (RocksDBException e) {
                log.error("Failed to create column family for region {}: {}", region, e.getMessage());
                throw new RuntimeException("Failed to create column family", e);
            }
        });
    }
    
    // ==================== Region Operations ====================
    
    @Override
    public void saveRegion(CacheRegion region) {
        try {
            String json = JsonUtils.toJson(regionToMap(region));
            db.put(regionsHandle, 
                   region.getName().getBytes(StandardCharsets.UTF_8),
                   json.getBytes(StandardCharsets.UTF_8));
            
            // Ensure column family exists for region
            getOrCreateColumnFamily(region.getName());
            
            log.info("Saved region '{}' to RocksDB", region.getName());
        } catch (RocksDBException e) {
            log.error("Failed to save region '{}': {}", region.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to save region", e);
        }
    }
    
    @Override
    public List<CacheRegion> loadAllRegions() {
        List<CacheRegion> regions = new ArrayList<>();
        
        try (RocksIterator iterator = db.newIterator(regionsHandle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String json = new String(iterator.value(), StandardCharsets.UTF_8);
                Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, 
                        new TypeReference<Map<String, Object>>() {});
                regions.add(mapToRegion(map));
            }
        } catch (Exception e) {
            log.error("Failed to load regions: {}", e.getMessage(), e);
        }
        
        log.info("Loaded {} regions from RocksDB", regions.size());
        return regions;
    }
    
    @Override
    public CacheRegion loadRegion(String name) {
        try {
            byte[] value = db.get(regionsHandle, name.getBytes(StandardCharsets.UTF_8));
            if (value != null) {
                String json = new String(value, StandardCharsets.UTF_8);
                Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, 
                        new TypeReference<Map<String, Object>>() {});
                return mapToRegion(map);
            }
        } catch (Exception e) {
            log.error("Failed to load region '{}': {}", name, e.getMessage(), e);
        }
        return null;
    }
    
    @Override
    public void deleteRegion(String name) {
        try {
            // Delete region metadata
            db.delete(regionsHandle, name.getBytes(StandardCharsets.UTF_8));
            
            // Drop column family for region entries
            String cfName = ENTRIES_PREFIX + name;
            ColumnFamilyHandle handle = columnFamilyHandles.remove(cfName);
            if (handle != null) {
                db.dropColumnFamily(handle);
                handle.close();
            }
            
            log.info("Deleted region '{}' from RocksDB", name);
        } catch (RocksDBException e) {
            log.error("Failed to delete region '{}': {}", name, e.getMessage(), e);
            throw new RuntimeException("Failed to delete region", e);
        }
    }
    
    @Override
    public void purgeRegion(String name) {
        try {
            String cfName = ENTRIES_PREFIX + name;
            ColumnFamilyHandle handle = columnFamilyHandles.get(cfName);
            
            if (handle != null) {
                // Delete all entries in the column family
                try (RocksIterator iterator = db.newIterator(handle)) {
                    WriteBatch batch = new WriteBatch();
                    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                        batch.delete(handle, iterator.key());
                    }
                    db.write(new WriteOptions(), batch);
                    batch.close();
                }
            }
            
            log.info("Purged region '{}' in RocksDB", name);
        } catch (RocksDBException e) {
            log.error("Failed to purge region '{}': {}", name, e.getMessage(), e);
            throw new RuntimeException("Failed to purge region", e);
        }
    }
    
    private Map<String, Object> regionToMap(CacheRegion region) {
        return Map.ofEntries(
                Map.entry("name", region.getName()),
                Map.entry("description", region.getDescription() != null ? region.getDescription() : ""),
                Map.entry("captive", region.isCaptive()),
                Map.entry("maxEntries", region.getMaxEntries()),
                Map.entry("defaultTtlSeconds", region.getDefaultTtlSeconds()),
                Map.entry("entryCount", region.getEntryCount()),
                Map.entry("createdAt", region.getCreatedAt() != null ? region.getCreatedAt().toEpochMilli() : 0),
                Map.entry("updatedAt", region.getUpdatedAt() != null ? region.getUpdatedAt().toEpochMilli() : 0),
                Map.entry("createdBy", region.getCreatedBy() != null ? region.getCreatedBy() : ""),
                Map.entry("enabled", region.isEnabled()),
                Map.entry("collectionName", region.getCollectionName() != null ? region.getCollectionName() : "")
        );
    }
    
    private CacheRegion mapToRegion(Map<String, Object> map) {
        return CacheRegion.builder()
                .name((String) map.get("name"))
                .description((String) map.get("description"))
                .captive((Boolean) map.getOrDefault("captive", false))
                .maxEntries(((Number) map.getOrDefault("maxEntries", -1L)).longValue())
                .defaultTtlSeconds(((Number) map.getOrDefault("defaultTtlSeconds", -1L)).longValue())
                .entryCount(((Number) map.getOrDefault("entryCount", 0L)).longValue())
                .createdAt(Instant.ofEpochMilli(((Number) map.getOrDefault("createdAt", 0L)).longValue()))
                .updatedAt(Instant.ofEpochMilli(((Number) map.getOrDefault("updatedAt", 0L)).longValue()))
                .createdBy((String) map.get("createdBy"))
                .enabled((Boolean) map.getOrDefault("enabled", true))
                .collectionName((String) map.get("collectionName"))
                .build();
    }
    
    // ==================== Entry Operations ====================
    
    @Override
    public void saveEntry(CacheEntry entry) {
        try {
            ColumnFamilyHandle handle = getOrCreateColumnFamily(entry.getRegion());
            String json = JsonUtils.toJson(entryToMap(entry));
            db.put(handle, 
                   entry.getKey().getBytes(StandardCharsets.UTF_8),
                   json.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            log.error("Failed to save entry '{}' in region '{}': {}", entry.getKey(), entry.getRegion(), e.getMessage(), e);
            throw new RuntimeException("Failed to save entry", e);
        }
    }
    
    @Override
    public void saveEntries(List<CacheEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        
        try {
            WriteBatch batch = new WriteBatch();
            
            for (CacheEntry entry : entries) {
                ColumnFamilyHandle handle = getOrCreateColumnFamily(entry.getRegion());
                String json = JsonUtils.toJson(entryToMap(entry));
                batch.put(handle, 
                         entry.getKey().getBytes(StandardCharsets.UTF_8),
                         json.getBytes(StandardCharsets.UTF_8));
            }
            
            db.write(new WriteOptions(), batch);
            batch.close();
            
            log.debug("Saved {} entries to RocksDB", entries.size());
        } catch (RocksDBException e) {
            log.error("Failed to save entries: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to save entries", e);
        }
    }
    
    @Override
    public CacheEntry loadEntry(String region, String key) {
        try {
            String cfName = ENTRIES_PREFIX + region;
            ColumnFamilyHandle handle = columnFamilyHandles.get(cfName);
            
            if (handle != null) {
                byte[] value = db.get(handle, key.getBytes(StandardCharsets.UTF_8));
                if (value != null) {
                    String json = new String(value, StandardCharsets.UTF_8);
                    Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, 
                            new TypeReference<Map<String, Object>>() {});
                    return mapToEntry(map, region);
                }
            }
        } catch (Exception e) {
            log.error("Failed to load entry '{}' from region '{}': {}", key, region, e.getMessage(), e);
        }
        return null;
    }
    
    @Override
    public List<CacheEntry> loadEntries(String region, int limit) {
        List<CacheEntry> entries = new ArrayList<>();
        String cfName = ENTRIES_PREFIX + region;
        ColumnFamilyHandle handle = columnFamilyHandles.get(cfName);
        
        if (handle != null) {
            try (RocksIterator iterator = db.newIterator(handle)) {
                int count = 0;
                for (iterator.seekToFirst(); iterator.isValid() && count < limit; iterator.next()) {
                    String json = new String(iterator.value(), StandardCharsets.UTF_8);
                    Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, 
                            new TypeReference<Map<String, Object>>() {});
                    entries.add(mapToEntry(map, region));
                    count++;
                }
            } catch (Exception e) {
                log.error("Failed to load entries from region '{}': {}", region, e.getMessage(), e);
            }
        }
        
        return entries;
    }
    
    @Override
    public void deleteEntry(String region, String key) {
        try {
            String cfName = ENTRIES_PREFIX + region;
            ColumnFamilyHandle handle = columnFamilyHandles.get(cfName);
            
            if (handle != null) {
                db.delete(handle, key.getBytes(StandardCharsets.UTF_8));
            }
        } catch (RocksDBException e) {
            log.error("Failed to delete entry '{}' from region '{}': {}", key, region, e.getMessage(), e);
            throw new RuntimeException("Failed to delete entry", e);
        }
    }
    
    @Override
    public void deleteEntries(String region, List<String> keys) {
        if (keys.isEmpty()) {
            return;
        }
        
        try {
            String cfName = ENTRIES_PREFIX + region;
            ColumnFamilyHandle handle = columnFamilyHandles.get(cfName);
            
            if (handle != null) {
                WriteBatch batch = new WriteBatch();
                for (String key : keys) {
                    batch.delete(handle, key.getBytes(StandardCharsets.UTF_8));
                }
                db.write(new WriteOptions(), batch);
                batch.close();
            }
        } catch (RocksDBException e) {
            log.error("Failed to delete entries from region '{}': {}", region, e.getMessage(), e);
            throw new RuntimeException("Failed to delete entries", e);
        }
    }
    
    @Override
    public long countEntries(String region) {
        String cfName = ENTRIES_PREFIX + region;
        ColumnFamilyHandle handle = columnFamilyHandles.get(cfName);
        
        if (handle == null) {
            return 0;
        }
        
        long count = 0;
        try (RocksIterator iterator = db.newIterator(handle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                count++;
            }
        }
        return count;
    }
    
    @Override
    public List<String> getKeys(String region, String pattern, int limit) {
        List<String> keys = new ArrayList<>();
        String cfName = ENTRIES_PREFIX + region;
        ColumnFamilyHandle handle = columnFamilyHandles.get(cfName);
        
        if (handle != null) {
            try (RocksIterator iterator = db.newIterator(handle)) {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    keys.add(new String(iterator.key(), StandardCharsets.UTF_8));
                }
            }
        }
        
        return filterKeys(keys, pattern, limit);
    }
    
    @Override
    public long deleteExpiredEntries(String region) {
        String cfName = ENTRIES_PREFIX + region;
        ColumnFamilyHandle handle = columnFamilyHandles.get(cfName);
        
        if (handle == null) {
            return 0;
        }
        
        List<String> expiredKeys = new ArrayList<>();
        Instant now = Instant.now();
        
        try (RocksIterator iterator = db.newIterator(handle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                try {
                    String json = new String(iterator.value(), StandardCharsets.UTF_8);
                    Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, 
                            new TypeReference<Map<String, Object>>() {});
                    
                    Object expiresAt = map.get("expiresAt");
                    if (expiresAt != null) {
                        Instant expiry = Instant.ofEpochMilli(((Number) expiresAt).longValue());
                        if (now.isAfter(expiry)) {
                            expiredKeys.add(new String(iterator.key(), StandardCharsets.UTF_8));
                        }
                    }
                } catch (Exception e) {
                    log.warn("Failed to check expiry for entry: {}", e.getMessage());
                }
            }
        }
        
        // Delete expired entries
        for (String key : expiredKeys) {
            try {
                db.delete(handle, key.getBytes(StandardCharsets.UTF_8));
            } catch (RocksDBException e) {
                log.error("Failed to delete expired entry '{}': {}", key, e.getMessage());
            }
        }
        
        if (!expiredKeys.isEmpty()) {
            log.info("Deleted {} expired entries from region '{}' in RocksDB", expiredKeys.size(), region);
        }
        
        return expiredKeys.size();
    }
    
    @Override
    public long deleteAllExpiredEntries() {
        long total = 0;
        for (CacheRegion region : loadAllRegions()) {
            total += deleteExpiredEntries(region.getName());
        }
        return total;
    }
    
    @Override
    public long countNonExpiredEntries(String region) {
        String cfName = ENTRIES_PREFIX + region;
        ColumnFamilyHandle handle = columnFamilyHandles.get(cfName);
        
        if (handle == null) {
            return 0;
        }
        
        long count = 0;
        Instant now = Instant.now();
        
        try (RocksIterator iterator = db.newIterator(handle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                try {
                    String json = new String(iterator.value(), StandardCharsets.UTF_8);
                    Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, 
                            new TypeReference<Map<String, Object>>() {});
                    
                    Object expiresAt = map.get("expiresAt");
                    if (expiresAt == null) {
                        count++;
                    } else {
                        Instant expiry = Instant.ofEpochMilli(((Number) expiresAt).longValue());
                        if (!now.isAfter(expiry)) {
                            count++;
                        }
                    }
                } catch (Exception e) {
                    // Count as non-expired if we can't parse
                    count++;
                }
            }
        }
        
        return count;
    }
    
    @Override
    public List<String> getNonExpiredKeys(String region, String pattern, int limit) {
        String cfName = ENTRIES_PREFIX + region;
        ColumnFamilyHandle handle = columnFamilyHandles.get(cfName);
        
        if (handle == null) {
            return new ArrayList<>();
        }
        
        List<String> keys = new ArrayList<>();
        Instant now = Instant.now();
        
        try (RocksIterator iterator = db.newIterator(handle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                try {
                    String json = new String(iterator.value(), StandardCharsets.UTF_8);
                    Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, 
                            new TypeReference<Map<String, Object>>() {});
                    
                    Object expiresAt = map.get("expiresAt");
                    boolean isValid = (expiresAt == null) || 
                            !now.isAfter(Instant.ofEpochMilli(((Number) expiresAt).longValue()));
                    
                    if (isValid) {
                        keys.add(new String(iterator.key(), StandardCharsets.UTF_8));
                    }
                } catch (Exception e) {
                    // Include if we can't parse
                    keys.add(new String(iterator.key(), StandardCharsets.UTF_8));
                }
            }
        }
        
        return filterKeys(keys, pattern, limit);
    }
    
    private Map<String, Object> entryToMap(CacheEntry entry) {
        java.util.HashMap<String, Object> map = new java.util.HashMap<>();
        map.put("key", entry.getKey());
        map.put("valueType", entry.getValueType().name());
        map.put("stringValue", entry.getStringValue());
        map.put("jsonValue", entry.getJsonValue() != null ? JsonUtils.toJson(entry.getJsonValue()) : null);
        map.put("ttlSeconds", entry.getTtlSeconds());
        map.put("createdAt", entry.getCreatedAt() != null ? entry.getCreatedAt().toEpochMilli() : null);
        map.put("updatedAt", entry.getUpdatedAt() != null ? entry.getUpdatedAt().toEpochMilli() : null);
        map.put("expiresAt", entry.getExpiresAt() != null ? entry.getExpiresAt().toEpochMilli() : null);
        map.put("version", entry.getVersion());
        map.put("accessCount", entry.getAccessCount());
        map.put("lastAccessedAt", entry.getLastAccessedAt() != null ? entry.getLastAccessedAt().toEpochMilli() : null);
        map.put("metadata", entry.getMetadata());
        return map;
    }
    
    private CacheEntry mapToEntry(Map<String, Object> map, String region) {
        CacheEntry.CacheEntryBuilder builder = CacheEntry.builder()
                .region(region)
                .key((String) map.get("key"))
                .valueType(CacheEntry.ValueType.valueOf((String) map.get("valueType")))
                .stringValue((String) map.get("stringValue"))
                .ttlSeconds(((Number) map.getOrDefault("ttlSeconds", -1L)).longValue())
                .version(((Number) map.getOrDefault("version", 1L)).longValue())
                .accessCount(((Number) map.getOrDefault("accessCount", 0L)).longValue())
                .metadata((String) map.get("metadata"));
        
        Object createdAt = map.get("createdAt");
        if (createdAt != null) {
            builder.createdAt(Instant.ofEpochMilli(((Number) createdAt).longValue()));
        }
        
        Object updatedAt = map.get("updatedAt");
        if (updatedAt != null) {
            builder.updatedAt(Instant.ofEpochMilli(((Number) updatedAt).longValue()));
        }
        
        Object expiresAt = map.get("expiresAt");
        if (expiresAt != null) {
            builder.expiresAt(Instant.ofEpochMilli(((Number) expiresAt).longValue()));
        }
        
        Object lastAccessedAt = map.get("lastAccessedAt");
        if (lastAccessedAt != null) {
            builder.lastAccessedAt(Instant.ofEpochMilli(((Number) lastAccessedAt).longValue()));
        }
        
        String jsonValue = (String) map.get("jsonValue");
        if (jsonValue != null) {
            builder.jsonValue(JsonUtils.parse(jsonValue));
        }
        
        return builder.build();
    }
    
    // ==================== Compaction Operations ====================
    
    /**
     * Trigger manual compaction of all column families.
     * This reclaims disk space by removing deleted/expired entries from SST files.
     * 
     * @return CompactionResult with details about the compaction operation
     */
    public CompactionResult compact() {
        if (!available || db == null) {
            return new CompactionResult(false, "RocksDB not available", 0, 0, 0);
        }
        
        long startTime = System.currentTimeMillis();
        long sizeBeforeBytes = getDatabaseSizeBytes();
        int columnFamiliesCompacted = 0;
        
        try {
            log.info("Starting RocksDB compaction...");
            
            // Compact default column family
            if (defaultHandle != null) {
                db.compactRange(defaultHandle);
                columnFamiliesCompacted++;
            }
            
            // Compact regions column family
            if (regionsHandle != null) {
                db.compactRange(regionsHandle);
                columnFamiliesCompacted++;
            }
            
            // Compact all entry column families
            for (Map.Entry<String, ColumnFamilyHandle> entry : columnFamilyHandles.entrySet()) {
                try {
                    db.compactRange(entry.getValue());
                    columnFamiliesCompacted++;
                    log.debug("Compacted column family: {}", entry.getKey());
                } catch (RocksDBException e) {
                    log.warn("Failed to compact column family '{}': {}", entry.getKey(), e.getMessage());
                }
            }
            
            long duration = System.currentTimeMillis() - startTime;
            long sizeAfterBytes = getDatabaseSizeBytes();
            long reclaimedBytes = sizeBeforeBytes - sizeAfterBytes;
            
            log.info("RocksDB compaction completed: {} column families, duration={}ms, " +
                    "size before={}MB, size after={}MB, reclaimed={}MB",
                    columnFamiliesCompacted, duration,
                    sizeBeforeBytes / (1024 * 1024),
                    sizeAfterBytes / (1024 * 1024),
                    reclaimedBytes / (1024 * 1024));
            
            return new CompactionResult(true, "Compaction successful", 
                    columnFamiliesCompacted, duration, reclaimedBytes);
            
        } catch (RocksDBException e) {
            log.error("RocksDB compaction failed: {}", e.getMessage(), e);
            return new CompactionResult(false, "Compaction failed: " + e.getMessage(), 
                    columnFamiliesCompacted, System.currentTimeMillis() - startTime, 0);
        }
    }
    
    /**
     * Get the total size of the RocksDB database directory in bytes.
     */
    public long getDatabaseSizeBytes() {
        File dbDir = new File(dbPath);
        return calculateDirectorySize(dbDir);
    }
    
    private long calculateDirectorySize(File dir) {
        if (!dir.exists()) {
            return 0;
        }
        
        long size = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    size += file.length();
                } else if (file.isDirectory()) {
                    size += calculateDirectorySize(file);
                }
            }
        }
        return size;
    }
    
    /**
     * Get RocksDB statistics and storage info.
     */
    public RocksDbStats getStats() {
        long sizeBytes = getDatabaseSizeBytes();
        int regionCount = columnFamilyHandles.size();
        
        // Count total entries across all regions
        long totalEntries = 0;
        for (CacheRegion region : loadAllRegions()) {
            totalEntries += countNonExpiredEntries(region.getName());
        }
        
        return new RocksDbStats(
                dbPath,
                sizeBytes,
                sizeBytes / (1024.0 * 1024.0),
                regionCount,
                totalEntries,
                available
        );
    }
    
    /**
     * Result of a compaction operation.
     */
    public record CompactionResult(
            boolean success,
            String message,
            int columnFamiliesCompacted,
            long durationMs,
            long reclaimedBytes
    ) {
        public double reclaimedMB() {
            return reclaimedBytes / (1024.0 * 1024.0);
        }
    }
    
    /**
     * RocksDB statistics.
     */
    public record RocksDbStats(
            String path,
            long sizeBytes,
            double sizeMB,
            int regionCount,
            long totalEntries,
            boolean available
    ) {}
}
