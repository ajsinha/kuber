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
 * Each region gets its own dedicated RocksDB instance for better concurrency and isolation.
 * Region metadata is stored as a JSON file within each region's directory.
 * 
 * Directory structure:
 * - {basePath}/{regionName}/ - Separate RocksDB instance for each region's entries
 * - {basePath}/{regionName}/_region.json - Region metadata file
 */
@Slf4j
public class RocksDbPersistenceStore extends AbstractPersistenceStore {
    
    private static final String REGION_METADATA_FILE = "_region.json";
    
    private final KuberProperties properties;
    private final String basePath;
    
    // Separate RocksDB instance per region for entries
    private final Map<String, RocksDB> regionDatabases = new ConcurrentHashMap<>();
    private final Map<String, Options> regionOptions = new ConcurrentHashMap<>();
    
    static {
        RocksDB.loadLibrary();
    }
    
    public RocksDbPersistenceStore(KuberProperties properties) {
        this.properties = properties;
        this.basePath = properties.getPersistence().getRocksdb().getPath();
    }
    
    @Override
    public PersistenceType getType() {
        return PersistenceType.ROCKSDB;
    }
    
    @Override
    public void initialize() {
        log.info("Initializing RocksDB persistence store at: {}", basePath);
        log.info("Using separate RocksDB instance per region (no metadata database)");
        
        try {
            // Ensure base directory exists
            File baseDir = new File(basePath);
            if (!baseDir.exists()) {
                baseDir.mkdirs();
            }
            
            // Discover and open existing region databases
            discoverExistingRegions();
            
            available = true;
            log.info("RocksDB persistence store initialized successfully with {} region databases", 
                    regionDatabases.size());
        } catch (Exception e) {
            log.error("Failed to initialize RocksDB persistence store: {}", e.getMessage(), e);
            available = false;
        }
    }
    
    private void discoverExistingRegions() {
        // Discover and open existing region databases from directory structure
        // Directory names ARE the region names (with validation, no sanitization needed)
        File baseDir = new File(basePath);
        File[] subdirs = baseDir.listFiles(File::isDirectory);
        
        if (subdirs != null) {
            for (File subdir : subdirs) {
                String dirName = subdir.getName();
                // Skip hidden directories and validate region name
                if (!dirName.startsWith("_") && !dirName.startsWith(".") && isValidRegionName(dirName)) {
                    try {
                        // Open the database for this region and add to map
                        RocksDB db = getOrCreateRegionDatabase(dirName);
                        if (db != null) {
                            log.info("Discovered and opened region database: {}", dirName);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to open discovered region database '{}': {}", dirName, e.getMessage());
                    }
                }
            }
            if (!regionDatabases.isEmpty()) {
                log.info("Discovered and opened {} existing region databases", regionDatabases.size());
            }
        }
    }
    
    /**
     * Validate region name: alphanumeric and underscore only, at least one alphabetic character.
     */
    public static boolean isValidRegionName(String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }
        // Must contain only alphanumeric and underscore
        if (!name.matches("^[a-zA-Z0-9_]+$")) {
            return false;
        }
        // Must contain at least one alphabetic character
        if (!name.matches(".*[a-zA-Z].*")) {
            return false;
        }
        return true;
    }
    
    private RocksDB getOrCreateRegionDatabase(String region) {
        return regionDatabases.computeIfAbsent(region, this::openRegionDatabase);
    }
    
    private RocksDB openRegionDatabase(String region) {
        // Region names are validated to contain only alphanumeric and underscore
        // so we can use them directly as directory names
        String regionPath = basePath + File.separator + region;
        
        try {
            File regionDir = new File(regionPath);
            if (!regionDir.exists()) {
                regionDir.mkdirs();
            }
            
            Options options = new Options()
                    .setCreateIfMissing(true)
                    .setMaxBackgroundJobs(4)
                    .setMaxOpenFiles(-1)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setWriteBufferSize(64 * 1024 * 1024)  // 64MB write buffer
                    .setMaxWriteBufferNumber(3)
                    .setTargetFileSizeBase(64 * 1024 * 1024);  // 64MB SST files
            
            regionOptions.put(region, options);
            RocksDB db = RocksDB.open(options, regionPath);
            
            log.info("Opened RocksDB instance for region '{}' at: {}", region, regionPath);
            return db;
            
        } catch (RocksDBException e) {
            log.error("Failed to open RocksDB for region '{}': {}", region, e.getMessage(), e);
            throw new RuntimeException("Failed to open region database", e);
        }
    }
    
    @Override
    public void shutdown() {
        log.info("Shutting down RocksDB persistence store...");
        available = false;
        
        for (Map.Entry<String, RocksDB> entry : regionDatabases.entrySet()) {
            try {
                entry.getValue().close();
                log.debug("Closed region database: {}", entry.getKey());
            } catch (Exception e) {
                log.warn("Error closing region database '{}': {}", entry.getKey(), e.getMessage());
            }
        }
        regionDatabases.clear();
        
        for (Options options : regionOptions.values()) {
            options.close();
        }
        regionOptions.clear();
        
        log.info("RocksDB persistence store shutdown complete");
    }
    
    // ==================== Region Operations ====================
    
    @Override
    public void saveRegion(CacheRegion region) {
        try {
            // Ensure region database exists
            getOrCreateRegionDatabase(region.getName());
            
            // Save region metadata to JSON file in region directory
            String regionPath = basePath + File.separator + region.getName();
            File metadataFile = new File(regionPath, REGION_METADATA_FILE);
            
            String json = JsonUtils.toJson(regionToMap(region));
            java.nio.file.Files.writeString(metadataFile.toPath(), json, StandardCharsets.UTF_8);
            
            log.info("Saved region '{}' metadata to {}", region.getName(), metadataFile.getPath());
        } catch (Exception e) {
            log.error("Failed to save region '{}': {}", region.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to save region", e);
        }
    }
    
    @Override
    public List<CacheRegion> loadAllRegions() {
        List<CacheRegion> regions = new ArrayList<>();
        
        // Load regions based on discovered directories (databases are already opened in discoverExistingRegions)
        for (String regionName : regionDatabases.keySet()) {
            CacheRegion region = loadRegionMetadataFromFile(regionName);
            
            // If no metadata file, create a default CacheRegion from directory name
            if (region == null) {
                region = CacheRegion.builder()
                        .name(regionName)
                        .description("Discovered region")
                        .collectionName("kuber_" + regionName.toLowerCase())
                        .createdAt(Instant.now())
                        .updatedAt(Instant.now())
                        .build();
                log.info("Created default region metadata for discovered region '{}'", regionName);
                
                // Save the metadata for next time
                try {
                    saveRegion(region);
                } catch (Exception e) {
                    log.warn("Failed to save metadata for discovered region '{}': {}", regionName, e.getMessage());
                }
            }
            
            regions.add(region);
        }
        
        log.info("Loaded {} regions from RocksDB (directory discovery)", regions.size());
        return regions;
    }
    
    /**
     * Load region metadata from JSON file in region directory.
     */
    private CacheRegion loadRegionMetadataFromFile(String regionName) {
        try {
            String regionPath = basePath + File.separator + regionName;
            File metadataFile = new File(regionPath, REGION_METADATA_FILE);
            
            if (metadataFile.exists()) {
                String json = java.nio.file.Files.readString(metadataFile.toPath(), StandardCharsets.UTF_8);
                Map<String, Object> map = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {});
                CacheRegion region = mapToRegion(map);
                log.debug("Loaded region metadata for '{}' from file", regionName);
                return region;
            }
        } catch (Exception e) {
            log.warn("Failed to load metadata file for region '{}': {}", regionName, e.getMessage());
        }
        return null;
    }
    
    @Override
    public CacheRegion loadRegion(String name) {
        return loadRegionMetadataFromFile(name);
    }
    
    @Override
    public void deleteRegion(String name) {
        try {
            RocksDB regionDb = regionDatabases.remove(name);
            if (regionDb != null) {
                regionDb.close();
            }
            
            Options options = regionOptions.remove(name);
            if (options != null) {
                options.close();
            }
            
            // Delete entire region directory (includes metadata file and RocksDB data)
            String regionPath = basePath + File.separator + name;
            deleteDirectory(new File(regionPath));
            
            log.info("Deleted region '{}' and its database from RocksDB", name);
        } catch (Exception e) {
            log.error("Failed to delete region '{}': {}", name, e.getMessage(), e);
            throw new RuntimeException("Failed to delete region", e);
        }
    }
    
    private void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            dir.delete();
        }
    }
    
    @Override
    public void purgeRegion(String name) {
        try {
            RocksDB regionDb = regionDatabases.get(name);
            if (regionDb != null) {
                regionDb.close();
                regionDatabases.remove(name);
                
                Options options = regionOptions.remove(name);
                if (options != null) {
                    options.close();
                }
                
                String regionPath = basePath + File.separator + name;
                deleteDirectory(new File(regionPath));
                
                openRegionDatabase(name);
            }
            
            log.info("Purged region '{}' in RocksDB", name);
        } catch (Exception e) {
            log.error("Failed to purge region '{}': {}", name, e.getMessage(), e);
            throw new RuntimeException("Failed to purge region", e);
        }
    }
    
    // ==================== Entry Operations ====================
    
    @Override
    public void saveEntry(CacheEntry entry) {
        RocksDB regionDb = getOrCreateRegionDatabase(entry.getRegion());
        
        try {
            String json = JsonUtils.toJson(entryToMap(entry));
            regionDb.put(
                    entry.getKey().getBytes(StandardCharsets.UTF_8),
                    json.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            log.error("Failed to save entry '{}' to region '{}': {}", 
                    entry.getKey(), entry.getRegion(), e.getMessage(), e);
            throw new RuntimeException("Failed to save entry", e);
        }
    }
    
    @Override
    public void saveEntries(List<CacheEntry> entries) {
        if (entries.isEmpty()) return;
        
        Map<String, List<CacheEntry>> entriesByRegion = new ConcurrentHashMap<>();
        for (CacheEntry entry : entries) {
            entriesByRegion.computeIfAbsent(entry.getRegion(), k -> new ArrayList<>()).add(entry);
        }
        
        for (Map.Entry<String, List<CacheEntry>> regionEntries : entriesByRegion.entrySet()) {
            String region = regionEntries.getKey();
            RocksDB regionDb = getOrCreateRegionDatabase(region);
            
            try (WriteBatch batch = new WriteBatch();
                 WriteOptions writeOptions = new WriteOptions()) {
                
                for (CacheEntry entry : regionEntries.getValue()) {
                    String json = JsonUtils.toJson(entryToMap(entry));
                    batch.put(
                            entry.getKey().getBytes(StandardCharsets.UTF_8),
                            json.getBytes(StandardCharsets.UTF_8));
                }
                
                regionDb.write(writeOptions, batch);
            } catch (RocksDBException e) {
                log.error("Failed to batch save entries to region '{}': {}", region, e.getMessage(), e);
                throw new RuntimeException("Failed to batch save entries", e);
            }
        }
    }
    
    @Override
    public CacheEntry loadEntry(String region, String key) {
        RocksDB regionDb = regionDatabases.get(region);
        if (regionDb == null) return null;
        
        try {
            byte[] value = regionDb.get(key.getBytes(StandardCharsets.UTF_8));
            if (value != null) {
                String json = new String(value, StandardCharsets.UTF_8);
                Map<String, Object> map = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {});
                return mapToEntry(map, region);
            }
        } catch (Exception e) {
            log.error("Failed to load entry '{}' from region '{}': {}", key, region, e.getMessage(), e);
        }
        return null;
    }
    
    @Override
    public List<CacheEntry> loadEntries(String region, int limit) {
        List<CacheEntry> entries = new ArrayList<>();
        RocksDB regionDb = regionDatabases.get(region);
        if (regionDb == null) return entries;
        
        // Load all non-expired entries first
        List<CacheEntry> allEntries = new ArrayList<>();
        try (RocksIterator iterator = regionDb.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                try {
                    String json = new String(iterator.value(), StandardCharsets.UTF_8);
                    Map<String, Object> map = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {});
                    CacheEntry entry = mapToEntry(map, region);
                    if (!entry.isExpired()) {
                        allEntries.add(entry);
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse entry: {}", e.getMessage());
                }
            }
        }
        
        // Sort by last accessed (most recent first), then by updated time
        allEntries.sort((a, b) -> {
            // Prefer lastAccessedAt if available, otherwise use updatedAt
            Instant aTime = a.getLastAccessedAt() != null ? a.getLastAccessedAt() : a.getUpdatedAt();
            Instant bTime = b.getLastAccessedAt() != null ? b.getLastAccessedAt() : b.getUpdatedAt();
            if (aTime == null && bTime == null) return 0;
            if (aTime == null) return 1;
            if (bTime == null) return -1;
            return bTime.compareTo(aTime); // Descending order (most recent first)
        });
        
        // Return top 'limit' entries
        return allEntries.stream().limit(limit).collect(java.util.stream.Collectors.toList());
    }
    
    @Override
    public void deleteEntry(String region, String key) {
        RocksDB regionDb = regionDatabases.get(region);
        if (regionDb == null) return;
        
        try {
            regionDb.delete(key.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException e) {
            log.error("Failed to delete entry '{}' from region '{}': {}", key, region, e.getMessage(), e);
            throw new RuntimeException("Failed to delete entry", e);
        }
    }
    
    @Override
    public void deleteEntries(String region, List<String> keys) {
        if (keys.isEmpty()) return;
        
        RocksDB regionDb = regionDatabases.get(region);
        if (regionDb == null) return;
        
        try (WriteBatch batch = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions()) {
            
            for (String key : keys) {
                batch.delete(key.getBytes(StandardCharsets.UTF_8));
            }
            regionDb.write(writeOptions, batch);
        } catch (RocksDBException e) {
            log.error("Failed to batch delete entries from region '{}': {}", region, e.getMessage(), e);
            throw new RuntimeException("Failed to batch delete entries", e);
        }
    }
    
    @Override
    public long countEntries(String region) {
        RocksDB regionDb = regionDatabases.get(region);
        if (regionDb == null) return 0;
        
        long count = 0;
        try (RocksIterator iterator = regionDb.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * Fast estimate of entry count using RocksDB native property.
     * This is approximate but O(1) - suitable for dashboard display.
     */
    @Override
    public long estimateEntryCount(String region) {
        RocksDB regionDb = regionDatabases.get(region);
        if (regionDb == null) return 0;
        
        try {
            // Use RocksDB's native estimate - very fast O(1) operation
            return regionDb.getLongProperty("rocksdb.estimate-num-keys");
        } catch (Exception e) {
            log.debug("Could not get estimate-num-keys for region '{}': {}", region, e.getMessage());
            return 0;
        }
    }
    
    @Override
    public List<String> getKeys(String region, String pattern, int limit) {
        List<String> keys = new ArrayList<>();
        RocksDB regionDb = regionDatabases.get(region);
        if (regionDb == null) return keys;
        
        try (RocksIterator iterator = regionDb.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                keys.add(new String(iterator.key(), StandardCharsets.UTF_8));
            }
        }
        return filterKeys(keys, pattern, limit);
    }
    
    @Override
    public long deleteExpiredEntries(String region) {
        RocksDB regionDb = regionDatabases.get(region);
        if (regionDb == null) return 0;
        
        List<String> expiredKeys = new ArrayList<>();
        long now = System.currentTimeMillis();
        
        try (RocksIterator iterator = regionDb.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                try {
                    String json = new String(iterator.value(), StandardCharsets.UTF_8);
                    Map<String, Object> map = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {});
                    Object expiresAt = map.get("expiresAt");
                    if (expiresAt != null && ((Number) expiresAt).longValue() < now) {
                        expiredKeys.add(new String(iterator.key(), StandardCharsets.UTF_8));
                    }
                } catch (Exception e) { /* skip */ }
            }
        }
        
        if (!expiredKeys.isEmpty()) {
            deleteEntries(region, expiredKeys);
            log.info("Deleted {} expired entries from region '{}' in RocksDB", expiredKeys.size(), region);
        }
        return expiredKeys.size();
    }
    
    @Override
    public long deleteAllExpiredEntries() {
        long totalDeleted = 0;
        for (String region : regionDatabases.keySet()) {
            totalDeleted += deleteExpiredEntries(region);
        }
        return totalDeleted;
    }
    
    @Override
    public long countNonExpiredEntries(String region) {
        RocksDB regionDb = regionDatabases.get(region);
        if (regionDb == null) return 0;
        
        long count = 0;
        long now = System.currentTimeMillis();
        
        try (RocksIterator iterator = regionDb.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                try {
                    String json = new String(iterator.value(), StandardCharsets.UTF_8);
                    Map<String, Object> map = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {});
                    Object expiresAt = map.get("expiresAt");
                    if (expiresAt == null || ((Number) expiresAt).longValue() >= now) {
                        count++;
                    }
                } catch (Exception e) {
                    count++;
                }
            }
        }
        return count;
    }
    
    @Override
    public List<String> getNonExpiredKeys(String region, String pattern, int limit) {
        List<String> keys = new ArrayList<>();
        RocksDB regionDb = regionDatabases.get(region);
        if (regionDb == null) return keys;
        
        long now = System.currentTimeMillis();
        
        try (RocksIterator iterator = regionDb.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                try {
                    String json = new String(iterator.value(), StandardCharsets.UTF_8);
                    Map<String, Object> map = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {});
                    Object expiresAt = map.get("expiresAt");
                    if (expiresAt == null || ((Number) expiresAt).longValue() >= now) {
                        keys.add(new String(iterator.key(), StandardCharsets.UTF_8));
                    }
                } catch (Exception e) {
                    keys.add(new String(iterator.key(), StandardCharsets.UTF_8));
                }
            }
        }
        return filterKeys(keys, pattern, limit);
    }
    
    // ==================== Helper Methods ====================
    
    private Map<String, Object> regionToMap(CacheRegion region) {
        java.util.HashMap<String, Object> map = new java.util.HashMap<>();
        map.put("name", region.getName());
        map.put("description", region.getDescription());
        map.put("captive", region.isCaptive());
        map.put("maxEntries", region.getMaxEntries());
        map.put("defaultTtlSeconds", region.getDefaultTtlSeconds());
        map.put("entryCount", region.getEntryCount());
        map.put("createdAt", region.getCreatedAt() != null ? region.getCreatedAt().toEpochMilli() : null);
        map.put("updatedAt", region.getUpdatedAt() != null ? region.getUpdatedAt().toEpochMilli() : null);
        map.put("createdBy", region.getCreatedBy());
        map.put("enabled", region.isEnabled());
        map.put("collectionName", region.getCollectionName());
        return map;
    }
    
    private CacheRegion mapToRegion(Map<String, Object> map) {
        return CacheRegion.builder()
                .name((String) map.get("name"))
                .description((String) map.get("description"))
                .captive(Boolean.TRUE.equals(map.get("captive")))
                .maxEntries(((Number) map.getOrDefault("maxEntries", -1L)).longValue())
                .defaultTtlSeconds(((Number) map.getOrDefault("defaultTtlSeconds", -1L)).longValue())
                .entryCount(((Number) map.getOrDefault("entryCount", 0L)).longValue())
                .createdAt(map.get("createdAt") != null ? 
                        Instant.ofEpochMilli(((Number) map.get("createdAt")).longValue()) : null)
                .updatedAt(map.get("updatedAt") != null ? 
                        Instant.ofEpochMilli(((Number) map.get("updatedAt")).longValue()) : null)
                .createdBy((String) map.get("createdBy"))
                .enabled(map.get("enabled") == null || Boolean.TRUE.equals(map.get("enabled")))
                .collectionName((String) map.get("collectionName"))
                .build();
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
        if (createdAt != null) builder.createdAt(Instant.ofEpochMilli(((Number) createdAt).longValue()));
        
        Object updatedAt = map.get("updatedAt");
        if (updatedAt != null) builder.updatedAt(Instant.ofEpochMilli(((Number) updatedAt).longValue()));
        
        Object expiresAt = map.get("expiresAt");
        if (expiresAt != null) builder.expiresAt(Instant.ofEpochMilli(((Number) expiresAt).longValue()));
        
        Object lastAccessedAt = map.get("lastAccessedAt");
        if (lastAccessedAt != null) builder.lastAccessedAt(Instant.ofEpochMilli(((Number) lastAccessedAt).longValue()));
        
        String jsonValue = (String) map.get("jsonValue");
        if (jsonValue != null) builder.jsonValue(JsonUtils.parse(jsonValue));
        
        return builder.build();
    }
    
    // ==================== Compaction Operations ====================
    
    /**
     * Trigger manual compaction of all region databases.
     */
    public CompactionResult compact() {
        if (!available) {
            return new CompactionResult(false, "RocksDB not available", 0, 0, 0);
        }
        
        long startTime = System.currentTimeMillis();
        long sizeBeforeBytes = getDatabaseSizeBytes();
        int regionsCompacted = 0;
        
        log.info("Starting RocksDB compaction across {} region databases...", regionDatabases.size());
        
        for (Map.Entry<String, RocksDB> entry : regionDatabases.entrySet()) {
            try {
                entry.getValue().compactRange();
                regionsCompacted++;
                log.debug("Compacted region database: {}", entry.getKey());
            } catch (RocksDBException e) {
                log.warn("Failed to compact region '{}': {}", entry.getKey(), e.getMessage());
            }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        long sizeAfterBytes = getDatabaseSizeBytes();
        long reclaimedBytes = sizeBeforeBytes - sizeAfterBytes;
        
        log.info("RocksDB compaction completed: {} regions, duration={}ms, reclaimed={}MB",
                regionsCompacted, duration, reclaimedBytes / (1024 * 1024));
        
        return new CompactionResult(true, "Compaction successful", regionsCompacted, duration, reclaimedBytes);
    }
    
    public CompactionResult compactRegion(String region) {
        RocksDB regionDb = regionDatabases.get(region);
        if (regionDb == null) {
            return new CompactionResult(false, "Region not found: " + region, 0, 0, 0);
        }
        
        long startTime = System.currentTimeMillis();
        String regionPath = basePath + File.separator + region;
        long sizeBefore = calculateDirectorySize(new File(regionPath));
        
        try {
            regionDb.compactRange();
            long duration = System.currentTimeMillis() - startTime;
            long sizeAfter = calculateDirectorySize(new File(regionPath));
            long reclaimedBytes = sizeBefore - sizeAfter;
            
            log.info("Compacted region '{}': duration={}ms, reclaimed={}MB", 
                    region, duration, reclaimedBytes / (1024 * 1024));
            
            return new CompactionResult(true, "Region compaction successful", 1, duration, reclaimedBytes);
            
        } catch (RocksDBException e) {
            log.error("Failed to compact region '{}': {}", region, e.getMessage(), e);
            return new CompactionResult(false, "Compaction failed: " + e.getMessage(), 
                    0, System.currentTimeMillis() - startTime, 0);
        }
    }
    
    public long getDatabaseSizeBytes() {
        return calculateDirectorySize(new File(basePath));
    }
    
    public long getRegionSizeBytes(String region) {
        String regionPath = basePath + File.separator + region;
        return calculateDirectorySize(new File(regionPath));
    }
    
    private long calculateDirectorySize(File dir) {
        if (!dir.exists()) return 0;
        
        long size = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                size += file.isFile() ? file.length() : calculateDirectorySize(file);
            }
        }
        return size;
    }
    
    public RocksDbStats getStats() {
        long sizeBytes = getDatabaseSizeBytes();
        int regionCount = regionDatabases.size();
        
        long totalEntries = 0;
        for (String region : regionDatabases.keySet()) {
            totalEntries += countNonExpiredEntries(region);
        }
        
        return new RocksDbStats(basePath, sizeBytes, sizeBytes / (1024.0 * 1024.0), 
                regionCount, totalEntries, available);
    }
    
    public List<String> getRegionNames() {
        return new ArrayList<>(regionDatabases.keySet());
    }
    
    public record CompactionResult(
            boolean success, String message, int regionsCompacted, long durationMs, long reclaimedBytes
    ) {
        public double reclaimedMB() { return reclaimedBytes / (1024.0 * 1024.0); }
    }
    
    public record RocksDbStats(
            String path, long sizeBytes, double sizeMB, int regionCount, long totalEntries, boolean available
    ) {}
}
