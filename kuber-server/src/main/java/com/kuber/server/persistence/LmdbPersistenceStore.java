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
import org.lmdbjava.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * LMDB (Lightning Memory-Mapped Database) implementation of PersistenceStore.
 * Each region gets its own dedicated LMDB environment for better concurrency and isolation.
 * Region metadata is stored as a JSON file within each region's directory.
 * 
 * LMDB Benefits:
 * - Memory-mapped I/O for extremely fast reads (zero-copy)
 * - ACID transactions with MVCC
 * - Reader/writer lock (multiple readers, single writer)
 * - No recovery needed after crash (copy-on-write B+ tree)
 * - Very low memory overhead
 * 
 * Directory structure:
 * - {basePath}/{regionName}/ - Separate LMDB environment for each region
 * - {basePath}/{regionName}/_region.json - Region metadata file
 * - {basePath}/{regionName}/data.mdb - LMDB data file
 * - {basePath}/{regionName}/lock.mdb - LMDB lock file
 * 
 * @version 2.3.0
 */
@Slf4j
public class LmdbPersistenceStore extends AbstractPersistenceStore {
    
    private static final String REGION_METADATA_FILE = "_region.json";
    private static final String ENTRIES_DB_NAME = "entries";
    private static final long DEFAULT_MAP_SIZE = 1024L * 1024L * 1024L; // 1GB default
    
    // Batch size for writes - keeps transactions short to avoid blocking readers
    private static final int WRITE_BATCH_SIZE = 1000;
    
    // Reusable TypeReference to avoid creating new anonymous classes in loops (memory leak prevention)
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<Map<String, Object>>() {};
    
    private final KuberProperties properties;
    private final String basePath;
    private final long mapSize;
    
    // Separate LMDB environment per region
    private final Map<String, Env<ByteBuffer>> regionEnvironments = new ConcurrentHashMap<>();
    private final Map<String, Dbi<ByteBuffer>> regionDatabases = new ConcurrentHashMap<>();
    
    // Lock for environment creation to prevent race conditions
    private final Object envCreationLock = new Object();
    
    // Guard against double shutdown
    private volatile boolean alreadyShutdown = false;
    
    public LmdbPersistenceStore(KuberProperties properties) {
        this.properties = properties;
        this.basePath = properties.getPersistence().getLmdb().getPath();
        this.mapSize = properties.getPersistence().getLmdb().getMapSize();
    }
    
    @Override
    public PersistenceType getType() {
        return PersistenceType.LMDB;
    }
    
    @Override
    public void initialize() {
        log.info("Initializing LMDB persistence store at: {}", basePath);
        log.info("LMDB map size: {} MB", mapSize / (1024 * 1024));
        log.info("Using separate LMDB environment per region");
        
        try {
            // Ensure base directory exists
            File baseDir = new File(basePath);
            if (!baseDir.exists()) {
                baseDir.mkdirs();
            }
            
            // Discover and open existing region environments
            discoverExistingRegions();
            
            // Configure batched async persistence (v1.6.2)
            int batchSize = properties.getCache().getPersistenceBatchSize();
            int flushIntervalMs = properties.getCache().getPersistenceIntervalMs();
            configureBatching(batchSize, flushIntervalMs);
            
            available = true;
            log.info("LMDB persistence store initialized successfully with {} region environments", 
                    regionEnvironments.size());
        } catch (Exception e) {
            log.error("Failed to initialize LMDB persistence store: {}", e.getMessage(), e);
            available = false;
        }
    }
    
    private void discoverExistingRegions() {
        File baseDir = new File(basePath);
        File[] subdirs = baseDir.listFiles(File::isDirectory);
        
        if (subdirs != null) {
            for (File subdir : subdirs) {
                String dirName = subdir.getName();
                // Skip hidden directories and validate region name
                if (!dirName.startsWith(".") && !dirName.startsWith("_") && isValidRegionName(dirName)) {
                    try {
                        // Check if this is a valid LMDB directory (has data.mdb)
                        File dataFile = new File(subdir, "data.mdb");
                        if (dataFile.exists()) {
                            Env<ByteBuffer> env = getOrCreateRegionEnvironment(dirName);
                            if (env != null) {
                                log.info("Discovered existing LMDB region: {}", dirName);
                            }
                        } else {
                            // Check for region metadata to create new env
                            File metadataFile = new File(subdir, REGION_METADATA_FILE);
                            if (metadataFile.exists()) {
                                Env<ByteBuffer> env = getOrCreateRegionEnvironment(dirName);
                                if (env != null) {
                                    log.info("Created LMDB environment for region: {}", dirName);
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Failed to open LMDB region '{}': {}", dirName, e.getMessage());
                    }
                }
            }
            
            if (!regionEnvironments.isEmpty()) {
                log.info("Discovered and opened {} existing region environments", regionEnvironments.size());
            }
        }
    }
    
    private boolean isValidRegionName(String name) {
        if (name == null || name.isEmpty() || name.length() > 64) {
            return false;
        }
        return Pattern.matches("^[a-zA-Z][a-zA-Z0-9_]*$", name);
    }
    
    private Env<ByteBuffer> getOrCreateRegionEnvironment(String region) {
        // Fast path: check if already exists
        Env<ByteBuffer> existingEnv = regionEnvironments.get(region);
        if (existingEnv != null) {
            return existingEnv;
        }
        
        // Slow path: synchronized creation
        synchronized (envCreationLock) {
            // Double-check after acquiring lock
            existingEnv = regionEnvironments.get(region);
            if (existingEnv != null) {
                return existingEnv;
            }
            
            // Create new environment
            Env<ByteBuffer> newEnv = openRegionEnvironment(region);
            regionEnvironments.put(region, newEnv);
            return newEnv;
        }
    }
    
    private Env<ByteBuffer> openRegionEnvironment(String region) {
        String regionPath = basePath + File.separator + region;
        
        try {
            File regionDir = new File(regionPath);
            if (!regionDir.exists()) {
                regionDir.mkdirs();
            }
            
            // Create LMDB environment
            Env<ByteBuffer> env = Env.create()
                    .setMapSize(mapSize)
                    .setMaxDbs(1)
                    .setMaxReaders(126)
                    .open(regionDir, EnvFlags.MDB_WRITEMAP, EnvFlags.MDB_MAPASYNC);
            
            // Create/open the entries database
            Dbi<ByteBuffer> dbi = env.openDbi(ENTRIES_DB_NAME, DbiFlags.MDB_CREATE);
            regionDatabases.put(region, dbi);
            
            log.info("Opened LMDB environment for region '{}' at: {}", region, regionPath);
            return env;
            
        } catch (Exception e) {
            log.error("Failed to open LMDB environment for region '{}': {}", region, e.getMessage(), e);
            throw new RuntimeException("Failed to open region environment", e);
        }
    }
    
    @Override
    public void shutdown() {
        // Guard against double shutdown
        if (alreadyShutdown) {
            log.debug("LMDB shutdown already completed - skipping duplicate shutdown call");
            return;
        }
        alreadyShutdown = true;
        
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  LMDB GRACEFUL SHUTDOWN INITIATED                                   ║");
        log.info("╚════════════════════════════════════════════════════════════════════╝");
        
        // Mark as unavailable to prevent new operations
        available = false;
        
        // Step 1: Shutdown async save executor FIRST and wait for all pending saves
        log.info("Step 1: Shutting down async save executor...");
        shutdownAsyncExecutor();
        
        // Step 2: Give any remaining in-flight operations time to complete
        try {
            log.info("Step 2: Waiting for in-flight operations to complete...");
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        int totalRegions = regionEnvironments.size();
        int successCount = 0;
        int failCount = 0;
        
        log.info("Step 3: Closing {} region environment(s)...", totalRegions);
        
        // Close all region environments
        for (Map.Entry<String, Env<ByteBuffer>> entry : regionEnvironments.entrySet()) {
            String regionName = entry.getKey();
            try {
                gracefulCloseEnvironment(regionName, entry.getValue());
                successCount++;
            } catch (Exception e) {
                log.error("Failed to gracefully close region '{}': {}", regionName, e.getMessage(), e);
                failCount++;
                // Still try to close even if graceful shutdown failed
                try {
                    Dbi<ByteBuffer> dbi = regionDatabases.remove(regionName);
                    if (dbi != null) dbi.close();
                    entry.getValue().close();
                } catch (Exception closeEx) {
                    log.warn("Force close also failed for region '{}': {}", regionName, closeEx.getMessage());
                }
            }
        }
        
        regionEnvironments.clear();
        regionDatabases.clear();
        
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  LMDB SHUTDOWN COMPLETE                                             ║");
        log.info("║  Regions closed: {} success, {} failed                              ║", 
                String.format("%-3d", successCount), String.format("%-3d", failCount));
        log.info("╚════════════════════════════════════════════════════════════════════╝");
    }
    
    /**
     * Gracefully close an LMDB environment with proper sync.
     * 
     * The sequence is:
     * 1. Force sync to flush all pending writes to disk
     * 2. Close the database handle (Dbi)
     * 3. Wait for OS buffers to flush
     * 4. Close the environment
     * 
     * @param regionName Name of the region for logging
     * @param env The LMDB environment to close
     */
    private void gracefulCloseEnvironment(String regionName, Env<ByteBuffer> env) {
        log.info("Gracefully closing region '{}'...", regionName);
        long startTime = System.currentTimeMillis();
        
        try {
            // Step 1: Force sync to flush all pending writes
            log.debug("  [{}] Syncing data to disk...", regionName);
            env.sync(true);  // force=true for immediate sync
            log.debug("  [{}] Data synced", regionName);
            
            // Step 2: Close the database handle
            log.debug("  [{}] Closing database handle...", regionName);
            Dbi<ByteBuffer> dbi = regionDatabases.remove(regionName);
            if (dbi != null) {
                dbi.close();
            }
            log.debug("  [{}] Database handle closed", regionName);
            
            // Step 3: Wait for OS buffers to flush
            Thread.sleep(500);
            
            // Step 4: Close the environment
            log.debug("  [{}] Closing environment...", regionName);
            env.close();
            
            long elapsed = System.currentTimeMillis() - startTime;
            log.info("  [{}] Closed successfully in {}ms", regionName, elapsed);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during shutdown of region: " + regionName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to gracefully close region: " + regionName, e);
        }
    }
    
    // ==================== Region Operations ====================
    
    @Override
    public void saveRegion(CacheRegion region) {
        String regionPath = basePath + File.separator + region.getName();
        
        try {
            File regionDir = new File(regionPath);
            if (!regionDir.exists()) {
                regionDir.mkdirs();
            }
            
            // Ensure environment is created
            getOrCreateRegionEnvironment(region.getName());
            
            // Save metadata as JSON file
            File metadataFile = new File(regionDir, REGION_METADATA_FILE);
            Map<String, Object> metadata = regionToMap(region);
            String json = JsonUtils.toJson(metadata);
            Files.writeString(metadataFile.toPath(), json, StandardCharsets.UTF_8);
            
            log.debug("Saved region metadata: {}", region.getName());
        } catch (IOException e) {
            log.error("Failed to save region '{}': {}", region.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to save region", e);
        }
    }
    
    @Override
    public List<CacheRegion> loadAllRegions() {
        List<CacheRegion> regions = new ArrayList<>();
        File baseDir = new File(basePath);
        File[] subdirs = baseDir.listFiles(File::isDirectory);
        
        if (subdirs != null) {
            for (File subdir : subdirs) {
                String dirName = subdir.getName();
                if (!dirName.startsWith(".") && !dirName.startsWith("_") && isValidRegionName(dirName)) {
                    File metadataFile = new File(subdir, REGION_METADATA_FILE);
                    if (metadataFile.exists()) {
                        try {
                            String json = Files.readString(metadataFile.toPath(), StandardCharsets.UTF_8);
                            Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, MAP_TYPE_REF);
                            CacheRegion region = mapToRegion(map);
                            regions.add(region);
                        } catch (Exception e) {
                            log.warn("Failed to load region metadata from '{}': {}", dirName, e.getMessage());
                        }
                    }
                }
            }
        }
        
        return regions;
    }
    
    @Override
    public CacheRegion loadRegion(String name) {
        String metadataPath = basePath + File.separator + name + File.separator + REGION_METADATA_FILE;
        File metadataFile = new File(metadataPath);
        
        if (!metadataFile.exists()) {
            return null;
        }
        
        try {
            String json = Files.readString(metadataFile.toPath(), StandardCharsets.UTF_8);
            Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, MAP_TYPE_REF);
            return mapToRegion(map);
        } catch (Exception e) {
            log.error("Failed to load region '{}': {}", name, e.getMessage(), e);
            return null;
        }
    }
    
    @Override
    public void deleteRegion(String name) {
        // Close and remove the environment
        Dbi<ByteBuffer> dbi = regionDatabases.remove(name);
        if (dbi != null) {
            dbi.close();
        }
        Env<ByteBuffer> env = regionEnvironments.remove(name);
        if (env != null) {
            env.close();
        }
        
        // Delete the directory
        String regionPath = basePath + File.separator + name;
        File regionDir = new File(regionPath);
        if (regionDir.exists()) {
            deleteDirectory(regionDir);
        }
        
        log.info("Deleted region: {}", name);
    }
    
    private void deleteDirectory(File dir) {
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
    
    @Override
    public void purgeRegion(String name) {
        Env<ByteBuffer> env = regionEnvironments.get(name);
        Dbi<ByteBuffer> dbi = regionDatabases.get(name);
        
        if (env != null && dbi != null) {
            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                dbi.drop(txn);
                txn.commit();
                log.info("Purged all entries from region: {}", name);
            } catch (Exception e) {
                log.error("Failed to purge region '{}': {}", name, e.getMessage(), e);
            }
        }
    }
    
    // ==================== Entry Operations ====================
    
    @Override
    public void saveEntry(CacheEntry entry) {
        Env<ByteBuffer> env = getOrCreateRegionEnvironment(entry.getRegion());
        Dbi<ByteBuffer> dbi = regionDatabases.get(entry.getRegion());
        
        if (dbi == null) {
            dbi = env.openDbi(ENTRIES_DB_NAME, DbiFlags.MDB_CREATE);
            regionDatabases.put(entry.getRegion(), dbi);
        }
        
        try {
            String json = JsonUtils.toJson(entryToMap(entry));
            byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = json.getBytes(StandardCharsets.UTF_8);
            
            ByteBuffer keyBuffer = ByteBuffer.allocateDirect(keyBytes.length);
            keyBuffer.put(keyBytes).flip();
            
            ByteBuffer valueBuffer = ByteBuffer.allocateDirect(valueBytes.length);
            valueBuffer.put(valueBytes).flip();
            
            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                dbi.put(txn, keyBuffer, valueBuffer);
                txn.commit();
            }
        } catch (Exception e) {
            log.error("Failed to save entry '{}' to region '{}': {}", 
                    entry.getKey(), entry.getRegion(), e.getMessage(), e);
            throw new RuntimeException("Failed to save entry", e);
        }
    }
    
    @Override
    public void saveEntries(List<CacheEntry> entries) {
        if (entries.isEmpty()) {
            log.debug("saveEntries called with empty list - nothing to save");
            return;
        }
        
        log.info("LMDB saveEntries: received {} entries (batch size: {})", entries.size(), WRITE_BATCH_SIZE);
        long startTime = System.currentTimeMillis();
        
        // Group by region
        Map<String, List<CacheEntry>> entriesByRegion = new HashMap<>();
        for (CacheEntry entry : entries) {
            entriesByRegion.computeIfAbsent(entry.getRegion(), k -> new ArrayList<>()).add(entry);
        }
        
        log.info("LMDB saveEntries: grouped into {} region(s)", entriesByRegion.size());
        
        // Save each region's entries using batched transactions
        for (Map.Entry<String, List<CacheEntry>> regionEntries : entriesByRegion.entrySet()) {
            String region = regionEntries.getKey();
            List<CacheEntry> entriesToSave = regionEntries.getValue();
            int totalEntries = entriesToSave.size();
            int totalBatches = (totalEntries + WRITE_BATCH_SIZE - 1) / WRITE_BATCH_SIZE;
            
            log.info("LMDB saveEntries: processing region '{}' with {} entries in {} batch(es)", 
                    region, totalEntries, totalBatches);
            
            try {
                Env<ByteBuffer> env = getOrCreateRegionEnvironment(region);
                if (env == null) {
                    log.error("Failed to get LMDB environment for region '{}'", region);
                    continue;
                }
                
                Dbi<ByteBuffer> dbi = regionDatabases.get(region);
                if (dbi == null) {
                    log.warn("DBI not found for region '{}', creating new one", region);
                    dbi = env.openDbi(ENTRIES_DB_NAME, DbiFlags.MDB_CREATE);
                    regionDatabases.put(region, dbi);
                }
                
                int totalSaved = 0;
                int batchNum = 0;
                
                // Process in batches to avoid long-running transactions
                for (int i = 0; i < totalEntries; i += WRITE_BATCH_SIZE) {
                    batchNum++;
                    int end = Math.min(i + WRITE_BATCH_SIZE, totalEntries);
                    List<CacheEntry> batch = entriesToSave.subList(i, end);
                    
                    long batchStart = System.currentTimeMillis();
                    int batchSaved = saveBatch(env, dbi, region, batch);
                    long batchTime = System.currentTimeMillis() - batchStart;
                    
                    totalSaved += batchSaved;
                    
                    double progress = (totalSaved * 100.0) / totalEntries;
                    double entriesPerSec = batchSaved * 1000.0 / Math.max(1, batchTime);
                    
                    log.info("LMDB saveEntries: region '{}' batch {}/{} committed - {}/{} entries ({}%) - {} entries/sec", 
                            region, batchNum, totalBatches, totalSaved, totalEntries, 
                            String.format("%.1f", progress), String.format("%.0f", entriesPerSec));
                }
                
                log.info("LMDB saveEntries: region '{}' COMPLETE - {} entries saved", region, totalSaved);
                
            } catch (Throwable t) {
                handleSaveError(region, t);
            }
        }
        
        long totalTime = System.currentTimeMillis() - startTime;
        double avgRate = entries.size() * 1000.0 / Math.max(1, totalTime);
        log.info("LMDB saveEntries: COMPLETED {} entries in {}ms ({} entries/sec)", 
                entries.size(), totalTime, String.format("%.0f", avgRate));
    }
    
    /**
     * Save a batch of entries in a single transaction.
     * Returns the number of entries successfully saved.
     */
    private int saveBatch(Env<ByteBuffer> env, Dbi<ByteBuffer> dbi, String region, List<CacheEntry> batch) {
        Txn<ByteBuffer> txn = null;
        int savedCount = 0;
        
        try {
            txn = env.txnWrite();
            
            for (CacheEntry entry : batch) {
                try {
                    String json = JsonUtils.toJson(entryToMap(entry));
                    byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                    byte[] valueBytes = json.getBytes(StandardCharsets.UTF_8);
                    
                    ByteBuffer keyBuffer = ByteBuffer.allocateDirect(keyBytes.length);
                    keyBuffer.put(keyBytes).flip();
                    
                    ByteBuffer valueBuffer = ByteBuffer.allocateDirect(valueBytes.length);
                    valueBuffer.put(valueBytes).flip();
                    
                    dbi.put(txn, keyBuffer, valueBuffer);
                    savedCount++;
                } catch (Throwable t) {
                    log.error("LMDB saveBatch: error saving key '{}' in region '{}': {}", 
                            entry.getKey(), region, t.getMessage());
                    throw t;
                }
            }
            
            txn.commit();
            txn = null; // Mark as committed
            return savedCount;
            
        } catch (Throwable t) {
            if (txn != null) {
                try {
                    txn.abort();
                } catch (Throwable t2) {
                    log.error("LMDB saveBatch: failed to abort transaction: {}", t2.getMessage());
                }
            }
            throw new RuntimeException("Batch save failed for region '" + region + "'", t);
        } finally {
            if (txn != null) {
                try {
                    txn.close();
                } catch (Throwable t) {
                    log.error("LMDB saveBatch: failed to close transaction: {}", t.getMessage());
                }
            }
        }
    }
    
    /**
     * Handle save errors with helpful messages.
     */
    private void handleSaveError(String region, Throwable t) {
        String errorMsg = t.getMessage();
        if (t.getClass().getName().contains("MapFullException") || 
            (errorMsg != null && errorMsg.contains("mapsize reached"))) {
            log.error("╔════════════════════════════════════════════════════════════════════════════╗");
            log.error("║  LMDB MAP SIZE EXCEEDED                                                     ║");
            log.error("║  The LMDB database has reached its maximum size limit.                      ║");
            log.error("║                                                                             ║");
            log.error("║  TO FIX: Increase map size in application.yml:                              ║");
            log.error("║    kuber:                                                                   ║");
            log.error("║      persistence:                                                           ║");
            log.error("║        lmdb:                                                                ║");
            log.error("║          map-size: 21474836480  # 20GB (or larger)                          ║");
            log.error("║                                                                             ║");
            log.error("║  Common values: 10GB=10737418240, 20GB=21474836480, 50GB=53687091200        ║");
            log.error("║  NOTE: Restart required after changing map-size                             ║");
            log.error("╚════════════════════════════════════════════════════════════════════════════╝");
        }
        log.error("LMDB saveEntries: FATAL ERROR for region '{}': {} - {}", 
                region, t.getClass().getName(), t.getMessage(), t);
    }
    
    @Override
    public CacheEntry loadEntry(String region, String key) {
        Env<ByteBuffer> env = regionEnvironments.get(region);
        Dbi<ByteBuffer> dbi = regionDatabases.get(region);
        
        if (env == null || dbi == null) return null;
        
        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            ByteBuffer keyBuffer = ByteBuffer.allocateDirect(keyBytes.length);
            keyBuffer.put(keyBytes).flip();
            
            try (Txn<ByteBuffer> txn = env.txnRead()) {
                ByteBuffer valueBuffer = dbi.get(txn, keyBuffer);
                if (valueBuffer != null) {
                    byte[] valueBytes = new byte[valueBuffer.remaining()];
                    valueBuffer.get(valueBytes);
                    String json = new String(valueBytes, StandardCharsets.UTF_8);
                    Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, MAP_TYPE_REF);
                    return mapToEntry(map, region);
                }
            }
        } catch (Exception e) {
            log.error("Failed to load entry '{}' from region '{}': {}", key, region, e.getMessage(), e);
        }
        
        return null;
    }
    
    @Override
    public Map<String, CacheEntry> loadEntriesByKeys(String region, List<String> keys) {
        Map<String, CacheEntry> result = new HashMap<>();
        Env<ByteBuffer> env = regionEnvironments.get(region);
        Dbi<ByteBuffer> dbi = regionDatabases.get(region);
        
        if (env == null || dbi == null) return result;
        
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            for (String key : keys) {
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                ByteBuffer keyBuffer = ByteBuffer.allocateDirect(keyBytes.length);
                keyBuffer.put(keyBytes).flip();
                
                ByteBuffer valueBuffer = dbi.get(txn, keyBuffer);
                if (valueBuffer != null) {
                    byte[] valueBytes = new byte[valueBuffer.remaining()];
                    valueBuffer.get(valueBytes);
                    String json = new String(valueBytes, StandardCharsets.UTF_8);
                    Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, MAP_TYPE_REF);
                    result.put(key, mapToEntry(map, region));
                }
            }
        } catch (Exception e) {
            log.error("Failed to batch load entries from region '{}': {}", region, e.getMessage(), e);
        }
        
        return result;
    }
    
    @Override
    public List<CacheEntry> loadEntries(String region, int limit) {
        List<CacheEntry> entries = new ArrayList<>();
        Env<ByteBuffer> env = regionEnvironments.get(region);
        Dbi<ByteBuffer> dbi = regionDatabases.get(region);
        
        if (env == null || dbi == null) return entries;
        
        try (Txn<ByteBuffer> txn = env.txnRead();
             CursorIterable<ByteBuffer> cursor = dbi.iterate(txn)) {
            
            int count = 0;
            for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                if (limit > 0 && count >= limit) break;
                
                byte[] valueBytes = new byte[kv.val().remaining()];
                kv.val().get(valueBytes);
                String json = new String(valueBytes, StandardCharsets.UTF_8);
                Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, MAP_TYPE_REF);
                entries.add(mapToEntry(map, region));
                count++;
            }
        } catch (Exception e) {
            log.error("Failed to load entries from region '{}': {}", region, e.getMessage(), e);
        }
        
        return entries;
    }
    
    @Override
    public void deleteEntry(String region, String key) {
        Env<ByteBuffer> env = regionEnvironments.get(region);
        Dbi<ByteBuffer> dbi = regionDatabases.get(region);
        
        if (env == null || dbi == null) return;
        
        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            ByteBuffer keyBuffer = ByteBuffer.allocateDirect(keyBytes.length);
            keyBuffer.put(keyBytes).flip();
            
            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                dbi.delete(txn, keyBuffer);
                txn.commit();
            }
        } catch (Exception e) {
            log.error("Failed to delete entry '{}' from region '{}': {}", key, region, e.getMessage(), e);
        }
    }
    
    @Override
    public void deleteEntries(String region, List<String> keys) {
        Env<ByteBuffer> env = regionEnvironments.get(region);
        Dbi<ByteBuffer> dbi = regionDatabases.get(region);
        
        if (env == null || dbi == null) return;
        
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            for (String key : keys) {
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                ByteBuffer keyBuffer = ByteBuffer.allocateDirect(keyBytes.length);
                keyBuffer.put(keyBytes).flip();
                dbi.delete(txn, keyBuffer);
            }
            txn.commit();
        } catch (Exception e) {
            log.error("Failed to delete entries from region '{}': {}", region, e.getMessage(), e);
        }
    }
    
    @Override
    public long countEntries(String region) {
        Env<ByteBuffer> env = regionEnvironments.get(region);
        Dbi<ByteBuffer> dbi = regionDatabases.get(region);
        
        if (env == null || dbi == null) return 0;
        
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            return dbi.stat(txn).entries;
        } catch (Exception e) {
            log.error("Failed to count entries in region '{}': {}", region, e.getMessage(), e);
            return 0;
        }
    }
    
    @Override
    public long estimateEntryCount(String region) {
        // LMDB provides exact counts very fast via stat
        return countEntries(region);
    }
    
    /**
     * Iterate through all entries in a region without loading all into memory.
     * Memory-efficient for backup operations on large regions.
     */
    @Override
    public long forEachEntry(String region, java.util.function.Consumer<CacheEntry> consumer) {
        Env<ByteBuffer> env = regionEnvironments.get(region);
        Dbi<ByteBuffer> dbi = regionDatabases.get(region);
        
        if (env == null || dbi == null) {
            log.warn("forEachEntry: Region '{}' environment/database not found (env={}, dbi={})", 
                    region, env != null ? "OK" : "NULL", dbi != null ? "OK" : "NULL");
            return 0;
        }
        
        long count = 0;
        try (Txn<ByteBuffer> txn = env.txnRead();
             CursorIterable<ByteBuffer> cursor = dbi.iterate(txn)) {
            
            for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                try {
                    byte[] valueBytes = new byte[kv.val().remaining()];
                    kv.val().get(valueBytes);
                    String json = new String(valueBytes, StandardCharsets.UTF_8);
                    Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, MAP_TYPE_REF);
                    CacheEntry entry = mapToEntry(map, region);
                    if (!entry.isExpired()) {
                        consumer.accept(entry);
                        count++;
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse entry during iteration: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("Failed to iterate entries in region '{}': {}", region, e.getMessage(), e);
        }
        
        return count;
    }
    
    @Override
    public List<String> getKeys(String region, String pattern, int limit) {
        List<String> keys = new ArrayList<>();
        Env<ByteBuffer> env = regionEnvironments.get(region);
        Dbi<ByteBuffer> dbi = regionDatabases.get(region);
        
        if (env == null || dbi == null) return keys;
        
        Pattern regex = pattern != null && !pattern.equals("*") ? 
                globToRegex(pattern) : null;
        
        try (Txn<ByteBuffer> txn = env.txnRead();
             CursorIterable<ByteBuffer> cursor = dbi.iterate(txn)) {
            
            int count = 0;
            for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                if (limit > 0 && count >= limit) break;
                
                byte[] keyBytes = new byte[kv.key().remaining()];
                kv.key().get(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                
                if (regex == null || regex.matcher(key).matches()) {
                    keys.add(key);
                    count++;
                }
            }
        } catch (Exception e) {
            log.error("Failed to get keys from region '{}': {}", region, e.getMessage(), e);
        }
        
        return keys;
    }
    
    @Override
    public CacheEntry get(String region, String key) {
        return loadEntry(region, key);
    }
    
    @Override
    public long deleteExpiredEntries(String region) {
        Env<ByteBuffer> env = regionEnvironments.get(region);
        Dbi<ByteBuffer> dbi = regionDatabases.get(region);
        
        if (env == null || dbi == null) return 0;
        
        List<String> expiredKeys = new ArrayList<>();
        Instant now = Instant.now();
        
        // First, collect expired keys
        try (Txn<ByteBuffer> txn = env.txnRead();
             CursorIterable<ByteBuffer> cursor = dbi.iterate(txn)) {
            
            for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                byte[] valueBytes = new byte[kv.val().remaining()];
                kv.val().get(valueBytes);
                String json = new String(valueBytes, StandardCharsets.UTF_8);
                Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, MAP_TYPE_REF);
                
                Object expiresAtObj = map.get("expiresAt");
                if (expiresAtObj != null) {
                    Instant expiresAt = Instant.parse(expiresAtObj.toString());
                    if (now.isAfter(expiresAt)) {
                        byte[] keyBytes = new byte[kv.key().remaining()];
                        kv.key().get(keyBytes);
                        expiredKeys.add(new String(keyBytes, StandardCharsets.UTF_8));
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to find expired entries in region '{}': {}", region, e.getMessage(), e);
        }
        
        // Then delete them
        if (!expiredKeys.isEmpty()) {
            deleteEntries(region, expiredKeys);
        }
        
        return expiredKeys.size();
    }
    
    @Override
    public long deleteAllExpiredEntries() {
        long totalDeleted = 0;
        for (String region : regionEnvironments.keySet()) {
            totalDeleted += deleteExpiredEntries(region);
        }
        return totalDeleted;
    }
    
    @Override
    public long countNonExpiredEntries(String region) {
        Env<ByteBuffer> env = regionEnvironments.get(region);
        Dbi<ByteBuffer> dbi = regionDatabases.get(region);
        
        if (env == null || dbi == null) return 0;
        
        long count = 0;
        Instant now = Instant.now();
        
        try (Txn<ByteBuffer> txn = env.txnRead();
             CursorIterable<ByteBuffer> cursor = dbi.iterate(txn)) {
            
            for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                byte[] valueBytes = new byte[kv.val().remaining()];
                kv.val().get(valueBytes);
                String json = new String(valueBytes, StandardCharsets.UTF_8);
                Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, MAP_TYPE_REF);
                
                Object expiresAtObj = map.get("expiresAt");
                if (expiresAtObj == null) {
                    count++;
                } else {
                    Instant expiresAt = Instant.parse(expiresAtObj.toString());
                    if (!now.isAfter(expiresAt)) {
                        count++;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to count non-expired entries in region '{}': {}", region, e.getMessage(), e);
        }
        
        return count;
    }
    
    @Override
    public List<String> getNonExpiredKeys(String region, String pattern, int limit) {
        List<String> keys = new ArrayList<>();
        Env<ByteBuffer> env = regionEnvironments.get(region);
        Dbi<ByteBuffer> dbi = regionDatabases.get(region);
        
        if (env == null || dbi == null) return keys;
        
        Pattern regex = pattern != null && !pattern.equals("*") ? 
                globToRegex(pattern) : null;
        Instant now = Instant.now();
        
        try (Txn<ByteBuffer> txn = env.txnRead();
             CursorIterable<ByteBuffer> cursor = dbi.iterate(txn)) {
            
            int count = 0;
            for (CursorIterable.KeyVal<ByteBuffer> kv : cursor) {
                if (limit > 0 && count >= limit) break;
                
                // Check if expired
                byte[] valueBytes = new byte[kv.val().remaining()];
                kv.val().get(valueBytes);
                String json = new String(valueBytes, StandardCharsets.UTF_8);
                Map<String, Object> map = JsonUtils.getObjectMapper().readValue(json, MAP_TYPE_REF);
                
                Object expiresAtObj = map.get("expiresAt");
                boolean expired = false;
                if (expiresAtObj != null) {
                    Instant expiresAt = Instant.parse(expiresAtObj.toString());
                    expired = now.isAfter(expiresAt);
                }
                
                if (!expired) {
                    byte[] keyBytes = new byte[kv.key().remaining()];
                    kv.key().get(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);
                    
                    if (regex == null || regex.matcher(key).matches()) {
                        keys.add(key);
                        count++;
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to get non-expired keys from region '{}': {}", region, e.getMessage(), e);
        }
        
        return keys;
    }
    
    // ==================== Helper Methods ====================
    
    private Map<String, Object> regionToMap(CacheRegion region) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("name", region.getName());
        map.put("description", region.getDescription());
        map.put("captive", region.isCaptive());
        map.put("maxEntries", region.getMaxEntries());
        map.put("defaultTtlSeconds", region.getDefaultTtlSeconds());
        map.put("createdAt", region.getCreatedAt() != null ? region.getCreatedAt().toString() : null);
        map.put("updatedAt", region.getUpdatedAt() != null ? region.getUpdatedAt().toString() : null);
        map.put("createdBy", region.getCreatedBy());
        map.put("enabled", region.isEnabled());
        if (region.getAttributeMapping() != null && !region.getAttributeMapping().isEmpty()) {
            map.put("attributeMapping", region.getAttributeMapping());
        }
        return map;
    }
    
    @SuppressWarnings("unchecked")
    private CacheRegion mapToRegion(Map<String, Object> map) {
        CacheRegion region = new CacheRegion();
        region.setName((String) map.get("name"));
        region.setDescription((String) map.get("description"));
        region.setCaptive(Boolean.TRUE.equals(map.get("captive")));
        region.setMaxEntries(map.get("maxEntries") != null ? ((Number) map.get("maxEntries")).longValue() : -1);
        region.setDefaultTtlSeconds(map.get("defaultTtlSeconds") != null ? 
                ((Number) map.get("defaultTtlSeconds")).longValue() : -1);
        
        if (map.get("createdAt") != null) {
            region.setCreatedAt(Instant.parse((String) map.get("createdAt")));
        }
        if (map.get("updatedAt") != null) {
            region.setUpdatedAt(Instant.parse((String) map.get("updatedAt")));
        }
        
        region.setCreatedBy((String) map.get("createdBy"));
        region.setEnabled(map.get("enabled") == null || Boolean.TRUE.equals(map.get("enabled")));
        
        if (map.get("attributeMapping") != null) {
            region.setAttributeMapping((Map<String, String>) map.get("attributeMapping"));
        }
        
        return region;
    }
    
    private Map<String, Object> entryToMap(CacheEntry entry) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("key", entry.getKey());
        map.put("region", entry.getRegion());
        map.put("valueType", entry.getValueType().name());
        
        if (entry.getStringValue() != null) {
            map.put("stringValue", entry.getStringValue());
        }
        if (entry.getJsonValue() != null) {
            map.put("jsonValue", entry.getJsonValue());
        }
        
        map.put("ttlSeconds", entry.getTtlSeconds());
        map.put("createdAt", entry.getCreatedAt() != null ? entry.getCreatedAt().toString() : null);
        map.put("updatedAt", entry.getUpdatedAt() != null ? entry.getUpdatedAt().toString() : null);
        map.put("expiresAt", entry.getExpiresAt() != null ? entry.getExpiresAt().toString() : null);
        map.put("version", entry.getVersion());
        map.put("accessCount", entry.getAccessCount());
        map.put("lastAccessedAt", entry.getLastAccessedAt() != null ? entry.getLastAccessedAt().toString() : null);
        
        if (entry.getMetadata() != null && !entry.getMetadata().isEmpty()) {
            map.put("metadata", entry.getMetadata());
        }
        
        return map;
    }
    
    @SuppressWarnings("unchecked")
    private CacheEntry mapToEntry(Map<String, Object> map, String region) {
        CacheEntry entry = new CacheEntry();
        entry.setKey((String) map.get("key"));
        entry.setRegion(region);
        entry.setValueType(CacheEntry.ValueType.valueOf((String) map.get("valueType")));
        entry.setStringValue((String) map.get("stringValue"));
        
        if (map.get("jsonValue") != null) {
            entry.setJsonValue(JsonUtils.getObjectMapper().valueToTree(map.get("jsonValue")));
        }
        
        entry.setTtlSeconds(map.get("ttlSeconds") != null ? ((Number) map.get("ttlSeconds")).longValue() : -1);
        
        if (map.get("createdAt") != null) {
            entry.setCreatedAt(Instant.parse((String) map.get("createdAt")));
        }
        if (map.get("updatedAt") != null) {
            entry.setUpdatedAt(Instant.parse((String) map.get("updatedAt")));
        }
        if (map.get("expiresAt") != null) {
            entry.setExpiresAt(Instant.parse((String) map.get("expiresAt")));
        }
        
        entry.setVersion(map.get("version") != null ? ((Number) map.get("version")).longValue() : 1);
        entry.setAccessCount(map.get("accessCount") != null ? ((Number) map.get("accessCount")).longValue() : 0);
        
        if (map.get("lastAccessedAt") != null) {
            entry.setLastAccessedAt(Instant.parse((String) map.get("lastAccessedAt")));
        }
        
        if (map.get("metadata") != null) {
            entry.setMetadata((String) map.get("metadata"));
        }
        
        return entry;
    }
    
    /**
     * Sync all data to disk.
     * LMDB auto-syncs with MDB_MAPASYNC, but this forces immediate sync.
     */
    @Override
    public void sync() {
        log.info("Syncing all LMDB environments to disk...");
        for (Map.Entry<String, Env<ByteBuffer>> entry : regionEnvironments.entrySet()) {
            try {
                entry.getValue().sync(true);
                log.debug("Synced region '{}' to disk", entry.getKey());
            } catch (Exception e) {
                log.warn("Failed to sync LMDB environment for region '{}': {}", entry.getKey(), e.getMessage());
            }
        }
        log.info("All LMDB environments synced to disk");
    }
    
    /**
     * Get LMDB statistics for a region, including disk usage.
     */
    public Map<String, Object> getRegionStats(String region) {
        Map<String, Object> stats = new LinkedHashMap<>();
        Env<ByteBuffer> env = regionEnvironments.get(region);
        Dbi<ByteBuffer> dbi = regionDatabases.get(region);
        
        if (env != null && dbi != null) {
            try (Txn<ByteBuffer> txn = env.txnRead()) {
                Stat stat = dbi.stat(txn);
                stats.put("entries", stat.entries);
                stats.put("depth", stat.depth);
                stats.put("branchPages", stat.branchPages);
                stats.put("leafPages", stat.leafPages);
                stats.put("overflowPages", stat.overflowPages);
                stats.put("pageSize", stat.pageSize);
                
                // Calculate used pages
                long usedPages = stat.branchPages + stat.leafPages + stat.overflowPages;
                long usedBytes = usedPages * stat.pageSize;
                stats.put("usedPages", usedPages);
                stats.put("usedBytes", usedBytes);
                stats.put("usedMB", String.format("%.2f", usedBytes / (1024.0 * 1024.0)));
                
                EnvInfo info = env.info();
                stats.put("mapSize", info.mapSize);
                stats.put("mapSizeMB", String.format("%.2f", info.mapSize / (1024.0 * 1024.0)));
                stats.put("lastPageNo", info.lastPageNumber);
                stats.put("lastTxnId", info.lastTransactionId);
                stats.put("maxReaders", info.maxReaders);
                stats.put("numReaders", info.numReaders);
                
                // Calculate file size on disk
                String regionPath = basePath + File.separator + region;
                File dataFile = new File(regionPath, "data.mdb");
                if (dataFile.exists()) {
                    long fileSize = dataFile.length();
                    stats.put("fileBytes", fileSize);
                    stats.put("fileMB", String.format("%.2f", fileSize / (1024.0 * 1024.0)));
                    
                    // Estimate wasted space (file size - used bytes)
                    // Note: This is approximate since LMDB reserves some space for metadata
                    long wastedBytes = fileSize - usedBytes;
                    if (wastedBytes > 0) {
                        stats.put("freeBytes", wastedBytes);
                        stats.put("freeMB", String.format("%.2f", wastedBytes / (1024.0 * 1024.0)));
                        stats.put("freePercent", String.format("%.1f", (wastedBytes * 100.0) / fileSize));
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to get stats for region '{}': {}", region, e.getMessage());
            }
        }
        
        return stats;
    }
    
    /**
     * Compact a region's LMDB database to reclaim disk space.
     * 
     * IMPORTANT: LMDB never shrinks its database file automatically.
     * When you delete entries, the pages are added to an internal freelist
     * and reused for future writes, but the file size stays the same.
     * 
     * This method creates a compacted copy of the database, which:
     * 1. Only contains active data (no free pages)
     * 2. Results in a smaller file on disk
     * 3. May improve read performance due to better data locality
     * 
     * @param region The region name to compact
     * @return Map containing compaction results (originalSize, compactedSize, savedBytes, savedPercent)
     */
    public Map<String, Object> compactRegion(String region) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("region", region);
        result.put("success", false);
        
        Env<ByteBuffer> env = regionEnvironments.get(region);
        if (env == null) {
            result.put("error", "Region not found or not initialized");
            return result;
        }
        
        String regionPath = basePath + File.separator + region;
        File dataFile = new File(regionPath, "data.mdb");
        File compactFile = new File(regionPath, "data_compact.mdb");
        File backupFile = new File(regionPath, "data_backup.mdb");
        
        if (!dataFile.exists()) {
            result.put("error", "Data file does not exist");
            return result;
        }
        
        long originalSize = dataFile.length();
        result.put("originalSize", originalSize);
        result.put("originalSizeMB", String.format("%.2f", originalSize / (1024.0 * 1024.0)));
        
        log.info("Starting compaction for region '{}' (original size: {} MB)", 
                region, String.format("%.2f", originalSize / (1024.0 * 1024.0)));
        
        try {
            // Step 1: Copy database to compact file using LMDB's copy with compaction flag
            log.info("Copying database with compaction for region '{}'...", region);
            env.copy(compactFile, CopyFlags.MDB_CP_COMPACT);
            
            long compactedSize = compactFile.length();
            result.put("compactedSize", compactedSize);
            result.put("compactedSizeMB", String.format("%.2f", compactedSize / (1024.0 * 1024.0)));
            
            long savedBytes = originalSize - compactedSize;
            double savedPercent = (savedBytes * 100.0) / originalSize;
            result.put("savedBytes", savedBytes);
            result.put("savedMB", String.format("%.2f", savedBytes / (1024.0 * 1024.0)));
            result.put("savedPercent", String.format("%.1f", savedPercent));
            
            log.info("Compaction complete: {} MB -> {} MB (saved {} MB, {}%)", 
                    String.format("%.2f", originalSize / (1024.0 * 1024.0)),
                    String.format("%.2f", compactedSize / (1024.0 * 1024.0)),
                    String.format("%.2f", savedBytes / (1024.0 * 1024.0)),
                    String.format("%.1f", savedPercent));
            
            // Step 2: Close the environment so we can swap files
            log.info("Closing environment for region '{}'...", region);
            Dbi<ByteBuffer> dbi = regionDatabases.remove(region);
            if (dbi != null) {
                dbi.close();
            }
            regionEnvironments.remove(region);
            env.close();
            
            // Step 3: Backup original file
            log.info("Backing up original file...");
            if (backupFile.exists()) {
                backupFile.delete();
            }
            if (!dataFile.renameTo(backupFile)) {
                throw new IOException("Failed to rename original file to backup");
            }
            
            // Step 4: Move compacted file to replace original
            log.info("Installing compacted file...");
            if (!compactFile.renameTo(dataFile)) {
                // Restore backup
                backupFile.renameTo(dataFile);
                throw new IOException("Failed to rename compacted file");
            }
            
            // Step 5: Delete backup
            log.info("Cleaning up...");
            backupFile.delete();
            
            // Step 6: Re-open the environment
            log.info("Reopening environment for region '{}'...", region);
            getOrCreateRegionEnvironment(region);
            
            result.put("success", true);
            log.info("Compaction successful for region '{}'", region);
            
        } catch (Exception e) {
            log.error("Compaction failed for region '{}': {}", region, e.getMessage(), e);
            result.put("error", e.getMessage());
            
            // Clean up partial files
            if (compactFile.exists()) {
                compactFile.delete();
            }
            
            // Try to reopen environment if it was closed
            if (!regionEnvironments.containsKey(region)) {
                try {
                    getOrCreateRegionEnvironment(region);
                } catch (Exception reopenError) {
                    log.error("Failed to reopen environment after failed compaction: {}", reopenError.getMessage());
                }
            }
        }
        
        return result;
    }
    
    /**
     * Get disk usage summary for all regions.
     */
    public Map<String, Object> getDiskUsageSummary() {
        Map<String, Object> summary = new LinkedHashMap<>();
        long totalFileSize = 0;
        long totalUsedBytes = 0;
        
        List<Map<String, Object>> regionDetails = new ArrayList<>();
        
        for (String region : regionEnvironments.keySet()) {
            Map<String, Object> stats = getRegionStats(region);
            if (!stats.isEmpty()) {
                Map<String, Object> regionInfo = new LinkedHashMap<>();
                regionInfo.put("region", region);
                regionInfo.put("entries", stats.get("entries"));
                regionInfo.put("fileMB", stats.get("fileMB"));
                regionInfo.put("usedMB", stats.get("usedMB"));
                regionInfo.put("freeMB", stats.get("freeMB"));
                regionInfo.put("freePercent", stats.get("freePercent"));
                regionDetails.add(regionInfo);
                
                if (stats.get("fileBytes") != null) {
                    totalFileSize += (Long) stats.get("fileBytes");
                }
                if (stats.get("usedBytes") != null) {
                    totalUsedBytes += (Long) stats.get("usedBytes");
                }
            }
        }
        
        summary.put("regions", regionDetails);
        summary.put("totalFileMB", String.format("%.2f", totalFileSize / (1024.0 * 1024.0)));
        summary.put("totalUsedMB", String.format("%.2f", totalUsedBytes / (1024.0 * 1024.0)));
        
        long totalFreeBytes = totalFileSize - totalUsedBytes;
        summary.put("totalFreeMB", String.format("%.2f", totalFreeBytes / (1024.0 * 1024.0)));
        
        if (totalFileSize > 0) {
            summary.put("totalFreePercent", String.format("%.1f", (totalFreeBytes * 100.0) / totalFileSize));
        }
        
        summary.put("recommendation", totalFreeBytes > 100 * 1024 * 1024 
                ? "Consider running compaction to reclaim " + String.format("%.2f", totalFreeBytes / (1024.0 * 1024.0)) + " MB"
                : "No compaction needed");
        
        return summary;
    }
}
