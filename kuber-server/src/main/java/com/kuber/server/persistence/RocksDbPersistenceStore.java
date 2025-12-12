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
 * 
 * <p>DURABILITY (v1.3.6):
 * <ul>
 *   <li>All writes use WriteOptions with sync=true for critical data</li>
 *   <li>Shutdown gate prevents new writes during shutdown</li>
 *   <li>Proper WAL flush and sync before close</li>
 *   <li>ReadWriteLock protects against concurrent close/write races</li>
 * </ul>
 * 
 * @version 1.7.4
 */
@Slf4j
public class RocksDbPersistenceStore extends AbstractPersistenceStore {
    
    private static final String REGION_METADATA_FILE = "_region.json";
    
    // Reusable TypeReference to avoid creating new anonymous classes in loops (memory leak prevention)
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<Map<String, Object>>() {};
    
    private final KuberProperties properties;
    private final String basePath;
    
    // Separate RocksDB instance per region for entries
    private final Map<String, RocksDB> regionDatabases = new ConcurrentHashMap<>();
    private final Map<String, Options> regionOptions = new ConcurrentHashMap<>();
    
    // Shutdown gate - prevents new writes during shutdown
    private volatile boolean shuttingDown = false;
    
    // Guard against double shutdown (ShutdownOrchestrator + Spring @PreDestroy)
    private volatile boolean alreadyShutdown = false;
    
    // Read-write lock to protect against concurrent close/write races
    // Multiple readers (writes) can proceed concurrently, but writer (shutdown) is exclusive
    private final java.util.concurrent.locks.ReadWriteLock shutdownLock = 
            new java.util.concurrent.locks.ReentrantReadWriteLock();
    
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
            
            // Configure batched async persistence (v1.6.2)
            // This improves write throughput by 5-20x for high-volume operations
            int batchSize = properties.getCache().getPersistenceBatchSize();
            int flushIntervalMs = properties.getCache().getPersistenceIntervalMs();
            configureBatching(batchSize, flushIntervalMs);
            
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
    
    // Lock for database creation to prevent race conditions
    private final Object dbCreationLock = new Object();
    
    private RocksDB getOrCreateRegionDatabase(String region) {
        // Fast path: check if already exists
        RocksDB existingDb = regionDatabases.get(region);
        if (existingDb != null) {
            return existingDb;
        }
        
        // Slow path: synchronized creation
        synchronized (dbCreationLock) {
            // Double-check after acquiring lock
            existingDb = regionDatabases.get(region);
            if (existingDb != null) {
                return existingDb;
            }
            
            // Create new database
            RocksDB newDb = openRegionDatabase(region);
            regionDatabases.put(region, newDb);
            return newDb;
        }
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
            
            // Configure options for durability and performance
            Options options = new Options()
                    .setCreateIfMissing(true)
                    .setMaxBackgroundJobs(4)
                    .setMaxOpenFiles(-1)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setWriteBufferSize(64 * 1024 * 1024)  // 64MB write buffer
                    .setMaxWriteBufferNumber(3)
                    .setTargetFileSizeBase(64 * 1024 * 1024)  // 64MB SST files
                    .setParanoidChecks(true)  // Enable paranoid checks for data integrity
                    // WAL configuration for durability
                    .setWalTtlSeconds(0)  // Keep WAL until explicitly cleaned
                    .setWalSizeLimitMB(0)  // No size limit on WAL
                    .setManualWalFlush(false)  // Let RocksDB manage WAL flushes
                    // Avoid losing data on crash
                    .setAvoidFlushDuringShutdown(false)  // Flush during shutdown
                    .setAvoidFlushDuringRecovery(false);  // Flush during recovery
            
            regionOptions.put(region, options);
            
            RocksDB db = null;
            try {
                db = RocksDB.open(options, regionPath);
            } catch (RocksDBException e) {
                // Check if this is a corruption error
                String errorMsg = e.getMessage();
                if (errorMsg != null && (
                        errorMsg.contains("Corruption") || 
                        errorMsg.contains("MANIFEST") ||
                        (errorMsg.contains("No such file or directory") && errorMsg.contains(".sst")))) {
                    
                    log.warn("╔════════════════════════════════════════════════════════════════════╗");
                    log.warn("║  DATABASE CORRUPTION DETECTED                                      ║");
                    log.warn("║  Region: {}                                              ║", String.format("%-42s", region));
                    log.warn("║  Attempting automatic repair...                                    ║");
                    log.warn("╚════════════════════════════════════════════════════════════════════╝");
                    
                    // Try to repair the database
                    db = attemptDatabaseRepair(region, regionPath, options);
                    
                    if (db == null) {
                        log.error("Failed to repair database for region '{}'. Data may be lost.", region);
                        regionOptions.remove(region);
                        throw new RuntimeException("Failed to open or repair region database: " + region, e);
                    }
                } else {
                    // Not a corruption error, rethrow
                    throw e;
                }
            }
            
            log.info("Opened RocksDB instance for region '{}' at: {}", region, regionPath);
            return db;
            
        } catch (RocksDBException e) {
            log.error("Failed to open RocksDB for region '{}': {}", region, e.getMessage(), e);
            regionOptions.remove(region);
            throw new RuntimeException("Failed to open region database", e);
        }
    }
    
    /**
     * Attempt to repair a corrupted RocksDB database.
     * 
     * Steps:
     * 1. Create a backup of the corrupted data
     * 2. Try opening with error-tolerant options
     * 3. If that fails, start fresh (data loss)
     * 
     * Note: RocksDB Java API does not expose repairDB directly, so we rely on
     * tolerant open options and fresh start as recovery strategies.
     * 
     * @param region Region name
     * @param regionPath Path to the region database
     * @param options Original options
     * @return Opened RocksDB instance, or null if repair failed
     */
    private RocksDB attemptDatabaseRepair(String region, String regionPath, Options options) {
        // Step 1: Create backup of corrupted data
        String backupPath = regionPath + "_corrupted_" + System.currentTimeMillis();
        try {
            File regionDir = new File(regionPath);
            File backupDir = new File(backupPath);
            
            if (regionDir.exists()) {
                log.info("Creating backup of corrupted data at: {}", backupPath);
                copyDirectory(regionDir, backupDir);
            }
        } catch (Exception e) {
            log.warn("Failed to create backup: {}", e.getMessage());
        }
        
        // Step 2: Try opening with error-tolerant options (skip corrupt data)
        log.info("Attempting to open with error-tolerant options for region '{}'...", region);
        try {
            Options tolerantOptions = new Options()
                    .setCreateIfMissing(true)
                    .setMaxBackgroundJobs(4)
                    .setMaxOpenFiles(-1)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setWriteBufferSize(64 * 1024 * 1024)
                    .setMaxWriteBufferNumber(3)
                    .setTargetFileSizeBase(64 * 1024 * 1024)
                    .setParanoidChecks(false);  // Disable paranoid checks to skip corrupt data
            
            // Close old options and use new tolerant ones
            regionOptions.put(region, tolerantOptions);
            options.close();
            
            RocksDB db = RocksDB.open(tolerantOptions, regionPath);
            log.warn("Opened database with tolerant options for region '{}' - some data may be missing", region);
            return db;
            
        } catch (RocksDBException e) {
            log.warn("Error-tolerant open failed for region '{}': {}", region, e.getMessage());
        }
        
        // Step 3: Last resort - delete and start fresh
        log.warn("All recovery attempts failed for region '{}'. Starting fresh (data loss!).", region);
        try {
            // Delete the corrupted directory
            File regionDir = new File(regionPath);
            deleteDirectory(regionDir);
            
            // Create fresh database
            regionDir.mkdirs();
            Options freshOptions = new Options()
                    .setCreateIfMissing(true)
                    .setMaxBackgroundJobs(4)
                    .setMaxOpenFiles(-1)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setWriteBufferSize(64 * 1024 * 1024)
                    .setMaxWriteBufferNumber(3)
                    .setTargetFileSizeBase(64 * 1024 * 1024)
                    .setParanoidChecks(true);
            
            regionOptions.put(region, freshOptions);
            
            RocksDB db = RocksDB.open(freshOptions, regionPath);
            log.warn("Created fresh database for region '{}'. Corrupted data backed up at: {}", region, backupPath);
            return db;
            
        } catch (Exception e) {
            log.error("Failed to create fresh database for region '{}': {}", region, e.getMessage());
            return null;
        }
    }
    
    /**
     * Copy a directory recursively.
     */
    private void copyDirectory(File source, File destination) throws java.io.IOException {
        if (source.isDirectory()) {
            if (!destination.exists()) {
                destination.mkdirs();
            }
            String[] children = source.list();
            if (children != null) {
                for (String child : children) {
                    copyDirectory(new File(source, child), new File(destination, child));
                }
            }
        } else {
            java.nio.file.Files.copy(source.toPath(), destination.toPath(), 
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }
    }
    
    /**
     * Delete a directory recursively.
     */
    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
    
    /**
     * Prepare for shutdown by signaling that shutdown is imminent.
     * This should be called by ShutdownOrchestrator BEFORE CacheService.shutdown()
     * to ensure that subsequent batch writes use sync=true.
     * 
     * DURABILITY FIX (v1.6.1): This ensures the shuttingDown flag is set before
     * CacheService starts batch-saving entries during shutdown. Without this,
     * the batch writes would use sync=false and data could be lost if the process
     * exits before OS buffers are flushed.
     * 
     * @return true if successful, false if already shutting down
     */
    public boolean prepareForShutdown() {
        if (shuttingDown) {
            log.debug("RocksDB already preparing for shutdown");
            return false;
        }
        
        log.info("RocksDB preparing for shutdown - subsequent writes will use sync=true");
        shuttingDown = true;
        
        // Wait for any pending async operations to complete
        // This ensures a clean state before CacheService starts writing
        waitForPendingAsyncOperations();
        
        return true;
    }
    
    /**
     * Wait for any pending async save operations to complete.
     * Used during shutdown preparation to ensure clean state.
     */
    private void waitForPendingAsyncOperations() {
        // Set flag to stop accepting new async saves
        asyncShuttingDown = true;
        
        // Give a brief moment for any in-flight async operations
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        log.debug("Pending async operations cleared");
    }
    
    @Override
    public void shutdown() {
        // Guard against double shutdown (ShutdownOrchestrator calls us, then Spring @PreDestroy)
        if (alreadyShutdown) {
            log.debug("RocksDB shutdown already completed - skipping duplicate shutdown call");
            return;
        }
        alreadyShutdown = true;
        
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  ROCKSDB GRACEFUL SHUTDOWN INITIATED                                ║");
        log.info("╚════════════════════════════════════════════════════════════════════╝");
        
        // Step 1: Signal shutdown to stop new write attempts
        log.info("Step 1: Setting shutdown flag to reject new writes...");
        shuttingDown = true;
        available = false;
        
        // Step 2: Shutdown async save executor FIRST and wait for all pending saves
        // This ensures all async writes complete before we close databases
        log.info("Step 2: Shutting down async save executor and waiting for pending saves...");
        shutdownAsyncExecutor();
        log.info("Step 2: Async save executor shutdown complete");
        
        // Step 3: Acquire WRITE lock to wait for any remaining in-flight operations
        log.info("Step 3: Waiting for all in-flight write operations to complete...");
        shutdownLock.writeLock().lock();
        try {
            log.info("Step 4: All in-flight writes completed, proceeding with database close...");
            
            // Additional safety wait
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            int totalRegions = regionDatabases.size();
            int successCount = 0;
            int failCount = 0;
            
            log.info("Closing {} region database(s)...", totalRegions);
            
            for (Map.Entry<String, RocksDB> entry : regionDatabases.entrySet()) {
                String regionName = entry.getKey();
                RocksDB db = entry.getValue();
                
                try {
                    gracefulCloseDatabase(regionName, db);
                    successCount++;
                } catch (Exception e) {
                    log.error("Failed to gracefully close region '{}': {}", regionName, e.getMessage(), e);
                    failCount++;
                    // Still try to close even if graceful shutdown failed
                    try {
                        db.close();
                    } catch (Exception closeEx) {
                        log.warn("Force close also failed for region '{}': {}", regionName, closeEx.getMessage());
                    }
                }
            }
            regionDatabases.clear();
            
            // Close options after all databases are closed
            for (Map.Entry<String, Options> entry : regionOptions.entrySet()) {
                try {
                    entry.getValue().close();
                } catch (Exception e) {
                    log.warn("Error closing options for region '{}': {}", entry.getKey(), e.getMessage());
                }
            }
            regionOptions.clear();
            
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  ROCKSDB SHUTDOWN COMPLETE                                          ║");
            log.info("║  Regions closed: {} success, {} failed                              ║", 
                    String.format("%-3d", successCount), String.format("%-3d", failCount));
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
        } finally {
            shutdownLock.writeLock().unlock();
        }
    }
    
    /**
     * Gracefully close a RocksDB database with proper flush and sync.
     * This ensures all data is persisted to disk before closing.
     * 
     * The sequence is:
     * 1. Pause background work to prevent new compactions
     * 2. Cancel existing background work and wait for completion
     * 3. Flush all memtables to SST files with sync
     * 4. Sync the WAL (Write-Ahead Log)
     * 5. Wait for OS buffers to flush
     * 6. Close the database
     * 
     * @param regionName Name of the region for logging
     * @param db The RocksDB instance to close
     */
    private void gracefulCloseDatabase(String regionName, RocksDB db) throws RocksDBException {
        log.info("Gracefully closing region '{}'...", regionName);
        long startTime = System.currentTimeMillis();
        
        try {
            // CRITICAL: The order matters! We must flush BEFORE canceling background work.
            // cancelAllBackgroundWork(true) puts RocksDB in shutdown mode where flush() fails.
            
            // Step 1: Flush all memtables to SST files FIRST (while RocksDB is still active)
            log.debug("  [{}] Flushing memtables to disk...", regionName);
            try (FlushOptions flushOptions = new FlushOptions()) {
                flushOptions.setWaitForFlush(true);  // Wait for flush to complete
                flushOptions.setAllowWriteStall(true);  // Allow blocking if needed
                db.flush(flushOptions);
            }
            log.debug("  [{}] Memtables flushed", regionName);
            
            // Step 2: Sync WAL to ensure all writes are durable
            log.debug("  [{}] Syncing WAL...", regionName);
            db.syncWal();
            log.debug("  [{}] WAL synced", regionName);
            
            // Step 3: Flush and sync WAL one more time
            log.debug("  [{}] Final WAL flush with sync...", regionName);
            db.flushWal(true);  // true = sync
            log.debug("  [{}] WAL flushed and synced", regionName);
            
            // Step 4: Small delay to ensure all I/O is complete
            Thread.sleep(500);
            
            // Step 5: NOW cancel background work (after all data is safely on disk)
            // The 'true' parameter means wait for background work to finish
            log.debug("  [{}] Canceling background work...", regionName);
            db.cancelAllBackgroundWork(true);
            log.debug("  [{}] Background work canceled", regionName);
            
            // Step 6: Extended delay to ensure OS buffers are flushed
            log.debug("  [{}] Waiting for OS buffers...", regionName);
            Thread.sleep(500);
            
            // Step 7: Close the database
            log.debug("  [{}] Closing database handle...", regionName);
            db.close();
            
            long elapsed = System.currentTimeMillis() - startTime;
            log.info("  [{}] Closed successfully in {}ms", regionName, elapsed);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RocksDBException("Interrupted during shutdown of region: " + regionName);
        }
    }
    
    /**
     * Force sync all region databases to disk.
     * This can be called before shutdown to ensure data durability.
     */
    public void syncAllRegions() {
        log.info("Syncing all region databases to disk...");
        
        for (Map.Entry<String, RocksDB> entry : regionDatabases.entrySet()) {
            String regionName = entry.getKey();
            RocksDB db = entry.getValue();
            
            try {
                // Flush memtables
                try (FlushOptions flushOptions = new FlushOptions()) {
                    flushOptions.setWaitForFlush(true);
                    db.flush(flushOptions);
                }
                
                // Sync WAL
                db.syncWal();
                db.flushWal(true);
                
                log.debug("Synced region '{}' to disk", regionName);
            } catch (RocksDBException e) {
                log.warn("Failed to sync region '{}': {}", regionName, e.getMessage());
            }
        }
        
        log.info("All regions synced to disk");
    }
    
    @Override
    public void sync() {
        // DURABILITY FIX (v1.6.1): Wait for any pending async operations first
        // This ensures all async writes complete before we sync
        if (!asyncShuttingDown) {
            // Brief pause to let any in-flight async operations complete
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        syncAllRegions();
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
                Map<String, Object> map = JsonUtils.fromJson(json, MAP_TYPE_REF);
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
                
                // Reopen and put back into map
                RocksDB newDb = openRegionDatabase(name);
                regionDatabases.put(name, newDb);
            }
            
            log.info("Purged region '{}' in RocksDB", name);
        } catch (Exception e) {
            log.error("Failed to purge region '{}': {}", name, e.getMessage(), e);
            throw new RuntimeException("Failed to purge region", e);
        }
    }
    
    // ==================== Entry Operations ====================
    
    /**
     * Check if we can accept writes (not shutting down).
     * @throws RuntimeException if shutting down
     */
    private void checkWriteAllowed() {
        if (shuttingDown) {
            throw new RuntimeException("Cannot write: RocksDB is shutting down");
        }
    }
    
    @Override
    public void saveEntry(CacheEntry entry) {
        // Acquire read lock (allows concurrent writes, blocks during shutdown)
        shutdownLock.readLock().lock();
        try {
            checkWriteAllowed();
            
            RocksDB regionDb = getOrCreateRegionDatabase(entry.getRegion());
            
            // Use WriteOptions with sync for durability
            try (WriteOptions writeOptions = new WriteOptions()) {
                // Sync to disk for important single writes
                writeOptions.setSync(true);
                
                String json = JsonUtils.toJson(entryToMap(entry));
                regionDb.put(
                        writeOptions,
                        entry.getKey().getBytes(StandardCharsets.UTF_8),
                        json.getBytes(StandardCharsets.UTF_8));
            } catch (RocksDBException e) {
                log.error("Failed to save entry '{}' to region '{}': {}", 
                        entry.getKey(), entry.getRegion(), e.getMessage(), e);
                throw new RuntimeException("Failed to save entry", e);
            }
        } finally {
            shutdownLock.readLock().unlock();
        }
    }
    
    @Override
    public void saveEntries(List<CacheEntry> entries) {
        if (entries.isEmpty()) {
            log.debug("RocksDB saveEntries called with empty list - nothing to save");
            return;
        }
        
        log.info("RocksDB saveEntries: received {} entries", entries.size());
        
        // Acquire read lock (allows concurrent writes, blocks during shutdown)
        shutdownLock.readLock().lock();
        try {
            checkWriteAllowed();
            
            Map<String, List<CacheEntry>> entriesByRegion = new ConcurrentHashMap<>();
            for (CacheEntry entry : entries) {
                entriesByRegion.computeIfAbsent(entry.getRegion(), k -> new ArrayList<>()).add(entry);
            }
            
            log.info("RocksDB saveEntries: grouped into {} region(s)", entriesByRegion.size());
            
            for (Map.Entry<String, List<CacheEntry>> regionEntries : entriesByRegion.entrySet()) {
                String region = regionEntries.getKey();
                List<CacheEntry> entriesToSave = regionEntries.getValue();
                
                log.info("RocksDB saveEntries: processing region '{}' with {} entries", region, entriesToSave.size());
                
                try {
                    RocksDB regionDb = getOrCreateRegionDatabase(region);
                    log.info("RocksDB saveEntries: got database for region '{}': {}", region, regionDb != null ? "OK" : "NULL");
                    
                    if (regionDb == null) {
                        log.error("RocksDB saveEntries: Failed to get database for region '{}'", region);
                        continue;
                    }
                    
                    log.info("RocksDB saveEntries: creating WriteBatch for region '{}'", region);
                    
                    try (WriteBatch batch = new WriteBatch();
                         WriteOptions writeOptions = new WriteOptions()) {
                        
                        // DURABILITY FIX (v1.6.1): Force sync during shutdown to prevent data loss
                        // During normal operation, rely on WAL for crash recovery (faster)
                        // During shutdown, we MUST sync to ensure data is on disk before close
                        // This prevents corruption when JVM exits before OS buffers are flushed
                        writeOptions.setSync(shuttingDown);
                        
                        int addedCount = 0;
                        for (CacheEntry entry : entriesToSave) {
                            try {
                                String json = JsonUtils.toJson(entryToMap(entry));
                                batch.put(
                                        entry.getKey().getBytes(StandardCharsets.UTF_8),
                                        json.getBytes(StandardCharsets.UTF_8));
                                addedCount++;
                                
                                if (addedCount == 1) {
                                    log.info("RocksDB saveEntries: first entry added to batch for region '{}'", region);
                                }
                                if (addedCount % 10000 == 0) {
                                    log.info("RocksDB saveEntries: {} entries added to batch for region '{}'", addedCount, region);
                                }
                            } catch (Throwable t) {
                                log.error("RocksDB saveEntries: ENTRY ERROR for key '{}' in region '{}': {} - {}", 
                                        entry.getKey(), region, t.getClass().getName(), t.getMessage(), t);
                                throw t;
                            }
                        }
                        
                        log.info("RocksDB saveEntries: writing batch of {} entries to region '{}'", addedCount, region);
                        regionDb.write(writeOptions, batch);
                        log.info("RocksDB saveEntries: COMMITTED {} entries to region '{}'", addedCount, region);
                        
                    } catch (RocksDBException e) {
                        log.error("RocksDB saveEntries: FATAL ERROR for region '{}': {} - {}", 
                                region, e.getClass().getName(), e.getMessage(), e);
                        throw new RuntimeException("Failed to batch save entries", e);
                    }
                    
                } catch (Throwable t) {
                    log.error("RocksDB saveEntries: OUTER exception for region '{}': {} - {}", 
                            region, t.getClass().getName(), t.getMessage(), t);
                    if (t instanceof RuntimeException) {
                        throw (RuntimeException) t;
                    }
                    throw new RuntimeException("Failed to save entries to region: " + region, t);
                }
            }
            
            log.info("RocksDB saveEntries: method completed");
            
        } finally {
            shutdownLock.readLock().unlock();
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
                Map<String, Object> map = JsonUtils.fromJson(json, MAP_TYPE_REF);
                return mapToEntry(map, region);
            }
        } catch (Exception e) {
            log.error("Failed to load entry '{}' from region '{}': {}", key, region, e.getMessage(), e);
        }
        return null;
    }
    
    @Override
    public java.util.Map<String, CacheEntry> loadEntriesByKeys(String region, List<String> keys) {
        java.util.Map<String, CacheEntry> result = new java.util.HashMap<>();
        RocksDB regionDb = regionDatabases.get(region);
        if (regionDb == null || keys.isEmpty()) return result;
        
        try {
            // Convert keys to byte arrays
            List<byte[]> keyBytes = new ArrayList<>(keys.size());
            for (String key : keys) {
                keyBytes.add(key.getBytes(StandardCharsets.UTF_8));
            }
            
            // Use RocksDB multiGet for batch retrieval
            List<byte[]> values = regionDb.multiGetAsList(keyBytes);
            
            // Process results
            for (int i = 0; i < keys.size(); i++) {
                byte[] value = values.get(i);
                if (value != null) {
                    try {
                        String json = new String(value, StandardCharsets.UTF_8);
                        Map<String, Object> map = JsonUtils.fromJson(json, MAP_TYPE_REF);
                        CacheEntry entry = mapToEntry(map, region);
                        if (entry != null) {
                            result.put(keys.get(i), entry);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to parse entry for key '{}': {}", keys.get(i), e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to batch load entries from region '{}': {}", region, e.getMessage(), e);
            // Fall back to individual loads
            for (String key : keys) {
                CacheEntry entry = loadEntry(region, key);
                if (entry != null) {
                    result.put(key, entry);
                }
            }
        }
        return result;
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
                    Map<String, Object> map = JsonUtils.fromJson(json, MAP_TYPE_REF);
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
        // Acquire read lock (allows concurrent operations, blocks during shutdown)
        shutdownLock.readLock().lock();
        try {
            checkWriteAllowed();
            
            RocksDB regionDb = regionDatabases.get(region);
            if (regionDb == null) return;
            
            try (WriteOptions writeOptions = new WriteOptions()) {
                writeOptions.setSync(true);  // Sync deletes for durability
                regionDb.delete(writeOptions, key.getBytes(StandardCharsets.UTF_8));
            } catch (RocksDBException e) {
                log.error("Failed to delete entry '{}' from region '{}': {}", key, region, e.getMessage(), e);
                throw new RuntimeException("Failed to delete entry", e);
            }
        } finally {
            shutdownLock.readLock().unlock();
        }
    }
    
    @Override
    public void deleteEntries(String region, List<String> keys) {
        if (keys.isEmpty()) return;
        
        // Acquire read lock (allows concurrent operations, blocks during shutdown)
        shutdownLock.readLock().lock();
        try {
            checkWriteAllowed();
            
            RocksDB regionDb = regionDatabases.get(region);
            if (regionDb == null) return;
            
            try (WriteBatch batch = new WriteBatch();
                 WriteOptions writeOptions = new WriteOptions()) {
                
                // Batch deletes don't need sync per batch - WAL handles durability
                writeOptions.setSync(false);
                
                for (String key : keys) {
                    batch.delete(key.getBytes(StandardCharsets.UTF_8));
                }
                regionDb.write(writeOptions, batch);
            } catch (RocksDBException e) {
                log.error("Failed to batch delete entries from region '{}': {}", region, e.getMessage(), e);
                throw new RuntimeException("Failed to batch delete entries", e);
            }
        } finally {
            shutdownLock.readLock().unlock();
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
     * Iterate through all entries in a region without loading all into memory.
     * Memory-efficient for backup operations on large regions.
     */
    @Override
    public long forEachEntry(String region, java.util.function.Consumer<CacheEntry> consumer) {
        RocksDB regionDb = regionDatabases.get(region);
        if (regionDb == null) {
            log.warn("forEachEntry: Region '{}' database not found (not opened yet or doesn't exist)", region);
            return 0;
        }
        
        long count = 0;
        try (RocksIterator iterator = regionDb.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                try {
                    String json = new String(iterator.value(), StandardCharsets.UTF_8);
                    Map<String, Object> map = JsonUtils.fromJson(json, MAP_TYPE_REF);
                    CacheEntry entry = mapToEntry(map, region);
                    if (!entry.isExpired()) {
                        consumer.accept(entry);
                        count++;
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse entry during iteration: {}", e.getMessage());
                }
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
                    Map<String, Object> map = JsonUtils.fromJson(json, MAP_TYPE_REF);
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
                    Map<String, Object> map = JsonUtils.fromJson(json, MAP_TYPE_REF);
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
                    Map<String, Object> map = JsonUtils.fromJson(json, MAP_TYPE_REF);
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
    
    /**
     * Convert CacheEntry to a Map for JSON serialization.
     * 
     * <p>OPTIMIZATION (v1.6.1): Avoid redundant JSON serialization.
     * <ul>
     *   <li>If stringValue is set, use it directly (no re-serialization)</li>
     *   <li>Only serialize jsonValue if stringValue is null</li>
     *   <li>Store only stringValue for JSON entries (jsonValue reconstructed on load)</li>
     * </ul>
     */
    private Map<String, Object> entryToMap(CacheEntry entry) {
        java.util.HashMap<String, Object> map = new java.util.HashMap<>();
        map.put("key", entry.getKey());
        map.put("valueType", entry.getValueType().name());
        
        // OPTIMIZATION: Avoid double serialization
        // For JSON entries, stringValue already contains the serialized form
        // Only serialize jsonValue if stringValue is not available
        String strValue = entry.getStringValue();
        if (strValue == null && entry.getJsonValue() != null) {
            strValue = JsonUtils.toJson(entry.getJsonValue());
        }
        map.put("stringValue", strValue);
        // No longer storing jsonValue separately - it's reconstructed from stringValue on load
        // This saves storage space and avoids redundant serialization
        
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
    
    /**
     * Convert a Map back to CacheEntry.
     * 
     * <p>OPTIMIZATION (v1.6.1): Reconstruct jsonValue from stringValue.
     * Since we only store stringValue now, jsonValue is parsed on demand for JSON entries.
     */
    private CacheEntry mapToEntry(Map<String, Object> map, String region) {
        CacheEntry.ValueType valueType = CacheEntry.ValueType.valueOf((String) map.get("valueType"));
        String stringValue = (String) map.get("stringValue");
        
        CacheEntry.CacheEntryBuilder builder = CacheEntry.builder()
                .region(region)
                .key((String) map.get("key"))
                .valueType(valueType)
                .stringValue(stringValue)
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
        
        // OPTIMIZATION (v1.6.1): Reconstruct jsonValue from stringValue
        // First check for legacy jsonValue field (for backward compatibility)
        String jsonValue = (String) map.get("jsonValue");
        if (jsonValue != null) {
            builder.jsonValue(JsonUtils.parse(jsonValue));
        } else if (valueType == CacheEntry.ValueType.JSON && stringValue != null) {
            // New format: parse jsonValue from stringValue
            builder.jsonValue(JsonUtils.parse(stringValue));
        }
        
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
