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

import com.kuber.server.config.KuberProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.stereotype.Service;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Service for database maintenance operations (compaction/vacuum).
 * 
 * This service runs within Spring context after initialization to ensure
 * databases are in optimal state before cache recovery begins.
 * 
 * Supported operations:
 * - RocksDB: Full compaction of all region databases
 * - SQLite: VACUUM on all region database files
 * 
 * @version 1.2.8
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PersistenceMaintenanceService {
    
    static {
        // Load RocksDB native library
        RocksDB.loadLibrary();
    }
    
    private final KuberProperties properties;
    
    /**
     * Execute maintenance (compaction/vacuum) based on persistence type.
     * 
     * @return true if maintenance completed successfully, false otherwise
     */
    public boolean executeMaintenance() {
        String persistenceType = properties.getPersistence().getType().toLowerCase();
        
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  Persistence Maintenance - {} Backend                    ", String.format("%-10s", persistenceType.toUpperCase()));
        log.info("╚════════════════════════════════════════════════════════════════════╝");
        
        try {
            return switch (persistenceType) {
                case "rocksdb", "rocks" -> executeRocksDbCompaction();
                case "sqlite" -> executeSqliteVacuum();
                case "lmdb" -> {
                    log.info("LMDB does not require manual compaction (B+ tree auto-balances)");
                    yield true;
                }
                case "mongodb", "mongo", "postgresql", "postgres", "memory", "mem" -> {
                    log.info("Persistence type '{}' does not require startup maintenance", persistenceType);
                    yield true;
                }
                default -> {
                    log.warn("Unknown persistence type '{}', skipping maintenance", persistenceType);
                    yield true;
                }
            };
        } catch (Exception e) {
            log.error("Persistence maintenance failed: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Execute RocksDB compaction on all region databases.
     */
    private boolean executeRocksDbCompaction() {
        boolean compactionEnabled = properties.getPersistence().getRocksdb().isCompactionEnabled();
        
        if (!compactionEnabled) {
            log.info("RocksDB compaction is disabled in configuration");
            return true;
        }
        
        String basePath = properties.getPersistence().getRocksdb().getPath();
        File baseDir = new File(basePath);
        
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            log.info("RocksDB directory does not exist: {} (will be created on first use)", basePath);
            return true;
        }
        
        log.info("RocksDB compaction starting - path: {}", baseDir.getAbsolutePath());
        
        // Find all region directories (exclude hidden and metadata)
        File[] regionDirs = baseDir.listFiles(file -> 
                file.isDirectory() && 
                !file.getName().startsWith("_") && 
                !file.getName().startsWith("."));
        
        if (regionDirs == null || regionDirs.length == 0) {
            log.info("No region databases found");
            return true;
        }
        
        log.info("Found {} region database(s)", regionDirs.length);
        
        int successCount = 0;
        int failCount = 0;
        long totalSizeBefore = 0;
        long totalSizeAfter = 0;
        long startTime = System.currentTimeMillis();
        
        for (File regionDir : regionDirs) {
            String regionName = regionDir.getName();
            long sizeBefore = getDirectorySize(regionDir);
            totalSizeBefore += sizeBefore;
            
            log.info("  Compacting region '{}' ({})...", regionName, formatSize(sizeBefore));
            
            try {
                compactRocksDbRegion(regionDir);
                
                long sizeAfter = getDirectorySize(regionDir);
                totalSizeAfter += sizeAfter;
                long saved = sizeBefore - sizeAfter;
                
                log.info("  ✓ {} compacted{}", regionName, 
                        saved > 0 ? " (saved " + formatSize(saved) + ")" : "");
                successCount++;
                
            } catch (Exception e) {
                log.error("  ✗ {} failed: {}", regionName, e.getMessage());
                failCount++;
            }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        long totalSaved = totalSizeBefore - totalSizeAfter;
        
        log.info("Compaction complete: {} succeeded, {} failed", successCount, failCount);
        log.info("Total size: {} → {} (saved {})", 
                formatSize(totalSizeBefore), formatSize(totalSizeAfter), formatSize(totalSaved));
        log.info("Duration: {}ms", duration);
        
        return failCount == 0;
    }
    
    /**
     * Compact a single RocksDB region database.
     */
    private void compactRocksDbRegion(File regionDir) throws RocksDBException {
        try (Options options = new Options()) {
            options.setCreateIfMissing(false);
            
            try (RocksDB db = RocksDB.open(options, regionDir.getAbsolutePath())) {
                db.compactRange();
            }
        }
    }
    
    /**
     * Execute SQLite VACUUM on all region databases.
     */
    private boolean executeSqliteVacuum() {
        String basePath = properties.getPersistence().getSqlite().getPath();
        
        // Get directory from path (path might be a file path like ./data/kuber.db)
        File pathFile = new File(basePath);
        File baseDir = pathFile.isDirectory() ? pathFile : pathFile.getParentFile();
        
        if (baseDir == null || !baseDir.exists() || !baseDir.isDirectory()) {
            log.info("SQLite directory does not exist: {} (will be created on first use)", basePath);
            return true;
        }
        
        log.info("SQLite vacuum starting - path: {}", baseDir.getAbsolutePath());
        
        // Find all .db files (region databases)
        File[] dbFiles = baseDir.listFiles(file -> 
                file.isFile() && file.getName().endsWith(".db"));
        
        if (dbFiles == null || dbFiles.length == 0) {
            log.info("No SQLite databases found");
            return true;
        }
        
        log.info("Found {} database(s)", dbFiles.length);
        
        int successCount = 0;
        int failCount = 0;
        long totalSizeBefore = 0;
        long totalSizeAfter = 0;
        long startTime = System.currentTimeMillis();
        
        for (File dbFile : dbFiles) {
            String dbName = dbFile.getName();
            long sizeBefore = dbFile.length();
            totalSizeBefore += sizeBefore;
            
            log.info("  Vacuuming '{}' ({})...", dbName, formatSize(sizeBefore));
            
            try {
                vacuumSqliteDatabase(dbFile);
                
                long sizeAfter = dbFile.length();
                totalSizeAfter += sizeAfter;
                long saved = sizeBefore - sizeAfter;
                
                log.info("  ✓ {} vacuumed{}", dbName, 
                        saved > 0 ? " (saved " + formatSize(saved) + ")" : "");
                successCount++;
                
            } catch (Exception e) {
                log.error("  ✗ {} failed: {}", dbName, e.getMessage());
                failCount++;
            }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        long totalSaved = totalSizeBefore - totalSizeAfter;
        
        log.info("Vacuum complete: {} succeeded, {} failed", successCount, failCount);
        log.info("Total size: {} → {} (saved {})", 
                formatSize(totalSizeBefore), formatSize(totalSizeAfter), formatSize(totalSaved));
        log.info("Duration: {}ms", duration);
        
        return failCount == 0;
    }
    
    /**
     * Vacuum a single SQLite database.
     */
    private void vacuumSqliteDatabase(File dbFile) throws SQLException {
        String url = "jdbc:sqlite:" + dbFile.getAbsolutePath();
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            stmt.execute("VACUUM");
        }
    }
    
    /**
     * Calculate directory size recursively.
     */
    private long getDirectorySize(File dir) {
        long size = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    size += file.length();
                } else if (file.isDirectory()) {
                    size += getDirectorySize(file);
                }
            }
        }
        return size;
    }
    
    /**
     * Format size in human-readable form.
     */
    private String formatSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
}
