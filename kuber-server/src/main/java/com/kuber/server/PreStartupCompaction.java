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
package com.kuber.server;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

/**
 * Standalone pre-startup compaction utility.
 * 
 * <strong>DEPRECATED:</strong> As of v1.2.6, database maintenance runs within Spring context
 * via {@link com.kuber.server.persistence.PersistenceMaintenanceService} called from
 * {@link com.kuber.server.startup.StartupOrchestrator}.
 * 
 * This class is retained for standalone testing and manual execution only.
 * It is no longer called from KuberApplication.main().
 * 
 * Key features:
 * - Loads configuration directly from application.properties (no Spring dependency)
 * - Detects persistence type (RocksDB or SQLite)
 * - Performs compaction/vacuum on all region databases
 * 
 * @author Ashutosh Sinha
 * @version 1.4.1
 * @deprecated Use {@link PersistenceMaintenanceService} instead for in-context maintenance
 */
@Deprecated(since = "1.2.6", forRemoval = false)
public class PreStartupCompaction {
    
    private static final String PROPERTIES_FILE = "application.properties";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_CYAN = "\u001B[36m";
    private static final String ANSI_RESET = "\u001B[0m";
    
    // Configuration keys
    private static final String PERSISTENCE_TYPE_KEY = "kuber.persistence.type";
    private static final String ROCKSDB_PATH_KEY = "kuber.persistence.rocksdb.path";
    private static final String ROCKSDB_COMPACTION_ENABLED_KEY = "kuber.persistence.rocksdb.compaction-enabled";
    private static final String SQLITE_PATH_KEY = "kuber.persistence.sqlite.path";
    
    // Defaults
    private static final String DEFAULT_PERSISTENCE_TYPE = "rocksdb";
    private static final String DEFAULT_ROCKSDB_PATH = "./data/rocksdb";
    private static final String DEFAULT_SQLITE_PATH = "./data/sqlite";
    private static final boolean DEFAULT_COMPACTION_ENABLED = true;
    
    private final Properties properties;
    
    static {
        // Load RocksDB native library
        RocksDB.loadLibrary();
    }
    
    public PreStartupCompaction() {
        this.properties = loadProperties();
    }
    
    /**
     * Execute pre-startup compaction/vacuum based on configured persistence type.
     * 
     * @return true if compaction completed successfully, false otherwise
     */
    public boolean execute() {
        String persistenceType = getProperty(PERSISTENCE_TYPE_KEY, DEFAULT_PERSISTENCE_TYPE).toLowerCase();
        
        printHeader();
        
        try {
            switch (persistenceType) {
                case "rocksdb", "rocks" -> {
                    return executeRocksDbCompaction();
                }
                case "sqlite" -> {
                    return executeSqliteVacuum();
                }
                case "mongodb", "mongo", "postgresql", "postgres", "memory", "mem" -> {
                    printInfo("Persistence type '" + persistenceType + "' does not require pre-startup maintenance");
                    return true;
                }
                default -> {
                    printWarning("Unknown persistence type '" + persistenceType + "', assuming RocksDB");
                    return executeRocksDbCompaction();
                }
            }
        } catch (Exception e) {
            printError("Pre-startup maintenance failed: " + e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            printFooter();
        }
    }
    
    /**
     * Execute RocksDB compaction on all region databases.
     */
    private boolean executeRocksDbCompaction() {
        boolean compactionEnabled = Boolean.parseBoolean(
                getProperty(ROCKSDB_COMPACTION_ENABLED_KEY, String.valueOf(DEFAULT_COMPACTION_ENABLED)));
        
        if (!compactionEnabled) {
            printInfo("RocksDB compaction is disabled in configuration");
            return true;
        }
        
        String basePath = getProperty(ROCKSDB_PATH_KEY, DEFAULT_ROCKSDB_PATH);
        File baseDir = new File(basePath);
        
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            printInfo("RocksDB directory does not exist: " + basePath);
            printInfo("Skipping compaction (will be created on first use)");
            return true;
        }
        
        printInfo("RocksDB compaction starting...");
        printInfo("Base path: " + baseDir.getAbsolutePath());
        
        // Find all region directories (exclude hidden and metadata)
        File[] regionDirs = baseDir.listFiles(file -> 
                file.isDirectory() && 
                !file.getName().startsWith("_") && 
                !file.getName().startsWith("."));
        
        if (regionDirs == null || regionDirs.length == 0) {
            printInfo("No region databases found");
            return true;
        }
        
        printInfo("Found " + regionDirs.length + " region database(s)");
        
        int successCount = 0;
        int failCount = 0;
        long totalSizeBefore = 0;
        long totalSizeAfter = 0;
        long startTime = System.currentTimeMillis();
        
        for (File regionDir : regionDirs) {
            String regionName = regionDir.getName();
            long sizeBefore = getDirectorySize(regionDir);
            totalSizeBefore += sizeBefore;
            
            printProgress("  Compacting region '" + regionName + "' (" + formatSize(sizeBefore) + ")...");
            
            try {
                compactRocksDbRegion(regionDir);
                
                long sizeAfter = getDirectorySize(regionDir);
                totalSizeAfter += sizeAfter;
                long saved = sizeBefore - sizeAfter;
                
                printSuccess("  ✓ " + regionName + " compacted" + 
                        (saved > 0 ? " (saved " + formatSize(saved) + ")" : ""));
                successCount++;
                
            } catch (Exception e) {
                printError("  ✗ " + regionName + " failed: " + e.getMessage());
                failCount++;
            }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        long totalSaved = totalSizeBefore - totalSizeAfter;
        
        printInfo("");
        printInfo("Compaction complete: " + successCount + " succeeded, " + failCount + " failed");
        printInfo("Total size: " + formatSize(totalSizeBefore) + " → " + formatSize(totalSizeAfter) + 
                " (saved " + formatSize(totalSaved) + ")");
        printInfo("Duration: " + duration + "ms");
        
        return failCount == 0;
    }
    
    /**
     * Compact a single RocksDB region database.
     */
    private void compactRocksDbRegion(File regionDir) throws RocksDBException {
        try (Options options = new Options()) {
            options.setCreateIfMissing(false);
            
            try (RocksDB db = RocksDB.open(options, regionDir.getAbsolutePath())) {
                // Compact the entire key range
                db.compactRange();
            }
        }
    }
    
    /**
     * Execute SQLite VACUUM on all region databases.
     */
    private boolean executeSqliteVacuum() {
        String basePath = getProperty(SQLITE_PATH_KEY, DEFAULT_SQLITE_PATH);
        File baseDir = new File(basePath);
        
        if (!baseDir.exists() || !baseDir.isDirectory()) {
            printInfo("SQLite directory does not exist: " + basePath);
            printInfo("Skipping vacuum (will be created on first use)");
            return true;
        }
        
        printInfo("SQLite vacuum starting...");
        printInfo("Base path: " + baseDir.getAbsolutePath());
        
        // Find all .db files (region databases)
        File[] dbFiles = baseDir.listFiles(file -> 
                file.isFile() && file.getName().endsWith(".db"));
        
        if (dbFiles == null || dbFiles.length == 0) {
            printInfo("No SQLite databases found");
            return true;
        }
        
        printInfo("Found " + dbFiles.length + " database(s)");
        
        int successCount = 0;
        int failCount = 0;
        long totalSizeBefore = 0;
        long totalSizeAfter = 0;
        long startTime = System.currentTimeMillis();
        
        // Load SQLite JDBC driver
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            printError("SQLite JDBC driver not found");
            return false;
        }
        
        for (File dbFile : dbFiles) {
            String dbName = dbFile.getName();
            long sizeBefore = dbFile.length();
            totalSizeBefore += sizeBefore;
            
            printProgress("  Vacuuming '" + dbName + "' (" + formatSize(sizeBefore) + ")...");
            
            try {
                vacuumSqliteDatabase(dbFile);
                
                long sizeAfter = dbFile.length();
                totalSizeAfter += sizeAfter;
                long saved = sizeBefore - sizeAfter;
                
                printSuccess("  ✓ " + dbName + " vacuumed" + 
                        (saved > 0 ? " (saved " + formatSize(saved) + ")" : ""));
                successCount++;
                
            } catch (Exception e) {
                printError("  ✗ " + dbName + " failed: " + e.getMessage());
                failCount++;
            }
        }
        
        long duration = System.currentTimeMillis() - startTime;
        long totalSaved = totalSizeBefore - totalSizeAfter;
        
        printInfo("");
        printInfo("Vacuum complete: " + successCount + " succeeded, " + failCount + " failed");
        printInfo("Total size: " + formatSize(totalSizeBefore) + " → " + formatSize(totalSizeAfter) + 
                " (saved " + formatSize(totalSaved) + ")");
        printInfo("Duration: " + duration + "ms");
        
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
     * Load properties from application.properties file.
     */
    private Properties loadProperties() {
        Properties props = new Properties();
        
        // Try multiple locations for application.properties
        List<String> locations = Arrays.asList(
                "application.properties",
                "src/main/resources/application.properties",
                "config/application.properties",
                "classpath:application.properties"
        );
        
        // First try classpath
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            if (is != null) {
                props.load(is);
                return props;
            }
        } catch (IOException e) {
            // Continue to file-based search
        }
        
        // Then try file locations
        for (String location : locations) {
            File file = new File(location);
            if (file.exists() && file.isFile()) {
                try (InputStream is = new FileInputStream(file)) {
                    props.load(is);
                    return props;
                } catch (IOException e) {
                    // Continue to next location
                }
            }
        }
        
        printWarning("Could not load application.properties, using defaults");
        return props;
    }
    
    /**
     * Get a property value with default.
     */
    private String getProperty(String key, String defaultValue) {
        // Also check system properties (for command-line overrides)
        String value = System.getProperty(key);
        if (value != null && !value.isEmpty()) {
            return value;
        }
        return properties.getProperty(key, defaultValue);
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
    
    // ==================== Console Output ====================
    
    private void printHeader() {
        System.out.println();
        System.out.println(ANSI_CYAN + "┌─────────────────────────────────────────────────────────────────┐" + ANSI_RESET);
        System.out.println(ANSI_CYAN + "│           Pre-Startup Database Maintenance                      │" + ANSI_RESET);
        System.out.println(ANSI_CYAN + "└─────────────────────────────────────────────────────────────────┘" + ANSI_RESET);
    }
    
    private void printFooter() {
        System.out.println(ANSI_CYAN + "───────────────────────────────────────────────────────────────────" + ANSI_RESET);
        System.out.println();
    }
    
    private void printInfo(String message) {
        System.out.println(ANSI_CYAN + "[PRE-STARTUP] " + ANSI_RESET + message);
    }
    
    private void printProgress(String message) {
        System.out.println(ANSI_YELLOW + "[PRE-STARTUP] " + ANSI_RESET + message);
    }
    
    private void printSuccess(String message) {
        System.out.println(ANSI_GREEN + "[PRE-STARTUP] " + ANSI_RESET + message);
    }
    
    private void printWarning(String message) {
        System.out.println(ANSI_YELLOW + "[PRE-STARTUP] WARNING: " + ANSI_RESET + message);
    }
    
    private void printError(String message) {
        System.out.println("\u001B[31m[PRE-STARTUP] ERROR: " + ANSI_RESET + message);
    }
    
    /**
     * Static entry point for running pre-startup maintenance.
     * Can be called from main() before Spring context initialization.
     * 
     * @return true if maintenance completed successfully
     */
    public static boolean run() {
        try {
            PreStartupCompaction compaction = new PreStartupCompaction();
            return compaction.execute();
        } catch (Exception e) {
            System.err.println("[PRE-STARTUP] Failed to initialize: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * Main method for standalone execution (testing/manual runs).
     */
    public static void main(String[] args) {
        boolean success = run();
        System.exit(success ? 0 : 1);
    }
}
