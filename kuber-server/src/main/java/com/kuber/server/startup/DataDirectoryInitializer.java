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
package com.kuber.server.startup;

import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Initializes data directories at application startup.
 * 
 * <p>Creates all necessary directories based on configuration:
 * <ul>
 *   <li>Base data directory (kuber.base.datadir)</li>
 *   <li>Persistence data directories (SQLite, RocksDB, LMDB)</li>
 *   <li>Autoload directories (inbox, outbox)</li>
 *   <li>Backup and restore directories</li>
 *   <li>Publishing file directories (audit, archive)</li>
 * </ul>
 * 
 * <p>This component runs with @Order(0) to ensure directories exist
 * before other startup components run.
 * 
 * @version 2.6.0
 */
@Component
@Slf4j
public class DataDirectoryInitializer {
    
    private final KuberProperties properties;
    
    public DataDirectoryInitializer(KuberProperties properties) {
        this.properties = properties;
    }
    
    /**
     * Initialize data directories when application is ready.
     * Runs before StartupOrchestrator (@Order(0) vs @Order(1)).
     */
    @EventListener(ApplicationReadyEvent.class)
    @Order(0)
    public void initializeDirectories() {
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  Phase 0: Data Directory Initialization                            ║");
        log.info("║           Creating required directories...                         ║");
        log.info("╚════════════════════════════════════════════════════════════════════╝");
        
        String baseDataDir = properties.getBase().getDatadir();
        log.info("Base data directory: {}", Paths.get(baseDataDir).toAbsolutePath());
        
        List<String> createdDirs = new ArrayList<>();
        List<String> existingDirs = new ArrayList<>();
        List<String> failedDirs = new ArrayList<>();
        
        // List of directories to create
        List<DirectoryInfo> directories = collectDirectories(baseDataDir);
        
        for (DirectoryInfo dir : directories) {
            try {
                Path path = Paths.get(dir.path);
                if (Files.exists(path)) {
                    existingDirs.add(dir.description + ": " + path.toAbsolutePath());
                } else {
                    Files.createDirectories(path);
                    createdDirs.add(dir.description + ": " + path.toAbsolutePath());
                    log.info("Created directory: {} ({})", path.toAbsolutePath(), dir.description);
                }
            } catch (IOException e) {
                failedDirs.add(dir.description + ": " + dir.path + " - " + e.getMessage());
                log.error("Failed to create directory: {} - {}", dir.path, e.getMessage());
            }
        }
        
        // Summary
        log.info("Directory initialization complete:");
        log.info("  - Created: {} directories", createdDirs.size());
        log.info("  - Existing: {} directories", existingDirs.size());
        if (!failedDirs.isEmpty()) {
            log.warn("  - Failed: {} directories", failedDirs.size());
            for (String failed : failedDirs) {
                log.warn("    - {}", failed);
            }
        }
    }
    
    /**
     * Collect all directories that need to be created.
     */
    private List<DirectoryInfo> collectDirectories(String baseDataDir) {
        List<DirectoryInfo> dirs = new ArrayList<>();
        
        // Base data directory
        dirs.add(new DirectoryInfo(baseDataDir, "Base data directory"));
        
        // Data subdirectory (for persistence stores)
        dirs.add(new DirectoryInfo(baseDataDir + "/data", "Persistence data directory"));
        
        // SQLite directory (parent of the .db file)
        String sqlitePath = properties.getPersistence().getSqlite().getPath();
        if (sqlitePath != null && !sqlitePath.isEmpty()) {
            Path sqliteParent = Paths.get(sqlitePath).getParent();
            if (sqliteParent != null) {
                dirs.add(new DirectoryInfo(sqliteParent.toString(), "SQLite data directory"));
            }
        }
        
        // RocksDB directory
        String rocksdbPath = properties.getPersistence().getRocksdb().getPath();
        if (rocksdbPath != null && !rocksdbPath.isEmpty()) {
            dirs.add(new DirectoryInfo(rocksdbPath, "RocksDB data directory"));
        }
        
        // LMDB directory
        String lmdbPath = properties.getPersistence().getLmdb().getPath();
        if (lmdbPath != null && !lmdbPath.isEmpty()) {
            dirs.add(new DirectoryInfo(lmdbPath, "LMDB data directory"));
        }
        
        // Autoload directories
        String autoloadDir = properties.getAutoload().getDirectory();
        if (autoloadDir != null && !autoloadDir.isEmpty()) {
            dirs.add(new DirectoryInfo(autoloadDir, "Autoload base directory"));
            dirs.add(new DirectoryInfo(autoloadDir + "/inbox", "Autoload inbox directory"));
            dirs.add(new DirectoryInfo(autoloadDir + "/outbox", "Autoload outbox directory"));
        }
        
        // Backup directory
        String backupDir = properties.getBackup().getBackupDirectory();
        if (backupDir != null && !backupDir.isEmpty()) {
            dirs.add(new DirectoryInfo(backupDir, "Backup directory"));
        }
        
        // Restore directory
        String restoreDir = properties.getBackup().getRestoreDirectory();
        if (restoreDir != null && !restoreDir.isEmpty()) {
            dirs.add(new DirectoryInfo(restoreDir, "Restore directory"));
        }
        
        // Publishing file directories (audit and archive)
        // These are configured in kuber.publishing.brokers.*.directory
        // We'll create default locations based on base data dir
        dirs.add(new DirectoryInfo(baseDataDir + "/log/kuber/audit", "Audit log directory"));
        dirs.add(new DirectoryInfo(baseDataDir + "/archive/kuber-events", "Event archive directory"));
        
        return dirs;
    }
    
    /**
     * Helper class to hold directory information.
     */
    private static class DirectoryInfo {
        final String path;
        final String description;
        
        DirectoryInfo(String path, String description) {
            this.path = path;
            this.description = description;
        }
    }
}
