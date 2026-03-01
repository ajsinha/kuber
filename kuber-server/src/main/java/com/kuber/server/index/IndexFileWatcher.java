/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 */
package com.kuber.server.index;

import com.kuber.server.config.KuberProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Watches for index operation trigger files and executes the corresponding actions.
 * 
 * <p>This provides a file-based mechanism to trigger index operations without using
 * the REST API or web UI, useful for scripting and automation.
 * 
 * <h2>Supported File Patterns:</h2>
 * <table border="1">
 *   <tr><th>File Name</th><th>Action</th></tr>
 *   <tr><td>{@code kuber.index.<region>.rebuild}</td><td>Rebuild all indexes for the region</td></tr>
 *   <tr><td>{@code kuber.index.<region>.drop}</td><td>Drop all indexes for the region</td></tr>
 *   <tr><td>{@code kuber.index.<region>.<field>.rebuild}</td><td>Rebuild specific index</td></tr>
 *   <tr><td>{@code kuber.index.<region>.<field>.drop}</td><td>Drop specific index</td></tr>
 *   <tr><td>{@code kuber.index.all.rebuild}</td><td>Rebuild ALL indexes across all regions</td></tr>
 *   <tr><td>{@code kuber.index.<region>.<field>.create.<type>}</td><td>Create new index (type: hash/btree/trigram/prefix)</td></tr>
 * </table>
 * 
 * <h2>Usage Examples:</h2>
 * <pre>
 * # Rebuild all indexes for 'customers' region:
 * touch kuber.index.customers.rebuild
 * 
 * # Drop all indexes for 'orders' region:
 * touch kuber.index.orders.drop
 * 
 * # Rebuild specific index:
 * touch kuber.index.customers.email.rebuild
 * 
 * # Drop specific index:
 * touch kuber.index.orders.status.drop
 * 
 * # Rebuild ALL indexes:
 * touch kuber.index.all.rebuild
 * 
 * # Create a new HASH index on customers.city:
 * touch kuber.index.customers.city.create.hash
 * 
 * # Create a new BTREE index on orders.amount:
 * touch kuber.index.orders.amount.create.btree
 * </pre>
 * 
 * <h2>Configuration:</h2>
 * <pre>
 * kuber:
 *   indexing:
 *     file-watcher-enabled: true     # Enable file-based triggers (default: true)
 *     file-watcher-interval-ms: 5000 # Check interval in milliseconds (default: 5s)
 *     file-watcher-directory: .      # Directory to watch (default: current dir)
 * </pre>
 * 
 * @version 2.6.4
 * @since 1.9.0
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class IndexFileWatcher {
    
    private static final String FILE_PREFIX = "kuber.index.";
    
    // Pattern: kuber.index.<region>.<action> or kuber.index.<region>.<field>.<action>
    // Actions: rebuild, drop, create.hash, create.btree, create.trigram, create.prefix
    private static final Pattern REGION_ACTION_PATTERN = 
        Pattern.compile("^kuber\\.index\\.([a-zA-Z0-9_-]+)\\.(rebuild|drop)$");
    
    private static final Pattern FIELD_ACTION_PATTERN = 
        Pattern.compile("^kuber\\.index\\.([a-zA-Z0-9_-]+)\\.([a-zA-Z0-9_.,-]+)\\.(rebuild|drop)$");
    
    private static final Pattern CREATE_INDEX_PATTERN = 
        Pattern.compile("^kuber\\.index\\.([a-zA-Z0-9_-]+)\\.([a-zA-Z0-9_.,-]+)\\.create\\.(hash|btree|trigram|prefix)$");
    
    private final IndexManager indexManager;
    private final IndexConfiguration indexConfiguration;
    private final KuberProperties properties;
    
    private Path watchDirectory;
    private Instant startTime;
    private boolean enabled = true;
    
    @PostConstruct
    public void initialize() {
        // Get watch directory from configuration or use current directory
        String configuredDir = properties.getIndexing().getFileWatcherDirectory();
        if (configuredDir != null && !configuredDir.isBlank()) {
            watchDirectory = Paths.get(configuredDir).toAbsolutePath();
        } else {
            watchDirectory = Paths.get(".").toAbsolutePath();
        }
        
        enabled = properties.getIndexing().isFileWatcherEnabled();
        startTime = Instant.now();
        
        // Clean up any stale trigger files from previous run
        cleanupTriggerFiles();
        
        if (enabled) {
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  INDEX FILE WATCHER INITIALIZED                                    ║");
            log.info("╠════════════════════════════════════════════════════════════════════╣");
            log.info("║  Watch Directory: {}", String.format("%-50s║", watchDirectory));
            log.info("║  Check Interval: {}ms", String.format("%-47s║", properties.getIndexing().getFileWatcherIntervalMs()));
            log.info("╠════════════════════════════════════════════════════════════════════╣");
            log.info("║  Supported Commands:                                               ║");
            log.info("║    touch kuber.index.<region>.rebuild      - Rebuild region indexes║");
            log.info("║    touch kuber.index.<region>.drop         - Drop region indexes   ║");
            log.info("║    touch kuber.index.<region>.<field>.rebuild - Rebuild one index  ║");
            log.info("║    touch kuber.index.<region>.<field>.drop    - Drop one index     ║");
            log.info("║    touch kuber.index.all.rebuild           - Rebuild ALL indexes   ║");
            log.info("║    touch kuber.index.<region>.<field>.create.<type> - Create index ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
        } else {
            log.info("Index file watcher is disabled");
        }
    }
    
    /**
     * Periodically check for index trigger files.
     * Default interval is 5 seconds.
     */
    @Scheduled(fixedDelayString = "${kuber.indexing.file-watcher-interval-ms:5000}")
    public void checkForTriggerFiles() {
        if (!enabled) {
            return;
        }
        
        // Don't check during the first 10 seconds after startup
        // This prevents accidental operations from stale files
        if (Instant.now().isBefore(startTime.plusSeconds(10))) {
            return;
        }
        
        try {
            // Find all files matching kuber.index.*
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(watchDirectory, FILE_PREFIX + "*")) {
                List<Path> triggerFiles = new ArrayList<>();
                for (Path file : stream) {
                    if (Files.isRegularFile(file)) {
                        triggerFiles.add(file);
                    }
                }
                
                // Sort to process in consistent order
                triggerFiles.sort(Comparator.comparing(Path::getFileName));
                
                for (Path triggerFile : triggerFiles) {
                    processTriggerFile(triggerFile);
                }
            }
        } catch (IOException e) {
            log.trace("Error scanning for trigger files: {}", e.getMessage());
        }
    }
    
    /**
     * Process a single trigger file.
     */
    private void processTriggerFile(Path triggerFile) {
        String fileName = triggerFile.getFileName().toString();
        
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  INDEX TRIGGER FILE DETECTED                                       ║");
        log.info("║  File: {}", String.format("%-59s║", fileName));
        log.info("╚════════════════════════════════════════════════════════════════════╝");
        
        try {
            boolean processed = false;
            
            // Check for "rebuild all" command
            if ("kuber.index.all.rebuild".equals(fileName)) {
                processRebuildAll();
                processed = true;
            }
            
            // Check for create index pattern: kuber.index.<region>.<field>.create.<type>
            if (!processed) {
                Matcher createMatcher = CREATE_INDEX_PATTERN.matcher(fileName);
                if (createMatcher.matches()) {
                    String region = createMatcher.group(1);
                    String field = createMatcher.group(2);
                    String type = createMatcher.group(3).toUpperCase();
                    processCreateIndex(region, field, type);
                    processed = true;
                }
            }
            
            // Check for field-level action: kuber.index.<region>.<field>.<action>
            if (!processed) {
                Matcher fieldMatcher = FIELD_ACTION_PATTERN.matcher(fileName);
                if (fieldMatcher.matches()) {
                    String region = fieldMatcher.group(1);
                    String field = fieldMatcher.group(2);
                    String action = fieldMatcher.group(3);
                    processFieldAction(region, field, action);
                    processed = true;
                }
            }
            
            // Check for region-level action: kuber.index.<region>.<action>
            if (!processed) {
                Matcher regionMatcher = REGION_ACTION_PATTERN.matcher(fileName);
                if (regionMatcher.matches()) {
                    String region = regionMatcher.group(1);
                    String action = regionMatcher.group(2);
                    processRegionAction(region, action);
                    processed = true;
                }
            }
            
            if (!processed) {
                log.warn("Unknown trigger file format: {}", fileName);
                log.warn("Expected: kuber.index.<region>.<action> or kuber.index.<region>.<field>.<action>");
            }
            
        } catch (Exception e) {
            log.error("Error processing trigger file {}: {}", fileName, e.getMessage(), e);
        } finally {
            // Always delete the trigger file after processing
            deleteTriggerFile(triggerFile);
        }
    }
    
    /**
     * Rebuild all indexes across all regions.
     */
    private void processRebuildAll() {
        log.info("Processing: REBUILD ALL INDEXES");
        
        long startTime = System.currentTimeMillis();
        
        try {
            indexManager.rebuildAllIndexes();
            long elapsed = System.currentTimeMillis() - startTime;
            
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  ALL INDEXES REBUILT SUCCESSFULLY                                  ║");
            log.info("║  Time: {}ms", String.format("%-56s║", elapsed));
            log.info("╚════════════════════════════════════════════════════════════════════╝");
        } catch (Exception e) {
            log.error("Failed to rebuild all indexes: {}", e.getMessage());
        }
    }
    
    /**
     * Process a region-level action (rebuild or drop all indexes for a region).
     */
    private void processRegionAction(String region, String action) {
        log.info("Processing: {} all indexes for region '{}'", action.toUpperCase(), region);
        
        long startTime = System.currentTimeMillis();
        
        try {
            if ("rebuild".equals(action)) {
                indexManager.rebuildRegionIndexes(region);
                long elapsed = System.currentTimeMillis() - startTime;
                
                log.info("╔════════════════════════════════════════════════════════════════════╗");
                log.info("║  REGION INDEXES REBUILT                                            ║");
                log.info("║  Region: {}", String.format("%-57s║", region));
                log.info("║  Time: {}ms", String.format("%-56s║", elapsed));
                log.info("╚════════════════════════════════════════════════════════════════════╝");
                
            } else if ("drop".equals(action)) {
                indexManager.dropAllIndexes(region);
                long elapsed = System.currentTimeMillis() - startTime;
                
                log.info("╔════════════════════════════════════════════════════════════════════╗");
                log.info("║  REGION INDEXES DROPPED                                            ║");
                log.info("║  Region: {}", String.format("%-57s║", region));
                log.info("║  Time: {}ms", String.format("%-56s║", elapsed));
                log.info("╚════════════════════════════════════════════════════════════════════╝");
            }
        } catch (Exception e) {
            log.error("Failed to {} indexes for region '{}': {}", action, region, e.getMessage());
        }
    }
    
    /**
     * Process a field-level action (rebuild or drop a specific index).
     */
    private void processFieldAction(String region, String field, String action) {
        log.info("Processing: {} index '{}.{}'", action.toUpperCase(), region, field);
        
        long startTime = System.currentTimeMillis();
        
        try {
            if ("rebuild".equals(action)) {
                indexManager.rebuildIndex(region, field);
                long elapsed = System.currentTimeMillis() - startTime;
                
                log.info("╔════════════════════════════════════════════════════════════════════╗");
                log.info("║  INDEX REBUILT                                                     ║");
                log.info("║  Index: {}.{}", String.format("%-55s║", region + "." + field));
                log.info("║  Time: {}ms", String.format("%-56s║", elapsed));
                log.info("╚════════════════════════════════════════════════════════════════════╝");
                
            } else if ("drop".equals(action)) {
                indexManager.dropIndex(region, field);
                long elapsed = System.currentTimeMillis() - startTime;
                
                log.info("╔════════════════════════════════════════════════════════════════════╗");
                log.info("║  INDEX DROPPED                                                     ║");
                log.info("║  Index: {}.{}", String.format("%-55s║", region + "." + field));
                log.info("║  Time: {}ms", String.format("%-56s║", elapsed));
                log.info("╚════════════════════════════════════════════════════════════════════╝");
            }
        } catch (Exception e) {
            log.error("Failed to {} index '{}.{}': {}", action, region, field, e.getMessage());
        }
    }
    
    /**
     * Create a new index via trigger file.
     */
    private void processCreateIndex(String region, String field, String type) {
        log.info("Processing: CREATE {} index on '{}.{}'", type, region, field);
        
        long startTime = System.currentTimeMillis();
        
        try {
            // Create the index using the simpler method signature
            boolean created = indexManager.createIndex(region, field, IndexType.valueOf(type), 
                "Created via file trigger");
            
            if (created) {
                // Rebuild to populate
                indexManager.rebuildIndex(region, field);
                
                long elapsed = System.currentTimeMillis() - startTime;
                
                log.info("╔════════════════════════════════════════════════════════════════════╗");
                log.info("║  INDEX CREATED                                                     ║");
                log.info("║  Index: {}.{}", String.format("%-55s║", region + "." + field));
                log.info("║  Type: {}", String.format("%-58s║", type));
                log.info("║  Time: {}ms", String.format("%-56s║", elapsed));
                log.info("╚════════════════════════════════════════════════════════════════════╝");
                
                // Save configuration
                indexConfiguration.saveConfiguration();
            } else {
                log.warn("Index creation failed or already exists: {}.{}", region, field);
            }
            
        } catch (Exception e) {
            log.error("Failed to create {} index on '{}.{}': {}", type, region, field, e.getMessage());
        }
    }
    
    /**
     * Delete a trigger file after processing.
     */
    private void deleteTriggerFile(Path triggerFile) {
        try {
            Files.deleteIfExists(triggerFile);
            log.debug("Deleted trigger file: {}", triggerFile.getFileName());
        } catch (IOException e) {
            log.warn("Could not delete trigger file {}: {}", triggerFile.getFileName(), e.getMessage());
        }
    }
    
    /**
     * Clean up any stale trigger files from previous runs.
     */
    private void cleanupTriggerFiles() {
        try {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(watchDirectory, FILE_PREFIX + "*")) {
                for (Path file : stream) {
                    if (Files.isRegularFile(file)) {
                        Files.deleteIfExists(file);
                        log.debug("Cleaned up stale trigger file: {}", file.getFileName());
                    }
                }
            }
        } catch (IOException e) {
            log.trace("Error cleaning up trigger files: {}", e.getMessage());
        }
    }
    
    /**
     * Check if file watcher is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }
    
    /**
     * Enable or disable the file watcher.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        log.info("Index file watcher {}", enabled ? "enabled" : "disabled");
    }
    
    /**
     * Get the watch directory path.
     */
    public Path getWatchDirectory() {
        return watchDirectory;
    }
}
