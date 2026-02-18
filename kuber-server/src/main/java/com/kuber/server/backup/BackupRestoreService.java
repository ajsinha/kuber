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
package com.kuber.server.backup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kuber.core.model.CacheEntry;
import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.persistence.PersistenceStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Service for backup and restore of cache regions.
 * 
 * <p>Supports all persistence stores except MEMORY: RocksDB, LMDB, PostgreSQL, 
 * SQLite, MongoDB, and Aerospike. Each store uses its own forEachEntry implementation
 * for efficient streaming backup.
 * 
 * <h3>Backup</h3>
 * <ul>
 *   <li>Cron-scheduled backup (default: 11:00 PM daily)</li>
 *   <li>Each region backed up to separate file: &lt;region&gt;.&lt;timestamp&gt;.backup[.gz]</li>
 *   <li>Optional gzip compression (enabled by default)</li>
 *   <li>Automatic cleanup of old backups based on retention policy</li>
 *   <li>Manual backup via REST API or Admin Dashboard</li>
 * </ul>
 * 
 * <h3>Restore</h3>
 * <ul>
 *   <li>Place backup file in restore directory to trigger restore</li>
 *   <li>Region name inferred from backup file name</li>
 *   <li>Region is locked during restore (no read/write operations allowed)</li>
 *   <li>Existing data is purged before restore</li>
 *   <li>Processed files moved to backup directory after restore</li>
 * </ul>
 * 
 * @version 2.6.0
 */
@Service
@Slf4j
public class BackupRestoreService {
    
    private static final DateTimeFormatter TIMESTAMP_FORMAT = 
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    
    // Backup file pattern: <region>.<timestamp>.backup or <region>.<timestamp>.backup.gz
    private static final Pattern BACKUP_FILE_PATTERN = 
            Pattern.compile("^(.+)\\.(\\d{8}_\\d{6})\\.backup(\\.gz)?$");
    
    private final KuberProperties properties;
    private final CacheService cacheService;
    private final PersistenceStore persistenceStore;
    private final ObjectMapper objectMapper;
    
    private Path backupDirectory;
    private Path restoreDirectory;
    
    private ThreadPoolTaskScheduler backupScheduler;
    private ScheduledFuture<?> backupFuture;
    private ScheduledExecutorService restoreWatcher;
    
    // Track regions being restored (locked for operations)
    private final ConcurrentHashMap<String, AtomicBoolean> regionsBeingRestored = new ConcurrentHashMap<>();
    
    // Statistics
    private final AtomicLong totalBackups = new AtomicLong(0);
    private final AtomicLong totalRestores = new AtomicLong(0);
    private final AtomicLong totalBackupBytes = new AtomicLong(0);
    private final AtomicLong totalRestoreBytes = new AtomicLong(0);
    private volatile Instant lastBackupTime;
    private volatile Instant lastRestoreTime;
    
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    
    public BackupRestoreService(KuberProperties properties, 
                                CacheService cacheService,
                                PersistenceStore persistenceStore) {
        this.properties = properties;
        this.cacheService = cacheService;
        this.persistenceStore = persistenceStore;
        
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
    
    /**
     * Start the backup/restore service after cache is initialized.
     * Called by StartupOrchestrator after cache recovery completes.
     */
    public void start() {
        if (!properties.getBackup().isEnabled()) {
            log.info("Backup/restore service is disabled");
            return;
        }
        
        if (started.getAndSet(true)) {
            log.warn("BackupRestoreService already started");
            return;
        }
        
        // Check persistence type - support all except MEMORY
        // v2.1.0: Extended support to PostgreSQL, SQLite, MongoDB, Aerospike (was RocksDB/LMDB only)
        PersistenceStore.PersistenceType type = persistenceStore.getType();
        if (type == PersistenceStore.PersistenceType.MEMORY) {
            log.info("Backup/restore service not supported for {} persistence. " +
                    "MEMORY persistence has nothing to backup.", type);
            return;
        }
        
        try {
            initializeDirectories();
            startSchedulers();
            log.info("Backup/restore service started - backup: {}, restore: {}, cron: {}",
                    backupDirectory, restoreDirectory, properties.getBackup().getCron());
        } catch (Exception e) {
            log.error("Failed to start backup/restore service", e);
        }
    }
    
    private void initializeDirectories() throws IOException {
        KuberProperties.Backup config = properties.getBackup();
        
        backupDirectory = Paths.get(config.getBackupDirectory()).toAbsolutePath();
        restoreDirectory = Paths.get(config.getRestoreDirectory()).toAbsolutePath();
        
        if (config.isCreateDirectories()) {
            Files.createDirectories(backupDirectory);
            Files.createDirectories(restoreDirectory);
            log.info("Created backup directories: backup={}, restore={}", 
                    backupDirectory, restoreDirectory);
        }
        
        if (!Files.exists(backupDirectory)) {
            throw new IOException("Backup directory does not exist: " + backupDirectory);
        }
        if (!Files.exists(restoreDirectory)) {
            throw new IOException("Restore directory does not exist: " + restoreDirectory);
        }
    }
    
    private void startSchedulers() {
        KuberProperties.Backup config = properties.getBackup();
        
        // Backup scheduler using cron expression
        backupScheduler = new ThreadPoolTaskScheduler();
        backupScheduler.setPoolSize(1);
        backupScheduler.setThreadNamePrefix("kuber-backup-");
        backupScheduler.setDaemon(true);
        backupScheduler.initialize();
        
        String cronExpression = config.getCron();
        CronTrigger cronTrigger = new CronTrigger(cronExpression);
        backupFuture = backupScheduler.schedule(this::performScheduledBackup, cronTrigger);
        log.info("Backup scheduler started with cron: {}", cronExpression);
        
        // Restore watcher (checks every 30 seconds)
        restoreWatcher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kuber-restore-watcher");
            t.setDaemon(true);
            return t;
        });
        
        restoreWatcher.scheduleAtFixedRate(this::checkForRestoreFiles, 
                30, 30, TimeUnit.SECONDS);
    }
    
    @PreDestroy
    public void shutdown() {
        if (!started.get() || shuttingDown.getAndSet(true)) {
            return;
        }
        
        log.info("Shutting down backup/restore service...");
        
        // Cancel scheduled backup task
        if (backupFuture != null) {
            backupFuture.cancel(false);
        }
        
        // Shutdown backup scheduler
        if (backupScheduler != null) {
            backupScheduler.shutdown();
        }
        
        // Shutdown restore watcher
        if (restoreWatcher != null) {
            restoreWatcher.shutdown();
            try {
                if (!restoreWatcher.awaitTermination(5, TimeUnit.SECONDS)) {
                    restoreWatcher.shutdownNow();
                }
            } catch (InterruptedException e) {
                restoreWatcher.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        log.info("Backup/restore service stopped. Total backups: {}, Total restores: {}", 
                totalBackups.get(), totalRestores.get());
    }
    
    // ==================== BACKUP ====================
    
    private void performScheduledBackup() {
        if (shuttingDown.get()) return;
        
        // Check if cache service is initialized
        if (!cacheService.isInitialized()) {
            log.warn("Scheduled backup skipped: Cache service is not yet initialized");
            return;
        }
        
        log.info("Starting scheduled backup of all regions...");
        long startTime = System.currentTimeMillis();
        int successCount = 0;
        int failCount = 0;
        int skippedCount = 0;
        
        try {
            Set<String> regions = cacheService.getRegionNames();
            for (String region : regions) {
                if (shuttingDown.get()) break;
                
                // Skip regions that are still loading
                if (cacheService.isRegionLoading(region)) {
                    log.info("Skipping backup of region '{}': still loading", region);
                    skippedCount++;
                    continue;
                }
                
                try {
                    backupRegion(region);
                    successCount++;
                } catch (IllegalStateException e) {
                    // Region not ready (loading, restoring, or empty)
                    log.warn("Skipped backup of region '{}': {}", region, e.getMessage());
                    skippedCount++;
                } catch (Exception e) {
                    log.error("Failed to backup region '{}': {}", region, e.getMessage(), e);
                    failCount++;
                }
            }
            
            // Cleanup old backups
            cleanupOldBackups();
            
        } catch (Exception e) {
            log.error("Error during scheduled backup: {}", e.getMessage(), e);
        }
        
        long elapsed = System.currentTimeMillis() - startTime;
        log.info("Scheduled backup completed in {} ms. Success: {}, Failed: {}, Skipped: {}", 
                elapsed, successCount, failCount, skippedCount);
    }
    
    /**
     * Backup a single region to a file.
     * 
     * @param region Region name
     * @return Path to the backup file
     * @throws IOException if backup fails
     * @throws IllegalStateException if region is being restored or loaded, or cache not ready
     */
    public Path backupRegion(String region) throws IOException {
        // Check if cache service is initialized
        if (!cacheService.isInitialized()) {
            throw new IllegalStateException("Cache service is not yet initialized, cannot backup");
        }
        
        // Check if region is being restored
        if (isRegionBeingRestored(region)) {
            throw new IllegalStateException("Region '" + region + "' is being restored, cannot backup");
        }
        
        // Check if region is being loaded during startup
        if (cacheService.isRegionLoading(region)) {
            throw new IllegalStateException("Region '" + region + "' is still being loaded, cannot backup");
        }
        
        // Check if region exists
        if (!cacheService.getRegionNames().contains(region)) {
            throw new IllegalStateException("Region '" + region + "' does not exist");
        }
        
        KuberProperties.Backup config = properties.getBackup();
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);
        String fileName = region + "." + timestamp + ".backup" + (config.isCompress() ? ".gz" : "");
        Path backupFile = backupDirectory.resolve(fileName);
        
        log.info("Backing up region '{}' to {}", region, backupFile);
        long startTime = System.currentTimeMillis();
        
        Charset charset = Charset.forName(config.getFileEncoding());
        
        // Use AtomicLong for counting in lambda
        final java.util.concurrent.atomic.AtomicLong entryCount = new java.util.concurrent.atomic.AtomicLong(0);
        
        try (OutputStream fos = Files.newOutputStream(backupFile);
             OutputStream os = config.isCompress() ? new GZIPOutputStream(fos) : fos;
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, charset))) {
            
            // Write header with metadata
            Map<String, Object> header = new LinkedHashMap<>();
            header.put("version", "1.4.2");
            header.put("region", region);
            header.put("timestamp", Instant.now().toString());
            header.put("compressed", config.isCompress());
            writer.write("# KUBER_BACKUP_HEADER: " + objectMapper.writeValueAsString(header));
            writer.newLine();
            
            // Stream through all entries using forEachEntry - memory efficient
            persistenceStore.forEachEntry(region, entry -> {
                if (shuttingDown.get()) return;
                
                try {
                    String json = objectMapper.writeValueAsString(entry);
                    writer.write(json);
                    writer.newLine();
                    
                    long count = entryCount.incrementAndGet();
                    if (count > 0 && count % 50000 == 0) {
                        log.info("[{}] Backed up {} entries so far...", region, count);
                        // Periodic flush to reduce memory pressure
                        try {
                            writer.flush();
                        } catch (IOException flushEx) {
                            log.warn("Failed to flush during backup: {}", flushEx.getMessage());
                        }
                    }
                } catch (IOException e) {
                    log.warn("Failed to write entry to backup: {}", e.getMessage());
                }
            });
            
            writer.flush();
        }
        
        // Check if backup is empty (only header, no entries)
        if (entryCount.get() == 0) {
            log.warn("[{}] BACKUP WARNING: No entries found in persistence! " +
                    "Region may still be loading or persistence store not ready. " +
                    "Deleting empty backup file: {}", region, backupFile.getFileName());
            try {
                Files.deleteIfExists(backupFile);
            } catch (IOException deleteEx) {
                log.warn("Failed to delete empty backup file: {}", deleteEx.getMessage());
            }
            throw new IllegalStateException("Region '" + region + "' has no entries in persistence. " +
                    "The region may not be fully loaded yet.");
        }
        
        long byteCount = Files.size(backupFile);
        long elapsed = System.currentTimeMillis() - startTime;
        
        totalBackups.incrementAndGet();
        totalBackupBytes.addAndGet(byteCount);
        lastBackupTime = Instant.now();
        
        log.info("[{}] BACKUP COMPLETED: {} entries, {} bytes, {} ms -> {}", 
                region, entryCount.get(), byteCount, elapsed, backupFile.getFileName());
        
        return backupFile;
    }
    
    private void cleanupOldBackups() {
        int maxBackups = properties.getBackup().getMaxBackupsPerRegion();
        if (maxBackups <= 0) return;
        
        try {
            // Group backup files by region
            Map<String, List<Path>> backupsByRegion = new HashMap<>();
            
            try (Stream<Path> files = Files.list(backupDirectory)) {
                files.filter(Files::isRegularFile)
                     .filter(p -> BACKUP_FILE_PATTERN.matcher(p.getFileName().toString()).matches())
                     .forEach(p -> {
                         Matcher m = BACKUP_FILE_PATTERN.matcher(p.getFileName().toString());
                         if (m.matches()) {
                             String region = m.group(1);
                             backupsByRegion.computeIfAbsent(region, k -> new ArrayList<>()).add(p);
                         }
                     });
            }
            
            // Delete old backups for each region
            for (Map.Entry<String, List<Path>> entry : backupsByRegion.entrySet()) {
                String region = entry.getKey();
                List<Path> backups = entry.getValue();
                
                if (backups.size() > maxBackups) {
                    // Sort by modification time (newest first)
                    backups.sort((a, b) -> {
                        try {
                            return Files.getLastModifiedTime(b).compareTo(Files.getLastModifiedTime(a));
                        } catch (IOException e) {
                            return 0;
                        }
                    });
                    
                    // Delete older backups
                    for (int i = maxBackups; i < backups.size(); i++) {
                        Path oldBackup = backups.get(i);
                        Files.deleteIfExists(oldBackup);
                        log.debug("Deleted old backup: {}", oldBackup.getFileName());
                    }
                    
                    log.info("Cleaned up {} old backups for region '{}'", 
                            backups.size() - maxBackups, region);
                }
            }
        } catch (Exception e) {
            log.warn("Error cleaning up old backups: {}", e.getMessage());
        }
    }
    
    // ==================== RESTORE ====================
    
    private void checkForRestoreFiles() {
        if (shuttingDown.get()) return;
        
        try {
            List<Path> restoreFiles;
            try (Stream<Path> files = Files.list(restoreDirectory)) {
                restoreFiles = files
                        .filter(Files::isRegularFile)
                        .filter(p -> BACKUP_FILE_PATTERN.matcher(p.getFileName().toString()).matches())
                        .collect(Collectors.toList());
            }
            
            for (Path restoreFile : restoreFiles) {
                if (shuttingDown.get()) break;
                
                try {
                    restoreFromFile(restoreFile);
                } catch (Exception e) {
                    log.error("Failed to restore from file '{}': {}", 
                            restoreFile.getFileName(), e.getMessage(), e);
                    // Move failed file to backup dir with .failed suffix
                    try {
                        Path failedPath = backupDirectory.resolve(
                                restoreFile.getFileName().toString() + ".failed");
                        Files.move(restoreFile, failedPath, StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException ioe) {
                        log.warn("Could not move failed restore file: {}", ioe.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error checking for restore files: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Restore a region from a backup file.
     * 
     * @param restoreFile Path to the backup file
     * @throws IOException if restore fails
     */
    public void restoreFromFile(Path restoreFile) throws IOException {
        String fileName = restoreFile.getFileName().toString();
        Matcher matcher = BACKUP_FILE_PATTERN.matcher(fileName);
        
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid backup file name: " + fileName);
        }
        
        String region = matcher.group(1);
        boolean compressed = matcher.group(3) != null;
        
        log.info("Starting restore of region '{}' from {}", region, fileName);
        
        // Lock the region
        if (!lockRegionForRestore(region)) {
            throw new IllegalStateException("Region '" + region + "' is already being restored");
        }
        
        long startTime = System.currentTimeMillis();
        long entryCount = 0;
        long warmedCount = 0;  // Track how many entries are warmed to memory
        long lastLoggedCount = 0;  // For progress logging
        final long PROGRESS_INTERVAL = 10000;  // Log every 10,000 entries
        long byteCount = Files.size(restoreFile);
        
        try {
            KuberProperties.Backup config = properties.getBackup();
            Charset charset = Charset.forName(config.getFileEncoding());
            int batchSize = config.getBatchSize();
            
            // Ensure region exists
            cacheService.ensureRegionExistsPublic(region);
            
            // Purge existing data
            log.info("[{}] Purging existing data before restore...", region);
            persistenceStore.purgeRegion(region);
            cacheService.clearRegionCaches(region);
            
            // Get warm limit for logging
            int warmPercentage = properties.getAutoload().getWarmPercentage();
            int memoryLimit = properties.getCache().getMaxMemoryEntries();
            int warmLimit = (warmPercentage > 0) ? (int) ((memoryLimit * warmPercentage) / 100.0) : 0;
            log.info("[{}] Starting restore (warm-limit: {} entries, {}% of {})", 
                    region, warmLimit, warmPercentage, memoryLimit);
            
            // Read and restore entries
            List<CacheEntry> batch = new ArrayList<>(batchSize);
            
            try (InputStream fis = Files.newInputStream(restoreFile);
                 InputStream is = compressed ? new GZIPInputStream(fis) : fis;
                 BufferedReader reader = new BufferedReader(new InputStreamReader(is, charset))) {
                
                String line;
                while ((line = reader.readLine()) != null) {
                    if (shuttingDown.get()) {
                        throw new InterruptedException("Shutdown requested during restore");
                    }
                    
                    line = line.trim();
                    
                    // Skip header and empty lines
                    if (line.isEmpty() || line.startsWith("#")) {
                        continue;
                    }
                    
                    try {
                        CacheEntry entry = objectMapper.readValue(line, CacheEntry.class);
                        
                        // Update region to target region (in case file was renamed)
                        if (!region.equals(entry.getRegion())) {
                            entry = CacheEntry.builder()
                                    .id(entry.getId())
                                    .key(entry.getKey())
                                    .region(region)
                                    .valueType(entry.getValueType())
                                    .stringValue(entry.getStringValue())
                                    .jsonValue(entry.getJsonValue())
                                    .ttlSeconds(entry.getTtlSeconds())
                                    .createdAt(entry.getCreatedAt())
                                    .updatedAt(Instant.now())
                                    .expiresAt(entry.getExpiresAt())
                                    .build();
                        }
                        
                        batch.add(entry);
                        entryCount++;
                        
                        // Flush batch
                        if (batch.size() >= batchSize) {
                            persistenceStore.saveEntries(batch);
                            cacheService.loadEntriesIntoCache(region, batch);
                            // Count warmed entries (limited by warmLimit)
                            warmedCount = Math.min(entryCount, warmLimit);
                            batch.clear();
                            
                            // Log progress every 100,000 entries
                            if (entryCount - lastLoggedCount >= PROGRESS_INTERVAL) {
                                long elapsed = System.currentTimeMillis() - startTime;
                                log.info("[{}] PROGRESS - {} entries restored, {} values warmed ({} ms elapsed)", 
                                        region, entryCount, warmedCount, elapsed);
                                lastLoggedCount = entryCount;
                            }
                        }
                        
                    } catch (Exception e) {
                        log.debug("Skipping invalid entry: {}", e.getMessage());
                    }
                }
                
                // Flush remaining entries
                if (!batch.isEmpty()) {
                    persistenceStore.saveEntries(batch);
                    cacheService.loadEntriesIntoCache(region, batch);
                    warmedCount = Math.min(entryCount, warmLimit);
                }
            }
            
            long elapsed = System.currentTimeMillis() - startTime;
            
            totalRestores.incrementAndGet();
            totalRestoreBytes.addAndGet(byteCount);
            lastRestoreTime = Instant.now();
            
            log.info("[{}] RESTORE COMPLETED: {} entries restored, {} values warmed, {} bytes, {} ms", 
                    region, entryCount, warmedCount, byteCount, elapsed);
            
            // Move processed file to backup directory
            String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);
            Path processedPath = backupDirectory.resolve(
                    "restored_" + timestamp + "_" + fileName);
            Files.move(restoreFile, processedPath, StandardCopyOption.REPLACE_EXISTING);
            log.info("Moved processed restore file to: {}", processedPath.getFileName());
            
        } catch (InterruptedException e) {
            log.warn("Restore interrupted for region '{}'", region);
            Thread.currentThread().interrupt();
            throw new IOException("Restore interrupted", e);
        } finally {
            unlockRegionAfterRestore(region);
        }
    }
    
    // ==================== REGION LOCKING ====================
    
    /**
     * Check if a region is currently being restored.
     * Operations on this region should be blocked.
     */
    public boolean isRegionBeingRestored(String region) {
        AtomicBoolean lock = regionsBeingRestored.get(region);
        return lock != null && lock.get();
    }
    
    private boolean lockRegionForRestore(String region) {
        AtomicBoolean lock = regionsBeingRestored.computeIfAbsent(region, k -> new AtomicBoolean(false));
        return lock.compareAndSet(false, true);
    }
    
    private void unlockRegionAfterRestore(String region) {
        AtomicBoolean lock = regionsBeingRestored.get(region);
        if (lock != null) {
            lock.set(false);
        }
    }
    
    // ==================== MANUAL OPERATIONS ====================
    
    /**
     * Trigger immediate backup of all regions.
     * 
     * @return Map of region name to backup file path
     */
    public Map<String, String> backupAllRegions() {
        Map<String, String> results = new LinkedHashMap<>();
        
        for (String region : cacheService.getRegionNames()) {
            try {
                Path backupFile = backupRegion(region);
                results.put(region, backupFile.toString());
            } catch (Exception e) {
                results.put(region, "ERROR: " + e.getMessage());
            }
        }
        
        return results;
    }
    
    /**
     * Get list of available backup files.
     */
    public List<Map<String, Object>> listBackups() {
        List<Map<String, Object>> backups = new ArrayList<>();
        
        try (Stream<Path> files = Files.list(backupDirectory)) {
            files.filter(Files::isRegularFile)
                 .filter(p -> BACKUP_FILE_PATTERN.matcher(p.getFileName().toString()).matches())
                 .sorted((a, b) -> {
                     try {
                         return Files.getLastModifiedTime(b).compareTo(Files.getLastModifiedTime(a));
                     } catch (IOException e) {
                         return 0;
                     }
                 })
                 .forEach(p -> {
                     try {
                         Matcher m = BACKUP_FILE_PATTERN.matcher(p.getFileName().toString());
                         if (m.matches()) {
                             Map<String, Object> info = new LinkedHashMap<>();
                             info.put("file", p.getFileName().toString());
                             info.put("region", m.group(1));
                             info.put("timestamp", m.group(2));
                             info.put("compressed", m.group(3) != null);
                             info.put("size", Files.size(p));
                             info.put("modified", Files.getLastModifiedTime(p).toInstant().toString());
                             backups.add(info);
                         }
                     } catch (IOException e) {
                         log.debug("Error reading backup file info: {}", e.getMessage());
                     }
                 });
        } catch (IOException e) {
            log.error("Error listing backups: {}", e.getMessage());
        }
        
        return backups;
    }
    
    // ==================== STATISTICS ====================
    
    /**
     * Get backup/restore statistics.
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("enabled", properties.getBackup().isEnabled());
        stats.put("backupDirectory", backupDirectory != null ? backupDirectory.toString() : null);
        stats.put("restoreDirectory", restoreDirectory != null ? restoreDirectory.toString() : null);
        stats.put("cron", properties.getBackup().getCron());
        stats.put("maxBackupsPerRegion", properties.getBackup().getMaxBackupsPerRegion());
        stats.put("compression", properties.getBackup().isCompress());
        stats.put("batchSize", properties.getBackup().getBatchSize());
        
        Map<String, Object> totals = new LinkedHashMap<>();
        totals.put("backups", totalBackups.get());
        totals.put("restores", totalRestores.get());
        totals.put("backupBytes", totalBackupBytes.get());
        totals.put("restoreBytes", totalRestoreBytes.get());
        totals.put("lastBackupTime", lastBackupTime != null ? lastBackupTime.toString() : null);
        totals.put("lastRestoreTime", lastRestoreTime != null ? lastRestoreTime.toString() : null);
        stats.put("totals", totals);
        
        // Currently locked regions
        List<String> lockedRegions = regionsBeingRestored.entrySet().stream()
                .filter(e -> e.getValue().get())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        stats.put("regionsBeingRestored", lockedRegions);
        
        return stats;
    }
}
