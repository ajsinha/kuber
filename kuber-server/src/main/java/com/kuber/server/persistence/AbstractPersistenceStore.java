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

import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheRegion;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Abstract base class for persistence stores with common functionality.
 * 
 * <p><b>BATCHED ASYNC PERSISTENCE (v1.6.2)</b>: Instead of saving each entry individually,
 * entries are buffered and saved in batches for significantly improved throughput.
 * 
 * <p>Batching behavior:
 * <ul>
 *   <li>Entries are buffered per-region in concurrent queues</li>
 *   <li>Flush occurs when buffer reaches {@code persistence-batch-size} (default: 100)</li>
 *   <li>Flush also occurs every {@code persistence-interval-ms} (default: 1000ms)</li>
 *   <li>Whichever condition is met first triggers the flush</li>
 *   <li>All pending batches are flushed during shutdown</li>
 * </ul>
 * 
 * <p>Performance benefits:
 * <ul>
 *   <li>5-20x throughput improvement for high-volume writes</li>
 *   <li>Reduced WAL writes (one per batch instead of one per entry)</li>
 *   <li>Better disk I/O patterns (sequential instead of random)</li>
 *   <li>Lower thread pool overhead</li>
 * </ul>
 * 
 * <p>Uses a dedicated executor pool for async operations instead of ForkJoinPool.commonPool
 * to ensure proper shutdown control. The pool consists of 4 single-thread executors,
 * with each region consistently mapped to one executor using hash-based partitioning.
 * This ensures writes to the same region are always sequential while allowing
 * parallel writes across different regions.
 * 
 * @version 2.5.0
 */
@Slf4j
public abstract class AbstractPersistenceStore implements PersistenceStore {
    
    protected volatile boolean available = false;
    
    // ==================== BATCHED ASYNC PERSISTENCE (v1.6.2) ====================
    
    // Default batch configuration (can be overridden via configureBatching)
    private int batchSize = 100;
    private int flushIntervalMs = 1000;
    
    // Per-region buffers for pending entries
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<CacheEntry>> regionBuffers = new ConcurrentHashMap<>();
    
    // Per-region buffer sizes (for fast size checking without queue iteration)
    private final ConcurrentHashMap<String, AtomicLong> regionBufferSizes = new ConcurrentHashMap<>();
    
    // Scheduled executor for interval-based flushing
    private ScheduledExecutorService flushScheduler;
    
    // Flag to track if batching is enabled
    private volatile boolean batchingEnabled = false;
    
    // Statistics
    private final AtomicLong totalBatchesFlushed = new AtomicLong(0);
    private final AtomicLong totalEntriesBatched = new AtomicLong(0);
    
    // ==================== ASYNC EXECUTORS ====================
    
    // Number of partitioned executors for async saves
    private static final int EXECUTOR_COUNT = 4;
    
    // Array of single-thread executors for async saves
    // Each region is consistently mapped to one executor using hash partitioning
    // This ensures all writes for a region are sequential while allowing parallelism across regions
    private final ExecutorService[] asyncSaveExecutors = new ExecutorService[EXECUTOR_COUNT];
    
    // Shutdown flag to reject new async saves
    protected volatile boolean asyncShuttingDown = false;
    
    // Initialize executors in constructor
    {
        for (int i = 0; i < EXECUTOR_COUNT; i++) {
            final int executorId = i;
            asyncSaveExecutors[i] = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "persistence-async-save-" + executorId);
                // CRITICAL FIX (v1.6.1): Use USER threads, NOT daemon threads
                // Daemon threads are immediately killed by JVM on exit, even mid-write
                // This was causing RocksDB corruption when writes were interrupted
                // User threads allow JVM to wait for pending writes to complete
                t.setDaemon(false);
                return t;
            });
        }
        log.info("Initialized {} partitioned async save executors (user threads) for region-sequential writes", EXECUTOR_COUNT);
    }
    
    /**
     * Configure batched async persistence parameters.
     * Call this method during initialization to enable batching.
     * 
     * @param batchSize Number of entries to buffer before flushing (default: 100)
     * @param flushIntervalMs Maximum time between flushes in milliseconds (default: 1000)
     * @since 1.6.2
     */
    public void configureBatching(int batchSize, int flushIntervalMs) {
        if (batchingEnabled) {
            log.warn("Batching already configured, ignoring reconfiguration");
            return;
        }
        
        this.batchSize = Math.max(1, batchSize);
        this.flushIntervalMs = Math.max(100, flushIntervalMs);
        
        // Start the flush scheduler
        flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "persistence-batch-flush-scheduler");
            t.setDaemon(false); // User thread for clean shutdown
            return t;
        });
        
        // Schedule periodic flush
        flushScheduler.scheduleAtFixedRate(
            this::flushAllRegionBuffers,
            this.flushIntervalMs,
            this.flushIntervalMs,
            TimeUnit.MILLISECONDS
        );
        
        batchingEnabled = true;
        log.info("Batched async persistence ENABLED: batch-size={}, flush-interval={}ms", 
                this.batchSize, this.flushIntervalMs);
    }
    
    /**
     * Get the executor for a given region based on hash partitioning.
     * All operations for the same region will use the same executor,
     * ensuring sequential execution within a region.
     * 
     * @param region the region name
     * @return the executor assigned to this region
     */
    private ExecutorService getExecutorForRegion(String region) {
        // Use Math.abs to handle negative hash codes, then mod by executor count
        int hash = region.hashCode();
        int index = Math.abs(hash % EXECUTOR_COUNT);
        // Handle edge case where Math.abs(Integer.MIN_VALUE) returns negative
        if (index < 0) {
            index = 0;
        }
        return asyncSaveExecutors[index];
    }
    
    @Override
    public boolean isAvailable() {
        return available;
    }
    
    @Override
    public CompletableFuture<Void> saveEntryAsync(CacheEntry entry) {
        // Reject new async saves during shutdown
        if (asyncShuttingDown) {
            log.debug("Rejecting async save during shutdown for key: {}", entry.getKey());
            return CompletableFuture.completedFuture(null);
        }
        
        // If batching is enabled, add to buffer instead of immediate save
        if (batchingEnabled) {
            return addToBuffer(entry);
        }
        
        // Fallback: Use region-partitioned executor to ensure sequential writes per region
        ExecutorService executor = getExecutorForRegion(entry.getRegion());
        return CompletableFuture.runAsync(() -> saveEntry(entry), executor);
    }
    
    /**
     * Add entry to the region buffer for batched persistence.
     * Triggers immediate flush if buffer size reaches threshold.
     * 
     * @param entry the entry to buffer
     * @return completed future (batching is fire-and-forget)
     * @since 1.6.2
     */
    private CompletableFuture<Void> addToBuffer(CacheEntry entry) {
        String region = entry.getRegion();
        
        // Get or create buffer for this region
        ConcurrentLinkedQueue<CacheEntry> buffer = regionBuffers.computeIfAbsent(
            region, k -> new ConcurrentLinkedQueue<>()
        );
        AtomicLong bufferSize = regionBufferSizes.computeIfAbsent(
            region, k -> new AtomicLong(0)
        );
        
        // Add entry to buffer
        buffer.add(entry);
        long currentSize = bufferSize.incrementAndGet();
        totalEntriesBatched.incrementAndGet();
        
        // Check if we need to flush (size threshold reached)
        if (currentSize >= batchSize) {
            // Trigger flush asynchronously on the region's executor
            ExecutorService executor = getExecutorForRegion(region);
            executor.submit(() -> flushRegionBuffer(region));
        }
        
        return CompletableFuture.completedFuture(null);
    }
    
    /**
     * Flush the buffer for a specific region.
     * Called when batch size threshold is reached or by scheduled interval.
     * 
     * @param region the region to flush
     * @since 1.6.2
     */
    private void flushRegionBuffer(String region) {
        ConcurrentLinkedQueue<CacheEntry> buffer = regionBuffers.get(region);
        AtomicLong bufferSize = regionBufferSizes.get(region);
        
        if (buffer == null || bufferSize == null) {
            return;
        }
        
        // Drain buffer into a list
        List<CacheEntry> entriesToSave = new ArrayList<>();
        CacheEntry entry;
        while ((entry = buffer.poll()) != null) {
            entriesToSave.add(entry);
            bufferSize.decrementAndGet();
        }
        
        if (entriesToSave.isEmpty()) {
            return;
        }
        
        // Save batch
        try {
            saveEntries(entriesToSave);
            totalBatchesFlushed.incrementAndGet();
            
            if (log.isDebugEnabled()) {
                log.debug("Flushed batch of {} entries for region '{}'", entriesToSave.size(), region);
            }
        } catch (Exception e) {
            log.error("Failed to flush batch of {} entries for region '{}': {}", 
                    entriesToSave.size(), region, e.getMessage(), e);
            // Re-add entries to buffer for retry (at front)
            // Note: This may cause ordering issues but prevents data loss
            for (int i = entriesToSave.size() - 1; i >= 0; i--) {
                buffer.add(entriesToSave.get(i)); // Add to end since we can't add to front
                bufferSize.incrementAndGet();
            }
        }
    }
    
    /**
     * Flush all region buffers.
     * Called by the scheduled flush task and during shutdown.
     * 
     * @since 1.6.2
     */
    private void flushAllRegionBuffers() {
        if (!batchingEnabled) {
            return;
        }
        
        for (String region : regionBuffers.keySet()) {
            try {
                flushRegionBuffer(region);
            } catch (Exception e) {
                log.error("Error flushing buffer for region '{}': {}", region, e.getMessage());
            }
        }
    }
    
    /**
     * Get batching statistics.
     * 
     * @return map with batching stats
     * @since 1.6.2
     */
    public Map<String, Object> getBatchingStats() {
        Map<String, Object> stats = new ConcurrentHashMap<>();
        stats.put("batchingEnabled", batchingEnabled);
        stats.put("batchSize", batchSize);
        stats.put("flushIntervalMs", flushIntervalMs);
        stats.put("totalBatchesFlushed", totalBatchesFlushed.get());
        stats.put("totalEntriesBatched", totalEntriesBatched.get());
        
        // Current buffer sizes
        long totalBuffered = 0;
        for (AtomicLong size : regionBufferSizes.values()) {
            totalBuffered += size.get();
        }
        stats.put("currentlyBuffered", totalBuffered);
        stats.put("bufferedRegions", regionBuffers.size());
        
        return stats;
    }
    
    /**
     * Shutdown all async save executors and wait for all pending saves to complete.
     * Called by subclass shutdown methods BEFORE closing database handles.
     */
    protected void shutdownAsyncExecutor() {
        log.info("Shutting down async persistence...");
        
        // First, stop accepting new async saves
        asyncShuttingDown = true;
        
        // If batching is enabled, flush all pending batches first
        if (batchingEnabled) {
            log.info("Flushing all pending batches before shutdown...");
            
            // Stop the scheduled flusher
            if (flushScheduler != null) {
                flushScheduler.shutdown();
                try {
                    flushScheduler.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            
            // Flush all remaining entries
            flushAllRegionBuffers();
            
            // Log final stats
            log.info("Batching stats at shutdown: {} batches flushed, {} total entries batched",
                    totalBatchesFlushed.get(), totalEntriesBatched.get());
        }
        
        log.info("Shutting down {} async save executors...", EXECUTOR_COUNT);
        
        // Shutdown all executors
        for (int i = 0; i < EXECUTOR_COUNT; i++) {
            asyncSaveExecutors[i].shutdown();
        }
        
        try {
            // Wait up to 30 seconds for all pending saves to complete
            boolean allTerminated = true;
            for (int i = 0; i < EXECUTOR_COUNT; i++) {
                if (!asyncSaveExecutors[i].awaitTermination(30, TimeUnit.SECONDS)) {
                    log.warn("Async save executor {} did not terminate in 30s, forcing shutdown...", i);
                    List<Runnable> pending = asyncSaveExecutors[i].shutdownNow();
                    log.warn("Dropped {} pending async save tasks from executor {}", pending.size(), i);
                    allTerminated = false;
                }
            }
            
            if (allTerminated) {
                log.info("All async save executors shutdown complete - all pending saves finished");
            } else {
                // Wait a bit more for tasks to respond to interruption
                for (int i = 0; i < EXECUTOR_COUNT; i++) {
                    if (!asyncSaveExecutors[i].awaitTermination(5, TimeUnit.SECONDS)) {
                        log.error("Async save executor {} did not terminate after force shutdown", i);
                    }
                }
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for async executors, forcing shutdown");
            for (int i = 0; i < EXECUTOR_COUNT; i++) {
                asyncSaveExecutors[i].shutdownNow();
            }
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Convert glob pattern to regex pattern.
     */
    protected Pattern globToRegex(String glob) {
        if (glob == null || glob.isEmpty() || glob.equals("*")) {
            return Pattern.compile(".*");
        }
        
        StringBuilder regex = new StringBuilder("^");
        for (char c : glob.toCharArray()) {
            switch (c) {
                case '*':
                    regex.append(".*");
                    break;
                case '?':
                    regex.append(".");
                    break;
                case '.':
                case '(':
                case ')':
                case '+':
                case '|':
                case '^':
                case '$':
                case '@':
                case '%':
                case '\\':
                    regex.append("\\").append(c);
                    break;
                default:
                    regex.append(c);
            }
        }
        regex.append("$");
        return Pattern.compile(regex.toString());
    }
    
    /**
     * Generate a collection/table name for a region.
     */
    protected String getCollectionName(String region) {
        return "kuber_" + region.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }
    
    /**
     * Convert object to Instant (for date handling).
     */
    protected Instant toInstant(Object dateObj) {
        if (dateObj == null) {
            return null;
        }
        if (dateObj instanceof Instant) {
            return (Instant) dateObj;
        }
        if (dateObj instanceof java.util.Date) {
            return ((java.util.Date) dateObj).toInstant();
        }
        if (dateObj instanceof Long) {
            return Instant.ofEpochMilli((Long) dateObj);
        }
        return null;
    }
    
    /**
     * Check if an entry is expired.
     */
    protected boolean isExpired(CacheEntry entry) {
        if (entry == null) {
            return true;
        }
        Instant expiresAt = entry.getExpiresAt();
        return expiresAt != null && Instant.now().isAfter(expiresAt);
    }
    
    /**
     * Filter keys by pattern.
     */
    protected List<String> filterKeys(List<String> keys, String pattern, int limit) {
        if (pattern == null || pattern.isEmpty() || pattern.equals("*")) {
            return limit > 0 ? keys.subList(0, Math.min(keys.size(), limit)) : keys;
        }
        
        Pattern regex = globToRegex(pattern);
        return keys.stream()
                .filter(key -> regex.matcher(key).matches())
                .limit(limit > 0 ? limit : Long.MAX_VALUE)
                .toList();
    }
    
    /**
     * Default implementation of get - delegates to loadEntry.
     */
    @Override
    public CacheEntry get(String region, String key) {
        return loadEntry(region, key);
    }
    
    /**
     * Default implementation - subclasses should override for efficiency.
     */
    @Override
    public long deleteExpiredEntries(String region) {
        List<String> keys = getKeys(region, "*", Integer.MAX_VALUE);
        long deleted = 0;
        
        for (String key : keys) {
            CacheEntry entry = loadEntry(region, key);
            if (entry != null && isExpired(entry)) {
                deleteEntry(region, key);
                deleted++;
            }
        }
        
        if (deleted > 0) {
            log.info("Deleted {} expired entries from region '{}'", deleted, region);
        }
        
        return deleted;
    }
    
    /**
     * Delete all expired entries from all regions.
     * Subclasses should implement loadAllRegions to make this work.
     */
    @Override
    public long deleteAllExpiredEntries() {
        long total = 0;
        for (CacheRegion region : loadAllRegions()) {
            total += deleteExpiredEntries(region.getName());
        }
        return total;
    }
    
    /**
     * Count non-expired entries - default implementation.
     */
    @Override
    public long countNonExpiredEntries(String region) {
        List<String> keys = getKeys(region, "*", Integer.MAX_VALUE);
        long count = 0;
        
        for (String key : keys) {
            CacheEntry entry = loadEntry(region, key);
            if (entry != null && !isExpired(entry)) {
                count++;
            }
        }
        
        return count;
    }
    
    /**
     * Get non-expired keys - default implementation.
     */
    @Override
    public List<String> getNonExpiredKeys(String region, String pattern, int limit) {
        List<String> allKeys = getKeys(region, pattern, Integer.MAX_VALUE);
        List<String> nonExpired = new java.util.ArrayList<>();
        
        for (String key : allKeys) {
            if (nonExpired.size() >= limit && limit > 0) {
                break;
            }
            CacheEntry entry = loadEntry(region, key);
            if (entry != null && !isExpired(entry)) {
                nonExpired.add(key);
            }
        }
        
        return nonExpired;
    }
}
