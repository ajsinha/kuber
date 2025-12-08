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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Abstract base class for persistence stores with common functionality.
 * 
 * <p>Uses a dedicated executor pool for async operations instead of ForkJoinPool.commonPool
 * to ensure proper shutdown control. The pool consists of 4 single-thread executors,
 * with each region consistently mapped to one executor using hash-based partitioning.
 * This ensures writes to the same region are always sequential while allowing
 * parallel writes across different regions.
 * 
 * @version 1.5.0
 */
@Slf4j
public abstract class AbstractPersistenceStore implements PersistenceStore {
    
    protected volatile boolean available = false;
    
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
                t.setDaemon(true);
                return t;
            });
        }
        log.info("Initialized {} partitioned async save executors for region-sequential writes", EXECUTOR_COUNT);
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
        // Use region-partitioned executor to ensure sequential writes per region
        ExecutorService executor = getExecutorForRegion(entry.getRegion());
        return CompletableFuture.runAsync(() -> saveEntry(entry), executor);
    }
    
    /**
     * Shutdown all async save executors and wait for all pending saves to complete.
     * Called by subclass shutdown methods BEFORE closing database handles.
     */
    protected void shutdownAsyncExecutor() {
        log.info("Shutting down {} async save executors...", EXECUTOR_COUNT);
        
        // First, stop accepting new async saves
        asyncShuttingDown = true;
        
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
