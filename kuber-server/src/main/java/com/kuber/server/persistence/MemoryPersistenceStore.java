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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In-memory implementation of PersistenceStore.
 * Useful for testing and development. Data is not persisted across restarts.
 */
@Slf4j
public class MemoryPersistenceStore extends AbstractPersistenceStore {
    
    private final Map<String, CacheRegion> regions = new ConcurrentHashMap<>();
    private final Map<String, Map<String, CacheEntry>> entries = new ConcurrentHashMap<>();
    
    // Guard against double shutdown
    private volatile boolean alreadyShutdown = false;
    
    @Override
    public PersistenceType getType() {
        return PersistenceType.MEMORY;
    }
    
    @Override
    public void initialize() {
        log.info("Initializing in-memory persistence store");
        available = true;
        log.info("In-memory persistence store initialized successfully");
    }
    
    @Override
    public void shutdown() {
        // Guard against double shutdown
        if (alreadyShutdown) {
            log.debug("Memory shutdown already completed - skipping duplicate shutdown call");
            return;
        }
        alreadyShutdown = true;
        
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  MEMORY PERSISTENCE SHUTDOWN                                        ║");
        log.info("╚════════════════════════════════════════════════════════════════════╝");
        
        available = false;
        
        // Step 1: Shutdown async save executor FIRST
        log.info("Step 1: Shutting down async save executor...");
        shutdownAsyncExecutor();
        
        int regionCount = regions.size();
        int entryCount = entries.values().stream().mapToInt(Map::size).sum();
        
        log.info("Step 2: Clearing {} regions with {} entries from memory...", regionCount, entryCount);
        regions.clear();
        entries.clear();
        
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  MEMORY PERSISTENCE SHUTDOWN COMPLETE                               ║");
        log.info("║  Note: All data has been lost (in-memory only)                      ║");
        log.info("╚════════════════════════════════════════════════════════════════════╝");
    }
    
    /**
     * Sync is a no-op for Memory persistence store.
     * Data is volatile and not persisted to disk.
     */
    @Override
    public void sync() {
        log.debug("Memory sync called - no action needed (data is volatile)");
        // No persistence, nothing to sync
    }
    
    // ==================== Region Operations ====================
    
    @Override
    public void saveRegion(CacheRegion region) {
        regions.put(region.getName(), region);
        entries.computeIfAbsent(region.getName(), k -> new ConcurrentHashMap<>());
        log.info("Saved region '{}' to memory", region.getName());
    }
    
    @Override
    public List<CacheRegion> loadAllRegions() {
        log.info("Loaded {} regions from memory", regions.size());
        return new ArrayList<>(regions.values());
    }
    
    @Override
    public CacheRegion loadRegion(String name) {
        return regions.get(name);
    }
    
    @Override
    public void deleteRegion(String name) {
        regions.remove(name);
        entries.remove(name);
        log.info("Deleted region '{}' from memory", name);
    }
    
    @Override
    public void purgeRegion(String name) {
        Map<String, CacheEntry> regionEntries = entries.get(name);
        if (regionEntries != null) {
            int count = regionEntries.size();
            regionEntries.clear();
            log.info("Purged {} entries from region '{}' in memory", count, name);
        }
    }
    
    // ==================== Entry Operations ====================
    
    @Override
    public void saveEntry(CacheEntry entry) {
        entries.computeIfAbsent(entry.getRegion(), k -> new ConcurrentHashMap<>())
               .put(entry.getKey(), entry);
    }
    
    @Override
    public void saveEntries(List<CacheEntry> entryList) {
        for (CacheEntry entry : entryList) {
            saveEntry(entry);
        }
        log.debug("Saved {} entries to memory", entryList.size());
    }
    
    @Override
    public CacheEntry loadEntry(String region, String key) {
        Map<String, CacheEntry> regionEntries = entries.get(region);
        return regionEntries != null ? regionEntries.get(key) : null;
    }
    
    @Override
    public List<CacheEntry> loadEntries(String region, int limit) {
        Map<String, CacheEntry> regionEntries = entries.get(region);
        if (regionEntries == null) {
            return new ArrayList<>();
        }
        
        // Sort by last accessed time (most recent first), then by updated time
        return regionEntries.values().stream()
                .filter(entry -> !entry.isExpired())
                .sorted((a, b) -> {
                    // Prefer lastAccessedAt if available, otherwise use updatedAt
                    Instant aTime = a.getLastAccessedAt() != null ? a.getLastAccessedAt() : a.getUpdatedAt();
                    Instant bTime = b.getLastAccessedAt() != null ? b.getLastAccessedAt() : b.getUpdatedAt();
                    if (aTime == null && bTime == null) return 0;
                    if (aTime == null) return 1;
                    if (bTime == null) return -1;
                    return bTime.compareTo(aTime); // Descending order (most recent first)
                })
                .limit(limit)
                .collect(Collectors.toList());
    }
    
    @Override
    public void deleteEntry(String region, String key) {
        Map<String, CacheEntry> regionEntries = entries.get(region);
        if (regionEntries != null) {
            regionEntries.remove(key);
        }
    }
    
    @Override
    public void deleteEntries(String region, List<String> keys) {
        Map<String, CacheEntry> regionEntries = entries.get(region);
        if (regionEntries != null) {
            for (String key : keys) {
                regionEntries.remove(key);
            }
        }
    }
    
    @Override
    public long countEntries(String region) {
        Map<String, CacheEntry> regionEntries = entries.get(region);
        return regionEntries != null ? regionEntries.size() : 0;
    }
    
    @Override
    public List<String> getKeys(String region, String pattern, int limit) {
        Map<String, CacheEntry> regionEntries = entries.get(region);
        if (regionEntries == null) {
            return new ArrayList<>();
        }
        
        return filterKeys(new ArrayList<>(regionEntries.keySet()), pattern, limit);
    }
    
    @Override
    public long deleteExpiredEntries(String region) {
        Map<String, CacheEntry> regionEntries = entries.get(region);
        if (regionEntries == null) {
            return 0;
        }
        
        List<String> expiredKeys = new ArrayList<>();
        Instant now = Instant.now();
        
        for (Map.Entry<String, CacheEntry> entry : regionEntries.entrySet()) {
            CacheEntry cacheEntry = entry.getValue();
            if (cacheEntry.getExpiresAt() != null && now.isAfter(cacheEntry.getExpiresAt())) {
                expiredKeys.add(entry.getKey());
            }
        }
        
        for (String key : expiredKeys) {
            regionEntries.remove(key);
        }
        
        if (!expiredKeys.isEmpty()) {
            log.info("Deleted {} expired entries from region '{}' in memory", expiredKeys.size(), region);
        }
        
        return expiredKeys.size();
    }
    
    @Override
    public long deleteAllExpiredEntries() {
        long total = 0;
        for (String region : entries.keySet()) {
            total += deleteExpiredEntries(region);
        }
        return total;
    }
    
    @Override
    public long countNonExpiredEntries(String region) {
        Map<String, CacheEntry> regionEntries = entries.get(region);
        if (regionEntries == null) {
            return 0;
        }
        
        Instant now = Instant.now();
        return regionEntries.values().stream()
                .filter(entry -> entry.getExpiresAt() == null || !now.isAfter(entry.getExpiresAt()))
                .count();
    }
    
    @Override
    public List<String> getNonExpiredKeys(String region, String pattern, int limit) {
        Map<String, CacheEntry> regionEntries = entries.get(region);
        if (regionEntries == null) {
            return new ArrayList<>();
        }
        
        Instant now = Instant.now();
        List<String> keys = regionEntries.entrySet().stream()
                .filter(entry -> entry.getValue().getExpiresAt() == null || 
                        !now.isAfter(entry.getValue().getExpiresAt()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        
        return filterKeys(keys, pattern, limit);
    }
}
