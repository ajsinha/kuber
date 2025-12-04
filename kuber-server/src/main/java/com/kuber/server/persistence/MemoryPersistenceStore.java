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
        log.info("Shutting down in-memory persistence store...");
        available = false;
        regions.clear();
        entries.clear();
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
        
        return regionEntries.values().stream()
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
}
