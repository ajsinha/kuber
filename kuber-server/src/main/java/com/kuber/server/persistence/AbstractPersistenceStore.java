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
import java.util.regex.Pattern;

/**
 * Abstract base class for persistence stores with common functionality.
 */
@Slf4j
public abstract class AbstractPersistenceStore implements PersistenceStore {
    
    protected volatile boolean available = false;
    
    @Override
    public boolean isAvailable() {
        return available;
    }
    
    @Override
    public CompletableFuture<Void> saveEntryAsync(CacheEntry entry) {
        return CompletableFuture.runAsync(() -> saveEntry(entry));
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
