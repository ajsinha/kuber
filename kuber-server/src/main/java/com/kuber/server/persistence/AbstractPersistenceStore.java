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
}
