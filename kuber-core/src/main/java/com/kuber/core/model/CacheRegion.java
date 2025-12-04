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
package com.kuber.core.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a cache region in the Kuber distributed cache.
 * Regions provide logical isolation and organization of cache entries.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CacheRegion implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    public static final String DEFAULT_REGION = "default";
    
    /**
     * Unique name of the region
     */
    private String name;
    
    /**
     * Description of this region
     */
    private String description;
    
    /**
     * Whether this is a captive/system region that cannot be deleted
     */
    @Builder.Default
    private boolean captive = false;
    
    /**
     * Maximum number of entries allowed in this region (-1 for unlimited)
     */
    @Builder.Default
    private long maxEntries = -1;
    
    /**
     * Default TTL for entries in this region (-1 for no expiration)
     */
    @Builder.Default
    private long defaultTtlSeconds = -1;
    
    /**
     * Current number of entries in this region (total)
     */
    @Builder.Default
    private long entryCount = 0;
    
    /**
     * Number of entries currently in memory cache
     */
    @Builder.Default
    private long memoryEntryCount = 0;
    
    /**
     * Number of entries in persistence store
     */
    @Builder.Default
    private long persistenceEntryCount = 0;
    
    /**
     * Total size of entries in bytes
     */
    @Builder.Default
    private long totalSizeBytes = 0;
    
    /**
     * Timestamp when this region was created
     */
    private Instant createdAt;
    
    /**
     * Timestamp when this region was last updated
     */
    private Instant updatedAt;
    
    /**
     * User who created this region
     */
    private String createdBy;
    
    /**
     * Statistics for this region
     */
    @Builder.Default
    private Map<String, Long> statistics = new HashMap<>();
    
    /**
     * Additional configuration for this region
     */
    @Builder.Default
    private Map<String, String> config = new HashMap<>();
    
    /**
     * Attribute mapping configuration for JSON data transformation.
     * Maps source attribute names to target attribute names.
     * When JSON is stored in this region, attributes are renamed according to this mapping.
     * Example: {"firstName": "first_name", "lastName": "last_name"}
     */
    @Builder.Default
    private Map<String, String> attributeMapping = new HashMap<>();
    
    /**
     * MongoDB collection name for this region
     */
    private String collectionName;
    
    /**
     * Whether this region is enabled
     */
    @Builder.Default
    private boolean enabled = true;
    
    /**
     * Check if this region is the default region
     */
    public boolean isDefault() {
        return DEFAULT_REGION.equalsIgnoreCase(name);
    }
    
    /**
     * Check if this region can be deleted
     */
    public boolean isDeletable() {
        return !captive && !isDefault();
    }
    
    /**
     * Get the MongoDB collection name for this region
     */
    public String getCollectionName() {
        if (collectionName != null && !collectionName.isEmpty()) {
            return collectionName;
        }
        return "kuber_" + name.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }
    
    /**
     * Increment entry count
     */
    public void incrementEntryCount() {
        this.entryCount++;
        this.updatedAt = Instant.now();
    }
    
    /**
     * Decrement entry count
     */
    public void decrementEntryCount() {
        if (this.entryCount > 0) {
            this.entryCount--;
        }
        this.updatedAt = Instant.now();
    }
    
    /**
     * Record a hit statistic
     */
    public void recordHit() {
        statistics.merge("hits", 1L, Long::sum);
    }
    
    /**
     * Record a miss statistic
     */
    public void recordMiss() {
        statistics.merge("misses", 1L, Long::sum);
    }
    
    /**
     * Get hit ratio
     */
    public double getHitRatio() {
        long hits = statistics.getOrDefault("hits", 0L);
        long misses = statistics.getOrDefault("misses", 0L);
        long total = hits + misses;
        return total > 0 ? (double) hits / total : 0.0;
    }
    
    /**
     * Create the default captive region
     */
    public static CacheRegion createDefault() {
        return CacheRegion.builder()
                .name(DEFAULT_REGION)
                .description("Default captive region")
                .captive(true)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .createdBy("system")
                .build();
    }
}
