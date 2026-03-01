/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.index;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

/**
 * Definition of a secondary index on a region.
 * 
 * @version 2.6.3
 * @since 1.9.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IndexDefinition {
    
    /**
     * Region this index belongs to.
     */
    private String region;
    
    /**
     * Field name to index. For composite indexes, fields are comma-separated.
     * Examples: "status", "city", "status,city"
     */
    private String field;
    
    /**
     * Type of index (HASH or BTREE).
     */
    private IndexType type;
    
    /**
     * Optional description of the index purpose.
     */
    private String description;
    
    /**
     * When the index was created.
     */
    private Instant createdAt;
    
    /**
     * When the index was last rebuilt.
     */
    private Instant lastRebuiltAt;
    
    /**
     * Whether this is a composite index (multiple fields).
     */
    public boolean isComposite() {
        return field != null && field.contains(",");
    }
    
    /**
     * Get list of fields for composite index.
     */
    public List<String> getFields() {
        if (field == null) {
            return List.of();
        }
        return Arrays.asList(field.split(","));
    }
    
    /**
     * Get a unique name for this index.
     */
    public String getIndexName() {
        if (field == null) {
            return "unknown";
        }
        return "idx_" + field.replace(",", "_");
    }
    
    /**
     * Get fully qualified index name including region.
     */
    public String getFullName() {
        return region + ":" + getIndexName();
    }
    
    /**
     * Create from YAML configuration.
     */
    public static IndexDefinition fromConfig(String region, String field, String type, String description) {
        return IndexDefinition.builder()
                .region(region)
                .field(field != null ? field.trim() : null)
                .type(IndexType.fromString(type))
                .description(description)
                .createdAt(Instant.now())
                .build();
    }
}
