/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.index;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Interface for secondary index implementations.
 * 
 * @version 2.6.3
 * @since 1.9.0
 */
public interface SecondaryIndex {
    
    /**
     * Get the index definition.
     */
    IndexDefinition getDefinition();
    
    /**
     * Add a document key to the index.
     * 
     * @param fieldValue The value of the indexed field
     * @param documentKey The document key to add
     */
    void add(Object fieldValue, String documentKey);
    
    /**
     * Remove a document key from the index.
     * 
     * @param fieldValue The value of the indexed field
     * @param documentKey The document key to remove
     */
    void remove(Object fieldValue, String documentKey);
    
    /**
     * Update a document's index entry (remove old, add new).
     * 
     * @param oldValue The old field value
     * @param newValue The new field value
     * @param documentKey The document key
     */
    void update(Object oldValue, Object newValue, String documentKey);
    
    /**
     * Find all document keys with the given field value (equality).
     * 
     * @param fieldValue The value to search for
     * @return Set of matching document keys
     */
    Set<String> findEquals(Object fieldValue);
    
    /**
     * Find all document keys where field value is in the given set (IN clause).
     * 
     * @param values The values to search for
     * @return Set of matching document keys
     */
    Set<String> findIn(Set<Object> values);
    
    /**
     * Find all document keys where field value matches the regex pattern.
     * Most efficient with TRIGRAM indexes; falls back to full scan with others.
     * 
     * @param pattern Compiled regex pattern
     * @return Set of candidate document keys (may need verification for non-TRIGRAM)
     */
    Set<String> findRegex(Pattern pattern);
    
    /**
     * Find all document keys where field value starts with the given prefix.
     * Most efficient with PREFIX indexes; supported by TRIGRAM indexes.
     * 
     * @param prefix The prefix to search for
     * @return Set of matching document keys
     */
    Set<String> findPrefix(String prefix);
    
    /**
     * Find all document keys where field value contains the given substring.
     * Most efficient with TRIGRAM indexes.
     * 
     * @param substring The substring to search for
     * @return Set of candidate document keys
     */
    Set<String> findContains(String substring);
    
    /**
     * Find all document keys where field value is greater than given value.
     * Only supported by BTREE indexes.
     * 
     * @param value The threshold value
     * @param inclusive Whether to include equal values
     * @return Set of matching document keys
     */
    Set<String> findGreaterThan(Object value, boolean inclusive);
    
    /**
     * Find all document keys where field value is less than given value.
     * Only supported by BTREE indexes.
     * 
     * @param value The threshold value
     * @param inclusive Whether to include equal values
     * @return Set of matching document keys
     */
    Set<String> findLessThan(Object value, boolean inclusive);
    
    /**
     * Find all document keys where field value is within range.
     * Only supported by BTREE indexes.
     * 
     * @param from Lower bound
     * @param to Upper bound
     * @param fromInclusive Include lower bound
     * @param toInclusive Include upper bound
     * @return Set of matching document keys
     */
    Set<String> findRange(Object from, Object to, boolean fromInclusive, boolean toInclusive);
    
    /**
     * Clear all entries from the index.
     */
    void clear();
    
    /**
     * Get the number of entries in the index.
     */
    long size();
    
    /**
     * Get the number of unique values in the index.
     */
    int uniqueValues();
    
    /**
     * Get approximate memory usage in bytes.
     */
    long memoryUsageBytes();
    
    /**
     * Get statistics about the index.
     */
    Map<String, Object> getStatistics();
    
    /**
     * Check if index supports range queries.
     */
    boolean supportsRangeQueries();
    
    /**
     * Check if index supports regex/pattern queries efficiently.
     */
    default boolean supportsRegexQueries() {
        return false;
    }
    
    /**
     * Check if index supports prefix queries efficiently.
     */
    default boolean supportsPrefixQueries() {
        return false;
    }
    
    /**
     * Check if this index uses off-heap storage.
     */
    default boolean isOffHeap() {
        return false;
    }
    
    /**
     * Get the storage type for this index.
     */
    default IndexStorageType getStorageType() {
        return isOffHeap() ? IndexStorageType.OFFHEAP : IndexStorageType.HEAP;
    }
    
    /**
     * Get off-heap bytes used (0 if on-heap).
     */
    default long getOffHeapBytesUsed() {
        return 0;
    }
    
    /**
     * Shutdown and release resources (especially off-heap memory).
     */
    default void shutdown() {
        clear();
    }
}
