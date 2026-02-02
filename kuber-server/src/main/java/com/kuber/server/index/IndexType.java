/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.index;

/**
 * Types of secondary indexes supported by Kuber.
 * 
 * @version 1.8.2
 * @since 1.8.2
 */
public enum IndexType {
    
    /**
     * Hash index for equality queries.
     * Best for: status = "active", city = "NYC"
     * Lookup: O(1)
     */
    HASH,
    
    /**
     * B-Tree index for range queries.
     * Best for: age > 30, price BETWEEN 100 AND 500
     * Lookup: O(log n)
     */
    BTREE,
    
    /**
     * Trigram index for regex/substring searches.
     * Best for: email MATCHES ".*@gmail\.com$", name CONTAINS "smith"
     * Lookup: O(trigram matches) - typically very fast for selective patterns
     * 
     * <p>Trigram indexes break each value into 3-character sequences and build
     * an inverted index. For regex queries, trigrams are extracted from the
     * pattern's literals to quickly narrow down candidate documents.
     */
    TRIGRAM,
    
    /**
     * Prefix index (Trie) for prefix/starts-with queries.
     * Best for: key STARTS WITH "NYC-", zipcode STARTS WITH "100"
     * Lookup: O(prefix length)
     */
    PREFIX;
    
    public static IndexType fromString(String type) {
        if (type == null) {
            return HASH;
        }
        return switch (type.toLowerCase()) {
            case "hash" -> HASH;
            case "btree", "b-tree", "tree" -> BTREE;
            case "trigram", "tri", "ngram" -> TRIGRAM;
            case "prefix", "trie" -> PREFIX;
            default -> HASH;
        };
    }
}
