/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 */
package com.kuber.server.index;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Trie-based prefix index for efficient starts-with queries.
 * 
 * <p>A Prefix/Trie index organizes values in a tree structure where each node
 * represents a character. This allows O(prefix-length) lookup for prefix queries.
 * 
 * <h3>Query Types Supported:</h3>
 * <ul>
 *   <li><strong>Prefix:</strong> zipcode STARTS WITH "100" - O(prefix length)</li>
 *   <li><strong>Equality:</strong> status = "active" - O(value length)</li>
 *   <li><strong>Regex (^prefix.*):</strong> Anchored prefix patterns - O(prefix length)</li>
 * </ul>
 * 
 * <h3>Performance:</h3>
 * <ul>
 *   <li>Prefix lookup: O(prefix length)</li>
 *   <li>Equality: O(value length)</li>
 *   <li>Memory: Efficient for values with common prefixes</li>
 * </ul>
 * 
 * @version 2.6.4
 * @since 1.9.0
 */
@Slf4j
public class PrefixIndex implements SecondaryIndex {
    
    private final IndexDefinition definition;
    
    // Trie root node
    private final TrieNode root;
    
    // Forward index: documentKey -> original value
    private final ConcurrentHashMap<String, String> valueIndex;
    
    // Statistics
    private final AtomicLong totalEntries = new AtomicLong(0);
    
    public PrefixIndex(IndexDefinition definition) {
        this.definition = definition;
        this.root = new TrieNode();
        this.valueIndex = new ConcurrentHashMap<>();
        log.debug("Created PrefixIndex for field: {}", definition.getField());
    }
    
    /**
     * Trie node structure.
     */
    private static class TrieNode {
        final ConcurrentHashMap<Character, TrieNode> children = new ConcurrentHashMap<>();
        final Set<String> documentKeys = ConcurrentHashMap.newKeySet();
        volatile boolean isEndOfWord = false;
    }
    
    @Override
    public IndexDefinition getDefinition() {
        return definition;
    }
    
    @Override
    public void add(Object fieldValue, String documentKey) {
        if (fieldValue == null || documentKey == null) {
            return;
        }
        
        String value = fieldValue.toString().toLowerCase();
        
        // Store original value
        valueIndex.put(documentKey, value);
        
        // Insert into trie
        TrieNode current = root;
        for (char c : value.toCharArray()) {
            current = current.children.computeIfAbsent(c, k -> new TrieNode());
        }
        current.isEndOfWord = true;
        current.documentKeys.add(documentKey);
        
        totalEntries.incrementAndGet();
    }
    
    @Override
    public void remove(Object fieldValue, String documentKey) {
        if (documentKey == null) {
            return;
        }
        
        String value = valueIndex.remove(documentKey);
        if (value == null) {
            return;
        }
        
        // Remove from trie
        removeFromTrie(root, value, 0, documentKey);
        totalEntries.decrementAndGet();
    }
    
    private boolean removeFromTrie(TrieNode node, String value, int index, String documentKey) {
        if (index == value.length()) {
            node.documentKeys.remove(documentKey);
            if (node.documentKeys.isEmpty()) {
                node.isEndOfWord = false;
            }
            return node.children.isEmpty() && !node.isEndOfWord;
        }
        
        char c = value.charAt(index);
        TrieNode child = node.children.get(c);
        if (child == null) {
            return false;
        }
        
        boolean shouldRemoveChild = removeFromTrie(child, value, index + 1, documentKey);
        if (shouldRemoveChild) {
            node.children.remove(c);
            return node.children.isEmpty() && !node.isEndOfWord && node.documentKeys.isEmpty();
        }
        
        return false;
    }
    
    @Override
    public void update(Object oldValue, Object newValue, String documentKey) {
        if (documentKey == null) {
            return;
        }
        
        String oldStr = oldValue != null ? oldValue.toString().toLowerCase() : null;
        String newStr = newValue != null ? newValue.toString().toLowerCase() : null;
        
        if (Objects.equals(oldStr, newStr)) {
            return;
        }
        
        remove(oldValue, documentKey);
        add(newValue, documentKey);
    }
    
    @Override
    public Set<String> findEquals(Object fieldValue) {
        if (fieldValue == null) {
            return Collections.emptySet();
        }
        
        String value = fieldValue.toString().toLowerCase();
        TrieNode node = findNode(value);
        
        if (node != null && node.isEndOfWord) {
            return new HashSet<>(node.documentKeys);
        }
        
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> findIn(Set<Object> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }
        
        Set<String> result = new HashSet<>();
        for (Object value : values) {
            result.addAll(findEquals(value));
        }
        return result;
    }
    
    @Override
    public Set<String> findPrefix(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            // Return all
            return new HashSet<>(valueIndex.keySet());
        }
        
        String lowerPrefix = prefix.toLowerCase();
        TrieNode node = findNode(lowerPrefix);
        
        if (node == null) {
            return Collections.emptySet();
        }
        
        // Collect all document keys under this prefix
        Set<String> result = new HashSet<>();
        collectAllKeys(node, result);
        return result;
    }
    
    private void collectAllKeys(TrieNode node, Set<String> result) {
        result.addAll(node.documentKeys);
        for (TrieNode child : node.children.values()) {
            collectAllKeys(child, result);
        }
    }
    
    @Override
    public Set<String> findRegex(Pattern pattern) {
        if (pattern == null) {
            return Collections.emptySet();
        }
        
        String patternStr = pattern.pattern();
        
        // Check if this is an anchored prefix pattern (^prefix.*)
        if (patternStr.startsWith("^")) {
            String afterAnchor = patternStr.substring(1);
            
            // Find the literal prefix before any regex metacharacters
            StringBuilder prefixBuilder = new StringBuilder();
            int i = 0;
            while (i < afterAnchor.length()) {
                char c = afterAnchor.charAt(i);
                
                // Handle escape sequences
                if (c == '\\' && i + 1 < afterAnchor.length()) {
                    char next = afterAnchor.charAt(i + 1);
                    if (next == '.' || next == '*' || next == '+' || next == '?') {
                        prefixBuilder.append(next);
                        i += 2;
                        continue;
                    }
                }
                
                // Stop at regex metacharacters
                if (c == '.' || c == '*' || c == '+' || c == '?' || c == '[' ||
                    c == '(' || c == '{' || c == '|') {
                    break;
                }
                
                prefixBuilder.append(c);
                i++;
            }
            
            if (prefixBuilder.length() > 0) {
                // Use prefix search, then verify with regex
                Set<String> candidates = findPrefix(prefixBuilder.toString());
                
                // Verify each candidate
                Set<String> result = new HashSet<>();
                Pattern caseInsensitive = Pattern.compile(pattern.pattern(), Pattern.CASE_INSENSITIVE);
                for (String docKey : candidates) {
                    String value = valueIndex.get(docKey);
                    if (value != null && caseInsensitive.matcher(value).matches()) {
                        result.add(docKey);
                    }
                }
                return result;
            }
        }
        
        // Can't optimize - fall back to full scan
        log.debug("PrefixIndex cannot optimize regex '{}' - falling back to full scan", patternStr);
        return null;
    }
    
    @Override
    public Set<String> findContains(String substring) {
        // PrefixIndex doesn't efficiently support contains - return null to indicate full scan needed
        log.debug("PrefixIndex does not support contains queries. Use TRIGRAM index for field: {}", 
            definition.getField());
        return null;
    }
    
    @Override
    public Set<String> findGreaterThan(Object value, boolean inclusive) {
        log.warn("PrefixIndex does not support range queries. Use BTREE index for field: {}", 
            definition.getField());
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> findLessThan(Object value, boolean inclusive) {
        log.warn("PrefixIndex does not support range queries. Use BTREE index for field: {}", 
            definition.getField());
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> findRange(Object from, Object to, boolean fromInclusive, boolean toInclusive) {
        log.warn("PrefixIndex does not support range queries. Use BTREE index for field: {}", 
            definition.getField());
        return Collections.emptySet();
    }
    
    @Override
    public void clear() {
        root.children.clear();
        root.documentKeys.clear();
        root.isEndOfWord = false;
        valueIndex.clear();
        totalEntries.set(0);
        log.debug("Cleared PrefixIndex for field: {}", definition.getField());
    }
    
    @Override
    public long size() {
        return totalEntries.get();
    }
    
    @Override
    public int uniqueValues() {
        return valueIndex.size();
    }
    
    @Override
    public long memoryUsageBytes() {
        // Estimate: count trie nodes * ~80 bytes per node + value index
        long nodeCount = countNodes(root);
        return (nodeCount * 80) + (valueIndex.size() * 100);
    }
    
    private long countNodes(TrieNode node) {
        long count = 1;
        for (TrieNode child : node.children.values()) {
            count += countNodes(child);
        }
        return count;
    }
    
    @Override
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("field", definition.getField());
        stats.put("type", "PREFIX");
        stats.put("entries", totalEntries.get());
        stats.put("uniqueValues", valueIndex.size());
        stats.put("trieNodes", countNodes(root));
        stats.put("memoryBytes", memoryUsageBytes());
        stats.put("supportsRangeQueries", false);
        stats.put("supportsRegexQueries", true); // For ^prefix.* patterns
        stats.put("supportsPrefixQueries", true);
        
        // Trie depth statistics
        int[] depths = new int[1];
        int[] maxDepth = new int[1];
        calculateDepths(root, 0, depths, maxDepth);
        stats.put("maxDepth", maxDepth[0]);
        
        return stats;
    }
    
    private void calculateDepths(TrieNode node, int currentDepth, int[] totalDepth, int[] maxDepth) {
        if (node.isEndOfWord) {
            totalDepth[0] += currentDepth;
            if (currentDepth > maxDepth[0]) {
                maxDepth[0] = currentDepth;
            }
        }
        for (TrieNode child : node.children.values()) {
            calculateDepths(child, currentDepth + 1, totalDepth, maxDepth);
        }
    }
    
    @Override
    public boolean supportsRangeQueries() {
        return false;
    }
    
    @Override
    public boolean supportsRegexQueries() {
        return true; // For ^prefix.* patterns only
    }
    
    @Override
    public boolean supportsPrefixQueries() {
        return true;
    }
    
    // ==================== Helper Methods ====================
    
    /**
     * Find the trie node for a given string.
     */
    private TrieNode findNode(String value) {
        TrieNode current = root;
        for (char c : value.toCharArray()) {
            current = current.children.get(c);
            if (current == null) {
                return null;
            }
        }
        return current;
    }
    
    /**
     * Get all entries for persistence.
     */
    public Map<String, String> getAllValues() {
        return new HashMap<>(valueIndex);
    }
    
    /**
     * Load entries from persistence.
     */
    public void loadEntries(Map<String, String> entries) {
        clear();
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            add(entry.getValue(), entry.getKey());
        }
        log.debug("Loaded {} entries into PrefixIndex for field: {}", 
            totalEntries.get(), definition.getField());
    }
}
