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
package com.kuber.server.index;

import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Trigram-based secondary index for efficient regex and substring searches.
 * 
 * <p>A trigram index breaks each field value into 3-character sequences (trigrams)
 * and builds an inverted index mapping each trigram to the document keys containing it.
 * 
 * <p>Example: "john@gmail.com" produces trigrams:
 * [joh, ohn, hn@, n@g, @gm, gma, mai, ail, il., l.c, .co, com]
 * 
 * <p>For regex queries, the index extracts literals from the pattern and uses
 * trigram lookups to quickly narrow down candidate documents before applying
 * the full regex match.
 * 
 * <h3>Query Types Supported:</h3>
 * <ul>
 *   <li><strong>Regex:</strong> email MATCHES ".*@gmail\.com$"</li>
 *   <li><strong>Contains:</strong> name CONTAINS "smith"</li>
 *   <li><strong>Prefix:</strong> city STARTS WITH "New"</li>
 *   <li><strong>Equality:</strong> status = "active" (via trigram intersection)</li>
 * </ul>
 * 
 * <h3>Performance:</h3>
 * <ul>
 *   <li>Regex with literals: O(intersected trigram matches) - typically very fast</li>
 *   <li>Contains (substring): O(trigram matches for substring)</li>
 *   <li>Prefix: O(trigram matches for first 3+ chars)</li>
 *   <li>Without usable trigrams: Falls back to full scan</li>
 * </ul>
 * 
 * <h3>How Regex Acceleration Works:</h3>
 * <ol>
 *   <li>Extract literal strings from the regex pattern (e.g., "@gmail.com" from ".*@gmail\.com$")</li>
 *   <li>Generate trigrams from each literal</li>
 *   <li>Intersect the posting lists for those trigrams to get candidates</li>
 *   <li>Apply the full regex only to candidates (much smaller set than full scan)</li>
 * </ol>
 * 
 * @version 2.4.0
 * @since 1.9.0
 */
@Slf4j
public class TrigramIndex implements SecondaryIndex {
    
    /** Minimum trigram length */
    private static final int TRIGRAM_SIZE = 3;
    
    /** Special prefix/suffix markers for anchored patterns */
    private static final String START_MARKER = "$$";
    private static final String END_MARKER = "^^";
    
    private final IndexDefinition definition;
    
    // Inverted index: trigram -> Set of document keys
    private final ConcurrentHashMap<String, Set<String>> trigramIndex;
    
    // Forward index: documentKey -> original value (for verification)
    private final ConcurrentHashMap<String, String> valueIndex;
    
    // Statistics
    private final AtomicLong totalEntries = new AtomicLong(0);
    private final AtomicLong regexQueriesAccelerated = new AtomicLong(0);
    private final AtomicLong regexQueriesFullScan = new AtomicLong(0);
    
    public TrigramIndex(IndexDefinition definition) {
        this.definition = definition;
        this.trigramIndex = new ConcurrentHashMap<>();
        this.valueIndex = new ConcurrentHashMap<>();
        log.debug("Created TrigramIndex for field: {}", definition.getField());
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
        
        // Store original value for verification
        valueIndex.put(documentKey, value);
        
        // Generate and index trigrams
        Set<String> trigrams = generateTrigrams(value);
        for (String trigram : trigrams) {
            trigramIndex.computeIfAbsent(trigram, k -> ConcurrentHashMap.newKeySet())
                       .add(documentKey);
        }
        
        totalEntries.incrementAndGet();
    }
    
    @Override
    public void remove(Object fieldValue, String documentKey) {
        if (documentKey == null) {
            return;
        }
        
        // Get original value
        String value = valueIndex.remove(documentKey);
        if (value == null) {
            return;
        }
        
        // Remove from trigram index
        Set<String> trigrams = generateTrigrams(value);
        for (String trigram : trigrams) {
            Set<String> keys = trigramIndex.get(trigram);
            if (keys != null) {
                keys.remove(documentKey);
                if (keys.isEmpty()) {
                    trigramIndex.remove(trigram, keys);
                }
            }
        }
        
        totalEntries.decrementAndGet();
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
        Set<String> trigrams = generateTrigrams(value);
        
        if (trigrams.isEmpty()) {
            // Value too short for trigrams - fall back to linear search
            return findByFullScan(v -> v.equals(value));
        }
        
        // Intersect all trigram posting lists
        Set<String> candidates = intersectPostingLists(trigrams);
        
        // Verify exact match
        Set<String> result = new HashSet<>();
        for (String docKey : candidates) {
            String docValue = valueIndex.get(docKey);
            if (docValue != null && docValue.equals(value)) {
                result.add(docKey);
            }
        }
        
        return result;
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
    public Set<String> findRegex(Pattern pattern) {
        if (pattern == null) {
            return Collections.emptySet();
        }
        
        String patternStr = pattern.pattern();
        
        // Extract literals from regex
        List<String> literals = extractLiterals(patternStr);
        
        if (literals.isEmpty() || getLongestLiteral(literals).length() < TRIGRAM_SIZE) {
            // No usable literals - fall back to full scan
            regexQueriesFullScan.incrementAndGet();
            log.debug("Regex '{}' has no usable literals - falling back to full scan", patternStr);
            return findByFullScan(v -> pattern.matcher(v).matches());
        }
        
        regexQueriesAccelerated.incrementAndGet();
        
        // Use the longest literal for trigram lookup
        String bestLiteral = getLongestLiteral(literals);
        Set<String> trigrams = generateTrigrams(bestLiteral.toLowerCase());
        
        // Get candidates from trigram intersection
        Set<String> candidates = intersectPostingLists(trigrams);
        
        log.debug("Regex '{}' using literal '{}' - {} candidates from {} total", 
            patternStr, bestLiteral, candidates.size(), totalEntries.get());
        
        // Verify with full regex match
        Set<String> result = new HashSet<>();
        Pattern caseInsensitivePattern = Pattern.compile(pattern.pattern(), Pattern.CASE_INSENSITIVE);
        
        for (String docKey : candidates) {
            String docValue = valueIndex.get(docKey);
            if (docValue != null && caseInsensitivePattern.matcher(docValue).matches()) {
                result.add(docKey);
            }
        }
        
        return result;
    }
    
    @Override
    public Set<String> findPrefix(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return Collections.emptySet();
        }
        
        String lowerPrefix = prefix.toLowerCase();
        
        if (lowerPrefix.length() < TRIGRAM_SIZE) {
            // Prefix too short - fall back to scan
            return findByFullScan(v -> v.startsWith(lowerPrefix));
        }
        
        // Use trigrams from prefix
        Set<String> trigrams = generateTrigrams(lowerPrefix);
        Set<String> candidates = intersectPostingLists(trigrams);
        
        // Verify prefix match
        Set<String> result = new HashSet<>();
        for (String docKey : candidates) {
            String docValue = valueIndex.get(docKey);
            if (docValue != null && docValue.startsWith(lowerPrefix)) {
                result.add(docKey);
            }
        }
        
        return result;
    }
    
    @Override
    public Set<String> findContains(String substring) {
        if (substring == null || substring.isEmpty()) {
            return Collections.emptySet();
        }
        
        String lowerSubstring = substring.toLowerCase();
        
        if (lowerSubstring.length() < TRIGRAM_SIZE) {
            // Substring too short - fall back to scan
            return findByFullScan(v -> v.contains(lowerSubstring));
        }
        
        // Use trigrams from substring
        Set<String> trigrams = generateTrigrams(lowerSubstring);
        Set<String> candidates = intersectPostingLists(trigrams);
        
        // Verify contains match
        Set<String> result = new HashSet<>();
        for (String docKey : candidates) {
            String docValue = valueIndex.get(docKey);
            if (docValue != null && docValue.contains(lowerSubstring)) {
                result.add(docKey);
            }
        }
        
        return result;
    }
    
    @Override
    public Set<String> findGreaterThan(Object value, boolean inclusive) {
        log.warn("TrigramIndex does not support range queries. Use BTREE index for field: {}", 
            definition.getField());
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> findLessThan(Object value, boolean inclusive) {
        log.warn("TrigramIndex does not support range queries. Use BTREE index for field: {}", 
            definition.getField());
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> findRange(Object from, Object to, boolean fromInclusive, boolean toInclusive) {
        log.warn("TrigramIndex does not support range queries. Use BTREE index for field: {}", 
            definition.getField());
        return Collections.emptySet();
    }
    
    @Override
    public void clear() {
        trigramIndex.clear();
        valueIndex.clear();
        totalEntries.set(0);
        regexQueriesAccelerated.set(0);
        regexQueriesFullScan.set(0);
        log.debug("Cleared TrigramIndex for field: {}", definition.getField());
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
        // Estimate: each trigram entry ~50 bytes, each value ~100 bytes average
        long trigramEntries = trigramIndex.values().stream()
            .mapToLong(Set::size)
            .sum();
        return (trigramIndex.size() * 50) + (trigramEntries * 30) + (valueIndex.size() * 100);
    }
    
    @Override
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("field", definition.getField());
        stats.put("type", "TRIGRAM");
        stats.put("entries", totalEntries.get());
        stats.put("uniqueValues", valueIndex.size());
        stats.put("uniqueTrigrams", trigramIndex.size());
        stats.put("memoryBytes", memoryUsageBytes());
        stats.put("supportsRangeQueries", false);
        stats.put("supportsRegexQueries", true);
        stats.put("supportsPrefixQueries", true);
        stats.put("supportsContainsQueries", true);
        stats.put("regexQueriesAccelerated", regexQueriesAccelerated.get());
        stats.put("regexQueriesFullScan", regexQueriesFullScan.get());
        
        // Acceleration rate
        long total = regexQueriesAccelerated.get() + regexQueriesFullScan.get();
        if (total > 0) {
            stats.put("accelerationRate", String.format("%.1f%%", 
                (regexQueriesAccelerated.get() * 100.0) / total));
        }
        
        // Top trigrams by frequency
        List<Map<String, Object>> topTrigrams = new ArrayList<>();
        trigramIndex.entrySet().stream()
            .sorted((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()))
            .limit(10)
            .forEach(entry -> {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("trigram", entry.getKey());
                item.put("count", entry.getValue().size());
                topTrigrams.add(item);
            });
        stats.put("topTrigrams", topTrigrams);
        
        return stats;
    }
    
    @Override
    public boolean supportsRangeQueries() {
        return false;
    }
    
    @Override
    public boolean supportsRegexQueries() {
        return true;
    }
    
    @Override
    public boolean supportsPrefixQueries() {
        return true;
    }
    
    // ==================== Helper Methods ====================
    
    /**
     * Generate trigrams from a string value.
     * Also adds start/end markers for anchored pattern support.
     */
    private Set<String> generateTrigrams(String value) {
        Set<String> trigrams = new HashSet<>();
        
        if (value == null || value.length() < TRIGRAM_SIZE) {
            return trigrams;
        }
        
        // Add start marker for prefix matching
        String padded = START_MARKER + value + END_MARKER;
        
        for (int i = 0; i <= padded.length() - TRIGRAM_SIZE; i++) {
            trigrams.add(padded.substring(i, i + TRIGRAM_SIZE));
        }
        
        return trigrams;
    }
    
    /**
     * Extract literal strings from a regex pattern.
     * These are the parts that must appear in any matching string.
     */
    private List<String> extractLiterals(String pattern) {
        List<String> literals = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        
        int i = 0;
        while (i < pattern.length()) {
            char c = pattern.charAt(i);
            
            // Handle escape sequences
            if (c == '\\' && i + 1 < pattern.length()) {
                char next = pattern.charAt(i + 1);
                // Check for special regex escapes that represent literals
                if (next == '.' || next == '*' || next == '+' || next == '?' || 
                    next == '[' || next == ']' || next == '(' || next == ')' ||
                    next == '{' || next == '}' || next == '|' || next == '^' ||
                    next == '$' || next == '\\') {
                    current.append(next);
                    i += 2;
                    continue;
                } else if (next == 'd' || next == 'w' || next == 's' || 
                          next == 'D' || next == 'W' || next == 'S') {
                    // Character class - not a literal
                    if (current.length() > 0) {
                        literals.add(current.toString());
                        current = new StringBuilder();
                    }
                    i += 2;
                    continue;
                }
            }
            
            // Regex metacharacters end current literal
            if (c == '.' || c == '*' || c == '+' || c == '?' || c == '[' ||
                c == '(' || c == ')' || c == '{' || c == '|' || c == '^' || c == '$') {
                if (current.length() > 0) {
                    literals.add(current.toString());
                    current = new StringBuilder();
                }
                i++;
                continue;
            }
            
            // Regular character - add to current literal
            current.append(c);
            i++;
        }
        
        // Add final literal if any
        if (current.length() > 0) {
            literals.add(current.toString());
        }
        
        return literals;
    }
    
    /**
     * Get the longest literal from the list.
     */
    private String getLongestLiteral(List<String> literals) {
        return literals.stream()
            .max(Comparator.comparingInt(String::length))
            .orElse("");
    }
    
    /**
     * Intersect posting lists for multiple trigrams.
     */
    private Set<String> intersectPostingLists(Set<String> trigrams) {
        Set<String> result = null;
        
        // Sort by posting list size to start with smallest
        List<Map.Entry<String, Set<String>>> sortedEntries = new ArrayList<>();
        for (String trigram : trigrams) {
            Set<String> postingList = trigramIndex.get(trigram);
            if (postingList != null) {
                sortedEntries.add(Map.entry(trigram, postingList));
            }
        }
        
        sortedEntries.sort(Comparator.comparingInt(e -> e.getValue().size()));
        
        for (Map.Entry<String, Set<String>> entry : sortedEntries) {
            if (result == null) {
                result = new HashSet<>(entry.getValue());
            } else {
                result.retainAll(entry.getValue());
                if (result.isEmpty()) {
                    break; // No point continuing if empty
                }
            }
        }
        
        return result != null ? result : Collections.emptySet();
    }
    
    /**
     * Fall back to full scan when trigrams can't help.
     */
    private Set<String> findByFullScan(java.util.function.Predicate<String> predicate) {
        Set<String> result = new HashSet<>();
        for (Map.Entry<String, String> entry : valueIndex.entrySet()) {
            if (predicate.test(entry.getValue())) {
                result.add(entry.getKey());
            }
        }
        return result;
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
        log.debug("Loaded {} entries into TrigramIndex for field: {}", 
            totalEntries.get(), definition.getField());
    }
}
