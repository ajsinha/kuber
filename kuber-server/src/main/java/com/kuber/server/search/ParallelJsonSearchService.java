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
package com.kuber.server.search;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.index.IndexManager;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * High-performance parallel search service for JSON and key pattern searches.
 * 
 * <p>This service provides significant performance improvements for searches
 * on large datasets by utilizing multiple threads to process in parallel.
 * 
 * <h3>Search Optimization Strategy (v1.8.2):</h3>
 * <ol>
 *   <li><strong>Index Lookup</strong>: If secondary indexes exist, use them for O(1)/O(log n) lookups</li>
 *   <li><strong>Index Intersection</strong>: For multiple criteria, intersect index results</li>
 *   <li><strong>Parallel Scan</strong>: Fall back to parallel full scan if no indexes available</li>
 * </ol>
 * 
 * <h3>Parallel Search Types:</h3>
 * <ul>
 *   <li><strong>JSON Search</strong>: Index lookup + parallel document fetch</li>
 *   <li><strong>Pattern Search</strong>: Parallel key matching + value fetching</li>
 *   <li><strong>Multi-Pattern Search</strong>: Parallel processing of multiple regex patterns</li>
 * </ul>
 * 
 * <h3>Performance:</h3>
 * <ul>
 *   <li>With index: O(1) or O(log n) + document fetch</li>
 *   <li>Without index: O(n) parallel scan - ~8x speedup with 8 threads</li>
 * </ul>
 * 
 * @version 1.8.2
 * @since 1.7.9
 */
@Service
@Slf4j
public class ParallelJsonSearchService {

    private final CacheService cacheService;
    private final KuberProperties properties;
    private final IndexManager indexManager;
    
    // Thread pool for parallel search operations
    private ExecutorService searchExecutor;
    
    // Compiled regex pattern cache for performance
    private final ConcurrentHashMap<String, Pattern> regexCache = new ConcurrentHashMap<>();
    
    // Statistics
    private final AtomicLong totalSearches = new AtomicLong(0);
    private final AtomicLong parallelSearches = new AtomicLong(0);
    private final AtomicLong sequentialSearches = new AtomicLong(0);
    private final AtomicLong totalSearchTimeMs = new AtomicLong(0);
    private final AtomicLong patternSearches = new AtomicLong(0);
    private final AtomicLong jsonSearches = new AtomicLong(0);
    private final AtomicLong indexedSearches = new AtomicLong(0);
    private final AtomicLong fullScanSearches = new AtomicLong(0);
    
    public ParallelJsonSearchService(CacheService cacheService, 
                                     KuberProperties properties,
                                     @Lazy IndexManager indexManager) {
        this.cacheService = cacheService;
        this.properties = properties;
        this.indexManager = indexManager;
    }
    
    @PostConstruct
    public void initialize() {
        int threadCount = properties.getSearch().getThreadCount();
        boolean parallelEnabled = properties.getSearch().isParallelEnabled();
        
        if (parallelEnabled) {
            // Create thread pool with custom thread factory for naming
            searchExecutor = Executors.newFixedThreadPool(threadCount, new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(0);
                
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setName("kuber-search-" + counter.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                }
            });
            
            log.info("╔══════════════════════════════════════════════════════════════╗");
            log.info("║         PARALLEL SEARCH SERVICE INITIALIZED                  ║");
            log.info("╠══════════════════════════════════════════════════════════════╣");
            log.info("║  Status:    ENABLED                                          ║");
            log.info("║  Threads:   {}                                               ║", 
                String.format("%-8d", threadCount));
            log.info("║  Threshold: {} keys                                       ║", 
                String.format("%-8d", properties.getSearch().getParallelThreshold()));
            log.info("║  Timeout:   {} seconds                                       ║", 
                String.format("%-8d", properties.getSearch().getTimeoutSeconds()));
            log.info("║  Supports:  JSON Search, Pattern Search, Multi-Pattern       ║");
            log.info("╚══════════════════════════════════════════════════════════════╝");
        } else {
            log.info("Parallel search: DISABLED (sequential mode only)");
        }
    }
    
    // ==================== JSON Criteria Search ====================
    
    /**
     * Search JSON documents with automatic index usage and parallel/sequential mode selection.
     * 
     * <p>Search Strategy (v1.8.2):
     * <ol>
     *   <li>Check if indexes exist for search criteria fields</li>
     *   <li>If indexes available, use index lookup to narrow down candidates</li>
     *   <li>For remaining criteria without indexes, filter candidates</li>
     *   <li>Use parallel processing for large result sets</li>
     * </ol>
     * 
     * @param region    Cache region to search
     * @param criteria  Search criteria (field-value mappings with optional operators)
     * @param fields    Fields to project in results (null for all fields)
     * @param limit     Maximum number of results to return
     * @return List of matching documents with their keys
     */
    public List<Map<String, Object>> search(String region, 
                                            Map<String, Object> criteria, 
                                            List<String> fields,
                                            int limit) {
        
        long startTime = System.currentTimeMillis();
        totalSearches.incrementAndGet();
        jsonSearches.incrementAndGet();
        
        // Try index-based search first
        Set<String> candidateKeys = tryIndexLookup(region, criteria);
        
        List<Map<String, Object>> results;
        
        if (candidateKeys != null) {
            // Index was used - search only candidate keys
            indexedSearches.incrementAndGet();
            log.debug("Index search: {} candidate keys for region '{}' with {} criteria", 
                candidateKeys.size(), region, criteria != null ? criteria.size() : 0);
            
            if (candidateKeys.isEmpty()) {
                results = Collections.emptyList();
            } else {
                // Search within candidate keys (may still need to verify other criteria)
                results = searchWithinCandidates(region, candidateKeys, criteria, fields, limit);
            }
        } else {
            // No index available - fall back to full scan
            fullScanSearches.incrementAndGet();
            
            Set<String> allKeys = cacheService.keys(region, "*");
            
            if (allKeys == null || allKeys.isEmpty()) {
                log.debug("No keys found in region '{}' for search", region);
                return Collections.emptyList();
            }
            
            log.debug("Full scan search: {} keys in region '{}' with {} criteria", 
                allKeys.size(), region, criteria != null ? criteria.size() : 0);
            
            // Decide parallel vs sequential
            if (shouldUseParallelSearch(allKeys.size())) {
                parallelSearches.incrementAndGet();
                results = parallelJsonSearch(region, allKeys, criteria, fields, limit);
            } else {
                sequentialSearches.incrementAndGet();
                results = sequentialJsonSearch(region, allKeys, criteria, fields, limit);
            }
        }
        
        long elapsed = System.currentTimeMillis() - startTime;
        totalSearchTimeMs.addAndGet(elapsed);
        
        log.debug("JSON search completed: {} results in {}ms ({})", 
            results.size(), elapsed, 
            candidateKeys != null ? "indexed" : "full-scan");
        
        return results;
    }
    
    /**
     * Try to use secondary indexes to narrow down search candidates.
     * 
     * @param region Region to search
     * @param criteria Search criteria
     * @return Set of candidate keys if index was used, null if no applicable index
     */
    @SuppressWarnings("unchecked")
    private Set<String> tryIndexLookup(String region, Map<String, Object> criteria) {
        if (criteria == null || criteria.isEmpty() || indexManager == null) {
            return null;
        }
        
        Set<String> candidateKeys = null;
        Set<String> indexedFields = new HashSet<>();
        
        for (Map.Entry<String, Object> entry : criteria.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();
            
            // Skip if no index for this field
            if (!indexManager.hasIndex(region, field)) {
                continue;
            }
            
            Set<String> fieldMatches = null;
            
            // Handle different criteria types
            if (value instanceof List) {
                // IN clause: field IN [value1, value2, ...]
                Set<Object> values = new HashSet<>((List<?>) value);
                fieldMatches = indexManager.findIn(region, field, values);
            } else if (value instanceof Map) {
                // Operator-based: {gt: 10, lt: 100}
                Map<String, Object> operators = (Map<String, Object>) value;
                fieldMatches = applyOperatorsWithIndex(region, field, operators);
            } else {
                // Simple equality
                fieldMatches = indexManager.findEquals(region, field, value);
            }
            
            if (fieldMatches != null) {
                indexedFields.add(field);
                
                if (candidateKeys == null) {
                    // First indexed field
                    candidateKeys = new HashSet<>(fieldMatches);
                } else {
                    // Intersect with previous results (AND logic)
                    candidateKeys.retainAll(fieldMatches);
                }
                
                // Early termination if no candidates left
                if (candidateKeys.isEmpty()) {
                    log.debug("Index intersection resulted in empty set for region {}", region);
                    return candidateKeys;
                }
            }
        }
        
        if (candidateKeys != null && !indexedFields.isEmpty()) {
            log.debug("Index lookup used {} indexes, {} candidates for region {}", 
                indexedFields.size(), candidateKeys.size(), region);
        }
        
        return candidateKeys;
    }
    
    /**
     * Apply operators using index (for BTREE indexes supporting range queries,
     * and TRIGRAM indexes supporting regex queries).
     */
    private Set<String> applyOperatorsWithIndex(String region, String field, Map<String, Object> operators) {
        Set<String> result = null;
        
        for (Map.Entry<String, Object> op : operators.entrySet()) {
            String operator = op.getKey().toLowerCase();
            Object operand = op.getValue();
            
            Set<String> matches = switch (operator) {
                case "gt" -> indexManager.findGreaterThan(region, field, operand, false);
                case "gte" -> indexManager.findGreaterThan(region, field, operand, true);
                case "lt" -> indexManager.findLessThan(region, field, operand, false);
                case "lte" -> indexManager.findLessThan(region, field, operand, true);
                case "eq" -> indexManager.findEquals(region, field, operand);
                case "ne" -> null; // NOT queries need full scan
                case "regex" -> {
                    // Try to use TRIGRAM index for regex
                    if (operand != null && indexManager.supportsRegex(region, field)) {
                        try {
                            Pattern pattern = regexCache.computeIfAbsent(
                                operand.toString(), Pattern::compile);
                            yield indexManager.findRegex(region, field, pattern);
                        } catch (Exception e) {
                            log.debug("Invalid regex pattern: {}", operand);
                            yield null;
                        }
                    }
                    yield null;
                }
                case "prefix", "startswith" -> {
                    // Try to use PREFIX or TRIGRAM index for prefix search
                    if (operand != null && indexManager.supportsPrefix(region, field)) {
                        yield indexManager.findPrefix(region, field, operand.toString());
                    }
                    yield null;
                }
                case "contains" -> {
                    // Try to use TRIGRAM index for contains search
                    if (operand != null) {
                        yield indexManager.findContains(region, field, operand.toString());
                    }
                    yield null;
                }
                default -> null;
            };
            
            if (matches == null) {
                // This operator can't use index
                continue;
            }
            
            if (result == null) {
                result = new HashSet<>(matches);
            } else {
                result.retainAll(matches);
            }
        }
        
        return result;
    }
    
    /**
     * Search within a pre-filtered set of candidate keys.
     */
    private List<Map<String, Object>> searchWithinCandidates(String region,
                                                              Set<String> candidateKeys,
                                                              Map<String, Object> criteria,
                                                              List<String> fields,
                                                              int limit) {
        List<Map<String, Object>> results = new ArrayList<>();
        
        for (String key : candidateKeys) {
            if (results.size() >= limit) {
                break;
            }
            
            try {
                JsonNode document = cacheService.jsonGet(region, key, "$");
                
                if (document != null && matchesAllCriteria(document, criteria)) {
                    results.add(buildResultItem(key, document, fields));
                }
            } catch (Exception e) {
                log.trace("Error processing key {}: {}", key, e.getMessage());
            }
        }
        
        return results;
    }
    
    // ==================== Key Pattern Search ====================
    
    /**
     * Search by single key pattern with parallel value fetching.
     * 
     * @param region     Cache region to search
     * @param pattern    Regex pattern to match keys
     * @param fields     Fields to project in results (null for all fields)
     * @param limit      Maximum number of results to return
     * @return List of matching entries with their keys and values
     */
    public List<Map<String, Object>> searchByPattern(String region,
                                                      String pattern,
                                                      List<String> fields,
                                                      int limit) {
        return searchByPatterns(region, Collections.singletonList(pattern), fields, limit);
    }
    
    /**
     * Search by multiple key patterns (OR logic) with parallel processing.
     * 
     * <p>This method provides significant performance improvements when:
     * <ul>
     *   <li>Multiple patterns need to be checked (patterns processed in parallel)</li>
     *   <li>Many keys match (value fetching parallelized)</li>
     *   <li>Large regions (&gt;1000 keys)</li>
     * </ul>
     * 
     * @param region     Cache region to search
     * @param patterns   List of regex patterns (OR logic - match any)
     * @param fields     Fields to project in results (null for all fields)
     * @param limit      Maximum number of results to return
     * @return List of matching entries with their keys and values
     */
    public List<Map<String, Object>> searchByPatterns(String region,
                                                       List<String> patterns,
                                                       List<String> fields,
                                                       int limit) {
        
        long startTime = System.currentTimeMillis();
        totalSearches.incrementAndGet();
        patternSearches.incrementAndGet();
        
        // Get all keys in the region
        Set<String> allKeys = cacheService.keys(region, "*");
        
        if (allKeys == null || allKeys.isEmpty()) {
            log.debug("No keys found in region '{}' for pattern search", region);
            return Collections.emptyList();
        }
        
        log.debug("Pattern search: {} keys in region '{}' with {} patterns", 
            allKeys.size(), region, patterns.size());
        
        List<Map<String, Object>> results;
        
        // Decide parallel vs sequential
        if (shouldUseParallelSearch(allKeys.size())) {
            parallelSearches.incrementAndGet();
            results = parallelPatternSearch(region, allKeys, patterns, fields, limit);
        } else {
            sequentialSearches.incrementAndGet();
            results = sequentialPatternSearch(region, allKeys, patterns, fields, limit);
        }
        
        long elapsed = System.currentTimeMillis() - startTime;
        totalSearchTimeMs.addAndGet(elapsed);
        
        log.debug("Pattern search completed: {} results in {}ms ({})", 
            results.size(), elapsed, 
            shouldUseParallelSearch(allKeys.size()) ? "parallel" : "sequential");
        
        return results;
    }
    
    // ==================== Sequential Implementations ====================
    
    /**
     * Sequential JSON search for small datasets.
     */
    private List<Map<String, Object>> sequentialJsonSearch(String region,
                                                            Set<String> keys,
                                                            Map<String, Object> criteria,
                                                            List<String> fields,
                                                            int limit) {
        List<Map<String, Object>> results = new ArrayList<>();
        
        for (String key : keys) {
            if (results.size() >= limit) {
                break;  // Early termination
            }
            
            try {
                JsonNode document = cacheService.jsonGet(region, key, "$");
                
                if (document != null && matchesAllCriteria(document, criteria)) {
                    results.add(buildResultItem(key, document, fields));
                }
            } catch (Exception e) {
                log.trace("Error processing key {}: {}", key, e.getMessage());
            }
        }
        
        return results;
    }
    
    /**
     * Sequential pattern search for small datasets.
     */
    private List<Map<String, Object>> sequentialPatternSearch(String region,
                                                               Set<String> keys,
                                                               List<String> patterns,
                                                               List<String> fields,
                                                               int limit) {
        List<Map<String, Object>> results = new ArrayList<>();
        
        // Compile patterns once
        List<Pattern> compiledPatterns = compilePatterns(patterns);
        
        for (String key : keys) {
            if (results.size() >= limit) {
                break;  // Early termination
            }
            
            // Check if key matches any pattern (OR logic)
            if (matchesAnyPattern(key, compiledPatterns)) {
                try {
                    String value = cacheService.get(region, key);
                    if (value != null) {
                        results.add(buildResultItemFromString(key, value, fields));
                    }
                } catch (Exception e) {
                    log.trace("Error fetching value for key {}: {}", key, e.getMessage());
                }
            }
        }
        
        return results;
    }
    
    // ==================== Parallel Implementations ====================
    
    /**
     * Parallel JSON search using multiple threads.
     */
    private List<Map<String, Object>> parallelJsonSearch(String region,
                                                          Set<String> keys,
                                                          Map<String, Object> criteria,
                                                          List<String> fields,
                                                          int limit) {
        
        List<String> keyList = new ArrayList<>(keys);
        int threadCount = properties.getSearch().getThreadCount();
        int partitionSize = (keyList.size() + threadCount - 1) / threadCount;
        
        ConcurrentLinkedQueue<Map<String, Object>> results = new ConcurrentLinkedQueue<>();
        AtomicInteger foundCount = new AtomicInteger(0);
        AtomicBoolean limitReached = new AtomicBoolean(false);
        
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (int i = 0; i < keyList.size(); i += partitionSize) {
            int start = i;
            int end = Math.min(i + partitionSize, keyList.size());
            List<String> partition = keyList.subList(start, end);
            
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                searchJsonPartition(region, partition, criteria, fields, limit,
                    results, foundCount, limitReached);
            }, searchExecutor);
            
            futures.add(future);
        }
        
        waitForCompletion(futures, results.size());
        
        List<Map<String, Object>> resultList = new ArrayList<>(results);
        if (resultList.size() > limit) {
            return resultList.subList(0, limit);
        }
        return resultList;
    }
    
    /**
     * Parallel pattern search with two-phase processing:
     * Phase 1: Parallel key matching across partitions
     * Phase 2: Parallel value fetching for matches
     */
    private List<Map<String, Object>> parallelPatternSearch(String region,
                                                             Set<String> keys,
                                                             List<String> patterns,
                                                             List<String> fields,
                                                             int limit) {
        
        List<String> keyList = new ArrayList<>(keys);
        int threadCount = properties.getSearch().getThreadCount();
        int partitionSize = (keyList.size() + threadCount - 1) / threadCount;
        
        // Compile patterns once (thread-safe)
        List<Pattern> compiledPatterns = compilePatterns(patterns);
        
        // Phase 1: Parallel key matching
        ConcurrentLinkedQueue<String> matchedKeys = new ConcurrentLinkedQueue<>();
        AtomicBoolean enoughMatches = new AtomicBoolean(false);
        
        List<CompletableFuture<Void>> matchFutures = new ArrayList<>();
        
        for (int i = 0; i < keyList.size(); i += partitionSize) {
            int start = i;
            int end = Math.min(i + partitionSize, keyList.size());
            List<String> partition = keyList.subList(start, end);
            
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                for (String key : partition) {
                    if (enoughMatches.get()) return;  // Early exit
                    
                    if (matchesAnyPattern(key, compiledPatterns)) {
                        matchedKeys.add(key);
                        // Stop collecting if we have way more than needed
                        if (matchedKeys.size() > limit * 2) {
                            enoughMatches.set(true);
                            return;
                        }
                    }
                }
            }, searchExecutor);
            
            matchFutures.add(future);
        }
        
        waitForCompletion(matchFutures, 0);
        
        // Phase 2: Parallel value fetching for matched keys
        List<String> keysToFetch = new ArrayList<>(matchedKeys);
        if (keysToFetch.isEmpty()) {
            return Collections.emptyList();
        }
        
        // Limit keys to fetch to avoid over-fetching
        if (keysToFetch.size() > limit) {
            keysToFetch = keysToFetch.subList(0, limit);
        }
        
        log.debug("Pattern search phase 2: fetching values for {} matched keys", keysToFetch.size());
        
        ConcurrentLinkedQueue<Map<String, Object>> results = new ConcurrentLinkedQueue<>();
        AtomicInteger foundCount = new AtomicInteger(0);
        AtomicBoolean limitReached = new AtomicBoolean(false);
        
        int fetchPartitionSize = (keysToFetch.size() + threadCount - 1) / threadCount;
        List<CompletableFuture<Void>> fetchFutures = new ArrayList<>();
        
        for (int i = 0; i < keysToFetch.size(); i += fetchPartitionSize) {
            int start = i;
            int end = Math.min(i + fetchPartitionSize, keysToFetch.size());
            List<String> partition = keysToFetch.subList(start, end);
            
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                fetchValuesPartition(region, partition, fields, limit,
                    results, foundCount, limitReached);
            }, searchExecutor);
            
            fetchFutures.add(future);
        }
        
        waitForCompletion(fetchFutures, results.size());
        
        List<Map<String, Object>> resultList = new ArrayList<>(results);
        if (resultList.size() > limit) {
            return resultList.subList(0, limit);
        }
        return resultList;
    }
    
    // ==================== Partition Workers ====================
    
    /**
     * Search a partition of keys for JSON criteria (runs in separate thread).
     */
    private void searchJsonPartition(String region,
                                      List<String> keys,
                                      Map<String, Object> criteria,
                                      List<String> fields,
                                      int limit,
                                      ConcurrentLinkedQueue<Map<String, Object>> results,
                                      AtomicInteger foundCount,
                                      AtomicBoolean limitReached) {
        
        for (String key : keys) {
            if (limitReached.get()) return;
            
            try {
                JsonNode document = cacheService.jsonGet(region, key, "$");
                
                if (document != null && matchesAllCriteria(document, criteria)) {
                    results.add(buildResultItem(key, document, fields));
                    
                    if (foundCount.incrementAndGet() >= limit) {
                        limitReached.set(true);
                        return;
                    }
                }
            } catch (Exception e) {
                log.trace("Error processing key {} in partition: {}", key, e.getMessage());
            }
        }
    }
    
    /**
     * Fetch values for a partition of keys (runs in separate thread).
     */
    private void fetchValuesPartition(String region,
                                       List<String> keys,
                                       List<String> fields,
                                       int limit,
                                       ConcurrentLinkedQueue<Map<String, Object>> results,
                                       AtomicInteger foundCount,
                                       AtomicBoolean limitReached) {
        
        for (String key : keys) {
            if (limitReached.get()) return;
            
            try {
                String value = cacheService.get(region, key);
                
                if (value != null) {
                    results.add(buildResultItemFromString(key, value, fields));
                    
                    if (foundCount.incrementAndGet() >= limit) {
                        limitReached.set(true);
                        return;
                    }
                }
            } catch (Exception e) {
                log.trace("Error fetching value for key {}: {}", key, e.getMessage());
            }
        }
    }
    
    // ==================== Pattern Matching ====================
    
    /**
     * Compile regex patterns with caching.
     */
    private List<Pattern> compilePatterns(List<String> patterns) {
        List<Pattern> compiled = new ArrayList<>();
        for (String patternStr : patterns) {
            try {
                Pattern pattern = regexCache.computeIfAbsent(patternStr, Pattern::compile);
                compiled.add(pattern);
            } catch (PatternSyntaxException e) {
                log.warn("Invalid regex pattern '{}': {}", patternStr, e.getMessage());
            }
        }
        return compiled;
    }
    
    /**
     * Check if key matches any of the patterns (OR logic).
     */
    private boolean matchesAnyPattern(String key, List<Pattern> patterns) {
        for (Pattern pattern : patterns) {
            if (pattern.matcher(key).matches()) {
                return true;
            }
        }
        return false;
    }
    
    // ==================== Helper Methods ====================
    
    /**
     * Determine if parallel search should be used.
     */
    private boolean shouldUseParallelSearch(int keyCount) {
        if (!properties.getSearch().isParallelEnabled()) {
            return false;
        }
        if (searchExecutor == null || searchExecutor.isShutdown()) {
            return false;
        }
        return keyCount >= properties.getSearch().getParallelThreshold();
    }
    
    /**
     * Wait for all futures to complete with timeout.
     */
    private void waitForCompletion(List<CompletableFuture<Void>> futures, int currentResults) {
        try {
            int timeoutSeconds = properties.getSearch().getTimeoutSeconds();
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.warn("Search timed out after {}s, returning {} partial results", 
                properties.getSearch().getTimeoutSeconds(), currentResults);
            futures.forEach(f -> f.cancel(true));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Search interrupted, returning partial results");
        } catch (ExecutionException e) {
            log.error("Search execution error: {}", e.getMessage());
        }
    }
    
    /**
     * Check if JSON document matches all criteria (AND logic).
     */
    private boolean matchesAllCriteria(JsonNode json, Map<String, Object> criteria) {
        if (criteria == null || criteria.isEmpty()) {
            return true;
        }
        
        for (Map.Entry<String, Object> entry : criteria.entrySet()) {
            String fieldPath = entry.getKey();
            Object criteriaValue = entry.getValue();
            
            JsonNode fieldValue = getJsonField(json, fieldPath);
            
            if (!matchesCriterion(fieldValue, criteriaValue)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Get a field value from JSON, supporting dot notation.
     */
    private JsonNode getJsonField(JsonNode json, String fieldPath) {
        if (json == null || fieldPath == null) {
            return null;
        }
        
        String[] parts = fieldPath.split("\\.");
        JsonNode current = json;
        
        for (String part : parts) {
            if (current == null || !current.has(part)) {
                return null;
            }
            current = current.get(part);
        }
        
        return current;
    }
    
    /**
     * Check if a field value matches a criterion.
     */
    @SuppressWarnings("unchecked")
    private boolean matchesCriterion(JsonNode fieldValue, Object criteriaValue) {
        if (fieldValue == null || fieldValue.isNull() || fieldValue.isMissingNode()) {
            return false;
        }
        
        if (criteriaValue instanceof List) {
            List<?> valueList = (List<?>) criteriaValue;
            return valueList.stream().anyMatch(v -> valueMatches(fieldValue, v));
        }
        
        if (criteriaValue instanceof Map) {
            Map<String, Object> operators = (Map<String, Object>) criteriaValue;
            return matchesOperators(fieldValue, operators);
        }
        
        return valueMatches(fieldValue, criteriaValue);
    }
    
    private boolean valueMatches(JsonNode fieldValue, Object expected) {
        if (expected == null) {
            return fieldValue.isNull();
        }
        
        String actualValue = fieldValue.asText();
        String expectedValue = String.valueOf(expected);
        
        if (fieldValue.isNumber() && expected instanceof Number) {
            return fieldValue.asDouble() == ((Number) expected).doubleValue();
        }
        
        if (fieldValue.isBoolean() && expected instanceof Boolean) {
            return fieldValue.asBoolean() == (Boolean) expected;
        }
        
        return actualValue.equals(expectedValue);
    }
    
    private boolean matchesOperators(JsonNode fieldValue, Map<String, Object> operators) {
        for (Map.Entry<String, Object> op : operators.entrySet()) {
            String operator = op.getKey();
            Object operand = op.getValue();
            
            boolean matches = switch (operator.toLowerCase()) {
                case "gt" -> compareNumeric(fieldValue, operand) > 0;
                case "gte" -> compareNumeric(fieldValue, operand) >= 0;
                case "lt" -> compareNumeric(fieldValue, operand) < 0;
                case "lte" -> compareNumeric(fieldValue, operand) <= 0;
                case "eq" -> valueMatches(fieldValue, operand);
                case "ne" -> !valueMatches(fieldValue, operand);
                case "regex" -> matchesRegex(fieldValue.asText(), String.valueOf(operand));
                case "prefix", "startswith" -> 
                    fieldValue.asText().toLowerCase().startsWith(String.valueOf(operand).toLowerCase());
                case "contains" -> 
                    fieldValue.asText().toLowerCase().contains(String.valueOf(operand).toLowerCase());
                case "suffix", "endswith" ->
                    fieldValue.asText().toLowerCase().endsWith(String.valueOf(operand).toLowerCase());
                default -> {
                    log.trace("Unknown operator: {}", operator);
                    yield true;
                }
            };
            
            if (!matches) return false;
        }
        return true;
    }
    
    private int compareNumeric(JsonNode fieldValue, Object operand) {
        try {
            double fieldNum = fieldValue.asDouble();
            double operandNum = operand instanceof Number 
                ? ((Number) operand).doubleValue() 
                : Double.parseDouble(String.valueOf(operand));
            return Double.compare(fieldNum, operandNum);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
    
    private boolean matchesRegex(String value, String patternStr) {
        try {
            Pattern pattern = regexCache.computeIfAbsent(patternStr, Pattern::compile);
            return pattern.matcher(value).matches();
        } catch (PatternSyntaxException e) {
            log.warn("Invalid regex pattern '{}': {}", patternStr, e.getMessage());
            return false;
        }
    }
    
    // ==================== Result Building ====================
    
    private Map<String, Object> buildResultItem(String key, JsonNode document, List<String> fields) {
        Map<String, Object> item = new HashMap<>();
        item.put("key", key);
        
        if (fields != null && !fields.isEmpty()) {
            item.put("value", projectFields(document, fields));
        } else {
            item.put("value", document);
        }
        
        return item;
    }
    
    private Map<String, Object> buildResultItemFromString(String key, String value, List<String> fields) {
        Map<String, Object> item = new HashMap<>();
        item.put("key", key);
        
        try {
            // Try to parse as JSON for field projection
            JsonNode jsonValue = com.kuber.core.util.JsonUtils.parse(value);
            if (fields != null && !fields.isEmpty()) {
                item.put("value", projectFields(jsonValue, fields));
            } else {
                item.put("value", jsonValue);
            }
        } catch (Exception e) {
            // Not JSON, return as-is
            item.put("value", value);
        }
        
        return item;
    }
    
    private ObjectNode projectFields(JsonNode document, List<String> fields) {
        ObjectNode projected = JsonNodeFactory.instance.objectNode();
        
        for (String field : fields) {
            JsonNode value = getJsonField(document, field);
            if (value != null && !value.isMissingNode()) {
                if (field.contains(".")) {
                    setNestedField(projected, field, value);
                } else {
                    projected.set(field, value);
                }
            }
        }
        
        return projected;
    }
    
    private void setNestedField(ObjectNode root, String path, JsonNode value) {
        String[] parts = path.split("\\.");
        ObjectNode current = root;
        
        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i];
            if (!current.has(part)) {
                current.set(part, JsonNodeFactory.instance.objectNode());
            }
            current = (ObjectNode) current.get(part);
        }
        
        current.set(parts[parts.length - 1], value);
    }
    
    // ==================== Statistics ====================
    
    /**
     * Get search statistics.
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("totalSearches", totalSearches.get());
        stats.put("parallelSearches", parallelSearches.get());
        stats.put("sequentialSearches", sequentialSearches.get());
        stats.put("jsonSearches", jsonSearches.get());
        stats.put("patternSearches", patternSearches.get());
        stats.put("indexedSearches", indexedSearches.get());
        stats.put("fullScanSearches", fullScanSearches.get());
        stats.put("totalSearchTimeMs", totalSearchTimeMs.get());
        
        long total = totalSearches.get();
        if (total > 0) {
            stats.put("avgSearchTimeMs", totalSearchTimeMs.get() / total);
            stats.put("parallelSearchPercent", 
                String.format("%.1f%%", (parallelSearches.get() * 100.0) / total));
            stats.put("indexedSearchPercent",
                String.format("%.1f%%", (indexedSearches.get() * 100.0) / total));
        }
        
        stats.put("parallelEnabled", properties.getSearch().isParallelEnabled());
        stats.put("threadCount", properties.getSearch().getThreadCount());
        stats.put("parallelThreshold", properties.getSearch().getParallelThreshold());
        stats.put("cachedRegexPatterns", regexCache.size());
        
        return stats;
    }
    
    /**
     * Clear regex pattern cache.
     */
    public void clearRegexCache() {
        int size = regexCache.size();
        regexCache.clear();
        log.info("Cleared {} cached regex patterns", size);
    }
    
    @PreDestroy
    public void shutdown() {
        if (searchExecutor != null && !searchExecutor.isShutdown()) {
            log.info("Shutting down parallel search executor...");
            searchExecutor.shutdown();
            try {
                if (!searchExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    searchExecutor.shutdownNow();
                    log.warn("Search executor did not terminate gracefully");
                }
            } catch (InterruptedException e) {
                searchExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        log.info("Search service shut down. Stats: {} total ({} JSON, {} pattern), {} parallel, {} sequential", 
            totalSearches.get(), jsonSearches.get(), patternSearches.get(),
            parallelSearches.get(), sequentialSearches.get());
    }
}
