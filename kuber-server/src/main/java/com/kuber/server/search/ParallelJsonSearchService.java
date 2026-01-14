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
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * High-performance parallel JSON search service.
 * 
 * <p>This service provides significant performance improvements for JSON searches
 * on large datasets by utilizing multiple threads to scan documents in parallel.
 * 
 * <h3>Features:</h3>
 * <ul>
 *   <li>Automatic parallel/sequential mode selection based on dataset size</li>
 *   <li>Configurable thread count (default: 8)</li>
 *   <li>Early termination when result limit is reached</li>
 *   <li>Compiled regex pattern caching for performance</li>
 *   <li>Graceful timeout handling with partial results</li>
 * </ul>
 * 
 * <h3>Performance:</h3>
 * <ul>
 *   <li>100K documents: ~8x speedup with 8 threads</li>
 *   <li>Automatic fallback to sequential for small datasets (&lt;1000 keys)</li>
 *   <li>Thread-safe result collection with minimal contention</li>
 * </ul>
 * 
 * @version 1.7.9
 * @since 1.7.9
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ParallelJsonSearchService {

    private final CacheService cacheService;
    private final KuberProperties properties;
    
    // Thread pool for parallel search operations
    private ExecutorService searchExecutor;
    
    // Compiled regex pattern cache for performance
    private final ConcurrentHashMap<String, Pattern> regexCache = new ConcurrentHashMap<>();
    
    // Statistics
    private final AtomicLong totalSearches = new AtomicLong(0);
    private final AtomicLong parallelSearches = new AtomicLong(0);
    private final AtomicLong sequentialSearches = new AtomicLong(0);
    private final AtomicLong totalSearchTimeMs = new AtomicLong(0);
    
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
                    t.setName("kuber-json-search-" + counter.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                }
            });
            
            log.info("╔══════════════════════════════════════════════════════════════╗");
            log.info("║         PARALLEL JSON SEARCH SERVICE INITIALIZED             ║");
            log.info("╠══════════════════════════════════════════════════════════════╣");
            log.info("║  Status:    ENABLED                                          ║");
            log.info("║  Threads:   {}                                               ║", 
                String.format("%-8d", threadCount));
            log.info("║  Threshold: {} keys                                       ║", 
                String.format("%-8d", properties.getSearch().getParallelThreshold()));
            log.info("║  Timeout:   {} seconds                                       ║", 
                String.format("%-8d", properties.getSearch().getTimeoutSeconds()));
            log.info("╚══════════════════════════════════════════════════════════════╝");
        } else {
            log.info("Parallel JSON search: DISABLED (sequential mode only)");
        }
    }
    
    /**
     * Search JSON documents with automatic parallel/sequential mode selection.
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
        
        // Get all keys in the region
        Set<String> allKeys = cacheService.keys(region, "*");
        
        if (allKeys == null || allKeys.isEmpty()) {
            log.debug("No keys found in region '{}' for search", region);
            return Collections.emptyList();
        }
        
        log.debug("Searching {} keys in region '{}' with {} criteria", 
            allKeys.size(), region, criteria != null ? criteria.size() : 0);
        
        List<Map<String, Object>> results;
        
        // Decide parallel vs sequential
        if (shouldUseParallelSearch(allKeys.size())) {
            parallelSearches.incrementAndGet();
            results = parallelSearch(region, allKeys, criteria, fields, limit);
        } else {
            sequentialSearches.incrementAndGet();
            results = sequentialSearch(region, allKeys, criteria, fields, limit);
        }
        
        long elapsed = System.currentTimeMillis() - startTime;
        totalSearchTimeMs.addAndGet(elapsed);
        
        log.debug("Search completed: {} results in {}ms ({})", 
            results.size(), elapsed, 
            shouldUseParallelSearch(allKeys.size()) ? "parallel" : "sequential");
        
        return results;
    }
    
    /**
     * Determine if parallel search should be used based on configuration and data size.
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
     * Sequential search for small datasets or when parallel is disabled.
     */
    private List<Map<String, Object>> sequentialSearch(String region,
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
     * Parallel search using multiple threads for large datasets.
     */
    private List<Map<String, Object>> parallelSearch(String region,
                                                      Set<String> keys,
                                                      Map<String, Object> criteria,
                                                      List<String> fields,
                                                      int limit) {
        
        // Convert to list for partitioning
        List<String> keyList = new ArrayList<>(keys);
        
        // Calculate partition size based on thread count
        int threadCount = properties.getSearch().getThreadCount();
        int partitionSize = (keyList.size() + threadCount - 1) / threadCount;
        
        // Thread-safe result collection
        ConcurrentLinkedQueue<Map<String, Object>> results = new ConcurrentLinkedQueue<>();
        AtomicInteger foundCount = new AtomicInteger(0);
        AtomicBoolean limitReached = new AtomicBoolean(false);
        
        // Create and submit search tasks for each partition
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (int i = 0; i < keyList.size(); i += partitionSize) {
            int start = i;
            int end = Math.min(i + partitionSize, keyList.size());
            List<String> partition = keyList.subList(start, end);
            
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                searchPartition(region, partition, criteria, fields, limit,
                    results, foundCount, limitReached);
            }, searchExecutor);
            
            futures.add(future);
        }
        
        // Wait for all tasks with timeout
        try {
            int timeoutSeconds = properties.getSearch().getTimeoutSeconds();
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.warn("Parallel search timed out after {}s, returning {} partial results", 
                properties.getSearch().getTimeoutSeconds(), results.size());
            // Cancel remaining tasks
            futures.forEach(f -> f.cancel(true));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Parallel search interrupted, returning {} partial results", results.size());
        } catch (ExecutionException e) {
            log.error("Parallel search execution error: {}", e.getMessage());
        }
        
        // Convert to list and trim to limit (may slightly exceed due to concurrent adds)
        List<Map<String, Object>> resultList = new ArrayList<>(results);
        if (resultList.size() > limit) {
            return resultList.subList(0, limit);
        }
        return resultList;
    }
    
    /**
     * Search a partition of keys (runs in a separate thread).
     */
    private void searchPartition(String region,
                                  List<String> keys,
                                  Map<String, Object> criteria,
                                  List<String> fields,
                                  int limit,
                                  ConcurrentLinkedQueue<Map<String, Object>> results,
                                  AtomicInteger foundCount,
                                  AtomicBoolean limitReached) {
        
        for (String key : keys) {
            // Check if limit reached by another thread
            if (limitReached.get()) {
                return;  // Early termination
            }
            
            try {
                JsonNode document = cacheService.jsonGet(region, key, "$");
                
                if (document != null && matchesAllCriteria(document, criteria)) {
                    results.add(buildResultItem(key, document, fields));
                    
                    // Check limit
                    if (foundCount.incrementAndGet() >= limit) {
                        limitReached.set(true);
                        return;
                    }
                }
            } catch (Exception e) {
                // Log but continue - don't fail entire search for one bad document
                log.trace("Error processing key {} in partition: {}", key, e.getMessage());
            }
        }
    }
    
    /**
     * Check if JSON document matches all criteria (AND logic).
     */
    private boolean matchesAllCriteria(JsonNode json, Map<String, Object> criteria) {
        if (criteria == null || criteria.isEmpty()) {
            return true;  // No criteria = match all
        }
        
        for (Map.Entry<String, Object> entry : criteria.entrySet()) {
            String fieldPath = entry.getKey();
            Object criteriaValue = entry.getValue();
            
            JsonNode fieldValue = getJsonField(json, fieldPath);
            
            if (!matchesCriterion(fieldValue, criteriaValue)) {
                return false;  // AND logic - all must match
            }
        }
        return true;
    }
    
    /**
     * Get a field value from JSON, supporting dot notation for nested fields.
     */
    private JsonNode getJsonField(JsonNode json, String fieldPath) {
        if (json == null || fieldPath == null) {
            return null;
        }
        
        // Handle dot notation for nested fields (e.g., "address.city")
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
     * Check if a field value matches a single criterion.
     * Supports: equality, list (IN), regex, and comparison operators.
     */
    @SuppressWarnings("unchecked")
    private boolean matchesCriterion(JsonNode fieldValue, Object criteriaValue) {
        if (fieldValue == null || fieldValue.isNull() || fieldValue.isMissingNode()) {
            return false;
        }
        
        // Case 1: List of values (IN operator)
        if (criteriaValue instanceof List) {
            List<?> valueList = (List<?>) criteriaValue;
            return valueList.stream()
                .anyMatch(v -> valueMatches(fieldValue, v));
        }
        
        // Case 2: Map with operators (regex, comparison)
        if (criteriaValue instanceof Map) {
            Map<String, Object> operators = (Map<String, Object>) criteriaValue;
            return matchesOperators(fieldValue, operators);
        }
        
        // Case 3: Simple equality
        return valueMatches(fieldValue, criteriaValue);
    }
    
    /**
     * Check if field value matches expected value.
     */
    private boolean valueMatches(JsonNode fieldValue, Object expected) {
        if (expected == null) {
            return fieldValue.isNull();
        }
        
        String actualValue = fieldValue.asText();
        String expectedValue = String.valueOf(expected);
        
        // Handle numeric comparison
        if (fieldValue.isNumber() && expected instanceof Number) {
            return fieldValue.asDouble() == ((Number) expected).doubleValue();
        }
        
        // Handle boolean comparison
        if (fieldValue.isBoolean() && expected instanceof Boolean) {
            return fieldValue.asBoolean() == (Boolean) expected;
        }
        
        // String comparison
        return actualValue.equals(expectedValue);
    }
    
    /**
     * Handle comparison operators: gt, gte, lt, lte, eq, ne, regex.
     */
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
                default -> {
                    log.trace("Unknown operator: {}", operator);
                    yield true;  // Ignore unknown operators
                }
            };
            
            if (!matches) {
                return false;  // All operators must match
            }
        }
        return true;
    }
    
    /**
     * Compare numeric values.
     */
    private int compareNumeric(JsonNode fieldValue, Object operand) {
        try {
            double fieldNum = fieldValue.asDouble();
            double operandNum;
            
            if (operand instanceof Number) {
                operandNum = ((Number) operand).doubleValue();
            } else {
                operandNum = Double.parseDouble(String.valueOf(operand));
            }
            
            return Double.compare(fieldNum, operandNum);
        } catch (NumberFormatException e) {
            log.trace("Cannot compare non-numeric values: {} vs {}", fieldValue, operand);
            return 0;
        }
    }
    
    /**
     * Match string against regex pattern with caching.
     */
    private boolean matchesRegex(String value, String patternStr) {
        try {
            // Use cached compiled pattern for performance
            Pattern pattern = regexCache.computeIfAbsent(patternStr, Pattern::compile);
            return pattern.matcher(value).matches();
        } catch (PatternSyntaxException e) {
            log.warn("Invalid regex pattern '{}': {}", patternStr, e.getMessage());
            return false;
        }
    }
    
    /**
     * Build result item with optional field projection.
     */
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
    
    /**
     * Project specific fields from a JSON document.
     */
    private ObjectNode projectFields(JsonNode document, List<String> fields) {
        ObjectNode projected = JsonNodeFactory.instance.objectNode();
        
        for (String field : fields) {
            JsonNode value = getJsonField(document, field);
            if (value != null && !value.isMissingNode()) {
                // Handle dot notation - create nested structure
                if (field.contains(".")) {
                    setNestedField(projected, field, value);
                } else {
                    projected.set(field, value);
                }
            }
        }
        
        return projected;
    }
    
    /**
     * Set a nested field value using dot notation.
     */
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
    
    /**
     * Get search statistics.
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("totalSearches", totalSearches.get());
        stats.put("parallelSearches", parallelSearches.get());
        stats.put("sequentialSearches", sequentialSearches.get());
        stats.put("totalSearchTimeMs", totalSearchTimeMs.get());
        
        long total = totalSearches.get();
        if (total > 0) {
            stats.put("avgSearchTimeMs", totalSearchTimeMs.get() / total);
            stats.put("parallelSearchPercent", 
                String.format("%.1f%%", (parallelSearches.get() * 100.0) / total));
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
        
        log.info("Parallel JSON search service shut down. Final stats: {} total searches, " +
            "{} parallel, {} sequential", 
            totalSearches.get(), parallelSearches.get(), sequentialSearches.get());
    }
}
