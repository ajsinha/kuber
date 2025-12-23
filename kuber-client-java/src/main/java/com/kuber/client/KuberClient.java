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
package com.kuber.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Java client for Kuber Distributed Cache using Redis Protocol.
 * Supports Redis protocol with Kuber extensions for regions and JSON queries.
 * 
 * <p><strong>v1.6.5: API Key Authentication ONLY</strong></p>
 * <p>All programmatic access requires an API key (starts with "kub_").
 * Username/password authentication is only for the Web UI.</p>
 * 
 * <pre>
 * // Create an API key in the Kuber Web UI (Admin → API Keys)
 * // Then use it to authenticate:
 * 
 * try (KuberClient client = new KuberClient("localhost", 6380, "kub_your_api_key_here")) {
 *     client.set("user:1001", "John Doe");
 *     String name = client.get("user:1001");
 * }
 * </pre>
 * 
 * @version 1.6.5
 */
@Slf4j
public class KuberClient implements AutoCloseable {
    
    private static final String DEFAULT_REGION = "default";
    private static final int DEFAULT_PORT = 6380;
    private static final int DEFAULT_TIMEOUT = 30000;
    
    private final String host;
    private final int port;
    private final int timeout;
    private final String apiKey;
    
    private Socket socket;
    private BufferedWriter writer;
    private BufferedReader reader;
    
    @Getter
    private String currentRegion = DEFAULT_REGION;
    
    private final ObjectMapper objectMapper;
    
    /**
     * Create a new Kuber client with API key authentication.
     * 
     * @param host Server hostname
     * @param port Server port (default: 6380)
     * @param apiKey API Key for authentication (must start with "kub_")
     * @throws IOException if connection or authentication fails
     * @throws IllegalArgumentException if API key is null, empty, or invalid format
     */
    public KuberClient(String host, int port, String apiKey) throws IOException {
        this(host, port, apiKey, DEFAULT_TIMEOUT);
    }
    
    /**
     * Create a new Kuber client with API key and custom timeout.
     * 
     * @param host Server hostname
     * @param port Server port
     * @param apiKey API Key for authentication (must start with "kub_")
     * @param timeoutMs Connection timeout in milliseconds
     * @throws IOException if connection or authentication fails
     * @throws IllegalArgumentException if API key is null, empty, or invalid format
     */
    public KuberClient(String host, int port, String apiKey, int timeoutMs) throws IOException {
        if (apiKey == null || apiKey.isEmpty()) {
            throw new IllegalArgumentException("API Key is required for authentication");
        }
        if (!apiKey.startsWith("kub_")) {
            throw new IllegalArgumentException("Invalid API key format - must start with 'kub_'");
        }
        
        this.host = host;
        this.port = port;
        this.timeout = timeoutMs;
        this.apiKey = apiKey;
        
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        
        connect();
        
        // Authenticate with API key
        if (!auth(apiKey)) {
            throw new IOException("Authentication failed - invalid API key");
        }
    }
    
    private void connect() throws IOException {
        log.debug("Connecting to Kuber at {}:{}", host, port);
        
        socket = new Socket(host, port);
        socket.setSoTimeout(timeout);
        
        writer = new BufferedWriter(
                new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        
        log.info("Connected to Kuber at {}:{}", host, port);
    }
    
    /**
     * Authenticate with API key.
     * 
     * @param apiKey API Key (must start with "kub_")
     * @return true if authentication successful
     */
    public boolean auth(String apiKey) throws IOException {
        return "OK".equals(sendCommand("AUTH", apiKey));
    }
    
    /**
     * Ping the server
     */
    public String ping() throws IOException {
        return sendCommand("PING");
    }
    
    // ==================== Region Operations ====================
    
    /**
     * Select a region (creates if not exists)
     */
    public void selectRegion(String region) throws IOException {
        sendCommand("RSELECT", region);
        this.currentRegion = region;
    }
    
    /**
     * List all regions
     */
    public List<String> listRegions() throws IOException {
        return sendCommandForList("REGIONS");
    }
    
    /**
     * Create a new region
     */
    public void createRegion(String name, String description) throws IOException {
        sendCommand("RCREATE", name, description);
    }
    
    /**
     * Delete a region
     */
    public void deleteRegion(String name) throws IOException {
        sendCommand("RDROP", name);
    }
    
    /**
     * Purge all entries in a region
     */
    public void purgeRegion(String name) throws IOException {
        sendCommand("RPURGE", name);
    }
    
    // ==================== Attribute Mapping ====================
    
    /**
     * Set attribute mapping for a region.
     * When JSON data is stored in a region with attribute mapping,
     * the source attribute names are automatically renamed to target names.
     *
     * @param region Region name
     * @param mapping Map of source attribute names to target names
     * @throws IOException if communication error occurs
     * 
     * Example:
     * <pre>
     * Map&lt;String, String&gt; mapping = new HashMap&lt;&gt;();
     * mapping.put("firstName", "first_name");
     * mapping.put("lastName", "last_name");
     * client.setAttributeMapping("users", mapping);
     * </pre>
     */
    public void setAttributeMapping(String region, Map<String, String> mapping) throws IOException {
        String mappingJson = objectMapper.writeValueAsString(mapping);
        sendCommand("RSETMAP", region, mappingJson);
    }
    
    /**
     * Get attribute mapping for a region.
     *
     * @param region Region name
     * @return Map of attribute mappings, or null if no mapping set
     * @throws IOException if communication error occurs
     */
    public Map<String, String> getAttributeMapping(String region) throws IOException {
        String result = sendCommand("RGETMAP", region);
        if (result != null && !result.isEmpty()) {
            return objectMapper.readValue(result, new TypeReference<Map<String, String>>() {});
        }
        return null;
    }
    
    /**
     * Clear attribute mapping for a region.
     *
     * @param region Region name
     * @throws IOException if communication error occurs
     */
    public void clearAttributeMapping(String region) throws IOException {
        sendCommand("RCLEARMAP", region);
    }
    
    // ==================== String Operations ====================
    
    /**
     * Get a value by key
     */
    public String get(String key) throws IOException {
        return sendCommand("GET", key);
    }
    
    /**
     * Set a value
     */
    public void set(String key, String value) throws IOException {
        sendCommand("SET", key, value);
    }
    
    /**
     * Set a value with TTL
     */
    public void set(String key, String value, Duration ttl) throws IOException {
        sendCommand("SET", key, value, "EX", String.valueOf(ttl.getSeconds()));
    }
    
    /**
     * Set if not exists
     */
    public boolean setNx(String key, String value) throws IOException {
        return "1".equals(sendCommand("SETNX", key, value));
    }
    
    /**
     * Set with expiration
     */
    public void setEx(String key, String value, long seconds) throws IOException {
        sendCommand("SETEX", key, String.valueOf(seconds), value);
    }
    
    /**
     * Get multiple values
     */
    public List<String> mget(String... keys) throws IOException {
        String[] args = new String[keys.length + 1];
        args[0] = "MGET";
        System.arraycopy(keys, 0, args, 1, keys.length);
        return sendCommandForList(args);
    }
    
    /**
     * Set multiple values
     */
    public void mset(Map<String, String> entries) throws IOException {
        String[] args = new String[entries.size() * 2 + 1];
        args[0] = "MSET";
        int i = 1;
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            args[i++] = entry.getKey();
            args[i++] = entry.getValue();
        }
        sendCommand(args);
    }
    
    /**
     * Increment a value
     */
    public long incr(String key) throws IOException {
        return Long.parseLong(sendCommand("INCR", key));
    }
    
    /**
     * Increment by amount
     */
    public long incrBy(String key, long amount) throws IOException {
        return Long.parseLong(sendCommand("INCRBY", key, String.valueOf(amount)));
    }
    
    /**
     * Decrement a value
     */
    public long decr(String key) throws IOException {
        return Long.parseLong(sendCommand("DECR", key));
    }
    
    /**
     * Decrement by amount
     */
    public long decrBy(String key, long amount) throws IOException {
        return Long.parseLong(sendCommand("DECRBY", key, String.valueOf(amount)));
    }
    
    /**
     * Append to a value
     */
    public int append(String key, String value) throws IOException {
        return Integer.parseInt(sendCommand("APPEND", key, value));
    }
    
    /**
     * Get string length
     */
    public int strlen(String key) throws IOException {
        return Integer.parseInt(sendCommand("STRLEN", key));
    }
    
    // ==================== Key Operations ====================
    
    /**
     * Delete keys
     */
    public long del(String... keys) throws IOException {
        String[] args = new String[keys.length + 1];
        args[0] = "DEL";
        System.arraycopy(keys, 0, args, 1, keys.length);
        return Long.parseLong(sendCommand(args));
    }
    
    /**
     * Check if key exists
     */
    public boolean exists(String key) throws IOException {
        return "1".equals(sendCommand("EXISTS", key));
    }
    
    /**
     * Set expiration on a key
     */
    public boolean expire(String key, long seconds) throws IOException {
        return "1".equals(sendCommand("EXPIRE", key, String.valueOf(seconds)));
    }
    
    /**
     * Get TTL of a key
     */
    public long ttl(String key) throws IOException {
        return Long.parseLong(sendCommand("TTL", key));
    }
    
    /**
     * Remove expiration from a key
     */
    public boolean persist(String key) throws IOException {
        return "1".equals(sendCommand("PERSIST", key));
    }
    
    /**
     * Get the type of a key
     */
    public String type(String key) throws IOException {
        return sendCommand("TYPE", key);
    }
    
    /**
     * Find keys matching pattern
     */
    public List<String> keys(String pattern) throws IOException {
        return sendCommandForList("KEYS", pattern);
    }
    
    /**
     * Search keys by regex pattern and return key-value pairs.
     * Unlike keys() which uses glob patterns, this uses Java regex patterns.
     * 
     * @param regexPattern Java regex pattern (e.g., "user:\\d+", "order:.*")
     * @return List of results with key, value, type, ttl
     */
    public List<Map<String, Object>> ksearch(String regexPattern) throws IOException {
        return ksearch(regexPattern, 1000);
    }
    
    /**
     * Search keys by regex pattern with limit.
     * 
     * @param regexPattern Java regex pattern
     * @param limit Maximum number of results
     * @return List of results with key, value, type, ttl
     */
    public List<Map<String, Object>> ksearch(String regexPattern, int limit) throws IOException {
        List<String> results = sendCommandForList("KSEARCH", regexPattern, "LIMIT", String.valueOf(limit));
        List<Map<String, Object>> parsed = new ArrayList<>();
        
        for (String result : results) {
            try {
                JsonNode node = objectMapper.readTree(result);
                Map<String, Object> item = new HashMap<>();
                if (node.has("key")) item.put("key", node.get("key").asText());
                if (node.has("value")) item.put("value", node.get("value"));
                if (node.has("type")) item.put("type", node.get("type").asText());
                if (node.has("ttl")) item.put("ttl", node.get("ttl").asLong());
                parsed.add(item);
            } catch (Exception e) {
                // Skip invalid JSON
            }
        }
        return parsed;
    }
    
    /**
     * Rename a key
     */
    public void rename(String oldKey, String newKey) throws IOException {
        sendCommand("RENAME", oldKey, newKey);
    }
    
    // ==================== Hash Operations ====================
    
    /**
     * Get a hash field
     */
    public String hget(String key, String field) throws IOException {
        return sendCommand("HGET", key, field);
    }
    
    /**
     * Set a hash field
     */
    public void hset(String key, String field, String value) throws IOException {
        sendCommand("HSET", key, field, value);
    }
    
    /**
     * Set multiple hash fields
     */
    public void hmset(String key, Map<String, String> fields) throws IOException {
        String[] args = new String[fields.size() * 2 + 2];
        args[0] = "HMSET";
        args[1] = key;
        int i = 2;
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            args[i++] = entry.getKey();
            args[i++] = entry.getValue();
        }
        sendCommand(args);
    }
    
    /**
     * Get multiple hash fields
     */
    public List<String> hmget(String key, String... fields) throws IOException {
        String[] args = new String[fields.length + 2];
        args[0] = "HMGET";
        args[1] = key;
        System.arraycopy(fields, 0, args, 2, fields.length);
        return sendCommandForList(args);
    }
    
    /**
     * Get all hash fields and values
     */
    public Map<String, String> hgetall(String key) throws IOException {
        List<String> list = sendCommandForList("HGETALL", key);
        Map<String, String> result = new LinkedHashMap<>();
        for (int i = 0; i < list.size(); i += 2) {
            result.put(list.get(i), list.get(i + 1));
        }
        return result;
    }
    
    /**
     * Delete hash fields
     */
    public boolean hdel(String key, String... fields) throws IOException {
        String[] args = new String[fields.length + 2];
        args[0] = "HDEL";
        args[1] = key;
        System.arraycopy(fields, 0, args, 2, fields.length);
        return "1".equals(sendCommand(args));
    }
    
    /**
     * Check if hash field exists
     */
    public boolean hexists(String key, String field) throws IOException {
        return "1".equals(sendCommand("HEXISTS", key, field));
    }
    
    /**
     * Get all hash fields
     */
    public List<String> hkeys(String key) throws IOException {
        return sendCommandForList("HKEYS", key);
    }
    
    /**
     * Get all hash values
     */
    public List<String> hvals(String key) throws IOException {
        return sendCommandForList("HVALS", key);
    }
    
    /**
     * Get hash length
     */
    public int hlen(String key) throws IOException {
        return Integer.parseInt(sendCommand("HLEN", key));
    }
    
    // ==================== JSON Operations ====================
    
    /**
     * Set a JSON value
     */
    public void jsonSet(String key, String json) throws IOException {
        sendCommand("JSET", key, json);
    }
    
    /**
     * Set a JSON value with TTL
     */
    public void jsonSet(String key, String json, Duration ttl) throws IOException {
        sendCommand("JSET", key, json, "$", String.valueOf(ttl.getSeconds()));
    }
    
    /**
     * Set a JSON value from object
     */
    public void jsonSet(String key, Object value) throws IOException {
        String json = objectMapper.writeValueAsString(value);
        jsonSet(key, json);
    }
    
    /**
     * Update/merge a JSON value (upsert with deep merge).
     * 
     * Behavior:
     * - If key doesn't exist: creates new entry with the JSON value
     * - If key exists with JSON object: deep merges new JSON onto existing (new values override)
     * - If key exists but not valid JSON: replaces with new JSON value
     * 
     * @param key the cache key
     * @param json the JSON string to merge
     * @return the resulting merged JSON
     */
    public JsonNode jsonUpdate(String key, String json) throws IOException {
        String result = sendCommand("JUPDATE", key, json);
        if (result == null || result.isEmpty()) {
            return null;
        }
        return objectMapper.readTree(result);
    }
    
    /**
     * Update/merge a JSON value with TTL.
     * 
     * @param key the cache key
     * @param json the JSON string to merge
     * @param ttl time-to-live for the entry
     * @return the resulting merged JSON
     */
    public JsonNode jsonUpdate(String key, String json, Duration ttl) throws IOException {
        String result = sendCommand("JUPDATE", key, json, String.valueOf(ttl.getSeconds()));
        if (result == null || result.isEmpty()) {
            return null;
        }
        return objectMapper.readTree(result);
    }
    
    /**
     * Update/merge a JSON value from object.
     * 
     * @param key the cache key
     * @param value the object to serialize and merge
     * @return the resulting merged JSON
     */
    public JsonNode jsonUpdate(String key, Object value) throws IOException {
        String json = objectMapper.writeValueAsString(value);
        return jsonUpdate(key, json);
    }
    
    /**
     * Remove specified attributes from a JSON value.
     * 
     * Behavior:
     * - If key exists and has valid JSON object: removes specified attributes and saves
     * - If key doesn't exist or value is not JSON object: returns null (nothing done)
     * 
     * @param key the cache key
     * @param attributes list of attribute names to remove
     * @return the updated JSON, or null if nothing was done
     */
    public JsonNode jsonRemove(String key, List<String> attributes) throws IOException {
        String attributesJson = objectMapper.writeValueAsString(attributes);
        String result = sendCommand("JREMOVE", key, attributesJson);
        if (result == null || result.isEmpty() || "0".equals(result)) {
            return null;
        }
        return objectMapper.readTree(result);
    }
    
    /**
     * Remove specified attributes from a JSON value.
     * 
     * @param key the cache key
     * @param attributes attribute names to remove (varargs)
     * @return the updated JSON, or null if nothing was done
     */
    public JsonNode jsonRemove(String key, String... attributes) throws IOException {
        return jsonRemove(key, Arrays.asList(attributes));
    }
    
    /**
     * Get a JSON value
     */
    public JsonNode jsonGet(String key) throws IOException {
        return jsonGet(key, "$");
    }
    
    /**
     * Get a JSON value at path
     */
    public JsonNode jsonGet(String key, String path) throws IOException {
        String result = sendCommand("JGET", key, path);
        if (result == null || result.isEmpty()) {
            return null;
        }
        return objectMapper.readTree(result);
    }
    
    /**
     * Get JSON as typed object
     */
    public <T> T jsonGet(String key, Class<T> type) throws IOException {
        JsonNode json = jsonGet(key);
        if (json == null) {
            return null;
        }
        return objectMapper.treeToValue(json, type);
    }
    
    /**
     * Delete JSON path
     */
    public boolean jsonDelete(String key, String path) throws IOException {
        return "1".equals(sendCommand("JDEL", key, path));
    }
    
    /**
     * Search JSON documents by query string.
     * 
     * Query syntax:
     * - Single value: field=value
     * - IN clause: field=[value1|value2|value3]
     * - Multiple conditions: field1=value1,field2=value2
     * - Operators: =, !=, >, <, >=, <=, ~= (regex)
     * 
     * Examples:
     * - "status=active" - match status equals "active"
     * - "status=[active|pending]" - match status IN ("active", "pending")
     * - "status=[active|pending],country=[USA|UK]" - IN clause on multiple attributes
     * - "age>=18,age<=65" - numeric range
     * 
     * @param query Query string with conditions
     * @return List of matching JSON documents
     * @throws IOException If search fails
     * @since 1.7.7 - Added IN clause support with [value1|value2|...] syntax
     */
    public List<JsonNode> jsonSearch(String query) throws IOException {
        List<String> results = sendCommandForList("JSEARCH", query);
        List<JsonNode> nodes = new ArrayList<>();
        for (String result : results) {
            // Result format: "key:json"
            // Key might contain colons (e.g., "prod:1001"), so find where JSON starts
            // JSON always starts with { or [
            int jsonStart = -1;
            for (int i = 0; i < result.length() - 1; i++) {
                if (result.charAt(i) == ':' && (result.charAt(i + 1) == '{' || result.charAt(i + 1) == '[')) {
                    jsonStart = i;
                    break;
                }
            }
            if (jsonStart > 0) {
                String json = result.substring(jsonStart + 1);
                nodes.add(objectMapper.readTree(json));
            }
        }
        return nodes;
    }
    
    /**
     * Search JSON documents with IN clause support.
     * 
     * Convenience method for building queries with multiple attribute conditions,
     * each potentially matching multiple values (IN clause).
     * 
     * Example:
     * <pre>
     * Map&lt;String, List&lt;String&gt;&gt; conditions = new LinkedHashMap&lt;&gt;();
     * conditions.put("status", Arrays.asList("active", "pending"));
     * conditions.put("country", Arrays.asList("USA", "UK", "CA"));
     * List&lt;JsonNode&gt; results = client.jsonSearchIn(conditions);
     * </pre>
     * 
     * This generates query: "status=[active|pending],country=[USA|UK|CA]"
     * 
     * @param conditions Map of field name to list of acceptable values
     * @return List of matching JSON documents
     * @throws IOException If search fails
     * @since 1.7.7
     */
    public List<JsonNode> jsonSearchIn(Map<String, List<String>> conditions) throws IOException {
        return jsonSearch(buildInClauseQuery(conditions));
    }
    
    /**
     * Build a query string with IN clause syntax from conditions map.
     * 
     * @param conditions Map of field name to list of acceptable values
     * @return Query string in format: field1=[v1|v2],field2=[v3|v4]
     * @since 1.7.7
     */
    public static String buildInClauseQuery(Map<String, List<String>> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return "";
        }
        
        StringBuilder query = new StringBuilder();
        boolean first = true;
        
        for (Map.Entry<String, List<String>> entry : conditions.entrySet()) {
            String field = entry.getKey();
            List<String> values = entry.getValue();
            
            if (field == null || values == null || values.isEmpty()) {
                continue;
            }
            
            if (!first) {
                query.append(",");
            }
            first = false;
            
            query.append(field).append("=");
            
            if (values.size() == 1) {
                query.append(values.get(0));
            } else {
                query.append("[");
                for (int i = 0; i < values.size(); i++) {
                    if (i > 0) query.append("|");
                    query.append(values.get(i));
                }
                query.append("]");
            }
        }
        
        return query.toString();
    }
    
    // ==================== Generic Search (Convenience Methods) ====================
    
    /**
     * Perform generic search with flexible options.
     * 
     * @param request The search request containing search parameters
     * @return List of search results with key and value
     * @throws IOException If search fails
     */
    public List<Map<String, Object>> genericSearch(GenericSearchRequest request) throws IOException {
        List<Map<String, Object>> results = new ArrayList<>();
        
        if (request.getKey() != null) {
            // Mode 1: Simple key lookup
            String value = get(request.getKey());
            if (value != null) {
                Map<String, Object> item = new HashMap<>();
                item.put("key", request.getKey());
                try {
                    JsonNode jsonValue = objectMapper.readTree(value);
                    if (request.getFields() != null && !request.getFields().isEmpty()) {
                        item.put("value", projectFields(jsonValue, request.getFields()));
                    } else {
                        item.put("value", jsonValue);
                    }
                } catch (Exception e) {
                    item.put("value", value);
                }
                results.add(item);
            }
        } else if (request.getKeyPattern() != null) {
            // Mode 2: Key pattern (regex) search
            // ksearch returns List<Map<String, Object>> with key, value, type, ttl
            int limit = request.getLimit() != null ? request.getLimit() : 1000;
            List<Map<String, Object>> searchResults = ksearch(request.getKeyPattern(), limit);
            for (Map<String, Object> searchItem : searchResults) {
                String key = (String) searchItem.get("key");
                Object value = searchItem.get("value");
                if (key != null && value != null) {
                    Map<String, Object> item = new HashMap<>();
                    item.put("key", key);
                    try {
                        JsonNode jsonValue;
                        if (value instanceof JsonNode) {
                            jsonValue = (JsonNode) value;
                        } else if (value instanceof String) {
                            jsonValue = objectMapper.readTree((String) value);
                        } else {
                            jsonValue = objectMapper.valueToTree(value);
                        }
                        if (request.getFields() != null && !request.getFields().isEmpty()) {
                            item.put("value", projectFields(jsonValue, request.getFields()));
                        } else {
                            item.put("value", jsonValue);
                        }
                    } catch (Exception e) {
                        item.put("value", value);
                    }
                    results.add(item);
                }
            }
        } else if ("json".equalsIgnoreCase(request.getType()) && request.getValues() != null) {
            // Mode 3: JSON attribute search
            StringBuilder query = new StringBuilder();
            for (Map<String, Object> condition : request.getValues()) {
                String condType = "equals";
                if (condition.containsKey("type")) {
                    condType = String.valueOf(condition.get("type"));
                }
                for (Map.Entry<String, Object> entry : condition.entrySet()) {
                    if ("type".equals(entry.getKey())) continue;
                    if (query.length() > 0) query.append(",");
                    if (entry.getValue() instanceof List) {
                        // IN operator
                        List<?> values = (List<?>) entry.getValue();
                        for (Object v : values) {
                            query.append("$.").append(entry.getKey()).append("=").append(v);
                        }
                    } else if ("regex".equalsIgnoreCase(condType)) {
                        query.append("$.").append(entry.getKey()).append(" LIKE ").append(entry.getValue());
                    } else {
                        query.append("$.").append(entry.getKey()).append("=").append(entry.getValue());
                    }
                }
            }
            List<JsonNode> searchResults = jsonSearch(query.toString());
            int limit = request.getLimit() != null ? request.getLimit() : 1000;
            for (JsonNode node : searchResults) {
                if (results.size() >= limit) break;
                Map<String, Object> item = new HashMap<>();
                if (node.has("key")) {
                    item.put("key", node.get("key").asText());
                }
                if (node.has("value")) {
                    JsonNode value = node.get("value");
                    if (request.getFields() != null && !request.getFields().isEmpty()) {
                        item.put("value", projectFields(value, request.getFields()));
                    } else {
                        item.put("value", value);
                    }
                } else {
                    if (request.getFields() != null && !request.getFields().isEmpty()) {
                        item.put("value", projectFields(node, request.getFields()));
                    } else {
                        item.put("value", node);
                    }
                }
                results.add(item);
            }
        }
        
        return results;
    }
    
    /**
     * Perform a simple key lookup.
     */
    public List<Map<String, Object>> genericSearchByKey(String key) throws IOException {
        return genericSearchByKey(key, null);
    }
    
    /**
     * Perform a simple key lookup with optional field projection.
     */
    public List<Map<String, Object>> genericSearchByKey(String key, List<String> fields) throws IOException {
        GenericSearchRequest request = new GenericSearchRequest();
        request.setKey(key);
        request.setFields(fields);
        return genericSearch(request);
    }
    
    /**
     * Perform a key pattern (regex) search.
     */
    public List<Map<String, Object>> genericSearchByPattern(String keyPattern) throws IOException {
        return genericSearchByPattern(keyPattern, null, null);
    }
    
    /**
     * Perform a key pattern (regex) search with optional parameters.
     */
    public List<Map<String, Object>> genericSearchByPattern(String keyPattern, List<String> fields, Integer limit) 
            throws IOException {
        GenericSearchRequest request = new GenericSearchRequest();
        request.setKeyPattern(keyPattern);
        request.setFields(fields);
        request.setLimit(limit);
        return genericSearch(request);
    }
    
    /**
     * Perform a JSON attribute search.
     */
    public List<Map<String, Object>> genericSearchByJson(List<Map<String, Object>> values) throws IOException {
        return genericSearchByJson(values, null, null);
    }
    
    /**
     * Perform a JSON attribute search with optional parameters.
     */
    public List<Map<String, Object>> genericSearchByJson(List<Map<String, Object>> values, 
                                                          List<String> fields, Integer limit) throws IOException {
        GenericSearchRequest request = new GenericSearchRequest();
        request.setType("json");
        request.setValues(values);
        request.setFields(fields);
        request.setLimit(limit);
        return genericSearch(request);
    }
    
    /**
     * Project specified fields from a JsonNode.
     */
    private Map<String, Object> projectFields(JsonNode json, List<String> fields) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (String fieldPath : fields) {
            JsonNode fieldValue = getJsonField(json, fieldPath);
            if (fieldValue != null && !fieldValue.isNull() && !fieldValue.isMissingNode()) {
                setNestedField(result, fieldPath, jsonNodeToObject(fieldValue));
            }
        }
        return result;
    }
    
    /**
     * Get a field value from JSON, supporting nested paths.
     */
    private JsonNode getJsonField(JsonNode json, String fieldPath) {
        if (fieldPath.contains(".")) {
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
        return json.get(fieldPath);
    }
    
    /**
     * Set a nested field value in a result map.
     */
    @SuppressWarnings("unchecked")
    private void setNestedField(Map<String, Object> result, String fieldPath, Object value) {
        if (fieldPath.contains(".")) {
            String[] parts = fieldPath.split("\\.", 2);
            Map<String, Object> nested = (Map<String, Object>) result.computeIfAbsent(
                parts[0], k -> new LinkedHashMap<>());
            setNestedField(nested, parts[1], value);
        } else {
            result.put(fieldPath, value);
        }
    }
    
    /**
     * Convert JsonNode to appropriate Java object.
     */
    private Object jsonNodeToObject(JsonNode node) {
        if (node.isTextual()) return node.asText();
        if (node.isInt()) return node.asInt();
        if (node.isLong()) return node.asLong();
        if (node.isDouble()) return node.asDouble();
        if (node.isBoolean()) return node.asBoolean();
        if (node.isArray()) {
            List<Object> list = new ArrayList<>();
            node.forEach(item -> list.add(jsonNodeToObject(item)));
            return list;
        }
        if (node.isObject()) {
            Map<String, Object> map = new LinkedHashMap<>();
            node.fields().forEachRemaining(e -> map.put(e.getKey(), jsonNodeToObject(e.getValue())));
            return map;
        }
        return node.asText();
    }
    
    /**
     * Request DTO for generic search API.
     */
    public static class GenericSearchRequest {
        private String key;
        private String keyPattern;
        private String type;
        private List<Map<String, Object>> values;
        private List<String> fields;
        private Integer limit;
        
        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }
        
        public String getKeyPattern() { return keyPattern; }
        public void setKeyPattern(String keyPattern) { this.keyPattern = keyPattern; }
        
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        
        public List<Map<String, Object>> getValues() { return values; }
        public void setValues(List<Map<String, Object>> values) { this.values = values; }
        
        public List<String> getFields() { return fields; }
        public void setFields(List<String> fields) { this.fields = fields; }
        
        public Integer getLimit() { return limit; }
        public void setLimit(Integer limit) { this.limit = limit; }
    }
    
    // ==================== Server Operations ====================
    
    /**
     * Get server info
     */
    public String info() throws IOException {
        return sendCommand("INFO");
    }
    
    /**
     * Get database size
     */
    public long dbSize() throws IOException {
        return Long.parseLong(sendCommand("DBSIZE"));
    }
    
    /**
     * Flush current region
     */
    public void flushDb() throws IOException {
        sendCommand("FLUSHDB");
    }
    
    /**
     * Get server status
     */
    public String status() throws IOException {
        return sendCommand("STATUS");
    }
    
    /**
     * Get replication info
     */
    public String replInfo() throws IOException {
        return sendCommand("REPLINFO");
    }
    
    // ==================== Command Execution ====================
    
    private synchronized String sendCommand(String... args) throws IOException {
        StringBuilder cmd = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            if (i > 0) cmd.append(' ');
            String arg = args[i];
            if (arg.contains(" ") || arg.contains("\"") || arg.contains("\n")) {
                cmd.append('"').append(arg.replace("\\", "\\\\")
                        .replace("\"", "\\\"")
                        .replace("\n", "\\n")
                        .replace("\t", "\\t")).append('"');
            } else {
                cmd.append(arg);
            }
        }
        
        log.trace("Sending: {}", cmd);
        writer.write(cmd.toString());
        writer.newLine();
        writer.flush();
        
        return readResponse();
    }
    
    private List<String> sendCommandForList(String... args) throws IOException {
        String response = sendCommand(args);
        if (response == null || response.isEmpty()) {
            return new ArrayList<>();
        }
        // Parse array response
        return parseArrayResponse(response);
    }
    
    private String readResponse() throws IOException {
        String line = reader.readLine();
        if (line == null) {
            throw new IOException("Connection closed by server");
        }
        
        log.trace("Received: {}", line);
        
        if (line.startsWith("+")) {
            return line.substring(1);
        } else if (line.startsWith("-")) {
            throw new KuberException(line.substring(1));
        } else if (line.startsWith(":")) {
            return line.substring(1);
        } else if (line.startsWith("$")) {
            int len = Integer.parseInt(line.substring(1));
            if (len == -1) {
                return null;
            }
            char[] buf = new char[len];
            int read = 0;
            while (read < len) {
                int n = reader.read(buf, read, len - read);
                if (n == -1) throw new IOException("Unexpected EOF");
                read += n;
            }
            reader.readLine(); // consume CRLF
            return new String(buf);
        } else if (line.startsWith("*")) {
            int count = Integer.parseInt(line.substring(1));
            if (count == -1) {
                return null;
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < count; i++) {
                if (i > 0) sb.append('\n');
                sb.append(readResponse());
            }
            return sb.toString();
        }
        
        return line;
    }
    
    private List<String> parseArrayResponse(String response) {
        if (response == null || response.isEmpty()) {
            return new ArrayList<>();
        }
        return Arrays.asList(response.split("\n"));
    }
    
    @Override
    public void close() throws IOException {
        log.debug("Closing connection to Kuber");
        
        try {
            if (writer != null) {
                sendCommand("QUIT");
            }
        } catch (Exception e) {
            // Ignore
        }
        
        if (writer != null) writer.close();
        if (reader != null) reader.close();
        if (socket != null) socket.close();
        
        log.info("Connection closed");
    }
    
    /**
     * Kuber exception for server errors
     */
    public static class KuberException extends RuntimeException {
        public KuberException(String message) {
            super(message);
        }
    }
}
