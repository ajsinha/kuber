/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * Standalone REST API client for Kuber Distributed Cache.
 * 
 * <p><strong>v2.1.0: API Key Authentication ONLY</strong></p>
 * <p>All programmatic access requires an API key (starts with "kub_").
 * Username/password authentication is only for the Web UI.</p>
 * 
 * <pre>
 * // Create an API key in the Kuber Web UI (Admin → API Keys)
 * // Then use it to authenticate:
 * 
 * KuberRestClient client = new KuberRestClient("localhost", 8080, "kub_your_api_key_here");
 * 
 * // Basic operations
 * client.set("key", "value");
 * String value = client.get("key");
 * 
 * // JSON operations with specific region
 * client.jsonSet("user:1", userObject, "users");
 * JsonNode user = client.jsonGet("user:1", "users");
 * 
 * // JSON search
 * List&lt;JsonNode&gt; results = client.jsonSearch("$.age>30", "users");
 * 
 * client.close();
 * </pre>
 * 
 * @version 2.6.3
 */
@Slf4j
public class KuberRestClient implements AutoCloseable {
    
    private static final int DEFAULT_PORT = 8080;
    private static final int DEFAULT_TIMEOUT = 30000;
    
    private final String baseUrl;
    private final int timeout;
    private final String apiKey;
    private final ObjectMapper objectMapper;
    
    @Getter
    private String currentRegion = "default";
    
    /**
     * Create a new REST client with API Key authentication.
     * 
     * @param host Server hostname
     * @param port HTTP port (default: 8080)
     * @param apiKey API Key (must start with "kub_")
     * @throws IllegalArgumentException if API key is null, empty, or invalid format
     */
    public KuberRestClient(String host, int port, String apiKey) {
        this(host, port, apiKey, false, DEFAULT_TIMEOUT);
    }
    
    /**
     * Create a new REST client with API Key and SSL option.
     * 
     * @param host Server hostname
     * @param port HTTP port
     * @param apiKey API Key (must start with "kub_")
     * @param useSsl Use HTTPS if true
     * @throws IllegalArgumentException if API key is null, empty, or invalid format
     */
    public KuberRestClient(String host, int port, String apiKey, boolean useSsl) {
        this(host, port, apiKey, useSsl, DEFAULT_TIMEOUT);
    }
    
    /**
     * Create a new REST client with full configuration.
     * 
     * @param host Server hostname
     * @param port HTTP port
     * @param apiKey API Key (must start with "kub_")
     * @param useSsl Use HTTPS if true
     * @param timeoutMs Request timeout in milliseconds
     * @throws IllegalArgumentException if API key is null, empty, or invalid format
     */
    public KuberRestClient(String host, int port, String apiKey, boolean useSsl, int timeoutMs) {
        if (apiKey == null || apiKey.isEmpty()) {
            throw new IllegalArgumentException("API Key is required for authentication");
        }
        if (!apiKey.startsWith("kub_")) {
            throw new IllegalArgumentException("Invalid API key format - must start with 'kub_'");
        }
        
        String scheme = useSsl ? "https" : "http";
        this.baseUrl = String.format("%s://%s:%d", scheme, host, port);
        this.timeout = timeoutMs;
        this.apiKey = apiKey;
        this.objectMapper = new ObjectMapper();
        
        log.info("Kuber REST client initialized with API Key: {}", baseUrl);
    }
    
    // ==================== HTTP Methods ====================
    
    /**
     * URL-encode a path component (region name, key, field, etc.).
     * Encodes all special characters including '/' so that cache keys
     * containing slashes (e.g. "employee/EMP001") are transmitted as
     * "employee%2FEMP001" — a single path segment, not two.
     */
    private static String encodePath(String value) {
        try {
            return java.net.URLEncoder.encode(value, "UTF-8").replace("+", "%20");
        } catch (java.io.UnsupportedEncodingException e) {
            return value;
        }
    }
    
    private JsonNode request(String method, String path) throws IOException {
        return request(method, path, null, null);
    }
    
    private JsonNode request(String method, String path, Object body) throws IOException {
        return request(method, path, body, null);
    }
    
    private JsonNode request(String method, String path, Object body, 
                             Map<String, String> queryParams) throws IOException {
        StringBuilder urlBuilder = new StringBuilder(baseUrl).append(path);
        
        if (queryParams != null && !queryParams.isEmpty()) {
            urlBuilder.append("?");
            boolean first = true;
            for (Map.Entry<String, String> entry : queryParams.entrySet()) {
                if (!first) urlBuilder.append("&");
                urlBuilder.append(java.net.URLEncoder.encode(entry.getKey(), "UTF-8"))
                         .append("=")
                         .append(java.net.URLEncoder.encode(entry.getValue(), "UTF-8"));
                first = false;
            }
        }
        
        URL url = new URL(urlBuilder.toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        
        try {
            conn.setRequestMethod(method);
            conn.setConnectTimeout(timeout);
            conn.setReadTimeout(timeout);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");
            
            // API Key authentication
            conn.setRequestProperty("X-API-Key", apiKey);
            
            if (body != null) {
                conn.setDoOutput(true);
                try (OutputStream os = conn.getOutputStream()) {
                    byte[] data = objectMapper.writeValueAsBytes(body);
                    os.write(data);
                }
            }
            
            int responseCode = conn.getResponseCode();
            
            InputStream inputStream;
            if (responseCode >= 200 && responseCode < 300) {
                inputStream = conn.getInputStream();
            } else {
                inputStream = conn.getErrorStream();
                if (inputStream != null) {
                    String error = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                    throw new KuberRestException("HTTP " + responseCode + ": " + error);
                }
                throw new KuberRestException("HTTP " + responseCode);
            }
            
            if (inputStream != null) {
                byte[] responseBytes = inputStream.readAllBytes();
                if (responseBytes.length > 0) {
                    return objectMapper.readTree(responseBytes);
                }
            }
            return null;
            
        } finally {
            conn.disconnect();
        }
    }
    
    // ==================== Server Operations ====================
    
    /**
     * Ping the server
     */
    public boolean ping() {
        try {
            JsonNode result = request("GET", "/api/v1/ping");
            return result != null && "OK".equals(result.path("status").asText());
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Get server information
     */
    public JsonNode info() throws IOException {
        return request("GET", "/api/v1/info");
    }
    
    /**
     * Get server status
     */
    public JsonNode status() throws IOException {
        return request("GET", "/api/v1/status");
    }
    
    /**
     * Get server statistics
     */
    public JsonNode stats() throws IOException {
        return request("GET", "/api/v1/stats");
    }
    
    // ==================== Region Operations ====================
    
    /**
     * Select a region for subsequent operations
     */
    public void selectRegion(String region) {
        this.currentRegion = region;
    }
    
    /**
     * List all regions
     */
    public List<Map<String, Object>> listRegions() throws IOException {
        JsonNode result = request("GET", "/api/v1/regions");
        if (result != null && result.isArray()) {
            return objectMapper.convertValue(result, new TypeReference<List<Map<String, Object>>>() {});
        }
        return new ArrayList<>();
    }
    
    /**
     * Get region information
     */
    public JsonNode getRegion(String name) throws IOException {
        return request("GET", "/api/v1/regions/" + encodePath(name));
    }
    
    /**
     * Create a new region
     */
    public void createRegion(String name, String description) throws IOException {
        Map<String, String> body = new HashMap<>();
        body.put("name", name);
        body.put("description", description);
        request("POST", "/api/v1/regions", body);
    }
    
    /**
     * Delete a region
     */
    public void deleteRegion(String name) throws IOException {
        request("DELETE", "/api/v1/regions/" + encodePath(name));
    }
    
    /**
     * Purge all entries in a region
     */
    public void purgeRegion(String name) throws IOException {
        request("POST", "/api/v1/regions/" + name + "/purge");
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
        request("PUT", "/api/v1/regions/" + encodePath(region) + "/attributemapping", mapping);
    }
    
    /**
     * Get attribute mapping for a region.
     *
     * @param region Region name
     * @return Map of attribute mappings, or null if no mapping set
     * @throws IOException if communication error occurs
     */
    public Map<String, String> getAttributeMapping(String region) throws IOException {
        try {
            JsonNode result = request("GET", "/api/v1/regions/" + encodePath(region) + "/attributemapping");
            if (result != null && !result.isNull()) {
                return objectMapper.convertValue(result, new TypeReference<Map<String, String>>() {});
            }
            return null;
        } catch (KuberRestException e) {
            return null;
        }
    }
    
    /**
     * Clear attribute mapping for a region.
     *
     * @param region Region name
     * @throws IOException if communication error occurs
     */
    public void clearAttributeMapping(String region) throws IOException {
        request("DELETE", "/api/v1/regions/" + encodePath(region) + "/attributemapping");
    }
    
    // ==================== Cache Operations ====================
    
    /**
     * Get a value
     */
    public String get(String key) throws IOException {
        return get(key, currentRegion);
    }
    
    /**
     * Get a value from specific region
     */
    public String get(String key, String region) throws IOException {
        try {
            JsonNode result = request("GET", "/api/v1/cache/" + encodePath(region) + "/" + encodePath(key));
            if (result != null && result.has("value")) {
                return result.get("value").asText();
            }
            return null;
        } catch (KuberRestException e) {
            return null;
        }
    }
    
    /**
     * Set a value
     */
    public void set(String key, String value) throws IOException {
        set(key, value, currentRegion, null);
    }
    
    /**
     * Set a value with TTL
     */
    public void set(String key, String value, Duration ttl) throws IOException {
        set(key, value, currentRegion, ttl);
    }
    
    /**
     * Set a value in specific region with optional TTL
     */
    public void set(String key, String value, String region, Duration ttl) throws IOException {
        Map<String, Object> body = new HashMap<>();
        body.put("value", value);
        if (ttl != null) {
            body.put("ttl", ttl.getSeconds());
        }
        request("PUT", "/api/v1/cache/" + encodePath(region) + "/" + encodePath(key), body);
    }
    
    /**
     * Delete a key
     */
    public boolean delete(String key) throws IOException {
        return delete(key, currentRegion);
    }
    
    /**
     * Delete a key from specific region
     */
    public boolean delete(String key, String region) throws IOException {
        try {
            request("DELETE", "/api/v1/cache/" + encodePath(region) + "/" + encodePath(key));
            return true;
        } catch (KuberRestException e) {
            return false;
        }
    }
    
    /**
     * Check if key exists
     */
    public boolean exists(String key) throws IOException {
        return exists(key, currentRegion);
    }
    
    /**
     * Check if key exists in specific region
     */
    public boolean exists(String key, String region) throws IOException {
        try {
            JsonNode result = request("GET", "/api/v1/cache/" + encodePath(region) + "/" + encodePath(key) + "/exists");
            return result != null && result.path("exists").asBoolean(false);
        } catch (KuberRestException e) {
            return false;
        }
    }
    
    /**
     * Get TTL of a key
     */
    public long ttl(String key) throws IOException {
        return ttl(key, currentRegion);
    }
    
    /**
     * Get TTL of a key in specific region
     */
    public long ttl(String key, String region) throws IOException {
        JsonNode result = request("GET", "/api/v1/cache/" + encodePath(region) + "/" + encodePath(key) + "/ttl");
        return result != null ? result.path("ttl").asLong(-2) : -2;
    }
    
    /**
     * Set expiration on a key
     */
    public void expire(String key, long seconds) throws IOException {
        expire(key, seconds, currentRegion);
    }
    
    /**
     * Set expiration on a key in specific region
     */
    public void expire(String key, long seconds, String region) throws IOException {
        Map<String, Object> body = new HashMap<>();
        body.put("seconds", seconds);
        request("POST", "/api/v1/cache/" + encodePath(region) + "/" + encodePath(key) + "/expire", body);
    }
    
    /**
     * Get multiple values
     */
    public List<String> mget(String... keys) throws IOException {
        return mget(Arrays.asList(keys), currentRegion);
    }
    
    /**
     * Get multiple values from specific region
     */
    public List<String> mget(List<String> keys, String region) throws IOException {
        Map<String, Object> body = new HashMap<>();
        body.put("keys", keys);
        JsonNode result = request("POST", "/api/v1/cache/" + encodePath(region) + "/mget", body);
        
        List<String> values = new ArrayList<>();
        if (result != null && result.has("values")) {
            for (JsonNode node : result.get("values")) {
                values.add(node.isNull() ? null : node.asText());
            }
        }
        return values;
    }
    
    /**
     * Set multiple values
     */
    public void mset(Map<String, String> entries) throws IOException {
        mset(entries, currentRegion);
    }
    
    /**
     * Set multiple values in specific region
     */
    public void mset(Map<String, String> entries, String region) throws IOException {
        List<Map<String, String>> entryList = new ArrayList<>();
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            Map<String, String> item = new HashMap<>();
            item.put("key", entry.getKey());
            item.put("value", entry.getValue());
            entryList.add(item);
        }
        
        Map<String, Object> body = new HashMap<>();
        body.put("entries", entryList);
        request("POST", "/api/v1/cache/" + encodePath(region) + "/mset", body);
    }
    
    /**
     * Find keys matching pattern
     */
    public List<String> keys(String pattern) throws IOException {
        return keys(pattern, currentRegion);
    }
    
    /**
     * Find keys matching pattern in specific region
     */
    public List<String> keys(String pattern, String region) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("pattern", pattern);
        JsonNode result = request("GET", "/api/v1/cache/" + encodePath(region) + "/keys", null, params);
        
        List<String> keyList = new ArrayList<>();
        if (result != null && result.has("keys")) {
            for (JsonNode node : result.get("keys")) {
                keyList.add(node.asText());
            }
        }
        return keyList;
    }
    
    /**
     * Find keys matching a regular expression pattern.
     * This is more powerful than keys() which uses glob patterns.
     * Returns only keys (not values) for efficiency.
     *
     * @param regexPattern Java regex pattern (e.g., "^user:\\d+$")
     * @return List of matching keys
     */
    public List<String> keysRegex(String regexPattern) throws IOException {
        return keysRegex(regexPattern, currentRegion, 1000);
    }
    
    /**
     * Find keys matching a regular expression pattern with limit.
     *
     * @param regexPattern Java regex pattern (e.g., "^user:\\d+$")
     * @param limit Maximum number of keys to return
     * @return List of matching keys
     */
    public List<String> keysRegex(String regexPattern, int limit) throws IOException {
        return keysRegex(regexPattern, currentRegion, limit);
    }
    
    /**
     * Find keys matching a regular expression pattern in specific region.
     * Returns only keys (not values) for efficiency.
     * 
     * Example:
     *   // Find all keys starting with 'user:' followed by digits
     *   List&lt;String&gt; keys = client.keysRegex("^user:\\d+$", "customers", 500);
     *
     * @param regexPattern Java regex pattern (e.g., "^user:\\d+$", ".*@gmail\\.com$")
     * @param region Region to search in
     * @param limit Maximum number of keys to return
     * @return List of matching keys
     */
    public List<String> keysRegex(String regexPattern, String region, int limit) throws IOException {
        JsonNode result = request("GET", "/api/v1/cache/" + encodePath(region) + "/keys/regex?pattern=" + 
                java.net.URLEncoder.encode(regexPattern, "UTF-8") + "&limit=" + limit);
        
        List<String> keyList = new ArrayList<>();
        if (result != null && result.isArray()) {
            for (JsonNode node : result) {
                keyList.add(node.asText());
            }
        }
        return keyList;
    }
    
    /**
     * Get database size
     */
    public long dbSize() throws IOException {
        return dbSize(currentRegion);
    }
    
    /**
     * Get database size for specific region
     */
    public long dbSize(String region) throws IOException {
        JsonNode result = request("GET", "/api/v1/cache/" + encodePath(region) + "/size");
        return result != null ? result.path("size").asLong(0) : 0;
    }
    
    /**
     * Search keys by regex pattern and return full key-value objects.
     * Unlike keys() which uses glob pattern and returns only key names,
     * ksearch uses regex and returns complete key-value objects including
     * value, type, and TTL.
     *
     * @param regexPattern Regular expression pattern to match keys
     * @return List of maps with keys: key, value, type, ttl
     */
    public List<Map<String, Object>> ksearch(String regexPattern) throws IOException {
        return ksearch(regexPattern, currentRegion, 1000);
    }
    
    /**
     * Search keys by regex pattern with limit
     */
    public List<Map<String, Object>> ksearch(String regexPattern, int limit) throws IOException {
        return ksearch(regexPattern, currentRegion, limit);
    }
    
    /**
     * Search keys by regex pattern in specific region
     *
     * @param regexPattern Regular expression pattern to match keys
     * @param region Region to search in
     * @param limit Maximum number of results
     * @return List of maps with keys: key, value, type, ttl
     */
    public List<Map<String, Object>> ksearch(String regexPattern, String region, int limit) throws IOException {
        JsonNode result = request("GET", "/api/v1/cache/" + encodePath(region) + "/ksearch?pattern=" + 
                java.net.URLEncoder.encode(regexPattern, "UTF-8") + "&limit=" + limit);
        if (result != null && result.isArray()) {
            List<Map<String, Object>> results = new ArrayList<>();
            for (JsonNode node : result) {
                results.add(objectMapper.convertValue(node, new TypeReference<Map<String, Object>>() {}));
            }
            return results;
        }
        return new ArrayList<>();
    }
    
    // ==================== Hash Operations ====================
    
    /**
     * Get hash field
     */
    public String hget(String key, String field) throws IOException {
        return hget(key, field, currentRegion);
    }
    
    /**
     * Get hash field from specific region
     */
    public String hget(String key, String field, String region) throws IOException {
        JsonNode result = request("GET", 
                "/api/v1/cache/" + encodePath(region) + "/" + encodePath(key) + "/hash/" + field);
        return result != null ? result.path("value").asText(null) : null;
    }
    
    /**
     * Set hash field
     */
    public void hset(String key, String field, String value) throws IOException {
        hset(key, field, value, currentRegion);
    }
    
    /**
     * Set hash field in specific region
     */
    public void hset(String key, String field, String value, String region) throws IOException {
        Map<String, String> body = new HashMap<>();
        body.put("value", value);
        request("PUT", "/api/v1/cache/" + encodePath(region) + "/" + encodePath(key) + "/hash/" + field, body);
    }
    
    /**
     * Set multiple hash fields
     */
    public void hmset(String key, Map<String, String> fields) throws IOException {
        hmset(key, fields, currentRegion);
    }
    
    /**
     * Set multiple hash fields in specific region
     */
    public void hmset(String key, Map<String, String> fields, String region) throws IOException {
        Map<String, Object> body = new HashMap<>();
        body.put("fields", fields);
        request("PUT", "/api/v1/cache/" + encodePath(region) + "/" + encodePath(key) + "/hash", body);
    }
    
    /**
     * Get multiple hash fields
     */
    public List<String> hmget(String key, String... fields) throws IOException {
        return hmget(key, Arrays.asList(fields), currentRegion);
    }
    
    /**
     * Get multiple hash fields from specific region
     */
    public List<String> hmget(String key, List<String> fields, String region) throws IOException {
        Map<String, Object> body = new HashMap<>();
        body.put("fields", fields);
        JsonNode result = request("POST", 
                "/api/v1/cache/" + encodePath(region) + "/" + encodePath(key) + "/hash/mget", body);
        
        List<String> values = new ArrayList<>();
        if (result != null && result.has("values")) {
            for (JsonNode node : result.get("values")) {
                values.add(node.isNull() ? null : node.asText());
            }
        }
        return values;
    }
    
    /**
     * Get all hash fields
     */
    public Map<String, String> hgetall(String key) throws IOException {
        return hgetall(key, currentRegion);
    }
    
    /**
     * Get all hash fields from specific region
     */
    public Map<String, String> hgetall(String key, String region) throws IOException {
        JsonNode result = request("GET", "/api/v1/cache/" + encodePath(region) + "/" + encodePath(key) + "/hash");
        
        Map<String, String> fields = new LinkedHashMap<>();
        if (result != null && result.has("fields")) {
            JsonNode fieldsNode = result.get("fields");
            Iterator<String> names = fieldsNode.fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                fields.put(name, fieldsNode.get(name).asText());
            }
        }
        return fields;
    }
    
    /**
     * Delete hash field
     */
    public boolean hdel(String key, String field) throws IOException {
        return hdel(key, field, currentRegion);
    }
    
    /**
     * Delete hash field from specific region
     */
    public boolean hdel(String key, String field, String region) throws IOException {
        try {
            request("DELETE", "/api/v1/cache/" + encodePath(region) + "/" + encodePath(key) + "/hash/" + field);
            return true;
        } catch (KuberRestException e) {
            return false;
        }
    }
    
    /**
     * Get hash keys
     */
    public List<String> hkeys(String key) throws IOException {
        return hkeys(key, currentRegion);
    }
    
    /**
     * Get hash keys from specific region
     */
    public List<String> hkeys(String key, String region) throws IOException {
        JsonNode result = request("GET", "/api/v1/cache/" + encodePath(region) + "/" + encodePath(key) + "/hash/keys");
        
        List<String> keys = new ArrayList<>();
        if (result != null && result.has("keys")) {
            for (JsonNode node : result.get("keys")) {
                keys.add(node.asText());
            }
        }
        return keys;
    }
    
    // ==================== JSON Operations ====================
    
    /**
     * Set JSON value
     */
    public void jsonSet(String key, Object value) throws IOException {
        jsonSet(key, value, currentRegion, null);
    }
    
    /**
     * Set JSON value with TTL
     */
    public void jsonSet(String key, Object value, Duration ttl) throws IOException {
        jsonSet(key, value, currentRegion, ttl);
    }
    
    /**
     * Set JSON value in specific region
     */
    public void jsonSet(String key, Object value, String region) throws IOException {
        jsonSet(key, value, region, null);
    }
    
    /**
     * Set JSON value in specific region with optional TTL
     */
    public void jsonSet(String key, Object value, String region, Duration ttl) throws IOException {
        Map<String, Object> body = new HashMap<>();
        body.put("value", value);
        if (ttl != null) {
            body.put("ttl", ttl.getSeconds());
        }
        request("PUT", "/api/v1/json/" + encodePath(region) + "/" + encodePath(key), body);
    }
    
    /**
     * Get JSON value
     */
    public JsonNode jsonGet(String key) throws IOException {
        return jsonGet(key, "$", currentRegion);
    }
    
    /**
     * Get JSON value at path
     */
    public JsonNode jsonGet(String key, String path) throws IOException {
        return jsonGet(key, path, currentRegion);
    }
    
    /**
     * Get JSON value from specific region
     */
    public JsonNode jsonGet(String key, String path, String region) throws IOException {
        Map<String, String> params = null;
        if (!"$".equals(path)) {
            params = new HashMap<>();
            params.put("path", path);
        }
        JsonNode result = request("GET", "/api/v1/json/" + encodePath(region) + "/" + encodePath(key), null, params);
        return result != null ? result.get("value") : null;
    }
    
    /**
     * Get JSON as typed object
     */
    public <T> T jsonGet(String key, Class<T> type) throws IOException {
        return jsonGet(key, type, currentRegion);
    }
    
    /**
     * Get JSON as typed object from specific region
     */
    public <T> T jsonGet(String key, Class<T> type, String region) throws IOException {
        JsonNode node = jsonGet(key, "$", region);
        if (node == null) {
            return null;
        }
        return objectMapper.treeToValue(node, type);
    }
    
    /**
     * Delete JSON value
     */
    public boolean jsonDelete(String key) throws IOException {
        return jsonDelete(key, currentRegion);
    }
    
    /**
     * Delete JSON value from specific region
     */
    public boolean jsonDelete(String key, String region) throws IOException {
        try {
            request("DELETE", "/api/v1/json/" + encodePath(region) + "/" + encodePath(key));
            return true;
        } catch (KuberRestException e) {
            return false;
        }
    }
    
    /**
     * Search JSON documents
     */
    public List<JsonNode> jsonSearch(String query) throws IOException {
        return jsonSearch(query, currentRegion);
    }
    
    /**
     * Search JSON documents in specific region
     * 
     * Supported operators:
     * - Equality: $.field=value
     * - Comparison: $.field>value, $.field<value, $.field>=value, $.field<=value
     * - Inequality: $.field!=value
     * - Pattern: $.field LIKE %pattern%
     * - Array contains: $.array CONTAINS value
     * - Combined: $.field1=value1,$.field2>value2
     */
    public List<JsonNode> jsonSearch(String query, String region) throws IOException {
        Map<String, Object> body = new HashMap<>();
        body.put("query", query);
        JsonNode result = request("POST", "/api/v1/json/" + encodePath(region) + "/search", body);
        
        List<JsonNode> results = new ArrayList<>();
        if (result != null && result.has("results")) {
            for (JsonNode node : result.get("results")) {
                results.add(node);
            }
        }
        return results;
    }
    
    // ==================== Generic Search API ====================
    
    /**
     * Perform generic search using the unified search API.
     * This is a convenience method that supports multiple search modes.
     * 
     * @param request The search request containing search parameters
     * @return List of matching results as JsonNode objects
     * @throws IOException If the request fails
     */
    public List<JsonNode> genericSearch(GenericSearchRequest request) throws IOException {
        Map<String, Object> body = new HashMap<>();
        body.put("region", request.getRegion() != null ? request.getRegion() : currentRegion);
        
        if (request.getKey() != null) {
            body.put("key", request.getKey());
        }
        if (request.getKeyPattern() != null) {
            body.put("keypattern", request.getKeyPattern());
        }
        if (request.getType() != null) {
            body.put("type", request.getType());
        }
        if (request.getValues() != null) {
            body.put("values", request.getValues());
        }
        if (request.getFields() != null) {
            body.put("fields", request.getFields());
        }
        if (request.getLimit() != null) {
            body.put("limit", request.getLimit());
        }
        
        JsonNode result = request("POST", "/api/v1/genericsearch", body);
        
        List<JsonNode> results = new ArrayList<>();
        if (result != null && result.isArray()) {
            for (JsonNode node : result) {
                results.add(node);
            }
        }
        return results;
    }
    
    /**
     * Perform a simple key lookup.
     * 
     * @param key The exact key to lookup
     * @return List containing the single result if found, empty list otherwise
     * @throws IOException If the request fails
     */
    public List<JsonNode> genericSearchByKey(String key) throws IOException {
        return genericSearchByKey(key, null, null);
    }
    
    /**
     * Perform a simple key lookup with optional field projection.
     * 
     * @param key The exact key to lookup
     * @param fields Optional list of fields to return
     * @param region Optional region (uses current region if null)
     * @return List containing the single result if found, empty list otherwise
     * @throws IOException If the request fails
     */
    public List<JsonNode> genericSearchByKey(String key, List<String> fields, String region) throws IOException {
        GenericSearchRequest request = new GenericSearchRequest();
        request.setRegion(region != null ? region : currentRegion);
        request.setKey(key);
        request.setFields(fields);
        return genericSearch(request);
    }
    
    /**
     * Perform a key pattern (regex) search.
     * 
     * @param keyPattern Regex pattern to match keys
     * @return List of matching results
     * @throws IOException If the request fails
     */
    public List<JsonNode> genericSearchByPattern(String keyPattern) throws IOException {
        return genericSearchByPattern(keyPattern, null, null, null);
    }
    
    /**
     * Perform a key pattern (regex) search with optional parameters.
     * 
     * @param keyPattern Regex pattern to match keys
     * @param fields Optional list of fields to return
     * @param limit Optional maximum results
     * @param region Optional region (uses current region if null)
     * @return List of matching results
     * @throws IOException If the request fails
     */
    public List<JsonNode> genericSearchByPattern(String keyPattern, List<String> fields, 
                                                  Integer limit, String region) throws IOException {
        GenericSearchRequest request = new GenericSearchRequest();
        request.setRegion(region != null ? region : currentRegion);
        request.setKeyPattern(keyPattern);
        request.setFields(fields);
        request.setLimit(limit);
        return genericSearch(request);
    }
    
    /**
     * Perform a JSON attribute search.
     * 
     * @param values List of attribute conditions
     * @return List of matching results
     * @throws IOException If the request fails
     */
    public List<JsonNode> genericSearchByJson(List<Map<String, Object>> values) throws IOException {
        return genericSearchByJson(values, null, null, null);
    }
    
    /**
     * Perform a JSON attribute search with optional parameters.
     * 
     * @param values List of attribute conditions
     * @param fields Optional list of fields to return
     * @param limit Optional maximum results
     * @param region Optional region (uses current region if null)
     * @return List of matching results
     * @throws IOException If the request fails
     */
    public List<JsonNode> genericSearchByJson(List<Map<String, Object>> values, List<String> fields,
                                               Integer limit, String region) throws IOException {
        GenericSearchRequest request = new GenericSearchRequest();
        request.setRegion(region != null ? region : currentRegion);
        request.setType("json");
        request.setValues(values);
        request.setFields(fields);
        request.setLimit(limit);
        return genericSearch(request);
    }
    
    /**
     * Request DTO for generic search API.
     */
    public static class GenericSearchRequest {
        private String region;
        private String key;
        private String keyPattern;
        private String type;
        private List<Map<String, Object>> values;
        private List<String> fields;
        private Integer limit;
        
        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }
        
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
    
    // ==================== Bulk Operations ====================
    
    /**
     * Bulk import entries
     */
    public JsonNode bulkImport(List<Map<String, Object>> entries) throws IOException {
        return bulkImport(entries, currentRegion);
    }
    
    /**
     * Bulk import entries to specific region
     */
    public JsonNode bulkImport(List<Map<String, Object>> entries, String region) throws IOException {
        Map<String, Object> body = new HashMap<>();
        body.put("entries", entries);
        return request("POST", "/api/v1/cache/" + encodePath(region) + "/import", body);
    }
    
    /**
     * Bulk export entries
     */
    public List<JsonNode> bulkExport(String pattern) throws IOException {
        return bulkExport(pattern, currentRegion);
    }
    
    /**
     * Bulk export entries from specific region
     */
    public List<JsonNode> bulkExport(String pattern, String region) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("pattern", pattern);
        JsonNode result = request("GET", "/api/v1/cache/" + encodePath(region) + "/export", null, params);
        
        List<JsonNode> entries = new ArrayList<>();
        if (result != null && result.has("entries")) {
            for (JsonNode node : result.get("entries")) {
                entries.add(node);
            }
        }
        return entries;
    }
    
    @Override
    public void close() {
        log.debug("Kuber REST client closed");
    }
    
    /**
     * REST API exception
     */
    public static class KuberRestException extends RuntimeException {
        public KuberRestException(String message) {
            super(message);
        }
    }
}
