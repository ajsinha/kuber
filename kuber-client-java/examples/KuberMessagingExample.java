/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber Request/Response Messaging Example
 *
 * This example demonstrates how to interact with Kuber's request/response
 * messaging system using a message broker. It shows the JSON message format
 * for various cache operations.
 *
 * Prerequisites:
 *     - Kuber server running with request/response messaging enabled
 *     - Message broker (Kafka, ActiveMQ, RabbitMQ, or IBM MQ) configured
 *     - Valid API key from Kuber admin panel
 *
 * Dependencies (add to pom.xml):
 *     - com.fasterxml.jackson.core:jackson-databind
 *     - org.apache.kafka:kafka-clients (for Kafka)
 *     - org.apache.activemq:activemq-client (for ActiveMQ)
 *     - com.rabbitmq:amqp-client (for RabbitMQ)
 */
package com.kuber.client.examples;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.*;

/**
 * Kuber Request/Response Messaging Example.
 * 
 * <p>This class demonstrates the message format for Kuber's request/response
 * messaging system. In production, you would send these messages to your
 * configured message broker (Kafka, ActiveMQ, RabbitMQ, or IBM MQ).
 * 
 * <h2>Message Format</h2>
 * <pre>
 * Request:
 * {
 *   "api_key": "your-api-key",
 *   "message_id": "unique-correlation-id",
 *   "operation": "GET|SET|DELETE|MGET|MSET|etc.",
 *   "region": "default",
 *   "key": "key-name",
 *   "value": "value-for-set",
 *   ...
 * }
 * 
 * Response:
 * {
 *   "request_receive_timestamp": "ISO timestamp",
 *   "response_time": "ISO timestamp",
 *   "processing_time_ms": 123,
 *   "request": { original request },
 *   "response": {
 *     "success": true/false,
 *     "result": operation result,
 *     "error": "error message if failed"
 *   }
 * }
 * </pre>
 * 
 * @version 2.6.3
 */
public class KuberMessagingExample {
    
    private static final ObjectMapper mapper = new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT);
    
    // =========================================================================
    // Request/Response DTOs
    // =========================================================================
    
    /**
     * Cache request message.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CacheRequest {
        @JsonProperty("api_key")
        private String apiKey;
        
        @JsonProperty("message_id")
        private String messageId;
        
        private String operation;
        private String region = "default";
        private String key;
        private Object value;
        private List<String> keys;
        private Map<String, Object> entries;
        private String pattern;
        private Long ttl;
        private String field;
        private Map<String, String> fields;
        private String path;
        private String query;
        
        // Getters and setters
        public String getApiKey() { return apiKey; }
        public void setApiKey(String apiKey) { this.apiKey = apiKey; }
        
        public String getMessageId() { return messageId; }
        public void setMessageId(String messageId) { this.messageId = messageId; }
        
        public String getOperation() { return operation; }
        public void setOperation(String operation) { this.operation = operation; }
        
        public String getRegion() { return region; }
        public void setRegion(String region) { this.region = region; }
        
        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }
        
        public Object getValue() { return value; }
        public void setValue(Object value) { this.value = value; }
        
        public List<String> getKeys() { return keys; }
        public void setKeys(List<String> keys) { this.keys = keys; }
        
        public Map<String, Object> getEntries() { return entries; }
        public void setEntries(Map<String, Object> entries) { this.entries = entries; }
        
        public String getPattern() { return pattern; }
        public void setPattern(String pattern) { this.pattern = pattern; }
        
        public Long getTtl() { return ttl; }
        public void setTtl(Long ttl) { this.ttl = ttl; }
        
        public String getField() { return field; }
        public void setField(String field) { this.field = field; }
        
        public Map<String, String> getFields() { return fields; }
        public void setFields(Map<String, String> fields) { this.fields = fields; }
        
        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }
        
        public String getQuery() { return query; }
        public void setQuery(String query) { this.query = query; }
    }
    
    /**
     * Cache response message.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CacheResponse {
        @JsonProperty("request_receive_timestamp")
        private String requestReceiveTimestamp;
        
        @JsonProperty("response_time")
        private String responseTime;
        
        @JsonProperty("processing_time_ms")
        private long processingTimeMs;
        
        private CacheRequest request;
        private ResponsePayload response;
        
        // Getters and setters
        public String getRequestReceiveTimestamp() { return requestReceiveTimestamp; }
        public void setRequestReceiveTimestamp(String ts) { this.requestReceiveTimestamp = ts; }
        
        public String getResponseTime() { return responseTime; }
        public void setResponseTime(String responseTime) { this.responseTime = responseTime; }
        
        public long getProcessingTimeMs() { return processingTimeMs; }
        public void setProcessingTimeMs(long ms) { this.processingTimeMs = ms; }
        
        public CacheRequest getRequest() { return request; }
        public void setRequest(CacheRequest request) { this.request = request; }
        
        public ResponsePayload getResponse() { return response; }
        public void setResponse(ResponsePayload response) { this.response = response; }
    }
    
    /**
     * Response payload.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ResponsePayload {
        private boolean success;
        private Object result;
        private String error = "";
        
        @JsonProperty("error_code")
        private String errorCode;
        
        @JsonProperty("server_message")
        private String serverMessage = "";
        
        @JsonProperty("total_count")
        private Integer totalCount;
        
        @JsonProperty("returned_count")
        private Integer returnedCount;
        
        // Getters and setters
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        
        public Object getResult() { return result; }
        public void setResult(Object result) { this.result = result; }
        
        public String getError() { return error; }
        public void setError(String error) { this.error = error; }
        
        public String getErrorCode() { return errorCode; }
        public void setErrorCode(String errorCode) { this.errorCode = errorCode; }
        
        public String getServerMessage() { return serverMessage; }
        public void setServerMessage(String msg) { this.serverMessage = msg; }
        
        public Integer getTotalCount() { return totalCount; }
        public void setTotalCount(Integer totalCount) { this.totalCount = totalCount; }
        
        public Integer getReturnedCount() { return returnedCount; }
        public void setReturnedCount(Integer returnedCount) { this.returnedCount = returnedCount; }
    }
    
    // =========================================================================
    // Request Builder
    // =========================================================================
    
    /**
     * Builder for cache requests.
     */
    public static class RequestBuilder {
        private final String apiKey;
        private final String defaultRegion;
        
        public RequestBuilder(String apiKey) {
            this(apiKey, "default");
        }
        
        public RequestBuilder(String apiKey, String defaultRegion) {
            this.apiKey = apiKey;
            this.defaultRegion = defaultRegion;
        }
        
        private CacheRequest createBase(String operation) {
            CacheRequest request = new CacheRequest();
            request.setApiKey(apiKey);
            request.setMessageId(UUID.randomUUID().toString());
            request.setOperation(operation);
            request.setRegion(defaultRegion);
            return request;
        }
        
        // String Operations
        
        public CacheRequest get(String key) {
            CacheRequest request = createBase("GET");
            request.setKey(key);
            return request;
        }
        
        public CacheRequest set(String key, Object value) {
            return set(key, value, null);
        }
        
        public CacheRequest set(String key, Object value, Long ttl) {
            CacheRequest request = createBase("SET");
            request.setKey(key);
            request.setValue(value);
            if (ttl != null) {
                request.setTtl(ttl);
            }
            return request;
        }
        
        public CacheRequest delete(String key) {
            CacheRequest request = createBase("DELETE");
            request.setKey(key);
            return request;
        }
        
        public CacheRequest deleteMulti(List<String> keys) {
            CacheRequest request = createBase("DELETE");
            request.setKeys(keys);
            return request;
        }
        
        public CacheRequest exists(String key) {
            CacheRequest request = createBase("EXISTS");
            request.setKey(key);
            return request;
        }
        
        // Batch Operations
        
        public CacheRequest mget(List<String> keys) {
            CacheRequest request = createBase("MGET");
            request.setKeys(keys);
            return request;
        }
        
        public CacheRequest mset(Map<String, Object> entries) {
            return mset(entries, null);
        }
        
        public CacheRequest mset(Map<String, Object> entries, Long ttl) {
            CacheRequest request = createBase("MSET");
            request.setEntries(entries);
            if (ttl != null) {
                request.setTtl(ttl);
            }
            return request;
        }
        
        // Key Operations
        
        public CacheRequest keys(String pattern) {
            CacheRequest request = createBase("KEYS");
            request.setPattern(pattern);
            return request;
        }
        
        public CacheRequest ttl(String key) {
            CacheRequest request = createBase("TTL");
            request.setKey(key);
            return request;
        }
        
        public CacheRequest expire(String key, long ttl) {
            CacheRequest request = createBase("EXPIRE");
            request.setKey(key);
            request.setTtl(ttl);
            return request;
        }
        
        // Hash Operations
        
        public CacheRequest hget(String key, String field) {
            CacheRequest request = createBase("HGET");
            request.setKey(key);
            request.setField(field);
            return request;
        }
        
        public CacheRequest hset(String key, String field, Object value) {
            CacheRequest request = createBase("HSET");
            request.setKey(key);
            request.setField(field);
            request.setValue(value);
            return request;
        }
        
        public CacheRequest hgetall(String key) {
            CacheRequest request = createBase("HGETALL");
            request.setKey(key);
            return request;
        }
        
        public CacheRequest hmset(String key, Map<String, String> fields) {
            CacheRequest request = createBase("HMSET");
            request.setKey(key);
            request.setFields(fields);
            return request;
        }
        
        // JSON Operations
        
        public CacheRequest jset(String key, Object value) {
            CacheRequest request = createBase("JSET");
            request.setKey(key);
            request.setValue(value);
            return request;
        }
        
        public CacheRequest jget(String key) {
            return jget(key, null);
        }
        
        public CacheRequest jget(String key, String path) {
            CacheRequest request = createBase("JGET");
            request.setKey(key);
            if (path != null) {
                request.setPath(path);
            }
            return request;
        }
        
        public CacheRequest jsearch(String query) {
            CacheRequest request = createBase("JSEARCH");
            request.setQuery(query);
            return request;
        }
        
        // Admin Operations
        
        public CacheRequest ping() {
            return createBase("PING");
        }
        
        public CacheRequest info() {
            return createBase("INFO");
        }
        
        public CacheRequest regions() {
            return createBase("REGIONS");
        }
    }
    
    // =========================================================================
    // Demo Methods
    // =========================================================================
    
    private static void printRequest(String description, CacheRequest request) 
            throws JsonProcessingException {
        System.out.println("\n--- " + description + " ---");
        System.out.println("Request:");
        System.out.println(mapper.writeValueAsString(request));
    }
    
    private static void printExampleMessages() throws JsonProcessingException {
        RequestBuilder builder = new RequestBuilder("your-api-key-here");
        
        System.out.println("=".repeat(70));
        System.out.println("KUBER REQUEST/RESPONSE MESSAGING - MESSAGE FORMAT EXAMPLES");
        System.out.println("=".repeat(70));
        
        // GET operation
        CacheRequest getRequest = builder.get("user:1001");
        printRequest("GET Operation", getRequest);
        
        System.out.println("\nExpected Response:");
        CacheResponse getResponse = new CacheResponse();
        getResponse.setRequestReceiveTimestamp("2025-01-15T10:30:00.123Z");
        getResponse.setResponseTime("2025-01-15T10:30:00.125Z");
        getResponse.setProcessingTimeMs(2);
        getResponse.setRequest(getRequest);
        ResponsePayload getPayload = new ResponsePayload();
        getPayload.setSuccess(true);
        getPayload.setResult("{\"name\": \"John\", \"email\": \"john@example.com\"}");
        getResponse.setResponse(getPayload);
        System.out.println(mapper.writeValueAsString(getResponse));
        
        // SET operation
        Map<String, Object> userData = new LinkedHashMap<>();
        userData.put("name", "Jane");
        userData.put("email", "jane@example.com");
        CacheRequest setRequest = builder.set("user:1002", userData, 3600L);
        printRequest("SET Operation with TTL", setRequest);
        
        // MGET operation
        CacheRequest mgetRequest = builder.mget(Arrays.asList("user:1001", "user:1002", "user:1003"));
        printRequest("MGET (Batch GET) Operation", mgetRequest);
        
        System.out.println("\nExpected Response (key-value pairs format):");
        System.out.println("""
            {
              "response": {
                "success": true,
                "result": [
                  {"key": "user:1001", "value": "{\\"name\\": \\"John\\"}"},
                  {"key": "user:1002", "value": "{\\"name\\": \\"Jane\\"}"},
                  {"key": "user:1003", "value": null}
                ],
                "error": ""
              }
            }""");
        
        // MSET operation
        Map<String, Object> configEntries = new LinkedHashMap<>();
        configEntries.put("config:theme", "dark");
        configEntries.put("config:language", "en");
        configEntries.put("config:timezone", "UTC");
        CacheRequest msetRequest = builder.mset(configEntries, 86400L);
        printRequest("MSET (Batch SET) Operation", msetRequest);
        
        // KEYS operation
        CacheRequest keysRequest = builder.keys("user:*");
        printRequest("KEYS Operation", keysRequest);
        
        // Hash operations
        CacheRequest hsetRequest = builder.hset("session:abc123", "last_active", "2025-01-15T10:30:00Z");
        printRequest("HSET Operation", hsetRequest);
        
        Map<String, String> sessionFields = new LinkedHashMap<>();
        sessionFields.put("user_id", "1001");
        sessionFields.put("ip_address", "192.168.1.100");
        sessionFields.put("user_agent", "Mozilla/5.0");
        CacheRequest hmsetRequest = builder.hmset("session:abc123", sessionFields);
        printRequest("HMSET Operation", hmsetRequest);
        
        // JSON operations
        Map<String, Object> product = new LinkedHashMap<>();
        product.put("name", "Laptop");
        product.put("price", 999.99);
        Map<String, String> specs = new LinkedHashMap<>();
        specs.put("cpu", "Intel i7");
        specs.put("ram", "16GB");
        specs.put("storage", "512GB SSD");
        product.put("specs", specs);
        product.put("tags", Arrays.asList("electronics", "computers"));
        CacheRequest jsetRequest = builder.jset("product:5001", product);
        printRequest("JSET (JSON SET) Operation", jsetRequest);
        
        CacheRequest jgetRequest = builder.jget("product:5001", "$.specs.cpu");
        printRequest("JGET with JSONPath", jgetRequest);
        
        CacheRequest jsearchRequest = builder.jsearch("price > 500 AND tags CONTAINS 'electronics'");
        printRequest("JSEARCH Operation", jsearchRequest);
        
        // Error response example
        System.out.println("\n--- Error Response Example ---");
        System.out.println("""
            {
              "request_receive_timestamp": "2025-01-15T10:30:05.000Z",
              "response_time": "2025-01-15T10:30:05.001Z",
              "processing_time_ms": 1,
              "request": {
                "api_key": "invalid-key",
                "message_id": "msg-12345",
                "operation": "GET",
                "key": "test"
              },
              "response": {
                "success": false,
                "result": null,
                "error": "Invalid or expired API key",
                "error_code": "AUTHENTICATION_FAILED"
              }
            }""");
        
        System.out.println("\n" + "=".repeat(70));
        System.out.println("END OF EXAMPLES");
        System.out.println("=".repeat(70));
    }
    
    private static void interactiveDemo() throws JsonProcessingException {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("KUBER MESSAGING CLIENT - INTERACTIVE DEMO");
        System.out.println("=".repeat(70));
        System.out.println("\nThis demo shows how to build requests for various cache operations.");
        System.out.println("In production, you would send these to your message broker.");
        
        RequestBuilder builder = new RequestBuilder("demo-api-key-12345");
        
        System.out.println("\n--- Building Sample Requests ---\n");
        
        // 1. Store user data
        System.out.println("1. Storing user data with TTL:");
        Map<String, Object> user = new LinkedHashMap<>();
        user.put("name", "John Doe");
        user.put("email", "john@example.com");
        user.put("role", "admin");
        CacheRequest setReq = builder.set("user:john_doe", user, 3600L);
        System.out.println("   Topic: kuber_request");
        System.out.println("   Message: " + mapper.writeValueAsString(setReq));
        
        // 2. Batch store configuration
        System.out.println("\n2. Batch storing configuration:");
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("app:config:theme", "dark");
        config.put("app:config:lang", "en-US");
        config.put("app:config:version", "2.6.3");
        CacheRequest msetReq = builder.mset(config);
        System.out.println("   Topic: kuber_request");
        System.out.println("   Message: " + mapper.writeValueAsString(msetReq));
        
        // 3. Retrieve multiple users
        System.out.println("\n3. Batch retrieving users:");
        CacheRequest mgetReq = builder.mget(Arrays.asList("user:john_doe", "user:jane_doe", "user:bob_smith"));
        System.out.println("   Topic: kuber_request");
        System.out.println("   Message: " + mapper.writeValueAsString(mgetReq));
        
        // 4. Search for products
        System.out.println("\n4. Searching JSON documents:");
        CacheRequest searchReq = builder.jsearch("category = 'electronics' AND price < 1000");
        System.out.println("   Topic: kuber_request");
        System.out.println("   Message: " + mapper.writeValueAsString(searchReq));
        
        // 5. Check server health
        System.out.println("\n5. Health check (PING):");
        CacheRequest pingReq = builder.ping();
        System.out.println("   Topic: kuber_request");
        System.out.println("   Message: " + mapper.writeValueAsString(pingReq));
        
        System.out.println("\n" + "-".repeat(70));
        System.out.println("Response Topic: kuber_response");
        System.out.println("Responses include the original request for correlation via message_id");
        System.out.println("-".repeat(70));
    }
    
    // =========================================================================
    // Main
    // =========================================================================
    
    public static void main(String[] args) throws JsonProcessingException {
        System.out.println("""
            ╔══════════════════════════════════════════════════════════════════════╗
            ║          KUBER REQUEST/RESPONSE MESSAGING - JAVA CLIENT              ║
            ║                         Version 2.6.3                                ║
            ╚══════════════════════════════════════════════════════════════════════╝
            """);
        
        if (args.length > 0 && "--examples".equals(args[0])) {
            printExampleMessages();
        } else {
            interactiveDemo();
            System.out.println("\nRun with --examples flag to see detailed message format examples.");
            System.out.println("\nTo use with a real broker:");
            System.out.println("  1. Add broker client dependency (kafka-clients, activemq-client, etc.)");
            System.out.println("  2. Configure your broker in Kuber's request_response.json");
            System.out.println("  3. Get an API key from Kuber admin panel");
            System.out.println("  4. Implement send/receive logic for your broker");
        }
    }
}
