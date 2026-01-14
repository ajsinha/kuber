/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber ActiveMQ Request/Response Test Client (v1.7.9)
 * 
 * Tests the Kuber Request/Response messaging feature via Apache ActiveMQ.
 * Publishes cache operation requests and subscribes to responses.
 *
 * Prerequisites:
 * - ActiveMQ running on localhost:61616
 * - Kuber server running with messaging enabled
 * - activemq-client dependency in pom.xml:
 *     <dependency>
 *         <groupId>org.apache.activemq</groupId>
 *         <artifactId>activemq-client</artifactId>
 *         <version>5.18.3</version>
 *     </dependency>
 */
package com.kuber.client.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test client for Kuber's ActiveMQ-based Request/Response messaging.
 * 
 * <p>Demonstrates how to:
 * <ul>
 *   <li>Publish cache operation requests to ActiveMQ queues</li>
 *   <li>Subscribe to and receive responses</li>
 *   <li>Correlate requests with responses using message IDs</li>
 * </ul>
 * 
 * @version 1.7.9
 */
public class ActiveMqRequestResponseTest {
    
    // Configuration
    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "admin";
    private static final String REQUEST_QUEUE = "ccs_cache_request";
    private static final String RESPONSE_QUEUE = "ccs_cache_response";
    
    // Your Kuber API key (replace with actual key)
    private static final String API_KEY = "kub_your_api_key_here";
    
    // Test region
    private static final String TEST_REGION = "default";
    
    // JSON mapper
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // JMS objects
    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private MessageConsumer consumer;
    
    // State
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Map<String, CompletableFuture<JsonNode>> pendingRequests = new ConcurrentHashMap<>();
    private final List<JsonNode> receivedResponses = new CopyOnWriteArrayList<>();
    
    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        System.out.println("╔════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  Kuber ActiveMQ Request/Response Test Client (Java)                ║");
        System.out.println("║  Tests cache operations via ActiveMQ messaging                     ║");
        System.out.println("╚════════════════════════════════════════════════════════════════════╝");
        System.out.println();
        
        ActiveMqRequestResponseTest test = new ActiveMqRequestResponseTest();
        try {
            test.run();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Run the test suite.
     */
    public void run() throws Exception {
        try {
            // Initialize JMS connection
            initConnection();
            
            // Start response listener
            startResponseListener();
            
            // Give consumer time to connect
            Thread.sleep(2000);
            
            // Run tests
            runTestSuite();
            
            // Wait for responses
            System.out.println("\nWaiting for responses (10 seconds)...");
            Thread.sleep(10000);
            
        } finally {
            shutdown();
            printSummary();
        }
    }
    
    /**
     * Initialize JMS connection to ActiveMQ.
     */
    private void initConnection() throws JMSException {
        System.out.println("Connecting to ActiveMQ at " + BROKER_URL + "...");
        
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        factory.setUserName(USERNAME);
        factory.setPassword(PASSWORD);
        
        connection = factory.createConnection();
        connection.start();
        
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        // Create producer for request queue
        javax.jms.Queue requestQueue = session.createQueue(REQUEST_QUEUE);
        producer = session.createProducer(requestQueue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        
        // Create consumer for response queue
        javax.jms.Queue responseQueue = session.createQueue(RESPONSE_QUEUE);
        consumer = session.createConsumer(responseQueue);
        
        System.out.println("✓ Connected to ActiveMQ");
        System.out.println("  Request queue: " + REQUEST_QUEUE);
        System.out.println("  Response queue: " + RESPONSE_QUEUE);
    }
    
    /**
     * Start listening for responses.
     */
    private void startResponseListener() throws JMSException {
        consumer.setMessageListener(message -> {
            try {
                if (message instanceof TextMessage) {
                    String responseText = ((TextMessage) message).getText();
                    JsonNode response = objectMapper.readTree(responseText);
                    
                    // Kuber response structure is nested:
                    // {
                    //   "request_receive_timestamp": "...",
                    //   "response_time": "...",
                    //   "processing_time_ms": 123,
                    //   "request": { original request with message_id, operation, etc. },
                    //   "response": { "success": true/false, "result": ..., "error": "..." }
                    // }
                    JsonNode requestNode = response.get("request");
                    JsonNode responseNode = response.get("response");
                    
                    String messageId = requestNode != null && requestNode.has("message_id") ? 
                        requestNode.get("message_id").asText() : "unknown";
                    boolean success = responseNode != null && responseNode.has("success") && 
                        responseNode.get("success").asBoolean();
                    String operation = requestNode != null && requestNode.has("operation") ?
                        requestNode.get("operation").asText() : "unknown";
                    long processingTime = response.has("processing_time_ms") ?
                        response.get("processing_time_ms").asLong() : 0;
                    
                    System.out.println("\n════════════════════════════════════════════════════════════");
                    System.out.println("📥 RESPONSE RECEIVED");
                    System.out.println("   Message ID: " + messageId);
                    System.out.println("   Success: " + success);
                    System.out.println("   Operation: " + operation);
                    System.out.println("   Processing Time: " + processingTime + "ms");
                    
                    if (success) {
                        if (responseNode != null && responseNode.has("result")) {
                            String result = responseNode.get("result").toString();
                            if (result.length() > 100) {
                                System.out.println("   Result: " + result.substring(0, 100) + "...");
                            } else {
                                System.out.println("   Result: " + result);
                            }
                        }
                    } else {
                        if (responseNode != null) {
                            String errorCode = responseNode.has("error_code") ?
                                responseNode.get("error_code").asText() : "UNKNOWN";
                            String error = responseNode.has("error") ?
                                responseNode.get("error").asText() : "Unknown error";
                            System.out.println("   Error Code: " + errorCode);
                            System.out.println("   Error: " + error);
                        }
                    }
                    System.out.println("════════════════════════════════════════════════════════════");
                    
                    receivedResponses.add(response);
                    
                    // Complete pending future if exists
                    CompletableFuture<JsonNode> future = pendingRequests.remove(messageId);
                    if (future != null) {
                        future.complete(response);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing response: " + e.getMessage());
            }
        });
        
        System.out.println("✓ Response listener started");
    }
    
    /**
     * Create a request message.
     */
    private ObjectNode createRequest(String operation, String region, String key, 
                                      String value, Integer ttl) {
        ObjectNode request = objectMapper.createObjectNode();
        
        String messageId = "req-" + UUID.randomUUID().toString().substring(0, 12);
        
        request.put("api_key", API_KEY);
        request.put("message_id", messageId);
        request.put("operation", operation.toUpperCase());
        request.put("region", region);
        request.put("key", key);
        
        if (value != null) {
            request.put("value", value);
        }
        
        if (ttl != null) {
            request.put("ttl", ttl);
        }
        
        return request;
    }
    
    /**
     * Publish a request to ActiveMQ.
     */
    private CompletableFuture<JsonNode> publishRequest(ObjectNode request) throws Exception {
        String messageId = request.get("message_id").asText();
        String requestJson = objectMapper.writeValueAsString(request);
        
        CompletableFuture<JsonNode> responseFuture = new CompletableFuture<>();
        pendingRequests.put(messageId, responseFuture);
        
        TextMessage message = session.createTextMessage(requestJson);
        message.setJMSCorrelationID(messageId);
        producer.send(message);
        
        System.out.println("\n📤 REQUEST SENT");
        System.out.println("   Message ID: " + messageId);
        System.out.println("   Operation: " + request.get("operation").asText());
        System.out.println("   Region: " + request.get("region").asText());
        System.out.println("   Key: " + request.get("key").asText());
        System.out.println("   Queue: " + REQUEST_QUEUE);
        
        return responseFuture;
    }
    
    /**
     * Run the test suite.
     */
    private void runTestSuite() throws Exception {
        System.out.println("\n--- Test 1: SET Operation ---");
        ObjectNode setRequest = createRequest("SET", TEST_REGION, 
            "activemq-test:user:1001",
            "{\"name\":\"John Doe\",\"email\":\"john@example.com\",\"timestamp\":\"" + 
                Instant.now() + "\"}",
            3600);
        publishRequest(setRequest);
        Thread.sleep(1000);
        
        System.out.println("\n--- Test 2: GET Operation ---");
        ObjectNode getRequest = createRequest("GET", TEST_REGION, 
            "activemq-test:user:1001", null, null);
        publishRequest(getRequest);
        Thread.sleep(1000);
        
        System.out.println("\n--- Test 3: EXISTS Operation ---");
        ObjectNode existsRequest = createRequest("EXISTS", TEST_REGION, 
            "activemq-test:user:1001", null, null);
        publishRequest(existsRequest);
        Thread.sleep(1000);
        
        System.out.println("\n--- Test 4: SET Another Key ---");
        ObjectNode setRequest2 = createRequest("SET", TEST_REGION, 
            "activemq-test:user:1002",
            "{\"name\":\"Jane Smith\",\"role\":\"admin\"}", null);
        publishRequest(setRequest2);
        Thread.sleep(1000);
        
        System.out.println("\n--- Test 5: KEYS Operation ---");
        ObjectNode keysRequest = createRequest("KEYS", TEST_REGION, 
            "activemq-test:*", null, null);
        publishRequest(keysRequest);
        Thread.sleep(1000);
        
        System.out.println("\n--- Test 6: DELETE Operation ---");
        ObjectNode deleteRequest = createRequest("DELETE", TEST_REGION, 
            "activemq-test:user:1002", null, null);
        publishRequest(deleteRequest);
        Thread.sleep(1000);
        
        System.out.println("\n--- Test 7: GET Non-existent Key ---");
        ObjectNode getMissRequest = createRequest("GET", TEST_REGION, 
            "activemq-test:nonexistent:key", null, null);
        publishRequest(getMissRequest);
        Thread.sleep(1000);
        
        System.out.println("\n✓ All requests sent");
    }
    
    /**
     * Shutdown connections.
     */
    private void shutdown() {
        System.out.println("\nShutting down...");
        running.set(false);
        
        try {
            if (consumer != null) consumer.close();
            if (producer != null) producer.close();
            if (session != null) session.close();
            if (connection != null) connection.close();
        } catch (JMSException e) {
            System.err.println("Error closing connections: " + e.getMessage());
        }
    }
    
    /**
     * Print test summary.
     */
    private void printSummary() {
        System.out.println("\n════════════════════════════════════════════════════════════");
        System.out.println("TEST SUMMARY");
        System.out.println("════════════════════════════════════════════════════════════");
        System.out.println("Total responses received: " + receivedResponses.size());
        System.out.println("Pending requests (no response): " + pendingRequests.size());
        
        if (!pendingRequests.isEmpty()) {
            System.out.println("\nRequests without responses:");
            for (String messageId : pendingRequests.keySet()) {
                System.out.println("  - " + messageId);
            }
        }
        
        long successCount = receivedResponses.stream()
            .filter(r -> {
                JsonNode responseNode = r.get("response");
                return responseNode != null && responseNode.has("success") && responseNode.get("success").asBoolean();
            })
            .count();
        long errorCount = receivedResponses.size() - successCount;
        
        System.out.println("\nSuccessful operations: " + successCount);
        System.out.println("Failed operations: " + errorCount);
        System.out.println("════════════════════════════════════════════════════════════");
    }
}
