/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber IBM MQ Request/Response Test Client (v1.7.5)
 * 
 * Tests the Kuber Request/Response messaging feature via IBM MQ.
 *
 * Prerequisites:
 * - IBM MQ running with queue manager accessible
 * - Kuber server running with messaging enabled
 * - IBM MQ JMS client libraries:
 *     <dependency>
 *         <groupId>com.ibm.mq</groupId>
 *         <artifactId>com.ibm.mq.allclient</artifactId>
 *         <version>9.3.4.0</version>
 *     </dependency>
 */
package com.kuber.client.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.msg.client.jms.JmsConnectionFactory;
import com.ibm.msg.client.jms.JmsFactoryFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import javax.jms.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test client for Kuber's IBM MQ-based Request/Response messaging.
 * 
 * @version 1.7.5
 */
public class IbmMqRequestResponseTest {
    
    // Configuration
    private static final String QUEUE_MANAGER = "QM1";
    private static final String CHANNEL = "DEV.APP.SVRCONN";
    private static final String HOST = "localhost";
    private static final int PORT = 1414;
    
    private static final String REQUEST_QUEUE = "ccs_cache_request";
    private static final String RESPONSE_QUEUE = "ccs_cache_response";
    
    // Your Kuber API key
    private static final String API_KEY = "kub_your_api_key_here";
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
    
    public static void main(String[] args) {
        System.out.println("╔════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  Kuber IBM MQ Request/Response Test Client (Java)                  ║");
        System.out.println("║  Tests cache operations via IBM MQ messaging                       ║");
        System.out.println("╚════════════════════════════════════════════════════════════════════╝");
        System.out.println();
        
        IbmMqRequestResponseTest client = new IbmMqRequestResponseTest();
        try {
            client.run();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void run() throws Exception {
        System.out.println("Queue Manager: " + QUEUE_MANAGER);
        System.out.println("Channel: " + CHANNEL);
        System.out.println("Host: " + HOST + ":" + PORT);
        System.out.println("Request Queue: " + REQUEST_QUEUE);
        System.out.println("Response Queue: " + RESPONSE_QUEUE);
        System.out.println();
        
        try {
            connect();
            startResponseListener();
            
            Thread.sleep(1000);
            
            runTestSuite();
            
            System.out.println("\nWaiting for responses (15 seconds)...");
            Thread.sleep(15000);
            
        } finally {
            shutdown();
            printSummary();
        }
        
        System.out.println("\nTest complete.");
    }
    
    private void connect() throws Exception {
        System.out.println("Connecting to IBM MQ...");
        
        JmsFactoryFactory ff = JmsFactoryFactory.getInstance(WMQConstants.WMQ_PROVIDER);
        JmsConnectionFactory cf = ff.createConnectionFactory();
        
        cf.setStringProperty(WMQConstants.WMQ_HOST_NAME, HOST);
        cf.setIntProperty(WMQConstants.WMQ_PORT, PORT);
        cf.setStringProperty(WMQConstants.WMQ_CHANNEL, CHANNEL);
        cf.setIntProperty(WMQConstants.WMQ_CONNECTION_MODE, WMQConstants.WMQ_CM_CLIENT);
        cf.setStringProperty(WMQConstants.WMQ_QUEUE_MANAGER, QUEUE_MANAGER);
        
        connection = cf.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        Queue requestQueue = session.createQueue("queue:///" + REQUEST_QUEUE);
        Queue responseQueue = session.createQueue("queue:///" + RESPONSE_QUEUE);
        
        producer = session.createProducer(requestQueue);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        
        consumer = session.createConsumer(responseQueue);
        
        connection.start();
        System.out.println("✓ Connected to IBM MQ");
    }
    
    private void startResponseListener() throws JMSException {
        consumer.setMessageListener(message -> {
            try {
                if (message instanceof TextMessage) {
                    String responseText = ((TextMessage) message).getText();
                    JsonNode response = objectMapper.readTree(responseText);
                    
                    // Parse nested response
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
    
    private void runTestSuite() throws Exception {
        System.out.println("\n════════════════════════════════════════════════════════════");
        System.out.println("STARTING TEST SUITE");
        System.out.println("════════════════════════════════════════════════════════════");
        
        // Test 1: SET
        System.out.println("\n--- Test 1: SET operation ---");
        ObjectNode setRequest = createRequest("SET", TEST_REGION, "ibmmq-test:user:1001");
        ObjectNode userData = objectMapper.createObjectNode();
        userData.put("name", "John Doe");
        userData.put("email", "john@example.com");
        userData.put("broker", "ibmmq");
        setRequest.set("value", userData);
        setRequest.put("ttl", 300);
        publishRequest(setRequest);
        Thread.sleep(1000);
        
        // Test 2: GET
        System.out.println("\n--- Test 2: GET operation ---");
        publishRequest(createRequest("GET", TEST_REGION, "ibmmq-test:user:1001"));
        Thread.sleep(1000);
        
        // Test 3: EXISTS
        System.out.println("\n--- Test 3: EXISTS operation ---");
        publishRequest(createRequest("EXISTS", TEST_REGION, "ibmmq-test:user:1001"));
        Thread.sleep(1000);
        
        // Test 4: SET another
        System.out.println("\n--- Test 4: SET another key ---");
        ObjectNode setRequest2 = createRequest("SET", TEST_REGION, "ibmmq-test:user:1002");
        ObjectNode userData2 = objectMapper.createObjectNode();
        userData2.put("name", "Jane Smith");
        userData2.put("role", "admin");
        setRequest2.set("value", userData2);
        publishRequest(setRequest2);
        Thread.sleep(1000);
        
        // Test 5: KEYS
        System.out.println("\n--- Test 5: KEYS operation ---");
        publishRequest(createRequest("KEYS", TEST_REGION, "ibmmq-test:*"));
        Thread.sleep(1000);
        
        // Test 6: DELETE
        System.out.println("\n--- Test 6: DELETE operation ---");
        publishRequest(createRequest("DELETE", TEST_REGION, "ibmmq-test:user:1002"));
        Thread.sleep(1000);
        
        // Test 7: GET non-existent
        System.out.println("\n--- Test 7: GET non-existent key ---");
        publishRequest(createRequest("GET", TEST_REGION, "ibmmq-test:nonexistent:key"));
        Thread.sleep(1000);
        
        System.out.println("\n✓ All requests sent");
    }
    
    private ObjectNode createRequest(String operation, String region, String key) {
        String messageId = "req-" + UUID.randomUUID().toString().substring(0, 12);
        
        ObjectNode request = objectMapper.createObjectNode();
        request.put("operation", operation);
        request.put("region", region);
        request.put("key", key);
        request.put("api_key", API_KEY);
        request.put("message_id", messageId);
        
        pendingRequests.put(messageId, new CompletableFuture<>());
        return request;
    }
    
    private void publishRequest(ObjectNode request) throws Exception {
        String messageId = request.get("message_id").asText();
        String requestJson = objectMapper.writeValueAsString(request);
        
        System.out.println("📤 SENDING REQUEST");
        System.out.println("   Message ID: " + messageId);
        System.out.println("   Operation: " + request.get("operation").asText());
        System.out.println("   Key: " + request.get("key").asText());
        
        TextMessage message = session.createTextMessage(requestJson);
        message.setJMSCorrelationID(messageId);
        producer.send(message);
        
        System.out.println("   ✓ Sent");
    }
    
    private void shutdown() {
        System.out.println("\nShutting down...");
        running.set(false);
        
        try {
            if (producer != null) producer.close();
            if (consumer != null) consumer.close();
            if (session != null) session.close();
            if (connection != null) connection.close();
        } catch (Exception e) {
            System.err.println("Error closing connections: " + e.getMessage());
        }
    }
    
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
                return responseNode != null && responseNode.has("success") && 
                    responseNode.get("success").asBoolean();
            })
            .count();
        long errorCount = receivedResponses.size() - successCount;
        
        System.out.println("\nSuccessful operations: " + successCount);
        System.out.println("Failed operations: " + errorCount);
        System.out.println("════════════════════════════════════════════════════════════");
    }
}
