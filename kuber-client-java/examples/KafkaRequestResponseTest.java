/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber Kafka Request/Response Test Client (v1.7.9)
 * 
 * Tests the Kuber Request/Response messaging feature via Apache Kafka.
 * Publishes cache operation requests and subscribes to responses.
 *
 * Prerequisites:
 * - Kafka running on localhost:9092
 * - Kuber server running with messaging enabled
 * - kafka-clients dependency in pom.xml:
 *     <dependency>
 *         <groupId>org.apache.kafka</groupId>
 *         <artifactId>kafka-clients</artifactId>
 *         <version>3.6.0</version>
 *     </dependency>
 */
package com.kuber.client.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test client for Kuber's Kafka-based Request/Response messaging.
 * 
 * <p>Demonstrates how to:
 * <ul>
 *   <li>Publish cache operation requests to Kafka</li>
 *   <li>Subscribe to and receive responses</li>
 *   <li>Correlate requests with responses using message IDs</li>
 * </ul>
 * 
 * @version 1.9.0
 */
public class KafkaRequestResponseTest {
    
    // Configuration
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String REQUEST_TOPIC = "ccs_cache_request";
    private static final String RESPONSE_TOPIC = "ccs_cache_response";
    
    // Your Kuber API key (replace with actual key)
    private static final String API_KEY = "kub_your_api_key_here";
    
    // Test region
    private static final String TEST_REGION = "default";
    
    // JSON mapper
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // Kafka clients
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    
    // State
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Map<String, CompletableFuture<JsonNode>> pendingRequests = new ConcurrentHashMap<>();
    private final List<JsonNode> receivedResponses = new CopyOnWriteArrayList<>();
    private ExecutorService consumerExecutor;
    
    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        System.out.println("╔════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  Kuber Kafka Request/Response Test Client (Java)                   ║");
        System.out.println("║  Tests cache operations via Kafka messaging                        ║");
        System.out.println("╚════════════════════════════════════════════════════════════════════╝");
        System.out.println();
        
        KafkaRequestResponseTest test = new KafkaRequestResponseTest();
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
            // Initialize clients
            initProducer();
            initConsumer();
            
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
     * Initialize Kafka producer.
     */
    private void initProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        
        producer = new KafkaProducer<>(props);
        System.out.println("✓ Producer connected to " + BOOTSTRAP_SERVERS);
    }
    
    /**
     * Initialize Kafka consumer.
     */
    private void initConsumer() {
        String groupId = "kuber-test-client-" + UUID.randomUUID().toString().substring(0, 8);
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(RESPONSE_TOPIC));
        
        System.out.println("✓ Consumer connected (group: " + groupId + ")");
        System.out.println("  Subscribed to: " + RESPONSE_TOPIC);
    }
    
    /**
     * Start background thread to listen for responses.
     */
    private void startResponseListener() {
        consumerExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "response-listener");
            t.setDaemon(true);
            return t;
        });
        
        consumerExecutor.submit(() -> {
            System.out.println("Response listener started");
            
            while (running.get()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            JsonNode response = objectMapper.readTree(record.value());
                            
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
                            
                        } catch (Exception e) {
                            System.err.println("Error parsing response: " + e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        System.err.println("Consumer error: " + e.getMessage());
                    }
                }
            }
            
            System.out.println("Response listener stopped");
        });
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
     * Publish a request to Kafka.
     */
    private CompletableFuture<JsonNode> publishRequest(ObjectNode request) throws Exception {
        String messageId = request.get("message_id").asText();
        String requestJson = objectMapper.writeValueAsString(request);
        
        CompletableFuture<JsonNode> responseFuture = new CompletableFuture<>();
        pendingRequests.put(messageId, responseFuture);
        
        ProducerRecord<String, String> record = new ProducerRecord<>(
            REQUEST_TOPIC, messageId, requestJson);
        
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.err.println("✗ Failed to send: " + exception.getMessage());
                responseFuture.completeExceptionally(exception);
            } else {
                System.out.println("\n📤 REQUEST SENT");
                System.out.println("   Message ID: " + messageId);
                System.out.println("   Operation: " + request.get("operation").asText());
                System.out.println("   Region: " + request.get("region").asText());
                System.out.println("   Key: " + request.get("key").asText());
                System.out.println("   Partition: " + metadata.partition() + 
                                 ", Offset: " + metadata.offset());
            }
        });
        
        producer.flush();
        return responseFuture;
    }
    
    /**
     * Run the test suite.
     */
    private void runTestSuite() throws Exception {
        System.out.println("\n--- Test 1: SET Operation ---");
        ObjectNode setRequest = createRequest("SET", TEST_REGION, 
            "kafka-test:user:1001",
            "{\"name\":\"John Doe\",\"email\":\"john@example.com\",\"timestamp\":\"" + 
                Instant.now() + "\"}",
            3600);
        publishRequest(setRequest);
        Thread.sleep(1000);
        
        System.out.println("\n--- Test 2: GET Operation ---");
        ObjectNode getRequest = createRequest("GET", TEST_REGION, 
            "kafka-test:user:1001", null, null);
        publishRequest(getRequest);
        Thread.sleep(1000);
        
        System.out.println("\n--- Test 3: EXISTS Operation ---");
        ObjectNode existsRequest = createRequest("EXISTS", TEST_REGION, 
            "kafka-test:user:1001", null, null);
        publishRequest(existsRequest);
        Thread.sleep(1000);
        
        System.out.println("\n--- Test 4: SET Another Key ---");
        ObjectNode setRequest2 = createRequest("SET", TEST_REGION, 
            "kafka-test:user:1002",
            "{\"name\":\"Jane Smith\",\"role\":\"admin\"}", null);
        publishRequest(setRequest2);
        Thread.sleep(1000);
        
        System.out.println("\n--- Test 5: KEYS Operation ---");
        ObjectNode keysRequest = createRequest("KEYS", TEST_REGION, 
            "kafka-test:*", null, null);
        publishRequest(keysRequest);
        Thread.sleep(1000);
        
        System.out.println("\n--- Test 6: DELETE Operation ---");
        ObjectNode deleteRequest = createRequest("DELETE", TEST_REGION, 
            "kafka-test:user:1002", null, null);
        publishRequest(deleteRequest);
        Thread.sleep(1000);
        
        System.out.println("\n--- Test 7: GET Non-existent Key ---");
        ObjectNode getMissRequest = createRequest("GET", TEST_REGION, 
            "kafka-test:nonexistent:key", null, null);
        publishRequest(getMissRequest);
        Thread.sleep(1000);
        
        producer.flush();
        System.out.println("\n✓ All requests sent and flushed");
    }
    
    /**
     * Shutdown clients.
     */
    private void shutdown() {
        System.out.println("\nShutting down...");
        running.set(false);
        
        if (consumerExecutor != null) {
            consumerExecutor.shutdown();
            try {
                consumerExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
        
        if (consumer != null) {
            consumer.close(Duration.ofSeconds(5));
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
