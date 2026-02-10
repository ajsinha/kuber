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
package com.kuber.server.messaging;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Apache Kafka message broker adapter.
 * 
 * <p>Implements message consumption and publishing for Kafka topics.
 * Supports pause/resume for backpressure control.</p>
 * 
 * @version 2.1.0
 */
@Slf4j
public class KafkaBrokerAdapter implements MessageBrokerAdapter {
    
    private final String brokerName;
    private final MessagingConfig.BrokerConfig config;
    
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;
    private Thread consumerThread;
    private Consumer<ReceivedMessage> messageHandler;
    
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    private final AtomicLong messagesReceived = new AtomicLong(0);
    private final AtomicLong messagesPublished = new AtomicLong(0);
    private final AtomicLong errors = new AtomicLong(0);
    
    private List<String> subscribedTopics = new ArrayList<>();
    
    public KafkaBrokerAdapter(String brokerName, MessagingConfig.BrokerConfig config) {
        this.brokerName = brokerName;
        this.config = config;
    }
    
    @Override
    public String getBrokerType() {
        return "kafka";
    }
    
    @Override
    public String getBrokerName() {
        return brokerName;
    }
    
    @Override
    public boolean connect() {
        try {
            String bootstrapServers = config.getBootstrapServers();
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                log.error("[{}] Kafka bootstrap_servers not configured", brokerName);
                return false;
            }
            
            log.info("[{}] Checking Kafka broker availability at: {}", brokerName, bootstrapServers);
            
            // FIRST: Check if broker is actually reachable before creating consumer/producer
            // This prevents continuous reconnection attempts and log spam
            if (!isBrokerAvailable(bootstrapServers)) {
                log.error("[{}] ❌ Kafka broker is NOT AVAILABLE at: {}", brokerName, bootstrapServers);
                log.error("[{}] ❌ Connection NOT established. To fix:", brokerName);
                log.error("[{}]    1. Start Kafka broker at {}", brokerName, bootstrapServers);
                log.error("[{}]    2. Or disable this broker in request_response.json", brokerName);
                log.error("[{}]    3. Or update bootstrap_servers to correct address", brokerName);
                return false;
            }
            
            log.info("[{}] ✓ Broker is available, connecting to Kafka at: {}", brokerName, bootstrapServers);
            
            // Consumer properties
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getAutoOffsetReset());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
            
            // Add any additional connection properties
            for (String key : config.getConnection().keySet()) {
                if (key.startsWith("consumer.")) {
                    consumerProps.put(key.substring(9), config.getConnection().get(key));
                }
            }
            
            consumer = new KafkaConsumer<>(consumerProps);
            log.info("[{}] Consumer created with group_id: {}", brokerName, config.getGroupId());
            
            // Producer properties
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.ACKS_CONFIG, "all");  // Changed from "1" to "all" for stronger guarantees
            producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
            producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);  // Don't batch, send immediately
            producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);  // Don't batch
            
            // Add any additional connection properties
            for (String key : config.getConnection().keySet()) {
                if (key.startsWith("producer.")) {
                    producerProps.put(key.substring(9), config.getConnection().get(key));
                }
            }
            
            producer = new KafkaProducer<>(producerProps);
            log.info("[{}] Producer created with acks=all", brokerName);
            
            connected.set(true);
            log.info("[{}] ✅ Connected to Kafka at {}", brokerName, bootstrapServers);
            
            // Test publish to verify producer works
            testPublish();
            
            return true;
            
        } catch (Exception e) {
            log.error("[{}] ❌ Failed to connect to Kafka: {}", brokerName, e.getMessage(), e);
            errors.incrementAndGet();
            return false;
        }
    }
    
    /**
     * Check if Kafka broker is available by attempting to connect via AdminClient.
     * Uses a short timeout to avoid blocking.
     * 
     * @param bootstrapServers the Kafka bootstrap servers
     * @return true if broker is reachable, false otherwise
     */
    private boolean isBrokerAvailable(String bootstrapServers) {
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(org.apache.kafka.clients.admin.AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        props.put(org.apache.kafka.clients.admin.AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
        
        try (org.apache.kafka.clients.admin.AdminClient adminClient = 
                org.apache.kafka.clients.admin.AdminClient.create(props)) {
            // Try to get cluster info with a 5 second timeout
            adminClient.describeCluster().clusterId().get(5, java.util.concurrent.TimeUnit.SECONDS);
            return true;
        } catch (java.util.concurrent.TimeoutException e) {
            log.warn("[{}] Broker availability check timed out after 5 seconds", brokerName);
            return false;
        } catch (java.util.concurrent.ExecutionException e) {
            log.warn("[{}] Broker availability check failed: {}", brokerName, 
                    e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            return false;
        } catch (Exception e) {
            log.warn("[{}] Broker availability check error: {}", brokerName, e.getMessage());
            return false;
        }
    }
    
    /**
     * Test publish to verify producer connectivity.
     */
    private void testPublish() {
        String testTopic = "ccs_cache_response";  // Same topic we'll use for responses
        String testMessage = "{\"type\":\"KUBER_TEST\",\"timestamp\":\"" + java.time.Instant.now() + "\",\"message\":\"Kuber producer connectivity test\"}";
        
        log.info("[{}] ========================================", brokerName);
        log.info("[{}] 🔬 STARTUP TEST: Publishing to '{}'", brokerName, testTopic);
        log.info("[{}] 🔬 This verifies Kafka producer is working", brokerName);
        log.info("[{}] ========================================", brokerName);
        
        try {
            log.info("[{}] 🔬 Creating test ProducerRecord...", brokerName);
            ProducerRecord<String, String> record = new ProducerRecord<>(testTopic, "kuber-test", testMessage);
            
            log.info("[{}] 🔬 Calling producer.send()...", brokerName);
            java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> future = producer.send(record);
            
            log.info("[{}] 🔬 Calling producer.flush()...", brokerName);
            producer.flush();
            
            log.info("[{}] 🔬 Waiting for Kafka confirmation (10s timeout)...", brokerName);
            org.apache.kafka.clients.producer.RecordMetadata metadata = future.get(10, java.util.concurrent.TimeUnit.SECONDS);
            
            log.info("[{}] ========================================", brokerName);
            log.info("[{}] 🔬 ✅ TEST PUBLISH SUCCESSFUL", brokerName);
            log.info("[{}] 🔬    Topic: {}", brokerName, testTopic);
            log.info("[{}] 🔬    Partition: {}", brokerName, metadata.partition());
            log.info("[{}] 🔬    Offset: {}", brokerName, metadata.offset());
            log.info("[{}] 🔬 Kafka producer is WORKING!", brokerName);
            log.info("[{}] ========================================", brokerName);
            
        } catch (java.util.concurrent.TimeoutException e) {
            log.error("[{}] ========================================", brokerName);
            log.error("[{}] 🔬 ❌ TEST PUBLISH TIMEOUT after 10 seconds!", brokerName);
            log.error("[{}] 🔬 Kafka broker may be unreachable or slow", brokerName);
            log.error("[{}] ========================================", brokerName);
        } catch (Exception e) {
            log.error("[{}] ========================================", brokerName);
            log.error("[{}] 🔬 ❌ TEST PUBLISH FAILED: {}", brokerName, e.getMessage());
            log.error("[{}] 🔬 Exception type: {}", brokerName, e.getClass().getName());
            if (e.getCause() != null) {
                log.error("[{}] 🔬 Cause: {}", brokerName, e.getCause().getMessage());
            }
            log.error("[{}] ========================================", brokerName, e);
        }
    }
    
    @Override
    public boolean isConnected() {
        return connected.get();
    }
    
    @Override
    public boolean subscribe(List<String> topics, Consumer<ReceivedMessage> messageHandler) {
        if (!connected.get() || consumer == null) {
            log.error("[{}] Cannot subscribe - not connected", brokerName);
            return false;
        }
        
        try {
            this.messageHandler = messageHandler;
            this.subscribedTopics = new ArrayList<>(topics);
            
            consumer.subscribe(topics);
            
            // Start consumer thread
            running.set(true);
            consumerThread = new Thread(this::consumeLoop, "kafka-consumer-" + brokerName);
            consumerThread.setDaemon(true);
            consumerThread.start();
            
            log.info("[{}] Subscribed to topics: {}", brokerName, topics);
            return true;
            
        } catch (Exception e) {
            log.error("[{}] Failed to subscribe to topics: {}", brokerName, e.getMessage(), e);
            errors.incrementAndGet();
            return false;
        }
    }
    
    private void consumeLoop() {
        log.info("[{}] Consumer loop started", brokerName);
        
        while (running.get()) {
            try {
                // Check if paused (backpressure)
                if (paused.get()) {
                    Thread.sleep(100);
                    continue;
                }
                
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    if (!running.get()) break;
                    
                    // Skip if paused
                    if (paused.get()) break;
                    
                    try {
                        String messageKey = record.key() != null ? record.key() : String.valueOf(record.offset());
                        long count = messagesReceived.incrementAndGet();
                        
                        // Log received message details
                        log.info("[{}] 📥 MESSAGE RECEIVED #{} from topic '{}' | Partition: {} | Offset: {} | Key: {}",
                                brokerName, count, record.topic(), record.partition(), record.offset(), messageKey);
                        
                        // Log message content (truncated if too long)
                        String value = record.value();
                        if (value != null) {
                            if (value.length() > 200) {
                                log.debug("[{}]    Content: {}...", brokerName, value.substring(0, 200));
                            } else {
                                log.debug("[{}]    Content: {}", brokerName, value);
                            }
                        }
                        
                        ReceivedMessage msg = new ReceivedMessage(
                            record.topic(),
                            record.value(),
                            messageKey,
                            record.timestamp(),
                            KafkaBrokerAdapter.this  // Pass this adapter as the source
                        );
                        
                        if (messageHandler != null) {
                            messageHandler.accept(msg);
                        }
                        
                    } catch (Exception e) {
                        log.error("[{}] Error processing message: {}", brokerName, e.getMessage());
                        errors.incrementAndGet();
                    }
                }
                
            } catch (Exception e) {
                if (running.get()) {
                    log.error("[{}] Error in consumer loop: {}", brokerName, e.getMessage());
                    errors.incrementAndGet();
                    
                    // Brief pause before retry
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        
        log.info("[{}] Consumer loop stopped", brokerName);
    }
    
    @Override
    public void pauseConsumption() {
        paused.set(true);
        log.info("[{}] Message consumption paused (backpressure)", brokerName);
    }
    
    @Override
    public void resumeConsumption() {
        paused.set(false);
        log.info("[{}] Message consumption resumed", brokerName);
    }
    
    @Override
    public boolean isPaused() {
        return paused.get();
    }
    
    @Override
    public boolean publish(String responseTopic, String message) {
        log.info("[{}] ========== PUBLISH ATTEMPT ==========", brokerName);
        log.info("[{}] Target topic: '{}'", brokerName, responseTopic);
        log.info("[{}] Message length: {} bytes", brokerName, message.length());
        log.info("[{}] connected.get() = {}", brokerName, connected.get());
        log.info("[{}] producer = {}", brokerName, producer != null ? "NOT NULL" : "NULL");
        
        if (!connected.get()) {
            log.error("[{}] ❌ PUBLISH ABORTED - connected=false", brokerName);
            return false;
        }
        
        if (producer == null) {
            log.error("[{}] ❌ PUBLISH ABORTED - producer is NULL", brokerName);
            return false;
        }
        
        try {
            log.info("[{}] 📤 Creating ProducerRecord for topic '{}'", brokerName, responseTopic);
            
            // Log message content for debugging (truncate if too long)
            if (message.length() > 500) {
                log.info("[{}] 📤 MESSAGE CONTENT (first 500 chars): {}", brokerName, message.substring(0, 500));
            } else {
                log.info("[{}] 📤 MESSAGE CONTENT: {}", brokerName, message);
            }
            
            ProducerRecord<String, String> record = new ProducerRecord<>(responseTopic, message);
            log.info("[{}] 📤 ProducerRecord created, calling producer.send()...", brokerName);
            
            // Use synchronous send with get() to ensure message is actually sent
            java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> future = producer.send(record);
            log.info("[{}] 📤 producer.send() returned future, calling flush()...", brokerName);
            
            // Flush to ensure message is sent immediately
            producer.flush();
            log.info("[{}] 📤 flush() complete, calling future.get() with 5s timeout...", brokerName);
            
            // Wait for confirmation (with timeout)
            org.apache.kafka.clients.producer.RecordMetadata metadata = future.get(5, java.util.concurrent.TimeUnit.SECONDS);
            
            long count = messagesPublished.incrementAndGet();
            log.info("[{}] ✅ MESSAGE PUBLISHED #{} | topic='{}' | partition={} | offset={} | bytes={}",
                    brokerName, count, responseTopic, metadata.partition(), metadata.offset(), message.length());
            log.info("[{}] ========== PUBLISH SUCCESS ==========", brokerName);
            
            return true;
            
        } catch (java.util.concurrent.TimeoutException e) {
            log.error("[{}] ❌ TIMEOUT waiting for Kafka ack after 5 seconds", brokerName);
            log.error("[{}] ❌ This usually means Kafka broker is not responding", brokerName);
            errors.incrementAndGet();
            return false;
        } catch (java.util.concurrent.ExecutionException e) {
            log.error("[{}] ❌ EXECUTION EXCEPTION during publish: {}", brokerName, e.getMessage());
            log.error("[{}] ❌ Cause: {}", brokerName, e.getCause() != null ? e.getCause().getMessage() : "unknown");
            errors.incrementAndGet();
            return false;
        } catch (Exception e) {
            log.error("[{}] ❌ UNEXPECTED EXCEPTION during publish: {} - {}", brokerName, e.getClass().getSimpleName(), e.getMessage(), e);
            errors.incrementAndGet();
            return false;
        }
    }
    
    @Override
    public List<String> getSubscribedTopics() {
        return Collections.unmodifiableList(subscribedTopics);
    }
    
    @Override
    public void disconnect() {
        log.info("[{}] Disconnecting from Kafka...", brokerName);
        
        running.set(false);
        
        // Wait for consumer thread to stop
        if (consumerThread != null) {
            try {
                consumerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        // Close consumer
        if (consumer != null) {
            try {
                consumer.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                log.warn("[{}] Error closing consumer: {}", brokerName, e.getMessage());
            }
            consumer = null;
        }
        
        // Close producer
        if (producer != null) {
            try {
                producer.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                log.warn("[{}] Error closing producer: {}", brokerName, e.getMessage());
            }
            producer = null;
        }
        
        connected.set(false);
        subscribedTopics.clear();
        
        log.info("[{}] Disconnected from Kafka", brokerName);
    }
    
    @Override
    public BrokerStats getStats() {
        return new BrokerStats(
            brokerName,
            "kafka",
            connected.get(),
            paused.get(),
            messagesReceived.get(),
            messagesPublished.get(),
            errors.get(),
            new ArrayList<>(subscribedTopics)
        );
    }
}
