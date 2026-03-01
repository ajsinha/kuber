/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
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
 * Confluent Kafka message broker adapter.
 * 
 * <p>Implements message consumption and publishing for Confluent Cloud (or Confluent Platform)
 * Kafka clusters. Uses SASL_SSL with PLAIN mechanism for API key/secret authentication.
 * Supports pause/resume for backpressure control.</p>
 * 
 * <p>Broker type: {@code confluent-kafka}</p>
 * 
 * @version 2.6.3
 * @since 2.6.3
 */
@Slf4j
public class ConfluentKafkaBrokerAdapter implements MessageBrokerAdapter {
    
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
    
    public ConfluentKafkaBrokerAdapter(String brokerName, MessagingConfig.BrokerConfig config) {
        this.brokerName = brokerName;
        this.config = config;
    }
    
    @Override
    public String getBrokerType() {
        return "confluent-kafka";
    }
    
    @Override
    public String getBrokerName() {
        return brokerName;
    }
    
    @Override
    public boolean connect() {
        String bootstrapServers = config.getBootstrapServers();
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            log.error("[{}] Confluent Kafka bootstrap_servers not configured", brokerName);
            return false;
        }
        
        String apiKey = config.getApiKey();
        String apiSecret = config.getApiSecret();
        if (apiKey == null || apiKey.isEmpty() || apiSecret == null || apiSecret.isEmpty()) {
            log.error("[{}] Confluent Kafka api_key and api_secret are required", brokerName);
            return false;
        }
        
        int maxRetries = 5;
        long retryDelayMs = 3000;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.info("[{}] Connecting to Confluent Kafka at {} (attempt {}/{})", 
                        brokerName, bootstrapServers, attempt, maxRetries);
                
                // SASL_SSL JAAS config for Confluent Cloud
                String jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"" + apiKey + "\" password=\"" + apiSecret + "\";";
                
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
                consumerProps.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, "1000");
                consumerProps.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");
                consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
                // Confluent Cloud SASL_SSL authentication
                consumerProps.put("security.protocol", "SASL_SSL");
                consumerProps.put("sasl.mechanism", "PLAIN");
                consumerProps.put("sasl.jaas.config", jaasConfig);
                
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
                producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
                producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
                producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 0);
                producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
                producerProps.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000);
                producerProps.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000);
                producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
                producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
                // Confluent Cloud SASL_SSL authentication
                producerProps.put("security.protocol", "SASL_SSL");
                producerProps.put("sasl.mechanism", "PLAIN");
                producerProps.put("sasl.jaas.config", jaasConfig);
                
                // Add any additional connection properties
                for (String key : config.getConnection().keySet()) {
                    if (key.startsWith("producer.")) {
                        producerProps.put(key.substring(9), config.getConnection().get(key));
                    }
                }
                
                producer = new KafkaProducer<>(producerProps);
                log.info("[{}] Producer created with acks=all", brokerName);
                
                connected.set(true);
                log.info("[{}] ✅ Connected to Confluent Kafka at {} [SASL_SSL] (attempt {})", 
                        brokerName, bootstrapServers, attempt);
                return true;
                
            } catch (Exception e) {
                if (attempt < maxRetries) {
                    log.warn("[{}] Confluent Kafka connection attempt {}/{} failed: {} — retrying in {}ms",
                            brokerName, attempt, maxRetries, e.getMessage(), retryDelayMs);
                    try { Thread.sleep(retryDelayMs); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                    retryDelayMs = Math.min(retryDelayMs * 2, 15000);
                } else {
                    log.error("[{}] ❌ Failed to connect to Confluent Kafka after {} attempts: {}", 
                            brokerName, maxRetries, e.getMessage(), e);
                    errors.incrementAndGet();
                    return false;
                }
            }
        }
        return false;
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
            log.info("[{}] Subscribed to topics: {}", brokerName, topics);
            
            // Start consumer thread
            running.set(true);
            consumerThread = new Thread(this::consumeMessages, "confluent-kafka-consumer-" + brokerName);
            consumerThread.setDaemon(true);
            consumerThread.start();
            
            return true;
            
        } catch (Exception e) {
            log.error("[{}] Failed to subscribe: {}", brokerName, e.getMessage(), e);
            errors.incrementAndGet();
            return false;
        }
    }
    
    private void consumeMessages() {
        log.info("[{}] Consumer loop started", brokerName);
        
        while (running.get()) {
            try {
                if (paused.get()) {
                    Thread.sleep(100);
                    continue;
                }
                
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        String value = record.value();
                        String messageKey = record.key();
                        
                        long count = messagesReceived.incrementAndGet();
                        log.info("[{}] 📥 MESSAGE RECEIVED #{} from topic '{}' | partition={} | offset={} | key={}",
                                brokerName, count, record.topic(), record.partition(), record.offset(), messageKey);
                        
                        ReceivedMessage msg = new ReceivedMessage(
                            record.topic(),
                            value,
                            messageKey,
                            record.timestamp(),
                            ConfluentKafkaBrokerAdapter.this
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
                    try { Thread.sleep(1000); } catch (InterruptedException ie) {
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
        // v2.6.3: Try to recover if connection was lost
        if (!connected.get() || producer == null) {
            log.warn("[{}] Connection lost — attempting recovery...", brokerName);
            if (!recoverConnection()) {
                log.error("[{}] ❌ PUBLISH ABORTED - recovery failed", brokerName);
                return false;
            }
        }
        
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(responseTopic, message);
            
            java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> future = producer.send(record);
            producer.flush();
            org.apache.kafka.clients.producer.RecordMetadata metadata = future.get(5, java.util.concurrent.TimeUnit.SECONDS);
            
            long count = messagesPublished.incrementAndGet();
            log.info("[{}] ✅ MESSAGE PUBLISHED #{} | topic='{}' | partition={} | offset={} | bytes={}",
                    brokerName, count, responseTopic, metadata.partition(), metadata.offset(), message.length());
            
            return true;
            
        } catch (java.util.concurrent.TimeoutException e) {
            log.error("[{}] ❌ TIMEOUT waiting for Confluent Kafka ack after 5 seconds", brokerName);
            errors.incrementAndGet();
            markDisconnectedForRecovery();
            return false;
        } catch (java.util.concurrent.ExecutionException e) {
            log.error("[{}] ❌ EXECUTION EXCEPTION during publish: {}", brokerName, e.getMessage());
            errors.incrementAndGet();
            markDisconnectedForRecovery();
            return false;
        } catch (Exception e) {
            log.error("[{}] ❌ UNEXPECTED EXCEPTION during publish: {} - {}", 
                    brokerName, e.getClass().getSimpleName(), e.getMessage(), e);
            errors.incrementAndGet();
            markDisconnectedForRecovery();
            return false;
        }
    }
    
    private void markDisconnectedForRecovery() {
        connected.set(false);
        log.warn("[{}] Marked as disconnected — next operation will attempt recovery", brokerName);
    }
    
    private boolean recoverConnection() {
        String bootstrapServers = config.getBootstrapServers();
        String apiKey = config.getApiKey();
        String apiSecret = config.getApiSecret();
        
        log.info("[{}] Attempting Confluent Kafka connection recovery to {}", brokerName, bootstrapServers);
        
        if (producer != null) {
            try { producer.close(Duration.ofSeconds(3)); } catch (Exception ignored) {}
            producer = null;
        }
        
        String jaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"" + apiKey + "\" password=\"" + apiSecret + "\";";
        
        int maxRetries = 3;
        long retryDelayMs = 2000;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                Properties producerProps = new Properties();
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
                producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
                producerProps.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000);
                producerProps.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000);
                producerProps.put("security.protocol", "SASL_SSL");
                producerProps.put("sasl.mechanism", "PLAIN");
                producerProps.put("sasl.jaas.config", jaasConfig);
                
                producer = new KafkaProducer<>(producerProps);
                connected.set(true);
                log.info("[{}] ✅ Connection recovered successfully (attempt {})", brokerName, attempt);
                return true;
                
            } catch (Exception e) {
                log.warn("[{}] Recovery attempt {}/{} failed: {}", brokerName, attempt, maxRetries, e.getMessage());
                if (attempt < maxRetries) {
                    try { Thread.sleep(retryDelayMs); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                    retryDelayMs *= 2;
                }
            }
        }
        
        log.error("[{}] ❌ Connection recovery failed after {} attempts", brokerName, maxRetries);
        return false;
    }
    
    @Override
    public List<String> getSubscribedTopics() {
        return Collections.unmodifiableList(subscribedTopics);
    }
    
    @Override
    public void disconnect() {
        log.info("[{}] Disconnecting from Confluent Kafka...", brokerName);
        
        running.set(false);
        
        if (consumerThread != null) {
            try { consumerThread.join(5000); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        if (consumer != null) {
            try { consumer.close(Duration.ofSeconds(5)); } catch (Exception e) {
                log.warn("[{}] Error closing consumer: {}", brokerName, e.getMessage());
            }
            consumer = null;
        }
        
        if (producer != null) {
            try { producer.close(Duration.ofSeconds(5)); } catch (Exception e) {
                log.warn("[{}] Error closing producer: {}", brokerName, e.getMessage());
            }
            producer = null;
        }
        
        connected.set(false);
        subscribedTopics.clear();
        
        log.info("[{}] Disconnected from Confluent Kafka", brokerName);
    }
    
    @Override
    public BrokerStats getStats() {
        return new BrokerStats(
            brokerName,
            "confluent-kafka",
            connected.get(),
            paused.get(),
            messagesReceived.get(),
            messagesPublished.get(),
            errors.get(),
            new ArrayList<>(subscribedTopics)
        );
    }
}
