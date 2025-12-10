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
 * @version 1.7.0
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
            
            // Producer properties
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
            producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
            
            // Add any additional connection properties
            for (String key : config.getConnection().keySet()) {
                if (key.startsWith("producer.")) {
                    producerProps.put(key.substring(9), config.getConnection().get(key));
                }
            }
            
            producer = new KafkaProducer<>(producerProps);
            
            connected.set(true);
            log.info("[{}] Connected to Kafka at {}", brokerName, bootstrapServers);
            return true;
            
        } catch (Exception e) {
            log.error("[{}] Failed to connect to Kafka: {}", brokerName, e.getMessage(), e);
            errors.incrementAndGet();
            return false;
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
                        ReceivedMessage msg = new ReceivedMessage(
                            record.topic(),
                            record.value(),
                            record.key() != null ? record.key() : String.valueOf(record.offset()),
                            record.timestamp()
                        );
                        
                        messagesReceived.incrementAndGet();
                        
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
        if (!connected.get() || producer == null) {
            log.error("[{}] Cannot publish - not connected", brokerName);
            return false;
        }
        
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(responseTopic, message);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("[{}] Failed to publish to {}: {}", brokerName, responseTopic, exception.getMessage());
                    errors.incrementAndGet();
                } else {
                    messagesPublished.incrementAndGet();
                }
            });
            
            return true;
            
        } catch (Exception e) {
            log.error("[{}] Failed to publish to {}: {}", brokerName, responseTopic, e.getMessage());
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
