/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.server.messaging;

import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * RabbitMQ message broker adapter.
 * 
 * <p>Implements message consumption and publishing for RabbitMQ queues.
 * Supports pause/resume for backpressure control using basic.qos.</p>
 * 
 * @version 2.6.4
 */
@Slf4j
public class RabbitMqBrokerAdapter implements MessageBrokerAdapter {
    
    private final String brokerName;
    private final MessagingConfig.BrokerConfig config;
    
    private Connection connection;
    private Channel channel;
    private Consumer<ReceivedMessage> messageHandler;
    private final Map<String, String> consumerTags = new ConcurrentHashMap<>();
    
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    
    private final AtomicLong messagesReceived = new AtomicLong(0);
    private final AtomicLong messagesPublished = new AtomicLong(0);
    private final AtomicLong errors = new AtomicLong(0);
    
    private List<String> subscribedTopics = new ArrayList<>();
    
    public RabbitMqBrokerAdapter(String brokerName, MessagingConfig.BrokerConfig config) {
        this.brokerName = brokerName;
        this.config = config;
    }
    
    @Override
    public String getBrokerType() {
        return "rabbitmq";
    }
    
    @Override
    public String getBrokerName() {
        return brokerName;
    }
    
    @Override
    public boolean connect() {
        int maxRetries = 5;
        long retryDelayMs = 3000;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                ConnectionFactory factory = new ConnectionFactory();
                
                factory.setHost(config.getHost());
                factory.setPort(config.getPort());
                factory.setVirtualHost(config.getVirtualHost());
                
                String username = config.getUsername();
                String password = config.getPassword();
                if (username != null && !username.isEmpty()) {
                    factory.setUsername(username);
                    factory.setPassword(password);
                }
                
                // Additional connection properties
                if (config.getConnection().containsKey("connection_timeout")) {
                    factory.setConnectionTimeout(Integer.parseInt(config.getConnection().get("connection_timeout")));
                } else {
                    factory.setConnectionTimeout(10000);
                }
                if (config.getConnection().containsKey("requested_heartbeat")) {
                    factory.setRequestedHeartbeat(Integer.parseInt(config.getConnection().get("requested_heartbeat")));
                }
                
                // v2.6.4: Enable automatic recovery for resilience
                factory.setAutomaticRecoveryEnabled(true);
                factory.setNetworkRecoveryInterval(5000);
                
                log.info("[{}] Connecting to RabbitMQ at {}:{}{} (attempt {}/{})", 
                        brokerName, config.getHost(), config.getPort(), config.getVirtualHost(), attempt, maxRetries);
                
                connection = factory.newConnection();
                channel = connection.createChannel();
                
                // Set prefetch count for backpressure control
                channel.basicQos(10);
                
                connected.set(true);
                log.info("[{}] ✅ Connected to RabbitMQ at {}:{}{} (attempt {})", 
                        brokerName, config.getHost(), config.getPort(), config.getVirtualHost(), attempt);
                return true;
                
            } catch (Exception e) {
                if (attempt < maxRetries) {
                    log.warn("[{}] RabbitMQ connection attempt {}/{} failed: {} — retrying in {}ms",
                            brokerName, attempt, maxRetries, e.getMessage(), retryDelayMs);
                    try { Thread.sleep(retryDelayMs); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                    retryDelayMs = Math.min(retryDelayMs * 2, 15000);
                } else {
                    log.error("[{}] ❌ Failed to connect to RabbitMQ after {} attempts: {}", brokerName, maxRetries, e.getMessage(), e);
                    errors.incrementAndGet();
                    return false;
                }
            }
        }
        return false;
    }
    
    @Override
    public boolean isConnected() {
        return connected.get() && connection != null && connection.isOpen();
    }
    
    @Override
    public boolean subscribe(List<String> topics, Consumer<ReceivedMessage> messageHandler) {
        if (!connected.get() || channel == null) {
            log.error("[{}] Cannot subscribe - not connected", brokerName);
            return false;
        }
        
        try {
            this.messageHandler = messageHandler;
            this.subscribedTopics = new ArrayList<>(topics);
            
            for (String queueName : topics) {
                // Declare the queue (creates if doesn't exist)
                channel.queueDeclare(queueName, true, false, false, null);
                
                // Create consumer
                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    handleMessage(queueName, delivery);
                };
                
                CancelCallback cancelCallback = consumerTag -> {
                    log.warn("[{}] Consumer cancelled for queue: {}", brokerName, queueName);
                };
                
                String consumerTag = channel.basicConsume(queueName, false, deliverCallback, cancelCallback);
                consumerTags.put(queueName, consumerTag);
            }
            
            log.info("[{}] Subscribed to queues: {}", brokerName, topics);
            return true;
            
        } catch (Exception e) {
            log.error("[{}] Failed to subscribe: {}", brokerName, e.getMessage(), e);
            errors.incrementAndGet();
            return false;
        }
    }
    
    private void handleMessage(String queue, Delivery delivery) {
        // Backpressure check
        if (paused.get()) {
            // Don't ack, let message stay in queue
            try {
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            } catch (IOException e) {
                log.error("[{}] Error nacking message during pause: {}", brokerName, e.getMessage());
            }
            return;
        }
        
        try {
            String messageText = new String(delivery.getBody(), StandardCharsets.UTF_8);
            String messageId = delivery.getProperties().getMessageId();
            
            if (messageId == null) {
                messageId = String.valueOf(delivery.getEnvelope().getDeliveryTag());
            }
            
            long count = messagesReceived.incrementAndGet();
            
            // Log received message details
            log.info("[{}] 📥 MESSAGE RECEIVED #{} from queue '{}' | MessageID: {}",
                    brokerName, count, queue, messageId);
            
            // Log message content (truncated if too long)
            if (messageText != null) {
                if (messageText.length() > 200) {
                    log.debug("[{}]    Content: {}...", brokerName, messageText.substring(0, 200));
                } else {
                    log.debug("[{}]    Content: {}", brokerName, messageText);
                }
            }
            
            ReceivedMessage receivedMessage = new ReceivedMessage(
                queue,
                messageText,
                messageId,
                System.currentTimeMillis(),
                RabbitMqBrokerAdapter.this  // Pass this adapter as the source
            );
            
            if (messageHandler != null) {
                messageHandler.accept(receivedMessage);
            }
            
            // Acknowledge message
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            
        } catch (Exception e) {
            log.error("[{}] Error processing message: {}", brokerName, e.getMessage());
            errors.incrementAndGet();
            
            // Negative acknowledge - requeue message
            try {
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            } catch (IOException ioe) {
                log.error("[{}] Error nacking message: {}", brokerName, ioe.getMessage());
            }
        }
    }
    
    @Override
    public void pauseConsumption() {
        paused.set(true);
        // Set prefetch to 0 to stop receiving new messages
        try {
            if (channel != null && channel.isOpen()) {
                channel.basicQos(0);
            }
        } catch (IOException e) {
            log.error("[{}] Error pausing consumption: {}", brokerName, e.getMessage());
        }
        log.info("[{}] Message consumption paused (backpressure)", brokerName);
    }
    
    @Override
    public void resumeConsumption() {
        paused.set(false);
        // Restore prefetch
        try {
            if (channel != null && channel.isOpen()) {
                channel.basicQos(10);
            }
        } catch (IOException e) {
            log.error("[{}] Error resuming consumption: {}", brokerName, e.getMessage());
        }
        log.info("[{}] Message consumption resumed", brokerName);
    }
    
    @Override
    public boolean isPaused() {
        return paused.get();
    }
    
    @Override
    public boolean publish(String responseTopic, String message) {
        // v2.6.4: Try to recover if connection was lost
        if (!connected.get() || channel == null || !channel.isOpen()) {
            log.warn("[{}] RabbitMQ connection lost — attempting recovery...", brokerName);
            if (!recoverConnection()) {
                log.error("[{}] ❌ Cannot publish - recovery failed", brokerName);
                return false;
            }
        }
        
        try {
            // Declare response queue if needed
            channel.queueDeclare(responseTopic, true, false, false, null);
            
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(2) // Persistent
                .build();
            
            channel.basicPublish("", responseTopic, properties, message.getBytes(StandardCharsets.UTF_8));
            
            long count = messagesPublished.incrementAndGet();
            log.debug("[{}] 📤 MESSAGE PUBLISHED #{} to '{}' | Length: {} bytes",
                    brokerName, count, responseTopic, message.length());
            
            return true;
            
        } catch (Exception e) {
            log.error("[{}] Failed to publish to {}: {}", brokerName, responseTopic, e.getMessage());
            errors.incrementAndGet();
            markDisconnectedForRecovery();
            return false;
        }
    }
    
    /**
     * Mark connection as broken so the next operation triggers recovery.
     */
    private void markDisconnectedForRecovery() {
        connected.set(false);
        log.warn("[{}] Marked as disconnected — next operation will attempt recovery", brokerName);
    }
    
    /**
     * Attempt to recover a broken RabbitMQ connection by recreating the channel.
     */
    private boolean recoverConnection() {
        log.info("[{}] Attempting RabbitMQ connection recovery...", brokerName);
        
        // Close old channel/connection
        if (channel != null) {
            try { channel.close(); } catch (Exception ignored) {}
            channel = null;
        }
        if (connection != null) {
            try { connection.close(); } catch (Exception ignored) {}
            connection = null;
        }
        
        int maxRetries = 3;
        long retryDelayMs = 2000;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost(config.getHost());
                factory.setPort(config.getPort());
                factory.setVirtualHost(config.getVirtualHost());
                factory.setConnectionTimeout(10000);
                factory.setAutomaticRecoveryEnabled(true);
                factory.setNetworkRecoveryInterval(5000);
                
                String username = config.getUsername();
                if (username != null && !username.isEmpty()) {
                    factory.setUsername(username);
                    factory.setPassword(config.getPassword());
                }
                
                connection = factory.newConnection();
                channel = connection.createChannel();
                channel.basicQos(10);
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
        
        log.error("[{}] ❌ Connection recovery failed after retries", brokerName);
        return false;
    }
    
    @Override
    public List<String> getSubscribedTopics() {
        return Collections.unmodifiableList(subscribedTopics);
    }
    
    @Override
    public void disconnect() {
        log.info("[{}] Disconnecting from RabbitMQ...", brokerName);
        
        // Cancel consumers
        for (Map.Entry<String, String> entry : consumerTags.entrySet()) {
            try {
                if (channel != null && channel.isOpen()) {
                    channel.basicCancel(entry.getValue());
                }
            } catch (Exception e) {
                log.warn("[{}] Error cancelling consumer for {}: {}", brokerName, entry.getKey(), e.getMessage());
            }
        }
        consumerTags.clear();
        
        // Close channel
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception e) {
                log.warn("[{}] Error closing channel: {}", brokerName, e.getMessage());
            }
            channel = null;
        }
        
        // Close connection
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                log.warn("[{}] Error closing connection: {}", brokerName, e.getMessage());
            }
            connection = null;
        }
        
        connected.set(false);
        subscribedTopics.clear();
        
        log.info("[{}] Disconnected from RabbitMQ", brokerName);
    }
    
    @Override
    public BrokerStats getStats() {
        return new BrokerStats(
            brokerName,
            "rabbitmq",
            connected.get(),
            paused.get(),
            messagesReceived.get(),
            messagesPublished.get(),
            errors.get(),
            new ArrayList<>(subscribedTopics)
        );
    }
}
