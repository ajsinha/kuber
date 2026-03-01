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

import lombok.extern.slf4j.Slf4j;

import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Apache ActiveMQ message broker adapter.
 * 
 * <p>Implements message consumption and publishing for ActiveMQ queues/topics.
 * Supports pause/resume for backpressure control.</p>
 * 
 * @version 2.6.3
 */
@Slf4j
public class ActiveMqBrokerAdapter implements MessageBrokerAdapter {
    
    private final String brokerName;
    private final MessagingConfig.BrokerConfig config;
    
    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private final Map<String, MessageConsumer> consumers = new ConcurrentHashMap<>();
    private Consumer<ReceivedMessage> messageHandler;
    
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    
    private final AtomicLong messagesReceived = new AtomicLong(0);
    private final AtomicLong messagesPublished = new AtomicLong(0);
    private final AtomicLong errors = new AtomicLong(0);
    
    private List<String> subscribedTopics = new ArrayList<>();
    
    public ActiveMqBrokerAdapter(String brokerName, MessagingConfig.BrokerConfig config) {
        this.brokerName = brokerName;
        this.config = config;
    }
    
    @Override
    public String getBrokerType() {
        return "activemq";
    }
    
    @Override
    public String getBrokerName() {
        return brokerName;
    }
    
    @Override
    public boolean connect() {
        String brokerUrl = config.getBrokerUrl();
        if (brokerUrl == null || brokerUrl.isEmpty()) {
            log.error("[{}] ActiveMQ broker_url not configured", brokerName);
            return false;
        }
        
        int maxRetries = 5;
        long retryDelayMs = 3000;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
                
                // Set credentials if provided
                String username = config.getUsername();
                String password = config.getPassword();
                if (username != null && !username.isEmpty()) {
                    factory.setUserName(username);
                    factory.setPassword(password);
                }
                
                log.info("[{}] Connecting to ActiveMQ at {} (attempt {}/{})", brokerName, brokerUrl, attempt, maxRetries);
                
                connection = factory.createConnection();
                connection.start();
                
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                
                connected.set(true);
                log.info("[{}] ✅ Connected to ActiveMQ at {} (attempt {})", brokerName, brokerUrl, attempt);
                return true;
                
            } catch (Exception e) {
                if (attempt < maxRetries) {
                    log.warn("[{}] ActiveMQ connection attempt {}/{} failed: {} — retrying in {}ms",
                            brokerName, attempt, maxRetries, e.getMessage(), retryDelayMs);
                    try { Thread.sleep(retryDelayMs); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                    retryDelayMs = Math.min(retryDelayMs * 2, 15000);
                } else {
                    log.error("[{}] ❌ Failed to connect to ActiveMQ after {} attempts: {}", brokerName, maxRetries, e.getMessage(), e);
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
        if (!connected.get() || session == null) {
            log.error("[{}] Cannot subscribe - not connected", brokerName);
            return false;
        }
        
        try {
            this.messageHandler = messageHandler;
            this.subscribedTopics = new ArrayList<>(topics);
            
            for (String topicName : topics) {
                // Determine if it's a queue or topic based on naming convention
                Destination destination;
                if (topicName.startsWith("topic:")) {
                    destination = session.createTopic(topicName.substring(6));
                } else if (topicName.startsWith("queue:")) {
                    destination = session.createQueue(topicName.substring(6));
                } else {
                    // Default to queue for request/response pattern
                    destination = session.createQueue(topicName);
                }
                
                MessageConsumer consumer = session.createConsumer(destination);
                consumer.setMessageListener(message -> handleMessage(topicName, message));
                consumers.put(topicName, consumer);
            }
            
            log.info("[{}] Subscribed to queues/topics: {}", brokerName, topics);
            return true;
            
        } catch (Exception e) {
            log.error("[{}] Failed to subscribe: {}", brokerName, e.getMessage(), e);
            errors.incrementAndGet();
            return false;
        }
    }
    
    private void handleMessage(String topic, Message message) {
        if (paused.get()) {
            // If paused, we can't really "pause" JMS easily, so just process slowly
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        
        try {
            String messageText;
            String messageId;
            
            if (message instanceof TextMessage) {
                messageText = ((TextMessage) message).getText();
            } else if (message instanceof BytesMessage) {
                BytesMessage bytesMessage = (BytesMessage) message;
                byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
                bytesMessage.readBytes(bytes);
                messageText = new String(bytes);
            } else {
                log.warn("[{}] Unsupported message type: {}", brokerName, message.getClass().getName());
                return;
            }
            
            messageId = message.getJMSMessageID();
            long count = messagesReceived.incrementAndGet();
            
            // Log received message details
            log.info("[{}] 📥 MESSAGE RECEIVED #{} from queue/topic '{}' | JMSMessageID: {}",
                    brokerName, count, topic, messageId);
            
            // Log message content (truncated if too long)
            if (messageText != null) {
                if (messageText.length() > 200) {
                    log.debug("[{}]    Content: {}...", brokerName, messageText.substring(0, 200));
                } else {
                    log.debug("[{}]    Content: {}", brokerName, messageText);
                }
            }
            
            ReceivedMessage receivedMessage = new ReceivedMessage(
                topic,
                messageText,
                messageId != null ? messageId : String.valueOf(System.currentTimeMillis()),
                message.getJMSTimestamp(),
                ActiveMqBrokerAdapter.this  // Pass this adapter as the source
            );
            
            if (messageHandler != null) {
                messageHandler.accept(receivedMessage);
            }
            
        } catch (Exception e) {
            log.error("[{}] Error processing message: {}", brokerName, e.getMessage());
            errors.incrementAndGet();
        }
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
        if (!connected.get() || session == null) {
            log.warn("[{}] ActiveMQ connection lost — attempting recovery...", brokerName);
            if (!recoverConnection()) {
                log.error("[{}] ❌ Cannot publish - recovery failed", brokerName);
                return false;
            }
        }
        
        try {
            // Determine destination type
            Destination destination;
            if (responseTopic.startsWith("topic:")) {
                destination = session.createTopic(responseTopic.substring(6));
            } else if (responseTopic.startsWith("queue:")) {
                destination = session.createQueue(responseTopic.substring(6));
            } else {
                // Default to queue
                destination = session.createQueue(responseTopic);
            }
            
            // Create producer on-demand (or reuse)
            MessageProducer msgProducer = session.createProducer(destination);
            msgProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
            
            TextMessage textMessage = session.createTextMessage(message);
            msgProducer.send(textMessage);
            msgProducer.close();
            
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
     * Attempt to recover a broken ActiveMQ connection.
     */
    private boolean recoverConnection() {
        log.info("[{}] Attempting ActiveMQ connection recovery...", brokerName);
        
        // Close old resources
        if (session != null) {
            try { session.close(); } catch (Exception ignored) {}
            session = null;
        }
        if (connection != null) {
            try { connection.close(); } catch (Exception ignored) {}
            connection = null;
        }
        
        String brokerUrl = config.getBrokerUrl();
        int maxRetries = 3;
        long retryDelayMs = 2000;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
                String username = config.getUsername();
                if (username != null && !username.isEmpty()) {
                    factory.setUserName(username);
                    factory.setPassword(config.getPassword());
                }
                
                connection = factory.createConnection();
                connection.start();
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
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
        log.info("[{}] Disconnecting from ActiveMQ...", brokerName);
        
        // Close consumers
        for (MessageConsumer consumer : consumers.values()) {
            try {
                consumer.close();
            } catch (Exception e) {
                log.warn("[{}] Error closing consumer: {}", brokerName, e.getMessage());
            }
        }
        consumers.clear();
        
        // Close session
        if (session != null) {
            try {
                session.close();
            } catch (Exception e) {
                log.warn("[{}] Error closing session: {}", brokerName, e.getMessage());
            }
            session = null;
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
        
        log.info("[{}] Disconnected from ActiveMQ", brokerName);
    }
    
    @Override
    public BrokerStats getStats() {
        return new BrokerStats(
            brokerName,
            "activemq",
            connected.get(),
            paused.get(),
            messagesReceived.get(),
            messagesPublished.get(),
            errors.get(),
            new ArrayList<>(subscribedTopics)
        );
    }
}
