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

import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import lombok.extern.slf4j.Slf4j;

import javax.jms.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * IBM MQ message broker adapter.
 * 
 * <p>Implements message consumption and publishing for IBM MQ queues.
 * Supports pause/resume for backpressure control.</p>
 * 
 * @version 2.3.0
 */
@Slf4j
public class IbmMqBrokerAdapter implements MessageBrokerAdapter {
    
    private final String brokerName;
    private final MessagingConfig.BrokerConfig config;
    
    private Connection connection;
    private Session session;
    private final Map<String, MessageConsumer> consumers = new ConcurrentHashMap<>();
    private Consumer<ReceivedMessage> messageHandler;
    
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    
    private final AtomicLong messagesReceived = new AtomicLong(0);
    private final AtomicLong messagesPublished = new AtomicLong(0);
    private final AtomicLong errors = new AtomicLong(0);
    
    private List<String> subscribedTopics = new ArrayList<>();
    
    public IbmMqBrokerAdapter(String brokerName, MessagingConfig.BrokerConfig config) {
        this.brokerName = brokerName;
        this.config = config;
    }
    
    @Override
    public String getBrokerType() {
        return "ibmmq";
    }
    
    @Override
    public String getBrokerName() {
        return brokerName;
    }
    
    @Override
    public boolean connect() {
        try {
            String queueManager = config.getQueueManager();
            String channel = config.getChannel();
            String connName = config.getConnName();
            
            if (queueManager == null || queueManager.isEmpty()) {
                log.error("[{}] IBM MQ queue_manager not configured", brokerName);
                return false;
            }
            
            MQConnectionFactory factory = new MQConnectionFactory();
            factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            factory.setQueueManager(queueManager);
            
            if (channel != null && !channel.isEmpty()) {
                factory.setChannel(channel);
            }
            if (connName != null && !connName.isEmpty()) {
                factory.setConnectionNameList(connName);
            }
            
            // Set credentials if provided
            String username = config.getUsername();
            String password = config.getPassword();
            
            if (username != null && !username.isEmpty()) {
                connection = factory.createConnection(username, password);
            } else {
                connection = factory.createConnection();
            }
            
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            connected.set(true);
            log.info("[{}] Connected to IBM MQ queue manager: {}", brokerName, queueManager);
            return true;
            
        } catch (Exception e) {
            log.error("[{}] Failed to connect to IBM MQ: {}", brokerName, e.getMessage(), e);
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
        if (!connected.get() || session == null) {
            log.error("[{}] Cannot subscribe - not connected", brokerName);
            return false;
        }
        
        try {
            this.messageHandler = messageHandler;
            this.subscribedTopics = new ArrayList<>(topics);
            
            for (String queueName : topics) {
                Queue queue = session.createQueue(queueName);
                MessageConsumer consumer = session.createConsumer(queue);
                consumer.setMessageListener(message -> handleMessage(queueName, message));
                consumers.put(queueName, consumer);
            }
            
            log.info("[{}] Subscribed to queues: {}", brokerName, topics);
            return true;
            
        } catch (Exception e) {
            log.error("[{}] Failed to subscribe: {}", brokerName, e.getMessage(), e);
            errors.incrementAndGet();
            return false;
        }
    }
    
    private void handleMessage(String queue, Message message) {
        if (paused.get()) {
            // Brief pause during backpressure
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
            log.info("[{}] 📥 MESSAGE RECEIVED #{} from queue '{}' | JMSMessageID: {}",
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
                messageId != null ? messageId : String.valueOf(System.currentTimeMillis()),
                message.getJMSTimestamp(),
                IbmMqBrokerAdapter.this  // Pass this adapter as the source
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
        if (!connected.get() || session == null) {
            log.error("[{}] Cannot publish - not connected", brokerName);
            return false;
        }
        
        try {
            Queue responseQueue = session.createQueue(responseTopic);
            MessageProducer producer = session.createProducer(responseQueue);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            
            TextMessage textMessage = session.createTextMessage(message);
            producer.send(textMessage);
            producer.close();
            
            long count = messagesPublished.incrementAndGet();
            log.debug("[{}] 📤 MESSAGE PUBLISHED #{} to '{}' | Length: {} bytes",
                    brokerName, count, responseTopic, message.length());
            
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
        log.info("[{}] Disconnecting from IBM MQ...", brokerName);
        
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
        
        log.info("[{}] Disconnected from IBM MQ", brokerName);
    }
    
    @Override
    public BrokerStats getStats() {
        return new BrokerStats(
            brokerName,
            "ibmmq",
            connected.get(),
            paused.get(),
            messagesReceived.get(),
            messagesPublished.get(),
            errors.get(),
            new ArrayList<>(subscribedTopics)
        );
    }
}
