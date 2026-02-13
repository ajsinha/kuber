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

import java.util.List;
import java.util.function.Consumer;

/**
 * Interface for message broker adapters.
 * 
 * <p>Each adapter implementation handles connection, subscription,
 * message consumption, and publishing for a specific broker type.</p>
 * 
 * <p>Supported broker types:
 * <ul>
 *   <li>kafka - Apache Kafka</li>
 *   <li>activemq - Apache ActiveMQ</li>
 *   <li>rabbitmq - RabbitMQ</li>
 *   <li>ibmmq - IBM MQ</li>
 * </ul>
 * 
 * @version 2.5.0
 */
public interface MessageBrokerAdapter {
    
    /**
     * Get the broker type (kafka, activemq, rabbitmq, ibmmq).
     */
    String getBrokerType();
    
    /**
     * Get the broker name as configured.
     */
    String getBrokerName();
    
    /**
     * Connect to the broker.
     * 
     * @return true if connection successful
     */
    boolean connect();
    
    /**
     * Check if connected to the broker.
     */
    boolean isConnected();
    
    /**
     * Subscribe to request topics.
     * 
     * @param topics list of request topic names
     * @param messageHandler callback to handle received messages
     * @return true if subscription successful
     */
    boolean subscribe(List<String> topics, Consumer<ReceivedMessage> messageHandler);
    
    /**
     * Pause message consumption (for backpressure).
     */
    void pauseConsumption();
    
    /**
     * Resume message consumption.
     */
    void resumeConsumption();
    
    /**
     * Check if consumption is paused.
     */
    boolean isPaused();
    
    /**
     * Publish a response message to the response topic.
     * 
     * @param responseTopic the response topic name
     * @param message the JSON message to publish
     * @return true if publish successful
     */
    boolean publish(String responseTopic, String message);
    
    /**
     * Get list of subscribed topics.
     */
    List<String> getSubscribedTopics();
    
    /**
     * Disconnect from the broker and clean up resources.
     */
    void disconnect();
    
    /**
     * Get statistics for this adapter.
     */
    BrokerStats getStats();
    
    /**
     * Represents a message received from the broker.
     */
    class ReceivedMessage {
        private final String topic;
        private final String message;
        private final String messageId;
        private final long timestamp;
        private final MessageBrokerAdapter sourceAdapter;  // The adapter that received this message
        
        public ReceivedMessage(String topic, String message, String messageId, long timestamp) {
            this(topic, message, messageId, timestamp, null);
        }
        
        public ReceivedMessage(String topic, String message, String messageId, long timestamp, MessageBrokerAdapter sourceAdapter) {
            this.topic = topic;
            this.message = message;
            this.messageId = messageId;
            this.timestamp = timestamp;
            this.sourceAdapter = sourceAdapter;
        }
        
        public String getTopic() { return topic; }
        public String getMessage() { return message; }
        public String getMessageId() { return messageId; }
        public long getTimestamp() { return timestamp; }
        
        /**
         * Get the adapter that received this message.
         * Response should be sent through this same adapter.
         */
        public MessageBrokerAdapter getSourceAdapter() { return sourceAdapter; }
        
        /**
         * Get the inferred response topic.
         */
        public String getResponseTopic() {
            return MessagingConfig.getResponseTopic(topic);
        }
    }
    
    /**
     * Statistics for a broker adapter.
     */
    class BrokerStats {
        private final String brokerName;
        private final String brokerType;
        private final boolean connected;
        private final boolean paused;
        private final long messagesReceived;
        private final long messagesPublished;
        private final long errors;
        private final List<String> subscribedTopics;
        
        public BrokerStats(String brokerName, String brokerType, boolean connected, boolean paused,
                          long messagesReceived, long messagesPublished, long errors,
                          List<String> subscribedTopics) {
            this.brokerName = brokerName;
            this.brokerType = brokerType;
            this.connected = connected;
            this.paused = paused;
            this.messagesReceived = messagesReceived;
            this.messagesPublished = messagesPublished;
            this.errors = errors;
            this.subscribedTopics = subscribedTopics;
        }
        
        public String getBrokerName() { return brokerName; }
        public String getBrokerType() { return brokerType; }
        public boolean isConnected() { return connected; }
        public boolean isPaused() { return paused; }
        public long getMessagesReceived() { return messagesReceived; }
        public long getMessagesPublished() { return messagesPublished; }
        public long getErrors() { return errors; }
        public List<String> getSubscribedTopics() { return subscribedTopics; }
    }
}
