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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration model for Request/Response messaging via message brokers.
 * 
 * <p>This configuration is stored in request_response.json in the secure folder
 * alongside users.json and is hot-reloadable.</p>
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
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class MessagingConfig {
    
    /**
     * Whether request/response messaging is enabled globally.
     */
    private boolean enabled = false;
    
    /**
     * Maximum queue depth for backpressure control.
     * When the internal work queue reaches this size, message consumption pauses.
     * Default: 100
     */
    @JsonProperty("max_queue_depth")
    private int maxQueueDepth = 100;
    
    /**
     * Thread pool size for processing requests.
     * Default: 10
     */
    @JsonProperty("thread_pool_size")
    private int threadPoolSize = 10;
    
    /**
     * Maximum number of keys allowed in batch GET (MGET) operations.
     * Requests exceeding this limit will be rejected.
     * Default: 128
     */
    @JsonProperty("max_batch_get_size")
    private int maxBatchGetSize = 128;
    
    /**
     * Maximum number of entries allowed in batch SET (MSET) operations.
     * Requests exceeding this limit will be rejected.
     * Default: 128
     */
    @JsonProperty("max_batch_set_size")
    private int maxBatchSetSize = 128;
    
    /**
     * Maximum number of results returned for KEYS pattern search operations.
     * If more matches exist, results will be truncated and server_message will indicate this.
     * Default: 10000
     */
    @JsonProperty("max_search_results")
    private int maxSearchResults = 10000;
    
    /**
     * Enable request/response logging to files.
     * Default: true
     */
    @JsonProperty("logging_enabled")
    private boolean loggingEnabled = true;
    
    /**
     * Maximum number of messages per log file before rolling.
     * Default: 1000
     */
    @JsonProperty("max_log_messages")
    private int maxLogMessages = 1000;
    
    /**
     * Map of broker configurations keyed by broker name.
     */
    private Map<String, BrokerConfig> brokers = new HashMap<>();
    
    /**
     * Broker configuration.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BrokerConfig {
        
        /**
         * Whether this broker is enabled.
         */
        private boolean enabled = true;
        
        /**
         * Broker type: kafka, activemq, rabbitmq, ibmmq
         */
        private String type;
        
        /**
         * Display name for the broker.
         */
        @JsonProperty("display_name")
        private String displayName;
        
        /**
         * Connection properties specific to the broker type.
         */
        private Map<String, String> connection = new HashMap<>();
        
        /**
         * List of request topics/queues to subscribe to.
         * Response topics are inferred by replacing "_request" with "_response".
         */
        @JsonProperty("request_topics")
        private List<String> requestTopics = new ArrayList<>();
        
        // Connection property accessors for different broker types
        
        // --- Kafka ---
        public String getBootstrapServers() {
            return connection.get("bootstrap_servers");
        }
        
        public String getGroupId() {
            return connection.getOrDefault("group_id", "kuber-request-processor");
        }
        
        public String getAutoOffsetReset() {
            return connection.getOrDefault("auto_offset_reset", "earliest");
        }
        
        // --- ActiveMQ ---
        public String getBrokerUrl() {
            return connection.get("broker_url");
        }
        
        public String getUsername() {
            return connection.get("username");
        }
        
        public String getPassword() {
            return connection.get("password");
        }
        
        // --- RabbitMQ ---
        public String getHost() {
            return connection.getOrDefault("host", "localhost");
        }
        
        public int getPort() {
            String port = connection.get("port");
            if (port != null) {
                try {
                    return Integer.parseInt(port);
                } catch (NumberFormatException e) {
                    return 5672;
                }
            }
            return 5672;
        }
        
        public String getVirtualHost() {
            return connection.getOrDefault("virtual_host", "/");
        }
        
        // --- IBM MQ ---
        public String getQueueManager() {
            return connection.get("queue_manager");
        }
        
        public String getChannel() {
            return connection.get("channel");
        }
        
        public String getConnName() {
            return connection.get("conn_name");
        }
        
        /**
         * Get connection string for logging (masking sensitive info).
         */
        public String getConnectionSummary() {
            switch (type != null ? type.toLowerCase() : "") {
                case "kafka":
                    return "kafka://" + getBootstrapServers();
                case "activemq":
                    return getBrokerUrl();
                case "rabbitmq":
                    return "amqp://" + getHost() + ":" + getPort() + getVirtualHost();
                case "ibmmq":
                    return "ibmmq://" + getQueueManager() + "/" + getChannel();
                default:
                    return "unknown";
            }
        }
    }
    
    /**
     * Get inferred response topic from request topic.
     * Replaces "_request" suffix with "_response".
     */
    public static String getResponseTopic(String requestTopic) {
        if (requestTopic == null) {
            return null;
        }
        if (requestTopic.endsWith("_request")) {
            return requestTopic.substring(0, requestTopic.length() - "_request".length()) + "_response";
        }
        return requestTopic + "_response";
    }
}
