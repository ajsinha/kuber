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
package com.kuber.server.publishing;

/**
 * Interface for event publishers.
 * 
 * Implement this interface to add new publishing destinations.
 * Each publisher handles publishing to a specific type of destination
 * (Kafka, ActiveMQ, RabbitMQ, IBM MQ, File, etc.).
 * 
 * Publishers are:
 * - Initialized at startup via {@link #initialize()}
 * - Called asynchronously for each event via {@link #publish(String, CachePublishingEvent)}
 * - Cleaned up at shutdown via {@link #shutdown()}
 * 
 * To add a new publisher:
 * 1. Implement this interface
 * 2. Add configuration class to KuberProperties
 * 3. Register in PublisherRegistry
 * 4. Add Spring @Service annotation
 * 
 * @version 1.4.1
 */
public interface EventPublisher {
    
    /**
     * Get the unique type identifier for this publisher.
     * Used in configuration and logging.
     * 
     * @return Publisher type (e.g., "kafka", "activemq", "rabbitmq", "ibmmq", "file")
     */
    String getType();
    
    /**
     * Get a human-readable name for this publisher.
     * 
     * @return Display name (e.g., "Apache Kafka", "IBM MQ")
     */
    String getDisplayName();
    
    /**
     * Initialize the publisher.
     * Called once at startup after Spring context is loaded.
     * Should set up connections, create topics/queues if needed.
     */
    void initialize();
    
    /**
     * Perform any startup tasks that should happen during orchestration.
     * For example, Kafka topic creation.
     * Called during startup orchestration phase.
     */
    default void onStartupOrchestration() {
        // Default: no action needed
    }
    
    /**
     * Check if publishing is enabled for the specified region.
     * 
     * @param region Region name
     * @return true if this publisher should handle events for the region
     */
    boolean isEnabledForRegion(String region);
    
    /**
     * Publish an event to the destination.
     * This method is called asynchronously from the publishing thread pool.
     * 
     * Implementations should:
     * - Log errors but not throw exceptions
     * - Handle connection failures gracefully
     * - Use appropriate serialization
     * 
     * @param region Region name
     * @param event Event to publish
     */
    void publish(String region, CachePublishingEvent event);
    
    /**
     * Shutdown the publisher.
     * Called during application shutdown.
     * Should close connections and release resources.
     */
    void shutdown();
    
    /**
     * Get statistics for monitoring.
     * 
     * @return Publisher-specific statistics
     */
    default PublisherStats getStats() {
        return new PublisherStats(getType(), 0, 0, 0);
    }
    
    /**
     * Statistics record for monitoring.
     */
    record PublisherStats(
        String type,
        long eventsPublished,
        long errors,
        int enabledRegions
    ) {}
}
