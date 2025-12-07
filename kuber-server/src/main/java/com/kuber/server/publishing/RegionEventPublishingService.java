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

import com.kuber.server.config.KuberProperties;
import com.kuber.server.config.KuberProperties.RegionPublishingConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Region Event Publishing Service.
 * 
 * Orchestrates asynchronous publishing of cache events to all configured destinations.
 * This is the main entry point for the CacheService to trigger event publishing.
 * 
 * Key Features:
 * - Asynchronous publishing using a dedicated thread pool
 * - Non-blocking: Main cache operations never wait for publishing
 * - Interface-driven: Supports pluggable publishers (Kafka, ActiveMQ, RabbitMQ, IBM MQ, File)
 * - Per-region configuration support
 * - Error isolation: Publishing failures don't affect cache operations
 * - Statistics tracking for monitoring
 * 
 * Supported Publishers (v1.2.7):
 * - Apache Kafka - High throughput event streaming
 * - Apache ActiveMQ - Enterprise JMS messaging
 * - RabbitMQ - AMQP messaging with flexible routing
 * - IBM MQ - Enterprise messaging for IBM environments
 * - File System - Local/network file publishing
 * 
 * Architecture:
 * 
 *   CacheService.set() ──┐
 *                        │
 *   CacheService.delete()├──▶ RegionEventPublishingService.publishAsync()
 *                        │              │
 *                        │              ▼
 *                        │    ThreadPoolExecutor (background)
 *                        │              │
 *                        │              ▼
 *                        │    PublisherRegistry.getPublishersForRegion()
 *                        │              │
 *                        │    ┌────────┬┴────────┬─────────┬─────────┐
 *                        │    ▼        ▼         ▼         ▼         ▼
 *                        │  Kafka   ActiveMQ  RabbitMQ  IBM MQ     File
 * 
 * @version 1.3.10
 */
@Slf4j
@Service
public class RegionEventPublishingService {
    
    private final KuberProperties properties;
    private final PublisherRegistry publisherRegistry;
    
    private ThreadPoolExecutor executor;
    
    // Statistics
    private final AtomicLong totalEventsPublished = new AtomicLong(0);
    private final AtomicLong publishErrors = new AtomicLong(0);
    private final AtomicLong eventsDropped = new AtomicLong(0);
    
    // Track enabled regions
    private final Set<String> enabledRegions = ConcurrentHashMap.newKeySet();
    
    public RegionEventPublishingService(KuberProperties properties,
                                        PublisherRegistry publisherRegistry) {
        this.properties = properties;
        this.publisherRegistry = publisherRegistry;
    }
    
    @PostConstruct
    public void initialize() {
        log.info("Initializing Region Event Publishing Service...");
        
        KuberProperties.Publishing pubConfig = properties.getPublishing();
        
        // Create thread pool for async publishing
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(pubConfig.getQueueCapacity());
        
        executor = new ThreadPoolExecutor(
                pubConfig.getThreadPoolSize(),
                pubConfig.getThreadPoolSize(),
                60L, TimeUnit.SECONDS,
                workQueue,
                new ThreadFactory() {
                    private final AtomicLong counter = new AtomicLong(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r, "kuber-event-publisher-" + counter.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy() // If queue is full, run in caller thread
        );
        
        // Determine which regions have publishing enabled
        Map<String, RegionPublishingConfig> regionConfigs = pubConfig.getRegions();
        for (Map.Entry<String, RegionPublishingConfig> entry : regionConfigs.entrySet()) {
            if (entry.getValue().isEnabled()) {
                enabledRegions.add(entry.getKey());
            }
        }
        
        // Log registered publishers
        List<EventPublisher> publishers = publisherRegistry.getAllPublishers();
        
        log.info("Event Publishing Service initialized:");
        log.info("  Thread pool size: {}", pubConfig.getThreadPoolSize());
        log.info("  Queue capacity: {}", pubConfig.getQueueCapacity());
        log.info("  Enabled regions: {}", enabledRegions.isEmpty() ? "none" : enabledRegions);
        log.info("  Registered publishers: {}", publishers.size());
        
        for (EventPublisher publisher : publishers) {
            log.info("    ├─ {} [{}]", publisher.getDisplayName(), publisher.getType());
        }
    }
    
    /**
     * Publish an insert event for a key.
     * Called by CacheService when a new key is created.
     * 
     * @param region Region name
     * @param key Cache key
     * @param value The value that was inserted
     * @param nodeId The node ID where the operation occurred
     */
    public void publishInsert(String region, String key, String value, String nodeId) {
        if (!isPublishingEnabled(region)) {
            return;
        }
        
        CachePublishingEvent event = CachePublishingEvent.inserted(region, key, value, nodeId);
        publishAsync(region, event);
    }
    
    /**
     * Publish an update event for a key.
     * Called by CacheService when an existing key is updated.
     * 
     * @param region Region name
     * @param key Cache key
     * @param value The new value
     * @param nodeId The node ID where the operation occurred
     */
    public void publishUpdate(String region, String key, String value, String nodeId) {
        if (!isPublishingEnabled(region)) {
            return;
        }
        
        CachePublishingEvent event = CachePublishingEvent.updated(region, key, value, nodeId);
        publishAsync(region, event);
    }
    
    /**
     * Publish a delete event for a key.
     * Called by CacheService when a key is deleted.
     * 
     * @param region Region name
     * @param key Cache key
     * @param nodeId The node ID where the operation occurred
     */
    public void publishDelete(String region, String key, String nodeId) {
        if (!isPublishingEnabled(region)) {
            return;
        }
        
        CachePublishingEvent event = CachePublishingEvent.deleted(region, key, nodeId);
        publishAsync(region, event);
    }
    
    /**
     * Submit an event for async publishing to all configured destinations.
     */
    private void publishAsync(String region, CachePublishingEvent event) {
        try {
            executor.submit(() -> {
                try {
                    // Get all publishers enabled for this region
                    List<EventPublisher> publishers = publisherRegistry.getPublishersForRegion(region);
                    
                    for (EventPublisher publisher : publishers) {
                        try {
                            publisher.publish(region, event);
                        } catch (Exception e) {
                            publishErrors.incrementAndGet();
                            log.error("Error in {} publisher for region '{}', key '{}': {}",
                                    publisher.getType(), region, event.getKey(), e.getMessage());
                        }
                    }
                    
                    if (!publishers.isEmpty()) {
                        totalEventsPublished.incrementAndGet();
                    }
                    
                } catch (Exception e) {
                    publishErrors.incrementAndGet();
                    log.error("Error publishing event for region '{}', key '{}': {}",
                            region, event.getKey(), e.getMessage());
                }
            });
        } catch (RejectedExecutionException e) {
            eventsDropped.incrementAndGet();
            log.warn("Event publishing queue is full, dropping event for key '{}' in region '{}'",
                    event.getKey(), region);
        }
    }
    
    /**
     * Check if publishing is enabled for a region.
     */
    public boolean isPublishingEnabled(String region) {
        return enabledRegions.contains(region) && publisherRegistry.hasPublishersForRegion(region);
    }
    
    /**
     * Execute startup orchestration for all publishers.
     * Called during startup sequence.
     */
    public void executeStartupOrchestration() {
        publisherRegistry.executeStartupOrchestration();
    }
    
    /**
     * Get publishing statistics.
     */
    public PublishingStats getStats() {
        return new PublishingStats(
                totalEventsPublished.get(),
                publishErrors.get(),
                eventsDropped.get(),
                executor.getActiveCount(),
                executor.getQueue().size(),
                executor.getCompletedTaskCount(),
                enabledRegions.size(),
                publisherRegistry.getAllStats()
        );
    }
    
    /**
     * Statistics record for monitoring.
     */
    public record PublishingStats(
            long totalEventsPublished,
            long publishErrors,
            long eventsDropped,
            int activeThreads,
            int queueSize,
            long completedTasks,
            int enabledRegions,
            Map<String, EventPublisher.PublisherStats> publisherStats
    ) {}
    
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Region Event Publishing Service...");
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        log.info("Event Publishing Service shutdown complete. Stats: events={}, errors={}, dropped={}",
                totalEventsPublished.get(), publishErrors.get(), eventsDropped.get());
    }
}
