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

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for all event publishers.
 * 
 * This service manages the lifecycle of all EventPublisher implementations
 * and provides a central point for discovering available publishers.
 * 
 * Publishers are automatically discovered via Spring's dependency injection.
 * New publishers just need to implement EventPublisher and be annotated with @Service.
 * 
 * The registry:
 * - Collects all EventPublisher beans at startup
 * - Initializes them in order
 * - Provides lookup methods for the orchestration service
 * - Handles graceful shutdown
 * 
 * @version 2.6.0
 */
@Slf4j
@Service
public class PublisherRegistry {
    
    private final List<EventPublisher> publishers;
    private final Map<String, EventPublisher> publishersByType = new ConcurrentHashMap<>();
    
    /**
     * Constructor with auto-wired publishers.
     * Spring automatically injects all EventPublisher implementations.
     * EventPublishingConfigLoader is injected to guarantee JSON region configs
     * are loaded before publishers initialize.
     * 
     * @param publishers All available EventPublisher beans
     * @param configLoader Ensures external region config is loaded first
     */
    public PublisherRegistry(List<EventPublisher> publishers,
                             EventPublishingConfigLoader configLoader) {
        this.publishers = publishers != null ? publishers : Collections.emptyList();
    }
    
    @PostConstruct
    public void initialize() {
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  Publisher Registry - Discovering Event Publishers                 ║");
        log.info("╚════════════════════════════════════════════════════════════════════╝");
        
        if (publishers.isEmpty()) {
            log.info("No event publishers registered");
            return;
        }
        
        log.info("Discovered {} event publisher(s):", publishers.size());
        
        for (EventPublisher publisher : publishers) {
            String type = publisher.getType();
            publishersByType.put(type, publisher);
            
            log.info("  ├─ {} [{}]", publisher.getDisplayName(), type);
            
            try {
                publisher.initialize();
                log.debug("    └─ Initialized successfully");
            } catch (Exception e) {
                log.warn("    └─ Initialization warning: {}", e.getMessage());
            }
        }
        
        log.info("Publisher registry initialized with {} publisher type(s)", publishersByType.size());
    }
    
    /**
     * Execute startup orchestration tasks for all publishers.
     * Called during startup sequence (e.g., Kafka topic creation).
     */
    public void executeStartupOrchestration() {
        log.info("Executing startup orchestration for {} publisher(s)...", publishers.size());
        
        for (EventPublisher publisher : publishers) {
            try {
                publisher.onStartupOrchestration();
            } catch (Exception e) {
                log.warn("Startup orchestration warning for {}: {}", 
                        publisher.getDisplayName(), e.getMessage());
            }
        }
    }
    
    /**
     * Refresh all publishers' region bindings.
     * Called when publishing configuration changes at runtime.
     */
    public void refreshAll() {
        log.info("Refreshing all publisher region bindings...");
        for (EventPublisher publisher : publishers) {
            try {
                publisher.refreshBindings();
                log.info("  ├─ {} refreshed", publisher.getDisplayName());
            } catch (Exception e) {
                log.warn("  ├─ {} refresh failed: {}", publisher.getDisplayName(), e.getMessage());
            }
        }
        log.info("Publisher refresh complete");
    }
    
    /**
     * Get all registered publishers.
     * 
     * @return Unmodifiable list of all publishers
     */
    public List<EventPublisher> getAllPublishers() {
        return Collections.unmodifiableList(publishers);
    }
    
    /**
     * Get publishers that are enabled for a specific region.
     * 
     * @param region Region name
     * @return List of publishers enabled for the region
     */
    public List<EventPublisher> getPublishersForRegion(String region) {
        List<EventPublisher> result = new ArrayList<>();
        for (EventPublisher publisher : publishers) {
            if (publisher.isEnabledForRegion(region)) {
                result.add(publisher);
            }
        }
        return result;
    }
    
    /**
     * Get a publisher by type.
     * 
     * @param type Publisher type (e.g., "kafka", "file")
     * @return Optional containing the publisher if found
     */
    public Optional<EventPublisher> getPublisherByType(String type) {
        return Optional.ofNullable(publishersByType.get(type));
    }
    
    /**
     * Check if any publisher is enabled for a region.
     * 
     * @param region Region name
     * @return true if at least one publisher handles this region
     */
    public boolean hasPublishersForRegion(String region) {
        for (EventPublisher publisher : publishers) {
            if (publisher.isEnabledForRegion(region)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Get all registered publisher types.
     * 
     * @return Set of publisher type identifiers
     */
    public Set<String> getRegisteredTypes() {
        return Collections.unmodifiableSet(publishersByType.keySet());
    }
    
    /**
     * Get statistics from all publishers.
     * 
     * @return Map of type to stats
     */
    public Map<String, EventPublisher.PublisherStats> getAllStats() {
        Map<String, EventPublisher.PublisherStats> stats = new LinkedHashMap<>();
        for (EventPublisher publisher : publishers) {
            stats.put(publisher.getType(), publisher.getStats());
        }
        return stats;
    }
    
    /**
     * Get summary information about all registered publishers.
     * 
     * @return List of publisher info records
     */
    public List<PublisherInfo> getPublisherInfo() {
        List<PublisherInfo> info = new ArrayList<>();
        for (EventPublisher publisher : publishers) {
            EventPublisher.PublisherStats stats = publisher.getStats();
            info.add(new PublisherInfo(
                    publisher.getType(),
                    publisher.getDisplayName(),
                    stats.enabledRegions(),
                    stats.eventsPublished(),
                    stats.errors()
            ));
        }
        return info;
    }
    
    /**
     * Publisher information record.
     */
    public record PublisherInfo(
        String type,
        String displayName,
        int enabledRegions,
        long eventsPublished,
        long errors
    ) {}
    
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Publisher Registry...");
        
        for (EventPublisher publisher : publishers) {
            try {
                log.info("Shutting down {} publisher...", publisher.getDisplayName());
                publisher.shutdown();
            } catch (Exception e) {
                log.warn("Error shutting down {} publisher: {}", 
                        publisher.getDisplayName(), e.getMessage());
            }
        }
        
        publishersByType.clear();
        log.info("Publisher Registry shutdown complete");
    }
}
