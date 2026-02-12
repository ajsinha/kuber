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
package com.kuber.server.factory;

import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Provider for factory instances based on configuration.
 * 
 * <p>This component centralizes access to cache and collections factories,
 * selecting the appropriate implementation based on configuration.
 * 
 * <p>Configuration example in application.yml:
 * <pre>{@code
 * kuber:
 *   cache:
 *     cache-implementation: CAFFEINE  # CAFFEINE, GUAVA, EHCACHE
 *     collections-implementation: DEFAULT  # DEFAULT, CUSTOM
 * }</pre>
 * 
 * @version 2.3.0
 * @since 1.5.0
 */
@Slf4j
@Component
public class FactoryProvider {
    
    private final KuberProperties properties;
    private final CacheFactory cacheFactory;
    private final CollectionsFactory collectionsFactory;
    
    /**
     * Create a factory provider with configured implementations.
     */
    public FactoryProvider(KuberProperties properties,
                          CaffeineCacheFactory caffeineCacheFactory,
                          DefaultCollectionsFactory defaultCollectionsFactory) {
        this.properties = properties;
        
        // Select cache factory based on configuration
        String cacheImpl = properties.getCache().getCacheImplementation();
        this.cacheFactory = selectCacheFactory(cacheImpl, caffeineCacheFactory);
        log.info("Selected cache factory: {} (configured: {})", cacheFactory.getType(), cacheImpl);
        
        // Select collections factory based on configuration
        String collectionsImpl = properties.getCache().getCollectionsImplementation();
        this.collectionsFactory = selectCollectionsFactory(collectionsImpl, defaultCollectionsFactory);
        log.info("Selected collections factory: {} (configured: {})", collectionsFactory.getType(), collectionsImpl);
    }
    
    /**
     * Get the configured cache factory.
     * 
     * @return the cache factory
     */
    public CacheFactory getCacheFactory() {
        return cacheFactory;
    }
    
    /**
     * Get the configured collections factory.
     * 
     * @return the collections factory
     */
    public CollectionsFactory getCollectionsFactory() {
        return collectionsFactory;
    }
    
    /**
     * Select cache factory based on configuration.
     * Falls back to Caffeine if configuration is invalid.
     */
    private CacheFactory selectCacheFactory(String implementation, CaffeineCacheFactory caffeine) {
        if (implementation == null || implementation.isBlank()) {
            return caffeine;
        }
        
        return switch (implementation.toUpperCase()) {
            case "CAFFEINE" -> caffeine;
            // Future implementations can be added here:
            // case "GUAVA" -> guavaCacheFactory;
            // case "EHCACHE" -> ehCacheFactory;
            default -> {
                log.warn("Unknown cache implementation '{}', falling back to CAFFEINE", implementation);
                yield caffeine;
            }
        };
    }
    
    /**
     * Select collections factory based on configuration.
     * Falls back to Default if configuration is invalid.
     */
    private CollectionsFactory selectCollectionsFactory(String implementation, DefaultCollectionsFactory defaultFactory) {
        if (implementation == null || implementation.isBlank()) {
            return defaultFactory;
        }
        
        return switch (implementation.toUpperCase()) {
            case "DEFAULT" -> defaultFactory;
            // Future implementations can be added here:
            // case "CUSTOM" -> customCollectionsFactory;
            default -> {
                log.warn("Unknown collections implementation '{}', falling back to DEFAULT", implementation);
                yield defaultFactory;
            }
        };
    }
}
