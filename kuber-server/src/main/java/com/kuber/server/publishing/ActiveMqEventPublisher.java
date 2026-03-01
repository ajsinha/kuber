/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.server.publishing;

import com.kuber.server.config.KuberProperties;
import com.kuber.server.config.KuberProperties.ActiveMqConfig;
import com.kuber.server.config.KuberProperties.BrokerDefinition;
import com.kuber.server.config.KuberProperties.BrokerSsl;
import com.kuber.server.config.KuberProperties.DestinationConfig;
import com.kuber.server.config.KuberProperties.RegionPublishingConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.springframework.stereotype.Service;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ActiveMQ Event Publisher.
 * 
 * Publishes cache events to Apache ActiveMQ queues or topics.
 * Uses javax.jms for compatibility with ActiveMQ 5.x.
 * 
 * Supports both:
 * - Legacy per-region configuration (kuber.publishing.regions.X.activemq.*)
 * - New centralized broker configuration (kuber.publishing.brokers.* + regions.X.destinations[])
 * 
 * This publisher only initializes connections to brokers where enabled=true.
 * 
 * @version 2.6.4
 */
@Slf4j
@Service
public class ActiveMqEventPublisher implements EventPublisher {
    
    public static final String TYPE = "activemq";
    public static final String DISPLAY_NAME = "Apache ActiveMQ";
    
    private final KuberProperties properties;
    
    // Pooled connection factories per broker URL
    private final Map<String, PooledConnectionFactory> connectionFactories = new ConcurrentHashMap<>();
    
    // Region to destination bindings
    private final Map<String, List<DestinationBinding>> regionBindings = new ConcurrentHashMap<>();
    
    // Statistics
    private final AtomicLong eventsPublished = new AtomicLong(0);
    private final AtomicLong errors = new AtomicLong(0);
    
    /**
     * Internal binding configuration.
     */
    private static class DestinationBinding {
        final String brokerUrl;
        final String destination;
        final boolean useTopic;
        final int ttlSeconds;
        final boolean persistent;
        final String username;
        final String password;
        final BrokerSsl ssl;
        
        DestinationBinding(String brokerUrl, String destination, boolean useTopic,
                          int ttlSeconds, boolean persistent, String username, String password,
                          BrokerSsl ssl) {
            this.brokerUrl = brokerUrl;
            this.destination = destination;
            this.useTopic = useTopic;
            this.ttlSeconds = ttlSeconds;
            this.persistent = persistent;
            this.username = username;
            this.password = password;
            this.ssl = ssl != null ? ssl : new BrokerSsl();
        }
    }
    
    public ActiveMqEventPublisher(KuberProperties properties) {
        this.properties = properties;
    }
    
    @Override
    public String getType() {
        return TYPE;
    }
    
    @Override
    public String getDisplayName() {
        return DISPLAY_NAME;
    }
    
    @Override
    public void initialize() {
        log.info("Initializing ActiveMQ Event Publisher...");
        
        // Clear existing bindings for idempotent refresh
        regionBindings.clear();
        
        Map<String, BrokerDefinition> brokers = properties.getPublishing().getBrokers();
        Map<String, RegionPublishingConfig> regions = properties.getPublishing().getRegions();
        
        for (Map.Entry<String, RegionPublishingConfig> entry : regions.entrySet()) {
            String region = entry.getKey();
            RegionPublishingConfig config = entry.getValue();
            
            if (!config.isEnabled()) continue;
            
            List<DestinationBinding> bindings = new ArrayList<>();
            
            // Check new-style destinations
            if (config.getDestinations() != null) {
                for (DestinationConfig dest : config.getDestinations()) {
                    if (dest.getBroker() == null) continue;
                    BrokerDefinition broker = brokers.get(dest.getBroker());
                    if (broker != null && TYPE.equals(broker.getType()) && broker.isEnabled()) {
                        String destination = dest.getTopic();
                        if (destination == null || destination.isBlank()) {
                            destination = "kuber." + region + ".events";
                        }
                        
                        bindings.add(new DestinationBinding(
                                broker.getBrokerUrl(),
                                destination,
                                broker.isUseTopic(),
                                dest.getTtlSeconds() > 0 ? dest.getTtlSeconds() : broker.getTtlSeconds(),
                                dest.getPersistent() != null ? dest.getPersistent() : broker.isPersistent(),
                                broker.getUsername(),
                                broker.getPassword(),
                                broker.getSsl()
                        ));
                        
                        log.info("ActiveMQ destination for region '{}': broker={}, dest={}",
                                region, dest.getBroker(), destination);
                    }
                }
            }
            
            // Check legacy configuration
            ActiveMqConfig amqConfig = config.getActivemq();
            if (amqConfig != null && amqConfig.isEnabled()) {
                String destination = amqConfig.getDestination();
                if (destination == null || destination.isBlank()) {
                    destination = "kuber." + region + ".events";
                }
                
                bindings.add(new DestinationBinding(
                        amqConfig.getBrokerUrl(),
                        destination,
                        amqConfig.isUseTopic(),
                        amqConfig.getTtlSeconds(),
                        amqConfig.isPersistent(),
                        amqConfig.getUsername(),
                        amqConfig.getPassword(),
                        null
                ));
                
                log.info("ActiveMQ (legacy) for region '{}': broker={}, dest={}",
                        region, amqConfig.getBrokerUrl(), destination);
            }
            
            if (!bindings.isEmpty()) {
                regionBindings.put(region, bindings);
                // Pre-create connection factories
                for (DestinationBinding binding : bindings) {
                    getOrCreateConnectionFactory(binding);
                }
            }
        }
        
        if (regionBindings.isEmpty()) {
            log.info("No ActiveMQ brokers enabled for publishing");
        } else {
            log.info("ActiveMQ publishing configured for {} region(s)", regionBindings.size());
        }
    }
    
    @Override
    public boolean isEnabledForRegion(String region) {
        return regionBindings.containsKey(region);
    }
    
    @Override
    public void publish(String region, CachePublishingEvent event) {
        List<DestinationBinding> bindings = regionBindings.get(region);
        if (bindings == null || bindings.isEmpty()) {
            return;
        }
        
        for (DestinationBinding binding : bindings) {
            publishToDestination(region, event, binding);
        }
    }
    
    private void publishToDestination(String region, CachePublishingEvent event, DestinationBinding binding) {
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        
        try {
            PooledConnectionFactory factory = getOrCreateConnectionFactory(binding);
            if (factory == null) {
                log.warn("ActiveMQ connection factory is null for {} — attempting recovery", binding.brokerUrl);
                connectionFactories.remove(binding.brokerUrl);
                factory = getOrCreateConnectionFactory(binding);
                if (factory == null) {
                    errors.incrementAndGet();
                    log.error("Failed to recover ActiveMQ connection factory for {} — event dropped", binding.brokerUrl);
                    return;
                }
            }
            
            connection = factory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            // Create destination (queue or topic)
            Destination destination;
            if (binding.useTopic) {
                destination = session.createTopic(binding.destination);
            } else {
                destination = session.createQueue(binding.destination);
            }
            
            producer = session.createProducer(destination);
            
            // Set delivery mode
            if (binding.persistent) {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            } else {
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            }
            
            // Set TTL in milliseconds
            long ttlMs = binding.ttlSeconds * 1000L;
            producer.setTimeToLive(ttlMs);
            
            // Create and send message
            TextMessage message = session.createTextMessage(event.toJson());
            message.setStringProperty("region", region);
            message.setStringProperty("action", event.getAction());
            message.setStringProperty("key", event.getKey());
            
            producer.send(message);
            eventsPublished.incrementAndGet();
            
            log.info("Published event to ActiveMQ: destination={}, key={}, action={}",
                    binding.destination, event.getKey(), event.getAction());
            
        } catch (Exception e) {
            errors.incrementAndGet();
            log.error("Failed to publish event to ActiveMQ for region '{}', key '{}': {}",
                    region, event.getKey(), e.getMessage());
            // v2.6.4: Invalidate factory for recovery on next attempt
            connectionFactories.remove(binding.brokerUrl);
        } finally {
            closeQuietly(producer);
            closeQuietly(session);
            closeQuietly(connection);
        }
    }
    
    @Override
    public PublisherStats getStats() {
        return new PublisherStats(TYPE, eventsPublished.get(), errors.get(), regionBindings.size());
    }
    
    private PooledConnectionFactory getOrCreateConnectionFactory(DestinationBinding binding) {
        return connectionFactories.computeIfAbsent(binding.brokerUrl, url -> {
            int maxRetries = 3;
            long retryDelayMs = 2000;
            
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(url);
                    
                    // Set credentials if provided
                    if (binding.username != null && !binding.username.isBlank()) {
                        connectionFactory.setUserName(binding.username);
                        connectionFactory.setPassword(binding.password);
                    }
                    
                    // Configure connection factory
                    connectionFactory.setTrustAllPackages(true);
                    connectionFactory.setWatchTopicAdvisories(false);
                    
                    // Apply SSL/TLS trust store and key store if enabled
                    if (binding.ssl.isEnabled()) {
                        if (!binding.ssl.getTrustStorePath().isBlank()) {
                            System.setProperty("javax.net.ssl.trustStore", binding.ssl.getTrustStorePath());
                            System.setProperty("javax.net.ssl.trustStorePassword", binding.ssl.getTrustStorePassword());
                            System.setProperty("javax.net.ssl.trustStoreType", binding.ssl.getTrustStoreType());
                        }
                        if (!binding.ssl.getKeyStorePath().isBlank()) {
                            System.setProperty("javax.net.ssl.keyStore", binding.ssl.getKeyStorePath());
                            System.setProperty("javax.net.ssl.keyStorePassword", binding.ssl.getKeyStorePassword());
                            System.setProperty("javax.net.ssl.keyStoreType", binding.ssl.getKeyStoreType());
                        }
                        log.info("ActiveMQ SSL/TLS enabled for broker: {}", url);
                    }
                    
                    // Create pooled factory
                    PooledConnectionFactory pooledFactory = new PooledConnectionFactory();
                    pooledFactory.setConnectionFactory(connectionFactory);
                    pooledFactory.setMaxConnections(10);
                    pooledFactory.setIdleTimeout(30000);
                    pooledFactory.setMaximumActiveSessionPerConnection(100);
                    // v2.6.4: Enable reconnect on exception
                    pooledFactory.setReconnectOnException(true);
                    
                    // Verify connectivity by creating a test connection
                    javax.jms.Connection testConn = pooledFactory.createConnection();
                    testConn.close();
                    
                    log.info("✅ Created ActiveMQ connection pool for broker: {}{} (attempt {})", url,
                            binding.ssl.isEnabled() ? " [SSL]" : "", attempt);
                    return pooledFactory;
                    
                } catch (Exception e) {
                    log.warn("Failed to create ActiveMQ connection pool for {} (attempt {}/{}): {}",
                            url, attempt, maxRetries, e.getMessage());
                    if (attempt < maxRetries) {
                        try { Thread.sleep(retryDelayMs); } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        retryDelayMs *= 2;
                    }
                }
            }
            
            log.error("❌ Failed to create ActiveMQ connection pool for {} after retries", url);
            return null;
        });
    }
    
    private void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // Ignore
            }
        }
    }
    
    @Override
    public void shutdown() {
        log.info("Shutting down ActiveMQ Event Publisher...");
        for (Map.Entry<String, PooledConnectionFactory> entry : connectionFactories.entrySet()) {
            try {
                entry.getValue().stop();
                log.info("Stopped ActiveMQ connection pool for {}", entry.getKey());
            } catch (Exception e) {
                log.warn("Error stopping ActiveMQ connection pool for {}: {}", entry.getKey(), e.getMessage());
            }
        }
        connectionFactories.clear();
    }
}
