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

import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.config.KuberProperties.IbmMqConfig;
import com.kuber.server.config.KuberProperties.BrokerDefinition;
import com.kuber.server.config.KuberProperties.DestinationConfig;
import com.kuber.server.config.KuberProperties.RegionPublishingConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * IBM MQ Event Publisher.
 * 
 * Publishes cache events to IBM MQ queues.
 * Uses javax.jms for compatibility with IBM MQ client.
 * 
 * Supports both:
 * - Legacy per-region configuration (kuber.publishing.regions.X.ibmmq.*)
 * - New centralized broker configuration (kuber.publishing.brokers.* + regions.X.destinations[])
 * 
 * This publisher only initializes connections to brokers where enabled=true.
 * 
 * @version 2.6.4
 */
@Slf4j
@Service
public class IbmMqEventPublisher implements EventPublisher {
    
    public static final String TYPE = "ibmmq";
    public static final String DISPLAY_NAME = "IBM MQ";
    
    private final KuberProperties properties;
    
    // Connection factories per queue manager
    private final Map<String, MQConnectionFactory> connectionFactories = new ConcurrentHashMap<>();
    
    // Region to destination bindings
    private final Map<String, List<DestinationBinding>> regionBindings = new ConcurrentHashMap<>();
    
    // Statistics
    private final AtomicLong eventsPublished = new AtomicLong(0);
    private final AtomicLong errors = new AtomicLong(0);
    
    /**
     * Internal binding configuration.
     */
    private static class DestinationBinding {
        final String host;
        final int port;
        final String queueManager;
        final String channel;
        final String queue;
        final boolean useTopic;
        final int ttlSeconds;
        final boolean persistent;
        final String username;
        final String password;
        final int ccsid;
        final String sslCipherSuite;
        
        DestinationBinding(String host, int port, String queueManager, String channel,
                          String queue, boolean useTopic, int ttlSeconds, boolean persistent,
                          String username, String password, int ccsid, String sslCipherSuite) {
            this.host = host;
            this.port = port;
            this.queueManager = queueManager;
            this.channel = channel;
            this.queue = queue;
            this.useTopic = useTopic;
            this.ttlSeconds = ttlSeconds;
            this.persistent = persistent;
            this.username = username;
            this.password = password;
            this.ccsid = ccsid;
            this.sslCipherSuite = sslCipherSuite;
        }
        
        String connectionKey() {
            return queueManager + "@" + host + ":" + port;
        }
    }
    
    public IbmMqEventPublisher(KuberProperties properties) {
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
        log.info("Initializing IBM MQ Event Publisher...");
        
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
                        String queue = dest.getTopic();
                        if (queue == null || queue.isBlank()) {
                            queue = "KUBER." + region.toUpperCase() + ".EVENTS";
                        }
                        
                        bindings.add(new DestinationBinding(
                                broker.getHost(),
                                broker.getPort(),
                                broker.getQueueManager(),
                                broker.getChannel(),
                                queue,
                                broker.isUseTopic(),
                                dest.getTtlSeconds() > 0 ? dest.getTtlSeconds() : broker.getTtlSeconds(),
                                dest.getPersistent() != null ? dest.getPersistent() : broker.isPersistent(),
                                broker.getUsername(),
                                broker.getPassword(),
                                broker.getCcsid(),
                                broker.getSslCipherSuite()
                        ));
                        
                        log.info("IBM MQ destination for region '{}': broker={}, queue={}",
                                region, dest.getBroker(), queue);
                    }
                }
            }
            
            // Check legacy configuration
            IbmMqConfig mqConfig = config.getIbmmq();
            if (mqConfig != null && mqConfig.isEnabled()) {
                String queue = mqConfig.getQueue();
                if (queue == null || queue.isBlank()) {
                    queue = "KUBER." + region.toUpperCase() + ".EVENTS";
                }
                
                bindings.add(new DestinationBinding(
                        mqConfig.getHost(),
                        mqConfig.getPort(),
                        mqConfig.getQueueManager(),
                        mqConfig.getChannel(),
                        queue,
                        mqConfig.isUseTopic(),
                        mqConfig.getTtlSeconds(),
                        mqConfig.isPersistent(),
                        mqConfig.getUsername(),
                        mqConfig.getPassword(),
                        mqConfig.getCcsid(),
                        mqConfig.getSslCipherSuite()
                ));
                
                log.info("IBM MQ (legacy) for region '{}': qmgr={}, queue={}",
                        region, mqConfig.getQueueManager(), queue);
            }
            
            if (!bindings.isEmpty()) {
                regionBindings.put(region, bindings);
                // Pre-create connection factories
                for (DestinationBinding binding : bindings) {
                    try {
                        getOrCreateConnectionFactory(binding);
                    } catch (Exception e) {
                        log.warn("Failed to create IBM MQ connection factory for region '{}': {}", 
                                region, e.getMessage());
                    }
                }
            }
        }
        
        if (regionBindings.isEmpty()) {
            log.info("No IBM MQ brokers enabled for publishing");
        } else {
            log.info("IBM MQ publishing configured for {} region(s)", regionBindings.size());
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
            MQConnectionFactory factory = getOrCreateConnectionFactory(binding);
            if (factory == null) {
                log.warn("IBM MQ connection factory is null for {} — attempting recovery", binding.connectionKey());
                connectionFactories.remove(binding.connectionKey());
                factory = getOrCreateConnectionFactory(binding);
                if (factory == null) {
                    errors.incrementAndGet();
                    log.error("Failed to recover IBM MQ connection factory for {} — event dropped", binding.connectionKey());
                    return;
                }
            }
            
            // Create connection
            if (binding.username != null && !binding.username.isBlank()) {
                connection = factory.createConnection(binding.username, binding.password);
            } else {
                connection = factory.createConnection();
            }
            
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
            // Create destination (queue or topic)
            Destination destination;
            if (binding.useTopic) {
                destination = session.createTopic(binding.queue);
            } else {
                destination = session.createQueue(binding.queue);
            }
            
            producer = session.createProducer(destination);
            
            // Set delivery mode
            if (binding.persistent) {
                producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            } else {
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            }
            
            // Set TTL in milliseconds
            if (binding.ttlSeconds > 0) {
                long ttlMs = binding.ttlSeconds * 1000L;
                producer.setTimeToLive(ttlMs);
            }
            
            // Create and send message
            TextMessage message = session.createTextMessage(event.toJson());
            message.setStringProperty("region", region);
            message.setStringProperty("action", event.getAction());
            message.setStringProperty("key", event.getKey());
            message.setStringProperty("nodeId", event.getNodeId());
            
            producer.send(message);
            eventsPublished.incrementAndGet();
            
            log.info("Published event to IBM MQ: queue={}, key={}, action={}",
                    binding.queue, event.getKey(), event.getAction());
            
        } catch (Exception e) {
            errors.incrementAndGet();
            log.error("Failed to publish event to IBM MQ for region '{}', key '{}': {}",
                    region, event.getKey(), e.getMessage());
            // v2.6.4: Invalidate factory for recovery on next attempt
            connectionFactories.remove(binding.connectionKey());
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
    
    private MQConnectionFactory getOrCreateConnectionFactory(DestinationBinding binding) throws JMSException {
        return connectionFactories.computeIfAbsent(binding.connectionKey(), key -> {
            int maxRetries = 3;
            long retryDelayMs = 2000;
            
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    MQConnectionFactory factory = new MQConnectionFactory();
                    
                    // Set connection properties
                    factory.setHostName(binding.host);
                    factory.setPort(binding.port);
                    factory.setQueueManager(binding.queueManager);
                    factory.setChannel(binding.channel);
                    factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
                    
                    // Set CCSID if specified
                    if (binding.ccsid > 0) {
                        factory.setCCSID(binding.ccsid);
                    }
                    
                    // Enable SSL if configured
                    if (binding.sslCipherSuite != null && !binding.sslCipherSuite.isBlank()) {
                        factory.setSSLCipherSuite(binding.sslCipherSuite);
                    }
                    
                    // v2.6.4: Verify connectivity
                    Connection testConn = (binding.username != null && !binding.username.isBlank()) 
                            ? factory.createConnection(binding.username, binding.password)
                            : factory.createConnection();
                    testConn.close();
                    
                    log.info("✅ Created IBM MQ connection factory for queue manager '{}' at {}:{} (attempt {})", 
                            binding.queueManager, binding.host, binding.port, attempt);
                    return factory;
                    
                } catch (Exception e) {
                    log.warn("Failed to create IBM MQ connection factory for {} (attempt {}/{}): {}",
                            key, attempt, maxRetries, e.getMessage());
                    if (attempt < maxRetries) {
                        try { Thread.sleep(retryDelayMs); } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        retryDelayMs *= 2;
                    }
                }
            }
            
            log.error("❌ Failed to create IBM MQ connection factory for {} after retries", key);
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
        log.info("Shutting down IBM MQ Event Publisher...");
        connectionFactories.clear();
    }
}
