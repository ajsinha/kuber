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
import com.kuber.server.config.KuberProperties.RabbitMqConfig;
import com.kuber.server.config.KuberProperties.BrokerDefinition;
import com.kuber.server.config.KuberProperties.DestinationConfig;
import com.kuber.server.config.KuberProperties.RegionPublishingConfig;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RabbitMQ Event Publisher.
 * 
 * Publishes cache events to RabbitMQ exchanges and queues.
 * 
 * Supports both:
 * - Legacy per-region configuration (kuber.publishing.regions.X.rabbitmq.*)
 * - New centralized broker configuration (kuber.publishing.brokers.* + regions.X.destinations[])
 * 
 * This publisher only initializes connections to brokers where enabled=true.
 * 
 * @version 1.4.1
 */
@Slf4j
@Service
public class RabbitMqEventPublisher implements EventPublisher {
    
    public static final String TYPE = "rabbitmq";
    public static final String DISPLAY_NAME = "RabbitMQ";
    
    private final KuberProperties properties;
    
    // Connections per host
    private final Map<String, Connection> connections = new ConcurrentHashMap<>();
    
    // Channels per binding key
    private final Map<String, Channel> channels = new ConcurrentHashMap<>();
    
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
        final String virtualHost;
        final String username;
        final String password;
        final String exchange;
        final String exchangeType;
        final String queue;
        final String routingKey;
        final boolean durable;
        final boolean persistent;
        final int ttlSeconds;
        
        DestinationBinding(String host, int port, String virtualHost, String username,
                          String password, String exchange, String exchangeType, String queue,
                          String routingKey, boolean durable, boolean persistent, int ttlSeconds) {
            this.host = host;
            this.port = port;
            this.virtualHost = virtualHost;
            this.username = username;
            this.password = password;
            this.exchange = exchange;
            this.exchangeType = exchangeType;
            this.queue = queue;
            this.routingKey = routingKey;
            this.durable = durable;
            this.persistent = persistent;
            this.ttlSeconds = ttlSeconds;
        }
        
        String connectionKey() {
            return host + ":" + port + ":" + virtualHost;
        }
        
        String channelKey() {
            return connectionKey() + ":" + exchange;
        }
    }
    
    public RabbitMqEventPublisher(KuberProperties properties) {
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
        log.info("Initializing RabbitMQ Event Publisher...");
        
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
                        String exchange = dest.getTopic();
                        if (exchange == null || exchange.isBlank()) {
                            exchange = "kuber." + region + ".events";
                        }
                        String routingKey = dest.getRoutingKey();
                        if (routingKey == null || routingKey.isBlank()) {
                            routingKey = region + ".#";
                        }
                        
                        bindings.add(new DestinationBinding(
                                broker.getHost(),
                                broker.getPort(),
                                broker.getVirtualHost(),
                                broker.getUsername(),
                                broker.getPassword(),
                                exchange,
                                broker.getExchangeType(),
                                dest.getQueue(),
                                routingKey,
                                broker.isDurable(),
                                dest.getPersistent() != null ? dest.getPersistent() : broker.isPersistent(),
                                dest.getTtlSeconds() > 0 ? dest.getTtlSeconds() : broker.getTtlSeconds()
                        ));
                        
                        log.info("RabbitMQ destination for region '{}': broker={}, exchange={}",
                                region, dest.getBroker(), exchange);
                    }
                }
            }
            
            // Check legacy configuration
            RabbitMqConfig rmqConfig = config.getRabbitmq();
            if (rmqConfig != null && rmqConfig.isEnabled()) {
                String exchange = rmqConfig.getExchange();
                if (exchange == null || exchange.isBlank()) {
                    exchange = "kuber." + region + ".events";
                }
                String routingKey = rmqConfig.getRoutingKey();
                if (routingKey == null || routingKey.isBlank()) {
                    routingKey = region + ".#";
                }
                
                bindings.add(new DestinationBinding(
                        rmqConfig.getHost(),
                        rmqConfig.getPort(),
                        rmqConfig.getVirtualHost(),
                        rmqConfig.getUsername(),
                        rmqConfig.getPassword(),
                        exchange,
                        rmqConfig.getExchangeType(),
                        rmqConfig.getQueue(),
                        routingKey,
                        rmqConfig.isDurable(),
                        rmqConfig.isPersistent(),
                        rmqConfig.getTtlSeconds()
                ));
                
                log.info("RabbitMQ (legacy) for region '{}': host={}, exchange={}",
                        region, rmqConfig.getHost(), exchange);
            }
            
            if (!bindings.isEmpty()) {
                regionBindings.put(region, bindings);
            }
        }
        
        if (regionBindings.isEmpty()) {
            log.info("No RabbitMQ brokers enabled for publishing");
        } else {
            log.info("RabbitMQ publishing configured for {} region(s)", regionBindings.size());
        }
    }
    
    @Override
    public void onStartupOrchestration() {
        // Declare exchanges and queues at startup
        for (Map.Entry<String, List<DestinationBinding>> entry : regionBindings.entrySet()) {
            String region = entry.getKey();
            
            for (DestinationBinding binding : entry.getValue()) {
                try {
                    Channel channel = getOrCreateChannel(binding);
                    
                    // Declare exchange
                    String exchangeType = binding.exchangeType;
                    if (exchangeType == null || exchangeType.isBlank()) {
                        exchangeType = "topic";
                    }
                    
                    channel.exchangeDeclare(binding.exchange, exchangeType, binding.durable);
                    log.info("Declared RabbitMQ exchange '{}' ({}) for region '{}'", 
                            binding.exchange, exchangeType, region);
                    
                    // Declare queue if specified
                    if (binding.queue != null && !binding.queue.isBlank()) {
                        Map<String, Object> args = new HashMap<>();
                        if (binding.ttlSeconds > 0) {
                            args.put("x-message-ttl", binding.ttlSeconds * 1000L);
                        }
                        channel.queueDeclare(binding.queue, binding.durable, false, false, args);
                        channel.queueBind(binding.queue, binding.exchange, binding.routingKey);
                        log.info("Declared and bound RabbitMQ queue '{}' for region '{}'", 
                                binding.queue, region);
                    }
                    
                } catch (Exception e) {
                    log.warn("Failed to declare RabbitMQ exchange/queue for region '{}': {}", 
                            region, e.getMessage());
                }
            }
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
        try {
            Channel channel = getOrCreateChannel(binding);
            
            // Build message properties
            Map<String, Object> headers = new HashMap<>();
            headers.put("region", region);
            headers.put("action", event.getAction());
            headers.put("key", event.getKey());
            
            AMQP.BasicProperties.Builder propsBuilder = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(binding.persistent ? 2 : 1) // 2 = persistent
                    .headers(headers);
            
            // Set TTL if configured
            if (binding.ttlSeconds > 0) {
                propsBuilder.expiration(String.valueOf(binding.ttlSeconds * 1000L));
            }
            
            // Determine routing key
            String routingKey = binding.routingKey;
            if (routingKey.contains("#") || routingKey.contains("*")) {
                // Use action-based routing key for topic exchange
                routingKey = region + "." + event.getAction();
            }
            
            // Publish message
            channel.basicPublish(
                    binding.exchange,
                    routingKey,
                    propsBuilder.build(),
                    event.toJson().getBytes(StandardCharsets.UTF_8)
            );
            
            eventsPublished.incrementAndGet();
            log.debug("Published event to RabbitMQ: exchange={}, routingKey={}, key={}",
                    binding.exchange, routingKey, event.getKey());
            
        } catch (Exception e) {
            errors.incrementAndGet();
            log.error("Failed to publish event to RabbitMQ for region '{}', key '{}': {}",
                    region, event.getKey(), e.getMessage());
        }
    }
    
    @Override
    public PublisherStats getStats() {
        return new PublisherStats(TYPE, eventsPublished.get(), errors.get(), regionBindings.size());
    }
    
    private Channel getOrCreateChannel(DestinationBinding binding) throws IOException, TimeoutException {
        return channels.computeIfAbsent(binding.channelKey(), key -> {
            try {
                Connection connection = getOrCreateConnection(binding);
                Channel channel = connection.createChannel();
                log.info("Created RabbitMQ channel for {}", binding.channelKey());
                return channel;
            } catch (Exception e) {
                throw new RuntimeException("Failed to create RabbitMQ channel", e);
            }
        });
    }
    
    private Connection getOrCreateConnection(DestinationBinding binding) throws IOException, TimeoutException {
        return connections.computeIfAbsent(binding.connectionKey(), key -> {
            try {
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost(binding.host);
                factory.setPort(binding.port);
                factory.setVirtualHost(binding.virtualHost);
                
                if (binding.username != null && !binding.username.isBlank()) {
                    factory.setUsername(binding.username);
                    factory.setPassword(binding.password);
                }
                
                // Enable automatic recovery
                factory.setAutomaticRecoveryEnabled(true);
                factory.setNetworkRecoveryInterval(5000);
                factory.setConnectionTimeout(30000);
                
                log.info("Creating RabbitMQ connection to {}:{}/{}", 
                        binding.host, binding.port, binding.virtualHost);
                return factory.newConnection();
                
            } catch (Exception e) {
                throw new RuntimeException("Failed to create RabbitMQ connection", e);
            }
        });
    }
    
    @Override
    public void shutdown() {
        log.info("Shutting down RabbitMQ Event Publisher...");
        
        // Close channels
        for (Map.Entry<String, Channel> entry : channels.entrySet()) {
            try {
                if (entry.getValue().isOpen()) {
                    entry.getValue().close();
                }
            } catch (Exception e) {
                log.warn("Error closing RabbitMQ channel {}: {}", 
                        entry.getKey(), e.getMessage());
            }
        }
        channels.clear();
        
        // Close connections
        for (Map.Entry<String, Connection> entry : connections.entrySet()) {
            try {
                if (entry.getValue().isOpen()) {
                    entry.getValue().close();
                }
                log.info("Closed RabbitMQ connection: {}", entry.getKey());
            } catch (Exception e) {
                log.warn("Error closing RabbitMQ connection {}: {}", 
                        entry.getKey(), e.getMessage());
            }
        }
        connections.clear();
    }
}
