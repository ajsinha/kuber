/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 */
package com.kuber.server.publishing;

import com.kuber.server.config.KuberProperties;
import com.kuber.server.config.KuberProperties.BrokerDefinition;
import com.kuber.server.config.KuberProperties.DestinationConfig;
import com.kuber.server.config.KuberProperties.RegionPublishingConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Confluent Kafka Event Publisher.
 * 
 * <p>Publishes cache events to Confluent Cloud (or Confluent Platform) Kafka topics.
 * Uses SASL_SSL with PLAIN mechanism for API key/secret authentication.</p>
 * 
 * <p>Implements the EventPublisher interface for integration with the publisher registry.
 * Supports both legacy per-region configuration and centralized broker configuration.</p>
 * 
 * <p>Broker type: {@code confluent-kafka}</p>
 * 
 * @version 2.6.4
 * @since 2.6.4
 */
@Slf4j
@Service
public class ConfluentKafkaEventPublisher implements EventPublisher {
    
    public static final String TYPE = "confluent-kafka";
    public static final String DISPLAY_NAME = "Confluent Kafka";
    
    private final KuberProperties properties;
    
    // Producer instances per bootstrap server
    private final Map<String, KafkaProducer<String, String>> producers = new ConcurrentHashMap<>();
    
    // Region to destination bindings
    private final Map<String, List<DestinationBinding>> regionBindings = new ConcurrentHashMap<>();
    
    // Statistics
    private final AtomicLong eventsPublished = new AtomicLong(0);
    private final AtomicLong errors = new AtomicLong(0);
    
    /**
     * Internal binding configuration for Confluent Kafka destinations.
     */
    private static class DestinationBinding {
        final String bootstrapServers;
        final String topic;
        final int partitions;
        final short replicationFactor;
        final int retentionHours;
        final String acks;
        final int batchSize;
        final int lingerMs;
        final String apiKey;
        final String apiSecret;
        
        DestinationBinding(String bootstrapServers, String topic, int partitions,
                          short replicationFactor, int retentionHours, String acks,
                          int batchSize, int lingerMs, String apiKey, String apiSecret) {
            this.bootstrapServers = bootstrapServers;
            this.topic = topic;
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
            this.retentionHours = retentionHours;
            this.acks = acks;
            this.batchSize = batchSize;
            this.lingerMs = lingerMs;
            this.apiKey = apiKey;
            this.apiSecret = apiSecret;
        }
        
        String jaasConfig() {
            return "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                    "username=\"" + apiKey + "\" password=\"" + apiSecret + "\";";
        }
    }
    
    public ConfluentKafkaEventPublisher(KuberProperties properties) {
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
        log.info("Initializing Confluent Kafka Event Publisher...");
        
        regionBindings.clear();
        
        Map<String, BrokerDefinition> brokers = properties.getPublishing().getBrokers();
        Map<String, RegionPublishingConfig> regions = properties.getPublishing().getRegions();
        
        for (Map.Entry<String, RegionPublishingConfig> entry : regions.entrySet()) {
            String region = entry.getKey();
            RegionPublishingConfig config = entry.getValue();
            
            if (!config.isEnabled()) continue;
            
            List<DestinationBinding> bindings = new ArrayList<>();
            
            if (config.getDestinations() != null) {
                for (DestinationConfig dest : config.getDestinations()) {
                    if (dest.getBroker() == null) continue;
                    BrokerDefinition broker = brokers.get(dest.getBroker());
                    if (broker != null && TYPE.equals(broker.getType()) && broker.isEnabled()) {
                        String topic = dest.getTopic();
                        if (topic == null || topic.isBlank()) {
                            topic = "kuber-" + region + "-events";
                        }
                        
                        bindings.add(new DestinationBinding(
                                broker.getBootstrapServers(),
                                topic,
                                broker.getPartitions(),
                                broker.getReplicationFactor(),
                                broker.getRetentionHours(),
                                broker.getAcks(),
                                broker.getBatchSize(),
                                broker.getLingerMs(),
                                broker.getApiKey(),
                                broker.getApiSecret()
                        ));
                        
                        log.info("Confluent Kafka destination for region '{}': broker={}, topic={}",
                                region, dest.getBroker(), topic);
                    }
                }
            }
            
            if (!bindings.isEmpty()) {
                regionBindings.put(region, bindings);
            }
        }
        
        if (regionBindings.isEmpty()) {
            log.info("No Confluent Kafka brokers enabled for publishing");
        } else {
            log.info("Confluent Kafka publishing configured for {} region(s)", regionBindings.size());
        }
        
        createTopics();
    }
    
    private void createTopics() {
        if (regionBindings.isEmpty()) return;
        
        log.info("Creating Confluent Kafka topics for configured regions...");
        
        Map<String, List<DestinationBinding>> serverToBindings = new HashMap<>();
        for (List<DestinationBinding> bindings : regionBindings.values()) {
            for (DestinationBinding binding : bindings) {
                serverToBindings.computeIfAbsent(binding.bootstrapServers, k -> new ArrayList<>()).add(binding);
            }
        }
        
        for (Map.Entry<String, List<DestinationBinding>> serverEntry : serverToBindings.entrySet()) {
            String bootstrapServers = serverEntry.getKey();
            List<DestinationBinding> bindings = serverEntry.getValue();
            
            AdminClient adminClient = createAdminClient(bindings.get(0));
            if (adminClient == null) {
                log.error("Skipping topic creation for Confluent Kafka {} — AdminClient unavailable", bootstrapServers);
                continue;
            }
            
            try (adminClient) {
                Set<String> existingTopics = adminClient.listTopics().names().get(30, TimeUnit.SECONDS);
                List<NewTopic> topicsToCreate = new ArrayList<>();
                Set<String> processed = new HashSet<>();
                
                for (DestinationBinding binding : bindings) {
                    if (processed.contains(binding.topic)) continue;
                    processed.add(binding.topic);
                    
                    if (!existingTopics.contains(binding.topic)) {
                        long retentionMs = binding.retentionHours * 3600L * 1000L;
                        NewTopic newTopic = new NewTopic(binding.topic, binding.partitions, binding.replicationFactor);
                        newTopic.configs(Map.of(
                                "retention.ms", String.valueOf(retentionMs),
                                "cleanup.policy", "delete"
                        ));
                        topicsToCreate.add(newTopic);
                        log.info("Creating Confluent Kafka topic '{}' (partitions={}, replication={})",
                                binding.topic, binding.partitions, binding.replicationFactor);
                    }
                }
                
                if (!topicsToCreate.isEmpty()) {
                    adminClient.createTopics(topicsToCreate).all().get(60, TimeUnit.SECONDS);
                    log.info("Created {} Confluent Kafka topic(s)", topicsToCreate.size());
                }
                
            } catch (Exception e) {
                log.error("Failed to create Confluent Kafka topics for {}: {}", bootstrapServers, e.getMessage());
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
        if (bindings == null || bindings.isEmpty()) return;
        
        for (DestinationBinding binding : bindings) {
            publishToDestination(event, binding);
        }
    }
    
    private void publishToDestination(CachePublishingEvent event, DestinationBinding binding) {
        try {
            KafkaProducer<String, String> producer = getOrCreateProducer(binding);
            
            if (producer == null) {
                log.warn("Confluent Kafka producer is null for {} — attempting recovery", binding.bootstrapServers);
                producers.remove(binding.bootstrapServers);
                producer = getOrCreateProducer(binding);
                if (producer == null) {
                    errors.incrementAndGet();
                    log.error("Failed to recover Confluent Kafka producer for {} — event dropped", binding.bootstrapServers);
                    return;
                }
            }
            
            String key = event.getKey();
            String value = event.toJson();
            
            ProducerRecord<String, String> record = new ProducerRecord<>(binding.topic, key, value);
            
            producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                if (exception != null) {
                    errors.incrementAndGet();
                    log.error("Failed to publish event to Confluent Kafka topic '{}' for key '{}': {}",
                            binding.topic, key, exception.getMessage());
                    producers.remove(binding.bootstrapServers);
                } else {
                    eventsPublished.incrementAndGet();
                    log.info("Published event to Confluent Kafka: topic={}, partition={}, offset={}, key={}",
                            metadata.topic(), metadata.partition(), metadata.offset(), key);
                }
            });
            
        } catch (Exception e) {
            errors.incrementAndGet();
            log.error("Error publishing to Confluent Kafka topic '{}', key '{}': {}",
                    binding.topic, event.getKey(), e.getMessage());
            producers.remove(binding.bootstrapServers);
        }
    }
    
    @Override
    public PublisherStats getStats() {
        return new PublisherStats(TYPE, eventsPublished.get(), errors.get(), regionBindings.size());
    }
    
    private KafkaProducer<String, String> getOrCreateProducer(DestinationBinding binding) {
        return producers.computeIfAbsent(binding.bootstrapServers, servers -> {
            int maxRetries = 3;
            long retryDelayMs = 2000;
            
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    Properties props = new Properties();
                    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
                    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                    props.put(ProducerConfig.ACKS_CONFIG, binding.acks);
                    props.put(ProducerConfig.BATCH_SIZE_CONFIG, binding.batchSize);
                    props.put(ProducerConfig.LINGER_MS_CONFIG, binding.lingerMs);
                    props.put(ProducerConfig.RETRIES_CONFIG, 3);
                    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
                    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
                    props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000);
                    props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000);
                    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
                    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
                    // Confluent Cloud SASL_SSL
                    props.put("security.protocol", "SASL_SSL");
                    props.put("sasl.mechanism", "PLAIN");
                    props.put("sasl.jaas.config", binding.jaasConfig());
                    
                    log.info("Creating Confluent Kafka producer for {} [SASL_SSL] (attempt {}/{})", 
                            servers, attempt, maxRetries);
                    KafkaProducer<String, String> p = new KafkaProducer<>(props);
                    log.info("✅ Confluent Kafka producer created for {}", servers);
                    return p;
                    
                } catch (Exception e) {
                    log.warn("Failed to create Confluent Kafka producer for {} (attempt {}/{}): {}", 
                            servers, attempt, maxRetries, e.getMessage());
                    if (attempt < maxRetries) {
                        try { Thread.sleep(retryDelayMs); } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        retryDelayMs *= 2;
                    }
                }
            }
            
            log.error("❌ Failed to create Confluent Kafka producer for {} after retries", servers);
            return null;
        });
    }
    
    private AdminClient createAdminClient(DestinationBinding binding) {
        int maxRetries = 3;
        long retryDelayMs = 2000;
        
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                Properties props = new Properties();
                props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, binding.bootstrapServers);
                props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 15000);
                props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30000);
                props.put(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000);
                props.put(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000);
                props.put("security.protocol", "SASL_SSL");
                props.put("sasl.mechanism", "PLAIN");
                props.put("sasl.jaas.config", binding.jaasConfig());
                
                AdminClient client = AdminClient.create(props);
                client.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
                log.info("Confluent Kafka AdminClient connected to {} (attempt {})", binding.bootstrapServers, attempt);
                return client;
            } catch (Exception e) {
                log.warn("Confluent Kafka AdminClient connection to {} failed (attempt {}/{}): {}", 
                        binding.bootstrapServers, attempt, maxRetries, e.getMessage());
                if (attempt < maxRetries) {
                    try { Thread.sleep(retryDelayMs); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    retryDelayMs *= 2;
                }
            }
        }
        
        log.error("❌ Failed to create Confluent Kafka AdminClient for {} after retries", binding.bootstrapServers);
        return null;
    }
    
    @Override
    public void shutdown() {
        log.info("Shutting down Confluent Kafka Event Publisher...");
        for (Map.Entry<String, KafkaProducer<String, String>> entry : producers.entrySet()) {
            try {
                entry.getValue().close(Duration.ofSeconds(10));
                log.info("Closed Confluent Kafka producer for {}", entry.getKey());
            } catch (Exception e) {
                log.warn("Error closing Confluent Kafka producer for {}: {}", entry.getKey(), e.getMessage());
            }
        }
        producers.clear();
    }
}
