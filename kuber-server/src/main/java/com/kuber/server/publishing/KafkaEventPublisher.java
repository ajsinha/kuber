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
import com.kuber.server.config.KuberProperties.KafkaConfig;
import com.kuber.server.config.KuberProperties.BrokerDefinition;
import com.kuber.server.config.KuberProperties.BrokerSsl;
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
 * Kafka Event Publisher.
 * 
 * Publishes cache events to Apache Kafka topics.
 * Implements the EventPublisher interface for integration with the publisher registry.
 * 
 * Supports both:
 * - Legacy per-region configuration (kuber.publishing.regions.X.kafka.*)
 * - New centralized broker configuration (kuber.publishing.brokers.* + regions.X.destinations[])
 * 
 * This publisher only initializes connections to brokers where enabled=true.
 * 
 * @version 2.4.0
 */
@Slf4j
@Service
public class KafkaEventPublisher implements EventPublisher {
    
    public static final String TYPE = "kafka";
    public static final String DISPLAY_NAME = "Apache Kafka";
    
    private final KuberProperties properties;
    
    // Producer instances per bootstrap server
    private final Map<String, KafkaProducer<String, String>> producers = new ConcurrentHashMap<>();
    
    // Region to destination bindings
    private final Map<String, List<DestinationBinding>> regionBindings = new ConcurrentHashMap<>();
    
    // Statistics
    private final AtomicLong eventsPublished = new AtomicLong(0);
    private final AtomicLong errors = new AtomicLong(0);
    
    /**
     * Internal binding configuration.
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
        final BrokerSsl ssl;
        
        DestinationBinding(String bootstrapServers, String topic, int partitions,
                          short replicationFactor, int retentionHours, String acks,
                          int batchSize, int lingerMs, BrokerSsl ssl) {
            this.bootstrapServers = bootstrapServers;
            this.topic = topic;
            this.partitions = partitions;
            this.replicationFactor = replicationFactor;
            this.retentionHours = retentionHours;
            this.acks = acks;
            this.batchSize = batchSize;
            this.lingerMs = lingerMs;
            this.ssl = ssl != null ? ssl : new BrokerSsl();
        }
    }
    
    public KafkaEventPublisher(KuberProperties properties) {
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
        log.info("Initializing Kafka Event Publisher...");
        
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
                                broker.getSsl()
                        ));
                        
                        log.info("Kafka destination for region '{}': broker={}, topic={}",
                                region, dest.getBroker(), topic);
                    }
                }
            }
            
            // Check legacy configuration
            KafkaConfig kafkaConfig = config.getKafka();
            if (kafkaConfig != null && kafkaConfig.isEnabled()) {
                String topic = kafkaConfig.getTopic();
                if (topic == null || topic.isBlank()) {
                    topic = "kuber-" + region + "-events";
                }
                
                bindings.add(new DestinationBinding(
                        kafkaConfig.getBootstrapServers(),
                        topic,
                        kafkaConfig.getPartitions(),
                        kafkaConfig.getReplicationFactor(),
                        kafkaConfig.getRetentionHours(),
                        kafkaConfig.getAcks(),
                        kafkaConfig.getBatchSize(),
                        kafkaConfig.getLingerMs(),
                        null
                ));
                
                log.info("Kafka (legacy) for region '{}': server={}, topic={}",
                        region, kafkaConfig.getBootstrapServers(), topic);
            }
            
            if (!bindings.isEmpty()) {
                regionBindings.put(region, bindings);
            }
        }
        
        if (regionBindings.isEmpty()) {
            log.info("No Kafka brokers enabled for publishing");
        } else {
            log.info("Kafka publishing configured for {} region(s)", regionBindings.size());
        }
    }
    
    @Override
    public void onStartupOrchestration() {
        createTopics();
    }
    
    /**
     * Create Kafka topics for all configured regions.
     */
    private void createTopics() {
        if (regionBindings.isEmpty()) {
            return;
        }
        
        log.info("Creating Kafka topics for configured regions...");
        
        // Group bindings by bootstrap server
        Map<String, List<DestinationBinding>> serverToBindings = new HashMap<>();
        
        for (List<DestinationBinding> bindings : regionBindings.values()) {
            for (DestinationBinding binding : bindings) {
                serverToBindings.computeIfAbsent(binding.bootstrapServers, k -> new ArrayList<>()).add(binding);
            }
        }
        
        for (Map.Entry<String, List<DestinationBinding>> serverEntry : serverToBindings.entrySet()) {
            String bootstrapServers = serverEntry.getKey();
            List<DestinationBinding> bindings = serverEntry.getValue();
            
            try (AdminClient adminClient = createAdminClient(bootstrapServers)) {
                Set<String> existingTopics = adminClient.listTopics().names().get(30, TimeUnit.SECONDS);
                
                List<NewTopic> topicsToCreate = new ArrayList<>();
                Set<String> processedTopics = new HashSet<>();
                
                for (DestinationBinding binding : bindings) {
                    String topicName = binding.topic;
                    
                    if (processedTopics.contains(topicName)) continue;
                    processedTopics.add(topicName);
                    
                    if (!existingTopics.contains(topicName)) {
                        long retentionMs = binding.retentionHours * 3600L * 1000L;
                        
                        NewTopic newTopic = new NewTopic(topicName, binding.partitions, binding.replicationFactor);
                        newTopic.configs(Map.of(
                                "retention.ms", String.valueOf(retentionMs),
                                "cleanup.policy", "delete"
                        ));
                        
                        topicsToCreate.add(newTopic);
                        log.info("Creating Kafka topic '{}' (partitions={}, replication={}, retention={}h)",
                                topicName, binding.partitions, binding.replicationFactor, binding.retentionHours);
                    } else {
                        log.info("Kafka topic '{}' already exists", topicName);
                    }
                }
                
                if (!topicsToCreate.isEmpty()) {
                    adminClient.createTopics(topicsToCreate).all().get(60, TimeUnit.SECONDS);
                    log.info("Created {} Kafka topic(s)", topicsToCreate.size());
                }
                
            } catch (Exception e) {
                log.error("Failed to create Kafka topics for server {}: {}", bootstrapServers, e.getMessage());
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
            publishToDestination(event, binding);
        }
    }
    
    private void publishToDestination(CachePublishingEvent event, DestinationBinding binding) {
        try {
            KafkaProducer<String, String> producer = getOrCreateProducer(binding);
            String key = event.getKey();
            String value = event.toJson();
            
            ProducerRecord<String, String> record = new ProducerRecord<>(binding.topic, key, value);
            
            producer.send(record, (RecordMetadata metadata, Exception exception) -> {
                if (exception != null) {
                    errors.incrementAndGet();
                    log.error("Failed to publish event to Kafka topic '{}' for key '{}': {}",
                            binding.topic, key, exception.getMessage());
                } else {
                    eventsPublished.incrementAndGet();
                    log.info("Published event to Kafka: topic={}, partition={}, offset={}, key={}",
                            metadata.topic(), metadata.partition(), metadata.offset(), key);
                }
            });
            
        } catch (Exception e) {
            errors.incrementAndGet();
            log.error("Error publishing to Kafka topic '{}', key '{}': {}",
                    binding.topic, event.getKey(), e.getMessage());
        }
    }
    
    @Override
    public PublisherStats getStats() {
        return new PublisherStats(TYPE, eventsPublished.get(), errors.get(), regionBindings.size());
    }
    
    private KafkaProducer<String, String> getOrCreateProducer(DestinationBinding binding) {
        return producers.computeIfAbsent(binding.bootstrapServers, servers -> {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.ACKS_CONFIG, binding.acks);
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, binding.batchSize);
            props.put(ProducerConfig.LINGER_MS_CONFIG, binding.lingerMs);
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
            
            // Apply SSL/TLS settings if enabled
            applyKafkaSslConfig(props, binding.ssl);
            
            log.info("Creating Kafka producer for bootstrap servers: {}{}", servers,
                    binding.ssl.isEnabled() ? " [SSL]" : "");
            return new KafkaProducer<>(props);
        });
    }
    
    private AdminClient createAdminClient(String bootstrapServers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);
        return AdminClient.create(props);
    }
    
    /**
     * Apply SSL/TLS configuration to Kafka client properties.
     * Used for both producers and admin clients.
     */
    private void applyKafkaSslConfig(Properties props, BrokerSsl ssl) {
        if (ssl == null || !ssl.isEnabled()) return;
        
        props.put("security.protocol", "SSL");
        
        if (!ssl.getProtocol().isBlank()) {
            props.put("ssl.protocol", ssl.getProtocol());
        }
        if (!ssl.getTrustStorePath().isBlank()) {
            props.put("ssl.truststore.location", ssl.getTrustStorePath());
            props.put("ssl.truststore.password", ssl.getTrustStorePassword());
            props.put("ssl.truststore.type", ssl.getTrustStoreType());
        }
        if (!ssl.getKeyStorePath().isBlank()) {
            props.put("ssl.keystore.location", ssl.getKeyStorePath());
            props.put("ssl.keystore.password", ssl.getKeyStorePassword());
            props.put("ssl.keystore.type", ssl.getKeyStoreType());
            if (!ssl.getKeyPassword().isBlank()) {
                props.put("ssl.key.password", ssl.getKeyPassword());
            }
        }
        if (!ssl.isHostnameVerification()) {
            props.put("ssl.endpoint.identification.algorithm", "");
        }
    }
    
    @Override
    public void shutdown() {
        log.info("Shutting down Kafka Event Publisher...");
        for (Map.Entry<String, KafkaProducer<String, String>> entry : producers.entrySet()) {
            try {
                entry.getValue().close(Duration.ofSeconds(10));
                log.info("Closed Kafka producer for {}", entry.getKey());
            } catch (Exception e) {
                log.warn("Error closing Kafka producer for {}: {}", entry.getKey(), e.getMessage());
            }
        }
        producers.clear();
    }
}
