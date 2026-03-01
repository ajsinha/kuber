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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.config.KuberProperties.BrokerDefinition;
import com.kuber.server.config.KuberProperties.BrokerSsl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Loads message broker definitions from an external JSON configuration file.
 *
 * On startup, reads the JSON file specified by {@code kuber.publishing.broker-config-file}
 * (default: {@code config/message_brokers.json}) and merges the broker definitions into
 * the existing {@link KuberProperties.Publishing#getBrokers()} map.
 *
 * Brokers defined in the JSON file <b>override</b> any same-named brokers from
 * {@code application.properties}, allowing operators to manage broker configurations
 * (including SSL/TLS settings) in a dedicated, structured file.
 *
 * <h3>JSON Structure</h3>
 * <pre>{@code
 * {
 *   "brokers": {
 *     "kafka-prod": {
 *       "enabled": true,
 *       "type": "kafka",
 *       "bootstrap-servers": "kafka1:9093",
 *       "ssl": {
 *         "enabled": true,
 *         "protocol": "TLSv1.3",
 *         "trust-store-path": "/certs/truststore.jks",
 *         "trust-store-password": "changeit"
 *       }
 *     }
 *   }
 * }
 * }</pre>
 *
 * This component must be initialized <b>before</b> {@link EventPublishingConfigLoader},
 * {@link PublisherRegistry}, and {@link RegionEventPublishingService} so that broker
 * configs are available when region configs and publishers reference them.
 *
 * @version 2.6.3
 */
@Slf4j
@Component
public class MessageBrokerConfigLoader {

    private final KuberProperties properties;
    private final ObjectMapper objectMapper;

    private int brokersLoaded = 0;

    public MessageBrokerConfigLoader(KuberProperties properties) {
        this.properties = properties;
        this.objectMapper = new ObjectMapper();
    }

    @PostConstruct
    public void load() {
        String configPath = properties.getPublishing().getBrokerConfigFile();
        if (configPath == null || configPath.isBlank()) {
            log.info("Message broker config file not configured, skipping external config");
            return;
        }

        File configFile = new File(configPath);
        if (!configFile.isAbsolute()) {
            configFile = new File(System.getProperty("user.dir"), configPath);
        }

        if (!configFile.exists()) {
            log.info("Message broker config file not found at '{}', using application.properties only",
                    configFile.getAbsolutePath());
            return;
        }

        try {
            log.info("Loading message broker config from '{}'", configFile.getAbsolutePath());
            JsonNode root = objectMapper.readTree(configFile);

            JsonNode brokersNode = root.get("brokers");
            if (brokersNode == null || !brokersNode.isObject()) {
                log.warn("No 'brokers' object found in {}", configFile.getName());
                return;
            }

            Map<String, BrokerDefinition> existingBrokers = properties.getPublishing().getBrokers();

            Iterator<Map.Entry<String, JsonNode>> fields = brokersNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String brokerName = entry.getKey();
                JsonNode brokerNode = entry.getValue();

                try {
                    BrokerDefinition def = parseBrokerDefinition(brokerNode);
                    existingBrokers.put(brokerName, def);
                    brokersLoaded++;

                    String sslLabel = "";
                    if (def.getSsl().isEnabled()) {
                        String mode = def.getSsl().getMode();
                        sslLabel = (mode != null && !mode.isEmpty()) 
                                ? " [SSL:" + mode.toUpperCase() + "]" : " [SSL]";
                    }
                    if (def.isEnabled()) {
                        log.info("  ├─ Broker '{}': type={}, enabled{}",
                                brokerName, def.getType(), sslLabel);
                    } else {
                        log.info("  ├─ Broker '{}': type={}, disabled{}",
                                brokerName, def.getType(), sslLabel);
                    }
                } catch (Exception e) {
                    log.error("  ├─ Broker '{}': FAILED to parse - {}", brokerName, e.getMessage());
                }
            }

            log.info("Loaded {} broker(s) from {}", brokersLoaded, configFile.getName());

        } catch (IOException e) {
            log.error("Failed to read message broker config file '{}': {}",
                    configFile.getAbsolutePath(), e.getMessage());
        }
    }

    /**
     * Parse a single broker definition from JSON.
     * Supports both kebab-case (JSON convention) and camelCase property names.
     */
    private BrokerDefinition parseBrokerDefinition(JsonNode node) {
        BrokerDefinition def = new BrokerDefinition();

        // Core
        def.setEnabled(boolVal(node, "enabled", false));
        def.setType(strVal(node, "type", ""));

        // Kafka
        def.setBootstrapServers(strVal(node, "bootstrap-servers", "bootstrapServers", "localhost:9092"));
        def.setPartitions(intVal(node, "partitions", 3));
        def.setReplicationFactor((short) intVal(node, "replication-factor", "replicationFactor", 1));
        def.setRetentionHours(intVal(node, "retention-hours", "retentionHours", 168));
        def.setAcks(strVal(node, "acks", "1"));
        def.setBatchSize(intVal(node, "batch-size", "batchSize", 16384));
        def.setLingerMs(intVal(node, "linger-ms", "lingerMs", 5));

        // ActiveMQ
        def.setBrokerUrl(strVal(node, "broker-url", "brokerUrl", "tcp://localhost:61616"));

        // RabbitMQ
        def.setHost(strVal(node, "host", "localhost"));
        def.setPort(intVal(node, "port", 5672));
        def.setVirtualHost(strVal(node, "virtual-host", "virtualHost", "/"));
        def.setExchangeType(strVal(node, "exchange-type", "exchangeType", "topic"));
        def.setDurable(boolVal(node, "durable", true));

        // IBM MQ
        def.setQueueManager(strVal(node, "queue-manager", "queueManager", "QM1"));
        def.setChannel(strVal(node, "channel", "DEV.APP.SVRCONN"));
        def.setCcsid(intVal(node, "ccsid", 0));
        def.setSslCipherSuite(strVal(node, "ssl-cipher-suite", "sslCipherSuite", ""));

        // File
        def.setDirectory(strVal(node, "directory", "./events"));
        def.setMaxFileSizeMb(intVal(node, "max-file-size-mb", "maxFileSizeMb", 100));
        def.setRotationPolicy(strVal(node, "rotation-policy", "rotationPolicy", "daily"));
        def.setFormat(strVal(node, "format", "jsonl"));
        def.setCompress(boolVal(node, "compress", false));
        def.setRetentionDays(intVal(node, "retention-days", "retentionDays", 30));

        // Common
        def.setUsername(strVal(node, "username", ""));
        def.setPassword(strVal(node, "password", ""));
        def.setTtlSeconds(intVal(node, "ttl-seconds", "ttlSeconds", 86400));
        def.setPersistent(boolVal(node, "persistent", true));
        def.setUseTopic(boolVal(node, "use-topic", "useTopic", false));

        // SSL/TLS
        JsonNode sslNode = node.get("ssl");
        if (sslNode != null && sslNode.isObject()) {
            BrokerSsl ssl = new BrokerSsl();
            ssl.setEnabled(boolVal(sslNode, "enabled", false));
            ssl.setMode(strVal(sslNode, "mode", ""));
            ssl.setProtocol(strVal(sslNode, "protocol", "TLSv1.3"));
            // JKS/PKCS12 trust store
            ssl.setTrustStorePath(strVal(sslNode, "trust-store-path", "trustStorePath", ""));
            ssl.setTrustStorePassword(strVal(sslNode, "trust-store-password", "trustStorePassword", ""));
            ssl.setTrustStoreType(strVal(sslNode, "trust-store-type", "trustStoreType", "JKS"));
            // JKS/PKCS12 key store (mTLS)
            ssl.setKeyStorePath(strVal(sslNode, "key-store-path", "keyStorePath", ""));
            ssl.setKeyStorePassword(strVal(sslNode, "key-store-password", "keyStorePassword", ""));
            ssl.setKeyStoreType(strVal(sslNode, "key-store-type", "keyStoreType", "JKS"));
            ssl.setKeyPassword(strVal(sslNode, "key-password", "keyPassword", ""));
            // PEM certificate files
            ssl.setTrustCertPath(strVal(sslNode, "trust-cert-path", "trustCertPath", ""));
            ssl.setKeyCertPath(strVal(sslNode, "key-cert-path", "keyCertPath", ""));
            ssl.setKeyPath(strVal(sslNode, "key-path", "keyPath", ""));
            // SASL (Kafka)
            ssl.setSaslMechanism(strVal(sslNode, "sasl-mechanism", "saslMechanism", ""));
            ssl.setSaslJaasConfig(strVal(sslNode, "sasl-jaas-config", "saslJaasConfig", ""));
            // General
            ssl.setHostnameVerification(boolVal(sslNode, "hostname-verification", "hostnameVerification", true));
            ssl.setCipherSuite(strVal(sslNode, "cipher-suite", "cipherSuite", ""));
            def.setSsl(ssl);
        }

        return def;
    }

    // ==================== JSON Helper Methods ====================

    private String strVal(JsonNode node, String key, String defaultVal) {
        JsonNode v = node.get(key);
        return (v != null && !v.isNull()) ? v.asText() : defaultVal;
    }

    private String strVal(JsonNode node, String kebab, String camel, String defaultVal) {
        JsonNode v = node.get(kebab);
        if (v == null || v.isNull()) v = node.get(camel);
        return (v != null && !v.isNull()) ? v.asText() : defaultVal;
    }

    private int intVal(JsonNode node, String key, int defaultVal) {
        JsonNode v = node.get(key);
        return (v != null && !v.isNull()) ? v.asInt(defaultVal) : defaultVal;
    }

    private int intVal(JsonNode node, String kebab, String camel, int defaultVal) {
        JsonNode v = node.get(kebab);
        if (v == null || v.isNull()) v = node.get(camel);
        return (v != null && !v.isNull()) ? v.asInt(defaultVal) : defaultVal;
    }

    private boolean boolVal(JsonNode node, String key, boolean defaultVal) {
        JsonNode v = node.get(key);
        return (v != null && !v.isNull()) ? v.asBoolean(defaultVal) : defaultVal;
    }

    private boolean boolVal(JsonNode node, String kebab, String camel, boolean defaultVal) {
        JsonNode v = node.get(kebab);
        if (v == null || v.isNull()) v = node.get(camel);
        return (v != null && !v.isNull()) ? v.asBoolean(defaultVal) : defaultVal;
    }

    /**
     * Get the number of brokers loaded from the external config file.
     */
    public int getBrokersLoaded() {
        return brokersLoaded;
    }
}
