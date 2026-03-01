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
import com.kuber.server.config.KuberProperties.DestinationConfig;
import com.kuber.server.config.KuberProperties.RegionPublishingConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Loads region event publishing configuration from an external JSON file.
 *
 * On startup, reads the JSON file specified by {@code kuber.publishing.region-config-file}
 * (default: {@code config/event_publishing.json}) and merges the region definitions into
 * the existing {@link KuberProperties.Publishing#getRegions()} map.
 *
 * Regions defined in the JSON file <b>override</b> any same-named regions from
 * {@code application.properties}, allowing operators to manage event publishing
 * separately from the main config file.
 *
 * <h3>JSON Structure</h3>
 * <pre>{@code
 * {
 *   "regions": {
 *     "trades": {
 *       "enabled": true,
 *       "destinations": [
 *         { "broker": "kafka-prod", "topic": "kuber.trades.events" },
 *         { "broker": "activemq-legacy", "topic": "TRADES.EVENTS.QUEUE" },
 *         { "broker": "audit-files", "topic": "trades" }
 *       ]
 *     }
 *   }
 * }
 * }</pre>
 *
 * This component must be initialized <b>before</b> {@link PublisherRegistry} and
 * {@link RegionEventPublishingService} so that region configs are available when
 * publishers read them. Spring guarantees this via constructor injection ordering.
 *
 * @version 2.6.4
 */
@Slf4j
@Component
public class EventPublishingConfigLoader {

    private final KuberProperties properties;
    private final ObjectMapper objectMapper;

    private int regionsLoaded = 0;

    public EventPublishingConfigLoader(KuberProperties properties,
                                       MessageBrokerConfigLoader brokerLoader) {
        this.properties = properties;
        this.objectMapper = new ObjectMapper();
        // brokerLoader injected to guarantee broker definitions are loaded before region configs
    }

    @PostConstruct
    public void load() {
        String configPath = properties.getPublishing().getRegionConfigFile();
        if (configPath == null || configPath.isBlank()) {
            log.info("Event publishing region config file not configured, skipping external config");
            return;
        }

        File configFile = new File(configPath);
        if (!configFile.isAbsolute()) {
            // Resolve relative to working directory
            configFile = new File(System.getProperty("user.dir"), configPath);
        }

        if (!configFile.exists()) {
            log.info("Event publishing config file not found at '{}', using application.properties only",
                    configFile.getAbsolutePath());
            return;
        }

        try {
            log.info("Loading event publishing config from '{}'", configFile.getAbsolutePath());
            JsonNode root = objectMapper.readTree(configFile);

            JsonNode regionsNode = root.get("regions");
            if (regionsNode == null || !regionsNode.isObject()) {
                log.warn("No 'regions' object found in {}", configFile.getName());
                return;
            }

            Map<String, RegionPublishingConfig> existingRegions = properties.getPublishing().getRegions();

            Iterator<Map.Entry<String, JsonNode>> fields = regionsNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String regionName = entry.getKey();
                JsonNode regionNode = entry.getValue();

                try {
                    RegionPublishingConfig config = parseRegionConfig(regionNode);
                    existingRegions.put(regionName, config);
                    regionsLoaded++;

                    if (config.isEnabled()) {
                        log.info("  ├─ Region '{}': enabled, {} destination(s)",
                                regionName, config.getDestinations().size());
                        for (DestinationConfig dest : config.getDestinations()) {
                            log.info("  │    └─ broker={}, topic={}",
                                    dest.getBroker(), dest.getTopic());
                        }
                    } else {
                        log.info("  ├─ Region '{}': disabled", regionName);
                    }

                } catch (Exception e) {
                    log.error("  ├─ Region '{}': FAILED to parse - {}", regionName, e.getMessage());
                }
            }

            log.info("Loaded {} region(s) from {}", regionsLoaded, configFile.getName());

        } catch (IOException e) {
            log.error("Failed to read event publishing config file '{}': {}",
                    configFile.getAbsolutePath(), e.getMessage());
        }
    }

    /**
     * Parse a single region configuration from JSON.
     */
    private RegionPublishingConfig parseRegionConfig(JsonNode node) {
        RegionPublishingConfig config = new RegionPublishingConfig();

        if (node.has("enabled")) {
            config.setEnabled(node.get("enabled").asBoolean(false));
        }

        if (node.has("destinations") && node.get("destinations").isArray()) {
            List<DestinationConfig> destinations = new ArrayList<>();

            for (JsonNode destNode : node.get("destinations")) {
                DestinationConfig dest = new DestinationConfig();

                if (destNode.has("broker")) {
                    dest.setBroker(destNode.get("broker").asText());
                }
                if (destNode.has("topic")) {
                    dest.setTopic(destNode.get("topic").asText());
                }
                if (destNode.has("routing-key")) {
                    dest.setRoutingKey(destNode.get("routing-key").asText());
                } else if (destNode.has("routingKey")) {
                    dest.setRoutingKey(destNode.get("routingKey").asText());
                }
                if (destNode.has("queue")) {
                    dest.setQueue(destNode.get("queue").asText());
                }
                if (destNode.has("ttl-seconds")) {
                    dest.setTtlSeconds(destNode.get("ttl-seconds").asInt(0));
                } else if (destNode.has("ttlSeconds")) {
                    dest.setTtlSeconds(destNode.get("ttlSeconds").asInt(0));
                }
                if (destNode.has("persistent")) {
                    dest.setPersistent(destNode.get("persistent").asBoolean());
                }

                destinations.add(dest);
            }

            config.setDestinations(destinations);
        }

        return config;
    }

    /**
     * Get the number of regions loaded from the external config file.
     */
    public int getRegionsLoaded() {
        return regionsLoaded;
    }
}
