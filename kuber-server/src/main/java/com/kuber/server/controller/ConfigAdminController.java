/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.server.controller;

import com.kuber.server.cache.CacheService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.config.KuberProperties.BrokerDefinition;
import com.kuber.server.config.KuberProperties.RegionPublishingConfig;
import com.kuber.server.event.EventPublisher;
import com.kuber.server.publishing.RegionEventPublishingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

/**
 * Admin controller for CRUD management of external JSON configuration:
 * message brokers, event publishing regions, and request/response messaging.
 *
 * @version 2.6.4
 */
@Slf4j
@Controller
@RequestMapping("/admin")
@PreAuthorize("hasRole('ADMIN')")
@RequiredArgsConstructor
public class ConfigAdminController {

    private final KuberProperties properties;
    private final ObjectMapper objectMapper;
    private final CacheService cacheService;
    private final RegionEventPublishingService publishingService;
    private final EventPublisher eventPublisher;

    @ModelAttribute
    public void addCurrentPage(Model model) {
        model.addAttribute("currentPage", "admin");
    }

    // ======================== Broker List Page ========================

    @GetMapping("/brokers")
    public String brokersAdmin(Model model) {
        Map<String, BrokerDefinition> brokers = properties.getPublishing().getBrokers();
        String configPath = resolveConfigPath(properties.getPublishing().getBrokerConfigFile());

        List<Map<String, Object>> brokerList = new ArrayList<>();
        for (Map.Entry<String, BrokerDefinition> entry : brokers.entrySet()) {
            Map<String, Object> info = new LinkedHashMap<>();
            BrokerDefinition def = entry.getValue();
            info.put("name", entry.getKey());
            info.put("type", def.getType() != null ? def.getType() : "");
            info.put("enabled", def.isEnabled());
            info.put("connection", buildConnectionSummary(def));
            info.put("sslEnabled", def.getSsl() != null && def.getSsl().isEnabled());
            info.put("sslMode", def.getSsl() != null && def.getSsl().getMode() != null ? def.getSsl().getMode() : "");
            brokerList.add(info);
        }

        model.addAttribute("brokers", brokerList);
        model.addAttribute("brokerCount", brokerList.size());
        model.addAttribute("enabledCount", brokerList.stream().filter(b -> (boolean) b.get("enabled")).count());
        model.addAttribute("sslCount", brokerList.stream().filter(b -> (boolean) b.get("sslEnabled")).count());
        model.addAttribute("configFile", configPath);
        return "admin/brokers";
    }

    @GetMapping("/brokers/add")
    public String addBrokerPage(Model model) {
        return "admin/brokers-add";
    }

    @GetMapping("/event-publishing/add")
    public String addRegionPage(Model model) {
        Map<String, BrokerDefinition> brokers = properties.getPublishing().getBrokers();
        model.addAttribute("brokerNames", new ArrayList<>(brokers.keySet()));
        model.addAttribute("cacheRegionNames", new ArrayList<>(cacheService.getRegionNames()));
        return "admin/event-publishing-add";
    }

    @GetMapping("/messaging/add-channel")
    public String addChannelPage(Model model) {
        Map<String, BrokerDefinition> brokers = properties.getPublishing().getBrokers();
        List<Map<String, String>> brokerList = new ArrayList<>();
        for (Map.Entry<String, BrokerDefinition> entry : brokers.entrySet()) {
            Map<String, String> info = new LinkedHashMap<>();
            BrokerDefinition def = entry.getValue();
            info.put("name", entry.getKey());
            info.put("type", def.getType() != null ? def.getType() : "");
            info.put("connection", buildConnectionSummary(def));
            brokerList.add(info);
        }
        model.addAttribute("availableBrokers", brokerList);
        return "admin/messaging-add-channel";
    }

    // ======================== Broker REST APIs ========================

    @GetMapping("/api/config/brokers")
    @ResponseBody
    public ResponseEntity<?> getBrokersJson() {
        return readJsonConfig(resolveConfigPath(properties.getPublishing().getBrokerConfigFile()));
    }

    @GetMapping("/api/config/brokers/{name}/details")
    @ResponseBody
    public ResponseEntity<?> getBrokerDetails(@PathVariable String name) {
        BrokerDefinition def = properties.getPublishing().getBrokers().get(name);
        if (def == null) return ResponseEntity.notFound().build();
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("name", name);
        details.put("type", def.getType());
        details.put("enabled", def.isEnabled());
        // Build connection map for messaging API compatibility
        Map<String, String> connection = new LinkedHashMap<>();
        if ("kafka".equalsIgnoreCase(def.getType())) {
            connection.put("bootstrap_servers", def.getBootstrapServers());
            connection.put("group_id", "kuber-request-processor");
        } else if ("activemq".equalsIgnoreCase(def.getType())) {
            connection.put("broker_url", def.getBrokerUrl());
            if (def.getUsername() != null) connection.put("username", def.getUsername());
            if (def.getPassword() != null) connection.put("password", def.getPassword());
        } else if ("rabbitmq".equalsIgnoreCase(def.getType())) {
            connection.put("host", def.getHost());
            connection.put("port", String.valueOf(def.getPort()));
            connection.put("virtual_host", def.getVirtualHost() != null ? def.getVirtualHost() : "/");
            if (def.getUsername() != null) connection.put("username", def.getUsername());
            if (def.getPassword() != null) connection.put("password", def.getPassword());
        } else if ("ibmmq".equalsIgnoreCase(def.getType())) {
            connection.put("queue_manager", def.getQueueManager());
            connection.put("channel", def.getChannel());
            connection.put("conn_name", def.getHost() + "(" + def.getPort() + ")");
            if (def.getUsername() != null) connection.put("username", def.getUsername());
            if (def.getPassword() != null) connection.put("password", def.getPassword());
        }
        details.put("connection", connection);
        return ResponseEntity.ok(details);
    }

    @PostMapping("/api/config/brokers")
    @ResponseBody
    public ResponseEntity<?> addBroker(@RequestBody Map<String, Object> payload) {
        String name = (String) payload.get("name");
        if (name == null || name.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Broker name is required"));
        }
        if (properties.getPublishing().getBrokers().containsKey(name)) {
            return ResponseEntity.badRequest().body(Map.of("error", "Broker '" + name + "' already exists"));
        }
        try {
            ObjectNode brokerNode = objectMapper.valueToTree(payload.get("config"));
            return writeBrokerToFile(name, brokerNode);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/api/config/brokers/{name}")
    @ResponseBody
    public ResponseEntity<?> deleteBroker(@PathVariable String name) {
        try {
            File file = getConfigFile(properties.getPublishing().getBrokerConfigFile());
            ObjectNode root = readOrCreateRoot(file, "brokers");
            ObjectNode brokersNode = (ObjectNode) root.get("brokers");
            if (brokersNode == null || !brokersNode.has(name)) {
                return ResponseEntity.notFound().build();
            }
            brokersNode.remove(name);
            writeJsonFile(file, root);
            properties.getPublishing().getBrokers().remove(name);
            log.info("Admin: deleted broker '{}'", name);
            return ResponseEntity.ok(Map.of("status", "OK", "message", "Broker '" + name + "' deleted"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/api/config/brokers/{name}/enable")
    @ResponseBody
    public ResponseEntity<?> enableBroker(@PathVariable String name) {
        return setBrokerEnabled(name, true);
    }

    @PostMapping("/api/config/brokers/{name}/disable")
    @ResponseBody
    public ResponseEntity<?> disableBroker(@PathVariable String name) {
        return setBrokerEnabled(name, false);
    }

    @PutMapping("/api/config/brokers-raw")
    @ResponseBody
    public ResponseEntity<?> saveBrokersRawJson(@RequestBody String content) {
        return saveJsonConfig(resolveConfigPath(properties.getPublishing().getBrokerConfigFile()), content);
    }

    // ======================== Cache Regions API (for dropdowns) ========================

    @GetMapping("/api/config/cache-regions")
    @ResponseBody
    public ResponseEntity<?> getCacheRegionNames() {
        return ResponseEntity.ok(new ArrayList<>(cacheService.getRegionNames()));
    }

    // ======================== Event Publishing List Page ========================

    @GetMapping("/event-publishing")
    public String eventPublishingAdmin(Model model) {
        Map<String, RegionPublishingConfig> regions = properties.getPublishing().getRegions();
        String configPath = resolveConfigPath(properties.getPublishing().getRegionConfigFile());
        Map<String, BrokerDefinition> brokers = properties.getPublishing().getBrokers();

        List<Map<String, Object>> regionList = new ArrayList<>();
        for (Map.Entry<String, RegionPublishingConfig> entry : regions.entrySet()) {
            Map<String, Object> info = new LinkedHashMap<>();
            RegionPublishingConfig cfg = entry.getValue();
            info.put("name", entry.getKey());
            info.put("enabled", cfg.isEnabled());
            info.put("destinationCount", cfg.getDestinations() != null ? cfg.getDestinations().size() : 0);
            info.put("publishedCount", publishingService.getRegionPublishedCount(entry.getKey()));
            List<Map<String, String>> dests = new ArrayList<>();
            if (cfg.getDestinations() != null) {
                for (var dest : cfg.getDestinations()) {
                    Map<String, String> d = new LinkedHashMap<>();
                    d.put("broker", dest.getBroker() != null ? dest.getBroker() : "");
                    d.put("topic", dest.getTopic() != null ? dest.getTopic() : "");
                    d.put("brokerType", brokers.containsKey(dest.getBroker())
                            ? brokers.get(dest.getBroker()).getType() : "unknown");
                    dests.add(d);
                }
            }
            info.put("destinations", dests);
            regionList.add(info);
        }

        var stats = publishingService.getStats();
        model.addAttribute("regions", regionList);
        model.addAttribute("regionCount", regionList.size());
        model.addAttribute("enabledCount", regionList.stream().filter(r -> (boolean) r.get("enabled")).count());
        model.addAttribute("configFile", configPath);
        
        // Broker publishing stats
        model.addAttribute("totalPublished", stats.totalEventsPublished());
        model.addAttribute("totalErrors", stats.publishErrors());
        model.addAttribute("totalDropped", stats.eventsDropped());
        model.addAttribute("brokerQueueDepth", stats.queueSize());
        model.addAttribute("brokerActiveThreads", stats.activeThreads());
        
        // Internal event bus stats
        model.addAttribute("eventBusTotal", eventPublisher.getTotalEventsPublished());
        model.addAttribute("eventBusChannels", eventPublisher.getActiveChannelCount());
        model.addAttribute("eventBusSubscribers", eventPublisher.getTotalSubscriptions());
        model.addAttribute("eventBusListeners", eventPublisher.getListenerCount());
        
        return "admin/event-publishing";
    }

    // ======================== Event Publishing REST APIs ========================

    @GetMapping("/api/config/event-publishing")
    @ResponseBody
    public ResponseEntity<?> getEventPublishingJson() {
        return readJsonConfig(resolveConfigPath(properties.getPublishing().getRegionConfigFile()));
    }

    @PostMapping("/api/config/event-publishing")
    @ResponseBody
    public ResponseEntity<?> addRegion(@RequestBody Map<String, Object> payload) {
        String name = (String) payload.get("name");
        if (name == null || name.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "Region name is required"));
        }
        if (properties.getPublishing().getRegions().containsKey(name)) {
            return ResponseEntity.badRequest().body(Map.of("error", "Region '" + name + "' already exists"));
        }
        try {
            ObjectNode regionNode = objectMapper.valueToTree(payload.get("config"));
            return writeRegionToFile(name, regionNode);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/api/config/event-publishing/{name}")
    @ResponseBody
    public ResponseEntity<?> deleteRegion(@PathVariable String name) {
        try {
            File file = getConfigFile(properties.getPublishing().getRegionConfigFile());
            ObjectNode root = readOrCreateRoot(file, "regions");
            ObjectNode regionsNode = (ObjectNode) root.get("regions");
            if (regionsNode == null || !regionsNode.has(name)) {
                return ResponseEntity.notFound().build();
            }
            regionsNode.remove(name);
            writeJsonFile(file, root);
            properties.getPublishing().getRegions().remove(name);
            publishingService.refreshPublishing();
            log.info("Admin: deleted region '{}'", name);
            return ResponseEntity.ok(Map.of("status", "OK", "message", "Region '" + name + "' deleted"));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/api/config/event-publishing/{name}/enable")
    @ResponseBody
    public ResponseEntity<?> enableRegion(@PathVariable String name) {
        return setRegionEnabled(name, true);
    }

    @PostMapping("/api/config/event-publishing/{name}/disable")
    @ResponseBody
    public ResponseEntity<?> disableRegion(@PathVariable String name) {
        return setRegionEnabled(name, false);
    }

    @PutMapping("/api/config/event-publishing-raw")
    @ResponseBody
    public ResponseEntity<?> saveEventPublishingRawJson(@RequestBody String content) {
        ResponseEntity<?> result = saveJsonConfig(resolveConfigPath(properties.getPublishing().getRegionConfigFile()), content);
        // Reload regions from the saved JSON into in-memory properties and refresh publishers
        try {
            JsonNode root = objectMapper.readTree(content);
            JsonNode regionsNode = root.get("regions");
            if (regionsNode != null && regionsNode.isObject()) {
                Map<String, RegionPublishingConfig> regions = properties.getPublishing().getRegions();
                regions.clear();
                var fields = regionsNode.fields();
                while (fields.hasNext()) {
                    var entry = fields.next();
                    RegionPublishingConfig cfg = objectMapper.treeToValue(entry.getValue(), RegionPublishingConfig.class);
                    regions.put(entry.getKey(), cfg);
                }
            }
            publishingService.refreshPublishing();
        } catch (Exception e) {
            log.warn("Admin: raw JSON saved but could not sync to memory: {}", e.getMessage());
        }
        return result;
    }

    // ======================== Broker names API (for dropdowns) ========================

    @GetMapping("/api/config/broker-names")
    @ResponseBody
    public ResponseEntity<?> getBrokerNames() {
        return ResponseEntity.ok(new ArrayList<>(properties.getPublishing().getBrokers().keySet()));
    }

    // ======================== Helpers ========================

    private ResponseEntity<?> writeBrokerToFile(String name, ObjectNode brokerNode) {
        try {
            File file = getConfigFile(properties.getPublishing().getBrokerConfigFile());
            ObjectNode root = readOrCreateRoot(file, "brokers");
            ObjectNode brokersNode = (ObjectNode) root.get("brokers");
            if (brokersNode == null) {
                brokersNode = objectMapper.createObjectNode();
                root.set("brokers", brokersNode);
            }
            brokersNode.set(name, brokerNode);
            writeJsonFile(file, root);

            // Sync in-memory properties so the UI reflects the change immediately
            try {
                BrokerDefinition def = objectMapper.treeToValue(brokerNode, BrokerDefinition.class);
                properties.getPublishing().getBrokers().put(name, def);
            } catch (Exception ex) {
                log.warn("Admin: broker '{}' saved to file but could not sync to memory: {}", name, ex.getMessage());
            }

            log.info("Admin: created broker '{}'", name);
            return ResponseEntity.ok(Map.of("status", "OK", "message", "Broker '" + name + "' created."));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    private ResponseEntity<?> writeRegionToFile(String name, ObjectNode regionNode) {
        try {
            File file = getConfigFile(properties.getPublishing().getRegionConfigFile());
            ObjectNode root = readOrCreateRoot(file, "regions");
            ObjectNode regionsNode = (ObjectNode) root.get("regions");
            if (regionsNode == null) {
                regionsNode = objectMapper.createObjectNode();
                root.set("regions", regionsNode);
            }
            regionsNode.set(name, regionNode);
            writeJsonFile(file, root);

            // Sync in-memory properties so the UI reflects the change immediately
            try {
                RegionPublishingConfig cfg = objectMapper.treeToValue(regionNode, RegionPublishingConfig.class);
                properties.getPublishing().getRegions().put(name, cfg);
            } catch (Exception ex) {
                log.warn("Admin: region '{}' saved to file but could not sync to memory: {}", name, ex.getMessage());
            }

            publishingService.refreshPublishing();
            log.info("Admin: created region '{}'", name);
            return ResponseEntity.ok(Map.of("status", "OK", "message", "Region '" + name + "' created."));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    private ResponseEntity<?> setBrokerEnabled(String name, boolean enabled) {
        try {
            File file = getConfigFile(properties.getPublishing().getBrokerConfigFile());
            ObjectNode root = readOrCreateRoot(file, "brokers");
            ObjectNode brokersNode = (ObjectNode) root.get("brokers");
            if (brokersNode == null || !brokersNode.has(name)) {
                return ResponseEntity.notFound().build();
            }
            ((ObjectNode) brokersNode.get(name)).put("enabled", enabled);
            writeJsonFile(file, root);
            BrokerDefinition def = properties.getPublishing().getBrokers().get(name);
            if (def != null) def.setEnabled(enabled);
            publishingService.refreshPublishing();
            log.info("Admin: {} broker '{}'", enabled ? "enabled" : "disabled", name);
            return ResponseEntity.ok(Map.of("status", "OK", "message",
                    "Broker '" + name + "' " + (enabled ? "enabled" : "disabled")));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    private ResponseEntity<?> setRegionEnabled(String name, boolean enabled) {
        try {
            File file = getConfigFile(properties.getPublishing().getRegionConfigFile());
            ObjectNode root = readOrCreateRoot(file, "regions");
            ObjectNode regionsNode = (ObjectNode) root.get("regions");
            if (regionsNode == null || !regionsNode.has(name)) {
                return ResponseEntity.notFound().build();
            }
            ((ObjectNode) regionsNode.get(name)).put("enabled", enabled);
            writeJsonFile(file, root);
            RegionPublishingConfig cfg = properties.getPublishing().getRegions().get(name);
            if (cfg != null) cfg.setEnabled(enabled);
            publishingService.refreshPublishing();
            log.info("Admin: {} region '{}'", enabled ? "enabled" : "disabled", name);
            return ResponseEntity.ok(Map.of("status", "OK", "message",
                    "Region '" + name + "' " + (enabled ? "enabled" : "disabled")));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    private String resolveConfigPath(String path) {
        if (path == null || path.isBlank()) return "";
        File f = new File(path);
        if (!f.isAbsolute()) f = new File(System.getProperty("user.dir"), path);
        return f.getAbsolutePath();
    }

    private File getConfigFile(String path) {
        File f = new File(path);
        if (!f.isAbsolute()) f = new File(System.getProperty("user.dir"), path);
        return f;
    }

    private ObjectNode readOrCreateRoot(File file, String rootKey) throws IOException {
        if (file.exists()) {
            JsonNode node = objectMapper.readTree(file);
            if (node instanceof ObjectNode) return (ObjectNode) node;
        }
        ObjectNode root = objectMapper.createObjectNode();
        root.set(rootKey, objectMapper.createObjectNode());
        return root;
    }

    private void writeJsonFile(File file, ObjectNode root) throws IOException {
        file.getParentFile().mkdirs();
        Files.writeString(file.toPath(), objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
    }

    private String buildConnectionSummary(BrokerDefinition def) {
        if (def.getType() == null) return "";
        switch (def.getType().toLowerCase()) {
            case "kafka": return def.getBootstrapServers();
            case "activemq": return def.getBrokerUrl();
            case "rabbitmq": return def.getHost() + ":" + def.getPort() + def.getVirtualHost();
            case "ibmmq": return def.getHost() + ":" + def.getPort() + " (QM=" + def.getQueueManager() + ")";
            case "file": return def.getDirectory();
            default: return "";
        }
    }

    private ResponseEntity<?> readJsonConfig(String path) {
        try {
            File f = new File(path);
            if (!f.exists()) return ResponseEntity.notFound().build();
            return ResponseEntity.ok(Files.readString(f.toPath()));
        } catch (IOException e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    private ResponseEntity<?> saveJsonConfig(String path, String content) {
        try {
            objectMapper.readTree(content);
            Files.writeString(new File(path).toPath(), content);
            return ResponseEntity.ok(Map.of("status", "OK", "message", "Configuration saved. Restart to apply."));
        } catch (IOException e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
}
