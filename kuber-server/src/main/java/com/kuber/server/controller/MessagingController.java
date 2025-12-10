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
package com.kuber.server.controller;

import com.kuber.server.messaging.MessageBrokerAdapter;
import com.kuber.server.messaging.MessagingConfig;
import com.kuber.server.messaging.RequestResponseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * Controller for Request/Response Messaging administration.
 * 
 * <p>Provides:
 * <ul>
 *   <li>Admin UI for managing message brokers and topics</li>
 *   <li>API endpoints for configuration management</li>
 *   <li>Queue status and statistics monitoring</li>
 * </ul>
 * 
 * @version 1.7.0
 */
@Controller
@Slf4j
public class MessagingController {
    
    @Autowired(required = false)
    private RequestResponseService messagingService;
    
    // ==================== Admin Pages ====================
    
    /**
     * Messaging admin page - broker management.
     */
    @GetMapping("/admin/messaging")
    public String messagingAdmin(Model model) {
        if (messagingService == null) {
            model.addAttribute("serviceAvailable", false);
            return "admin/messaging";
        }
        
        model.addAttribute("serviceAvailable", true);
        model.addAttribute("stats", messagingService.getServiceStats());
        model.addAttribute("config", messagingService.getConfig());
        model.addAttribute("brokerStats", messagingService.getBrokerStats());
        model.addAttribute("failedSubscriptions", messagingService.getFailedSubscriptions());
        
        return "admin/messaging";
    }
    
    /**
     * Messaging queue status page.
     */
    @GetMapping("/admin/messaging/queue")
    public String messagingQueue(Model model) {
        if (messagingService == null) {
            model.addAttribute("serviceAvailable", false);
            return "admin/messaging-queue";
        }
        
        model.addAttribute("serviceAvailable", true);
        model.addAttribute("stats", messagingService.getServiceStats());
        model.addAttribute("pendingRequests", messagingService.getPendingRequests());
        model.addAttribute("failedSubscriptions", messagingService.getFailedSubscriptions());
        
        return "admin/messaging-queue";
    }
    
    // ==================== API Endpoints ====================
    
    /**
     * Get messaging service status.
     */
    @GetMapping("/api/v1/messaging/status")
    @ResponseBody
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> response = new HashMap<>();
        
        if (messagingService == null) {
            response.put("available", false);
            response.put("message", "Request/Response messaging service not configured");
            return ResponseEntity.ok(response);
        }
        
        response.put("available", true);
        response.put("stats", messagingService.getServiceStats());
        response.put("brokers", messagingService.getBrokerStats());
        
        return ResponseEntity.ok(response);
    }
    
    /**
     * Get pending requests in queue.
     */
    @GetMapping("/api/v1/messaging/queue")
    @ResponseBody
    public ResponseEntity<?> getQueue() {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        return ResponseEntity.ok(Map.of(
            "stats", messagingService.getServiceStats(),
            "pending", messagingService.getPendingRequests()
        ));
    }
    
    /**
     * Get configuration.
     */
    @GetMapping("/api/v1/messaging/config")
    @ResponseBody
    public ResponseEntity<?> getConfig() {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        return ResponseEntity.ok(messagingService.getConfig());
    }
    
    /**
     * Update configuration.
     */
    @PostMapping("/api/v1/messaging/config")
    @ResponseBody
    public ResponseEntity<?> updateConfig(@RequestBody MessagingConfig config) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        try {
            messagingService.saveConfig(config);
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Configuration saved. Changes will be applied automatically."
            ));
        } catch (Exception e) {
            log.error("Failed to save messaging config: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to save configuration: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Add a new broker.
     */
    @PostMapping("/api/v1/messaging/brokers")
    @ResponseBody
    public ResponseEntity<?> addBroker(@RequestBody Map<String, Object> request) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        try {
            String name = (String) request.get("name");
            String type = (String) request.get("type");
            String displayName = (String) request.get("displayName");
            Boolean enabled = (Boolean) request.getOrDefault("enabled", true);
            @SuppressWarnings("unchecked")
            Map<String, String> connection = (Map<String, String>) request.get("connection");
            @SuppressWarnings("unchecked")
            List<String> requestTopics = (List<String>) request.get("requestTopics");
            
            if (name == null || name.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of("error", "Broker name is required"));
            }
            if (type == null || type.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of("error", "Broker type is required"));
            }
            
            MessagingConfig.BrokerConfig brokerConfig = new MessagingConfig.BrokerConfig();
            brokerConfig.setEnabled(enabled);
            brokerConfig.setType(type);
            brokerConfig.setDisplayName(displayName != null ? displayName : name);
            if (connection != null) {
                brokerConfig.setConnection(connection);
            }
            if (requestTopics != null) {
                brokerConfig.setRequestTopics(requestTopics);
            }
            
            messagingService.addBroker(name, brokerConfig);
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Broker '" + name + "' added successfully"
            ));
            
        } catch (Exception e) {
            log.error("Failed to add broker: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to add broker: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Remove a broker.
     */
    @DeleteMapping("/api/v1/messaging/brokers/{name}")
    @ResponseBody
    public ResponseEntity<?> removeBroker(@PathVariable String name) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        try {
            messagingService.removeBroker(name);
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Broker '" + name + "' removed successfully"
            ));
        } catch (Exception e) {
            log.error("Failed to remove broker: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to remove broker: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Add a request topic to a broker.
     */
    @PostMapping("/api/v1/messaging/brokers/{brokerName}/topics")
    @ResponseBody
    public ResponseEntity<?> addTopic(@PathVariable String brokerName, 
                                      @RequestBody Map<String, String> request) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        try {
            String topic = request.get("topic");
            if (topic == null || topic.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of("error", "Topic name is required"));
            }
            
            messagingService.addRequestTopic(brokerName, topic);
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Topic '" + topic + "' added to broker '" + brokerName + "'"
            ));
            
        } catch (Exception e) {
            log.error("Failed to add topic: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to add topic: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Remove a request topic from a broker.
     */
    @DeleteMapping("/api/v1/messaging/brokers/{brokerName}/topics/{topic}")
    @ResponseBody
    public ResponseEntity<?> removeTopic(@PathVariable String brokerName, 
                                         @PathVariable String topic) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        try {
            messagingService.removeRequestTopic(brokerName, topic);
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Topic '" + topic + "' removed from broker '" + brokerName + "'"
            ));
        } catch (Exception e) {
            log.error("Failed to remove topic: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to remove topic: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Toggle messaging enabled/disabled.
     */
    @PostMapping("/api/v1/messaging/toggle")
    @ResponseBody
    public ResponseEntity<?> toggleMessaging(@RequestBody Map<String, Boolean> request) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        try {
            Boolean enabled = request.get("enabled");
            if (enabled == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "enabled field is required"));
            }
            
            MessagingConfig config = messagingService.getConfig();
            if (config == null) {
                config = new MessagingConfig();
            }
            config.setEnabled(enabled);
            messagingService.saveConfig(config);
            
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Messaging " + (enabled ? "enabled" : "disabled")
            ));
            
        } catch (Exception e) {
            log.error("Failed to toggle messaging: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to toggle messaging: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Drain requests from queue.
     */
    @PostMapping("/api/v1/messaging/queue/drain")
    @ResponseBody
    public ResponseEntity<?> drainQueue(@RequestBody Map<String, Integer> request) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        try {
            Integer count = request.get("count");
            int drainCount = (count == null) ? 0 : count;
            
            int drained = messagingService.drainQueue(drainCount);
            return ResponseEntity.ok(Map.of(
                "success", true,
                "drained", drained,
                "message", "Drained " + drained + " request(s) from queue"
            ));
            
        } catch (Exception e) {
            log.error("Failed to drain queue: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to drain queue: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Cancel a specific request.
     */
    @PostMapping("/api/v1/messaging/queue/cancel/{position}")
    @ResponseBody
    public ResponseEntity<?> cancelRequest(@PathVariable int position) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        try {
            boolean cancelled = messagingService.cancelRequest(position);
            if (cancelled) {
                return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Request cancelled"
                ));
            } else {
                return ResponseEntity.badRequest().body(Map.of(
                    "error", "Request not found at position " + position
                ));
            }
            
        } catch (Exception e) {
            log.error("Failed to cancel request: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to cancel request: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Get failed subscriptions.
     */
    @GetMapping("/api/v1/messaging/failed")
    @ResponseBody
    public ResponseEntity<?> getFailedSubscriptions() {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        return ResponseEntity.ok(Map.of(
            "failed", messagingService.getFailedSubscriptions()
        ));
    }
    
    /**
     * Retry a failed subscription.
     */
    @PostMapping("/api/v1/messaging/retry/{brokerName}")
    @ResponseBody
    public ResponseEntity<?> retrySubscription(@PathVariable String brokerName) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        try {
            boolean success = messagingService.retryFailedSubscription(brokerName);
            if (success) {
                return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Successfully reconnected to broker '" + brokerName + "'"
                ));
            } else {
                return ResponseEntity.badRequest().body(Map.of(
                    "error", "Failed to reconnect to broker '" + brokerName + "'"
                ));
            }
            
        } catch (Exception e) {
            log.error("Failed to retry subscription: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to retry subscription: " + e.getMessage()
            ));
        }
    }
    
    // ==================== Broker Control Endpoints ====================
    
    /**
     * Enable a broker - connect and start consuming messages.
     */
    @PostMapping("/api/v1/messaging/brokers/{brokerName}/enable")
    @ResponseBody
    public ResponseEntity<?> enableBroker(@PathVariable String brokerName) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        try {
            boolean success = messagingService.enableBroker(brokerName);
            if (success) {
                return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Broker '" + brokerName + "' enabled and connected"
                ));
            } else {
                return ResponseEntity.badRequest().body(Map.of(
                    "error", "Failed to enable broker '" + brokerName + "'"
                ));
            }
        } catch (Exception e) {
            log.error("Failed to enable broker: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to enable broker: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Disable a broker - disconnect and stop consuming messages.
     */
    @PostMapping("/api/v1/messaging/brokers/{brokerName}/disable")
    @ResponseBody
    public ResponseEntity<?> disableBroker(@PathVariable String brokerName) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        try {
            boolean success = messagingService.disableBroker(brokerName);
            if (success) {
                return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Broker '" + brokerName + "' disabled and disconnected"
                ));
            } else {
                return ResponseEntity.badRequest().body(Map.of(
                    "error", "Failed to disable broker '" + brokerName + "'"
                ));
            }
        } catch (Exception e) {
            log.error("Failed to disable broker: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to disable broker: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Pause a broker - stop consuming messages but keep connection open.
     */
    @PostMapping("/api/v1/messaging/brokers/{brokerName}/pause")
    @ResponseBody
    public ResponseEntity<?> pauseBroker(@PathVariable String brokerName) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        try {
            boolean success = messagingService.pauseBroker(brokerName);
            if (success) {
                return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Broker '" + brokerName + "' paused"
                ));
            } else {
                return ResponseEntity.badRequest().body(Map.of(
                    "error", "Failed to pause broker '" + brokerName + "' - broker may not be connected"
                ));
            }
        } catch (Exception e) {
            log.error("Failed to pause broker: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to pause broker: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Resume a broker - resume consuming messages.
     */
    @PostMapping("/api/v1/messaging/brokers/{brokerName}/resume")
    @ResponseBody
    public ResponseEntity<?> resumeBroker(@PathVariable String brokerName) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        try {
            boolean success = messagingService.resumeBroker(brokerName);
            if (success) {
                return ResponseEntity.ok(Map.of(
                    "success", true,
                    "message", "Broker '" + brokerName + "' resumed"
                ));
            } else {
                return ResponseEntity.badRequest().body(Map.of(
                    "error", "Failed to resume broker '" + brokerName + "' - broker may not be connected"
                ));
            }
        } catch (Exception e) {
            log.error("Failed to resume broker: {}", e.getMessage());
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Failed to resume broker: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Get status of a specific broker.
     */
    @GetMapping("/api/v1/messaging/brokers/{brokerName}/status")
    @ResponseBody
    public ResponseEntity<?> getBrokerStatus(@PathVariable String brokerName) {
        if (messagingService == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Messaging service not available"
            ));
        }
        
        Map<String, Object> status = messagingService.getBrokerStatus(brokerName);
        if (!(Boolean) status.getOrDefault("exists", false)) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(status);
    }
}
