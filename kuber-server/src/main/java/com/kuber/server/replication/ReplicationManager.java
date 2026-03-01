/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.server.replication;

import com.kuber.core.constants.KuberConstants;
import com.kuber.core.model.CacheEvent;
import com.kuber.core.util.JsonUtils;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.event.EventPublisher;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages replication and leader election using ZooKeeper.
 * 
 * <p>Coordinates PRIMARY/SECONDARY roles via Curator LeaderLatch.
 * When this node becomes SECONDARY, starts the {@link ReplicationSyncService}
 * to replicate data from the PRIMARY. When this node becomes PRIMARY,
 * resets the {@link ReplicationOpLog} and stops any active sync.
 * 
 * @since 1.9.0
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "kuber.zookeeper.enabled", havingValue = "true")
public class ReplicationManager implements LeaderLatchListener {
    
    private final KuberProperties properties;
    private final EventPublisher eventPublisher;
    private final Environment environment;
    
    @Autowired
    private ReplicationSyncService syncService;
    
    @Autowired
    private ReplicationOpLog opLog;
    
    private CuratorFramework zkClient;
    private LeaderLatch leaderLatch;
    
    @Getter
    private final AtomicBoolean isPrimary = new AtomicBoolean(false);
    
    @Getter
    private String mode = KuberConstants.REPL_STATE_STANDALONE;
    
    @Getter
    private Instant lastSyncTime;
    
    @Getter
    private String primaryNodeId;
    
    private boolean zkConnected = false;
    
    // Shutdown flag to stop scheduled tasks
    private volatile boolean shuttingDown = false;
    
    public ReplicationManager(KuberProperties properties, EventPublisher eventPublisher,
                              Environment environment) {
        this.properties = properties;
        this.eventPublisher = eventPublisher;
        this.environment = environment;
    }
    
    @PostConstruct
    public void initialize() {
        KuberProperties.Zookeeper zkConfig = properties.getZookeeper();
        
        if (!zkConfig.isEnabled()) {
            log.info("ZooKeeper is disabled, running in standalone mode as PRIMARY");
            isPrimary.set(true);
            mode = KuberConstants.REPL_STATE_STANDALONE;
            return;
        }
        
        try {
            log.info("Connecting to ZooKeeper: {}", zkConfig.getConnectString());
            
            zkClient = CuratorFrameworkFactory.builder()
                    .connectString(zkConfig.getConnectString())
                    .sessionTimeoutMs(zkConfig.getSessionTimeoutMs())
                    .connectionTimeoutMs(zkConfig.getConnectionTimeoutMs())
                    .retryPolicy(new ExponentialBackoffRetry(
                            zkConfig.getRetryBaseSleepMs(),
                            zkConfig.getRetryMaxAttempts()))
                    .build();
            
            zkClient.start();
            zkClient.blockUntilConnected();
            zkConnected = true;
            
            // Create base paths
            ensurePath(zkConfig.getBasePath());
            ensurePath(zkConfig.getBasePath() + "/leader");
            ensurePath(zkConfig.getBasePath() + "/nodes");
            
            // Start leader election
            String leaderPath = zkConfig.getBasePath() + "/leader";
            leaderLatch = new LeaderLatch(zkClient, leaderPath, properties.getNodeId());
            leaderLatch.addListener(this);
            leaderLatch.start();
            
            // Register this node
            registerNode();
            
            log.info("ZooKeeper connected, starting leader election...");
            
        } catch (Exception e) {
            log.error("Failed to connect to ZooKeeper: {}", e.getMessage());
            log.warn("Running in standalone mode as PRIMARY due to ZooKeeper failure");
            isPrimary.set(true);
            mode = KuberConstants.REPL_STATE_STANDALONE;
            zkConnected = false;
        }
    }
    
    private void ensurePath(String path) throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path);
        }
    }
    
    private void registerNode() throws Exception {
        String nodePath = properties.getZookeeper().getBasePath() + "/nodes/" + properties.getNodeId();
        
        Map<String, Object> nodeInfo = new HashMap<>();
        nodeInfo.put("nodeId", properties.getNodeId());
        nodeInfo.put("host", properties.getReplication().getAdvertisedAddress());
        nodeInfo.put("redisPort", properties.getNetwork().getPort());
        nodeInfo.put("httpPort", Integer.parseInt(environment.getProperty("server.port", "8080")));
        nodeInfo.put("startTime", Instant.now().toString());
        nodeInfo.put("isPrimary", isPrimary.get());
        nodeInfo.put("mode", mode);
        
        String data = com.kuber.core.util.JsonUtils.toJson(nodeInfo);
        
        if (zkClient.checkExists().forPath(nodePath) != null) {
            zkClient.setData().forPath(nodePath, data.getBytes());
        } else {
            zkClient.create().forPath(nodePath, data.getBytes());
        }
    }
    
    @Override
    public void isLeader() {
        log.info("This node is now the PRIMARY");
        isPrimary.set(true);
        mode = KuberConstants.REPL_STATE_PRIMARY;
        primaryNodeId = properties.getNodeId();
        
        // Stop sync service if it was running (we were SECONDARY before)
        if (syncService != null && syncService.isRunning()) {
            syncService.stopSync();
        }
        
        // Reset oplog for fresh journaling as new PRIMARY
        if (opLog != null) {
            opLog.reset();
        }
        
        eventPublisher.publish(CacheEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .eventType(CacheEvent.EventType.NODE_PRIMARY)
                .sourceNodeId(properties.getNodeId())
                .timestamp(Instant.now())
                .build());
    }
    
    @Override
    public void notLeader() {
        log.info("This node is now a SECONDARY");
        isPrimary.set(false);
        mode = KuberConstants.REPL_STATE_SECONDARY;
        
        // Get the current leader
        try {
            if (leaderLatch != null && leaderLatch.getLeader() != null) {
                primaryNodeId = leaderLatch.getLeader().getId();
            }
        } catch (Exception e) {
            log.warn("Failed to get leader info: {}", e.getMessage());
        }
        
        eventPublisher.publish(CacheEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .eventType(CacheEvent.EventType.NODE_SECONDARY)
                .sourceNodeId(properties.getNodeId())
                .timestamp(Instant.now())
                .build());
        
        // Start sync with primary
        startSync();
    }
    
    /**
     * Check if this node is the primary
     */
    public boolean isPrimary() {
        return isPrimary.get();
    }
    
    /**
     * Start synchronization with primary.
     * Discovers the PRIMARY's HTTP endpoint from ZooKeeper and starts the sync service.
     */
    @SuppressWarnings("unchecked")
    public void startSync() {
        if (isPrimary()) {
            return;
        }
        
        log.info("Starting sync with primary node: {}", primaryNodeId);
        mode = KuberConstants.REPL_STATE_SYNCING;
        
        eventPublisher.publish(CacheEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .eventType(CacheEvent.EventType.SYNC_STARTED)
                .sourceNodeId(properties.getNodeId())
                .timestamp(Instant.now())
                .build());
        
        // Discover PRIMARY's HTTP endpoint from ZooKeeper node registration
        String primaryHost = properties.getReplication().getAdvertisedAddress();
        int primaryHttpPort = Integer.parseInt(environment.getProperty("server.port", "8080"));
        
        try {
            if (primaryNodeId != null && zkClient != null) {
                String nodePath = properties.getZookeeper().getBasePath() + "/nodes/" + primaryNodeId;
                byte[] data = zkClient.getData().forPath(nodePath);
                if (data != null) {
                    Map<String, Object> nodeInfo = JsonUtils.fromJson(new String(data), 
                            new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
                    if (nodeInfo.get("host") != null) {
                        primaryHost = (String) nodeInfo.get("host");
                    }
                    if (nodeInfo.get("httpPort") != null) {
                        primaryHttpPort = ((Number) nodeInfo.get("httpPort")).intValue();
                    }
                    log.info("Discovered PRIMARY HTTP endpoint from ZooKeeper: {}:{}", primaryHost, primaryHttpPort);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to discover PRIMARY endpoint from ZooKeeper: {}. Using defaults.", e.getMessage());
        }
        
        // Start the background sync service
        syncService.startSync(primaryHost, primaryHttpPort);
        
        lastSyncTime = Instant.now();
        mode = KuberConstants.REPL_STATE_SECONDARY;
        
        eventPublisher.publish(CacheEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .eventType(CacheEvent.EventType.SYNC_COMPLETED)
                .sourceNodeId(properties.getNodeId())
                .timestamp(Instant.now())
                .build());
        
        log.info("Sync service launched - replication running in background");
    }
    
    /**
     * Get replication information
     */
    public Map<String, Object> getReplicationInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("nodeId", properties.getNodeId());
        info.put("mode", mode);
        info.put("isPrimary", isPrimary());
        info.put("primaryNodeId", primaryNodeId);
        info.put("lastSyncTime", lastSyncTime);
        info.put("zkConnected", zkConnected);
        
        if (zkConnected && leaderLatch != null) {
            try {
                info.put("participants", leaderLatch.getParticipants().size());
            } catch (Exception e) {
                info.put("participants", "unknown");
            }
        }
        
        // OpLog stats (available on PRIMARY)
        if (opLog != null) {
            Map<String, Object> opLogInfo = new HashMap<>();
            opLogInfo.put("currentSequence", opLog.getCurrentSequence());
            opLogInfo.put("size", opLog.size());
            opLogInfo.put("capacity", opLog.getCapacity());
            opLogInfo.put("oldestSequence", opLog.getOldestAvailableSequence());
            info.put("oplog", opLogInfo);
        }
        
        // Sync service stats (available on SECONDARY)
        if (syncService != null) {
            info.put("sync", syncService.getSyncStatus());
        }
        
        return info;
    }
    
    /**
     * Periodic health check
     */
    @Scheduled(fixedRateString = "${kuber.replication.heartbeat-interval-ms:5000}")
    public void healthCheck() {
        // Skip if shutting down
        if (shuttingDown) {
            return;
        }
        
        if (!zkConnected || zkClient == null) {
            return;
        }
        
        try {
            // Update node registration
            registerNode();
            
            // Check if we're still connected
            if (!zkClient.getZookeeperClient().isConnected()) {
                log.warn("Lost connection to ZooKeeper, switching to standalone PRIMARY mode");
                isPrimary.set(true);
                mode = KuberConstants.REPL_STATE_STANDALONE;
                zkConnected = false;
            }
        } catch (Exception e) {
            log.error("Health check failed: {}", e.getMessage());
        }
    }
    
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down replication manager...");
        
        // Set shutdown flag to stop scheduled tasks
        shuttingDown = true;
        
        // Stop sync service if running
        if (syncService != null && syncService.isRunning()) {
            syncService.stopSync();
        }
        
        try {
            if (leaderLatch != null) {
                leaderLatch.close();
            }
            if (zkClient != null) {
                zkClient.close();
            }
        } catch (Exception e) {
            log.error("Error during shutdown: {}", e.getMessage());
        }
        
        log.info("Replication manager shutdown complete");
    }
}
