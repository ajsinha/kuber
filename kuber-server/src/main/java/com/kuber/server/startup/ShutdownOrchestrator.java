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
package com.kuber.server.startup;

import com.kuber.server.autoload.AutoloadService;
import com.kuber.server.cache.CacheMetricsService;
import com.kuber.server.cache.CacheService;
import com.kuber.server.cache.MemoryWatcherService;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.network.RedisProtocolServer;
import com.kuber.server.persistence.PersistenceOperationLock;
import com.kuber.server.persistence.PersistenceStore;
import com.kuber.server.publishing.RegionEventPublishingService;
import com.kuber.server.replication.ReplicationManager;
import com.kuber.server.service.PersistenceExpirationService;
import com.kuber.server.service.RocksDbCompactionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Orchestrates graceful shutdown in the EXACT REVERSE order of startup.
 * 
 * <p>Shutdown Order:
 * <ol>
 *   <li>Phase 0: Signal shutdown, stop task scheduler and all scheduled services immediately</li>
 *   <li>Phase 1: Stop Autoload Service (stop processing files)</li>
 *   <li>Phase 2: Stop Redis Protocol Server (reject new connections)</li>
 *   <li>Phase 3: Stop Replication Manager (ZooKeeper coordination)</li>
 *   <li>Phase 4: Stop Event Publishing (flush queues)</li>
 *   <li>Phase 5a: Pre-sync persistence store (flush existing data)</li>
 *   <li>Phase 5b: Persist cache data (CacheService writes remaining entries)</li>
 *   <li>Phase 5c: Post-sync persistence store (ensure writes are durable)</li>
 *   <li>Phase 6: Close persistence store (RocksDB/LMDB/SQLite handles)</li>
 * </ol>
 * 
 * <p>The configurable gaps ensure:
 * <ul>
 *   <li>In-flight operations complete</li>
 *   <li>Buffers are flushed</li>
 *   <li>File handles are properly closed</li>
 *   <li>No corruption from concurrent shutdown</li>
 * </ul>
 * 
 * <p>DURABILITY (v1.3.6):
 * <ul>
 *   <li>RocksDB uses ReadWriteLock to prevent writes during close</li>
 *   <li>All writes complete before database handles are closed</li>
 *   <li>WAL is synced before closing each region</li>
 * </ul>
 * 
 * @version 1.3.10
 */
@Service
@Slf4j
public class ShutdownOrchestrator {
    
    // Core services (required)
    private final PersistenceOperationLock operationLock;
    private final AutoloadService autoloadService;
    private final RedisProtocolServer redisProtocolServer;
    private final CacheService cacheService;
    private final PersistenceStore persistenceStore;
    private final KuberProperties properties;
    
    // Task scheduler - to stop all scheduled tasks immediately
    @Autowired(required = false)
    private TaskScheduler taskScheduler;
    
    // Scheduled services (optional - may not be enabled)
    @Autowired(required = false)
    private CacheMetricsService cacheMetricsService;
    
    @Autowired(required = false)
    private MemoryWatcherService memoryWatcherService;
    
    @Autowired(required = false)
    private PersistenceExpirationService expirationService;
    
    @Autowired(required = false)
    private RocksDbCompactionService compactionService;
    
    @Autowired(required = false)
    private ReplicationManager replicationManager;
    
    @Autowired(required = false)
    private RegionEventPublishingService publishingService;
    
    @Autowired(required = false)
    private ShutdownFileWatcher shutdownFileWatcher;
    
    private final AtomicBoolean shutdownStarted = new AtomicBoolean(false);
    private final AtomicBoolean shutdownComplete = new AtomicBoolean(false);
    
    public ShutdownOrchestrator(PersistenceOperationLock operationLock,
                                 AutoloadService autoloadService,
                                 RedisProtocolServer redisProtocolServer,
                                 CacheService cacheService,
                                 PersistenceStore persistenceStore,
                                 KuberProperties properties) {
        this.operationLock = operationLock;
        this.autoloadService = autoloadService;
        this.redisProtocolServer = redisProtocolServer;
        this.cacheService = cacheService;
        this.persistenceStore = persistenceStore;
        this.properties = properties;
    }
    
    /**
     * Get the configured delay between shutdown phases.
     */
    private int getPhaseDelaySeconds() {
        return properties.getShutdown().getPhaseDelaySeconds();
    }
    
    /**
     * Handle Spring context closing event.
     * This runs BEFORE @PreDestroy methods, giving us control over shutdown order.
     */
    @EventListener(ContextClosedEvent.class)
    @Order(1) // Run first, before other shutdown handlers
    public void onApplicationShutdown(ContextClosedEvent event) {
        if (!shutdownStarted.compareAndSet(false, true)) {
            log.info("Shutdown already in progress, skipping...");
            return;
        }
        
        Instant startTime = Instant.now();
        int phaseDelay = getPhaseDelaySeconds();
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  KUBER GRACEFUL SHUTDOWN INITIATED                                  ║");
        log.info("║  Shutting down in reverse order of startup with {}s gaps           ║", phaseDelay);
        log.info("╚════════════════════════════════════════════════════════════════════╝");
        
        try {
            // Phase 0: Signal shutdown - block all new persistence operations AND stop scheduler
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 0: Signal Shutdown & Stop Scheduler                         ║");
            log.info("║           Blocking new operations, stopping all scheduled tasks... ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            // Signal shutdown to persistence lock - this stops new operations
            operationLock.signalShutdown();
            log.info("  ✓ Shutdown signal sent to persistence lock");
            
            // IMMEDIATELY stop the task scheduler to halt ALL scheduled tasks
            // This is critical - without this, @Scheduled methods continue running
            stopTaskScheduler();
            
            // Also notify individual services (belt and suspenders)
            stopScheduledServices();
            
            // Acquire exclusive lock to wait for any in-progress operations
            if (!operationLock.acquireExclusiveGlobalLock(
                    PersistenceOperationLock.OperationType.SHUTDOWN, 
                    "Graceful shutdown", 
                    60, java.util.concurrent.TimeUnit.SECONDS)) {
                log.warn("Could not acquire exclusive lock - some operations may still be in progress");
            }
            
            sleepBetweenPhases();
            
            // Phase 1: Stop Autoload Service
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 1: Stop Autoload Service                                    ║");
            log.info("║           Stopping file monitoring and processing...               ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            try {
                autoloadService.shutdown();
                log.info("  ✓ Autoload service stopped");
            } catch (Exception e) {
                log.warn("  ⚠ Error stopping autoload service: {}", e.getMessage());
            }
            
            sleepBetweenPhases();
            
            // Phase 2: Stop Redis Protocol Server
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 2: Stop Redis Protocol Server                               ║");
            log.info("║           Rejecting new client connections...                      ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            try {
                redisProtocolServer.stop();
                log.info("  ✓ Redis protocol server stopped");
            } catch (Exception e) {
                log.warn("  ⚠ Error stopping Redis server: {}", e.getMessage());
            }
            
            sleepBetweenPhases();
            
            // Phase 3: Stop Replication Manager
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 3: Stop Replication Manager                                 ║");
            log.info("║           Stopping ZooKeeper coordination...                       ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            if (replicationManager != null) {
                try {
                    replicationManager.shutdown();
                    log.info("  ✓ Replication manager stopped");
                } catch (Exception e) {
                    log.warn("  ⚠ Error stopping replication manager: {}", e.getMessage());
                }
            } else {
                log.info("  - Replication not configured");
            }
            
            sleepBetweenPhases();
            
            // Phase 4: Stop Event Publishing
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 4: Stop Event Publishing                                    ║");
            log.info("║           Flushing queues and closing connections...               ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            if (publishingService != null) {
                try {
                    publishingService.shutdown();
                    log.info("  ✓ Event publishing stopped");
                } catch (Exception e) {
                    log.warn("  ⚠ Error stopping event publishing: {}", e.getMessage());
                }
            } else {
                log.info("  - Event publishing not configured");
            }
            
            sleepBetweenPhases();
            
            // Phase 5a: Pre-sync persistence to ensure existing data is safe
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 5a: Pre-Sync Persistence Store                              ║");
            log.info("║            Flushing existing data to disk...                       ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            try {
                persistenceStore.sync();
                log.info("  ✓ Persistence store pre-synced");
            } catch (Exception e) {
                log.warn("  ⚠ Error during pre-sync: {}", e.getMessage());
            }
            
            sleepBetweenPhases();
            
            // Phase 5b: Persist Cache Service (may write additional data)
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 5b: Persist Cache Data                                      ║");
            log.info("║            Saving all cache entries to persistence store...        ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            try {
                cacheService.shutdown();
                log.info("  ✓ Cache data persisted");
            } catch (Exception e) {
                log.warn("  ⚠ Error persisting cache data: {}", e.getMessage());
            }
            
            sleepBetweenPhases();
            
            // Phase 5c: Post-sync to ensure newly written data is safe
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 5c: Post-Sync Persistence Store                             ║");
            log.info("║            Ensuring all writes are durable on disk...              ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            try {
                persistenceStore.sync();
                log.info("  ✓ Persistence store post-synced");
            } catch (Exception e) {
                log.warn("  ⚠ Error during post-sync: {}", e.getMessage());
            }
            
            // Extra delay after final sync to ensure OS buffers are flushed
            log.info("Waiting {} seconds for OS buffers to flush...", getPhaseDelaySeconds() * 2);
            try {
                Thread.sleep(getPhaseDelaySeconds() * 2000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Phase 6: Shutdown Persistence Store
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 6: Close Persistence Store                                  ║");
            log.info("║           Closing database handles and releasing files...          ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            try {
                persistenceStore.shutdown();
                log.info("  ✓ Persistence store closed");
            } catch (Exception e) {
                log.warn("  ⚠ Error closing persistence store: {}", e.getMessage());
            }
            
            sleepBetweenPhases();
            
            // Release exclusive lock
            operationLock.releaseExclusiveGlobalLock();
            
            shutdownComplete.set(true);
            Duration totalDuration = Duration.between(startTime, Instant.now());
            
            // Final announcement
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║                                                                    ║");
            log.info("║   KUBER SHUTDOWN COMPLETE                                          ║");
            log.info("║                                                                    ║");
            log.info("║   ✓ Scheduled services: stopped                                    ║");
            log.info("║   ✓ Autoload service: stopped                                      ║");
            log.info("║   ✓ Redis server: stopped                                          ║");
            log.info("║   ✓ Replication: {}                                    ║",
                    replicationManager != null ? "stopped       " : "not configured");
            log.info("║   ✓ Event publishing: {}                               ║",
                    publishingService != null ? "stopped       " : "not configured");
            log.info("║   ✓ Cache data: persisted                                          ║");
            log.info("║   ✓ Persistence store: closed                                      ║");
            log.info("║                                                                    ║");
            log.info("║   Total shutdown time: {} seconds                             ║",
                    String.format("%-5d", totalDuration.toSeconds()));
            log.info("║                                                                    ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
        } catch (Exception e) {
            log.error("Error during shutdown orchestration: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Stop the Spring TaskScheduler to immediately halt all @Scheduled methods.
     * This is the most effective way to stop all scheduled tasks at once.
     */
    private void stopTaskScheduler() {
        if (taskScheduler != null) {
            try {
                if (taskScheduler instanceof ThreadPoolTaskScheduler) {
                    ThreadPoolTaskScheduler threadPoolScheduler = (ThreadPoolTaskScheduler) taskScheduler;
                    
                    // Get the underlying executor and shut it down
                    log.info("  Shutting down task scheduler thread pool...");
                    threadPoolScheduler.shutdown();
                    
                    // Wait briefly for tasks to complete
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    
                    log.info("  ✓ Task scheduler stopped - all @Scheduled methods halted");
                } else {
                    log.info("  Task scheduler is not ThreadPoolTaskScheduler, cannot stop directly");
                }
            } catch (Exception e) {
                log.warn("  ⚠ Error stopping task scheduler: {}", e.getMessage());
            }
        } else {
            log.info("  - Task scheduler not available");
        }
    }
    
    /**
     * Stop all scheduled/background services.
     */
    private void stopScheduledServices() {
        int stopped = 0;
        int notConfigured = 0;
        
        // Stop Shutdown File Watcher (prevent re-triggering)
        if (shutdownFileWatcher != null) {
            try {
                shutdownFileWatcher.setEnabled(false);
                log.info("  ✓ Shutdown file watcher disabled");
                stopped++;
            } catch (Exception e) {
                log.warn("  ⚠ Error disabling shutdown file watcher: {}", e.getMessage());
            }
        } else {
            notConfigured++;
        }
        
        // Stop Cache Metrics Service
        if (cacheMetricsService != null) {
            try {
                cacheMetricsService.shutdown();
                log.info("  ✓ Cache metrics service stopped");
                stopped++;
            } catch (Exception e) {
                log.warn("  ⚠ Error stopping cache metrics service: {}", e.getMessage());
            }
        } else {
            notConfigured++;
        }
        
        // Stop Memory Watcher Service
        if (memoryWatcherService != null) {
            try {
                memoryWatcherService.shutdown();
                log.info("  ✓ Memory watcher service stopped");
                stopped++;
            } catch (Exception e) {
                log.warn("  ⚠ Error stopping memory watcher service: {}", e.getMessage());
            }
        } else {
            notConfigured++;
        }
        
        // Stop Persistence Expiration Service
        if (expirationService != null) {
            try {
                expirationService.shutdown();
                log.info("  ✓ Persistence expiration service stopped");
                stopped++;
            } catch (Exception e) {
                log.warn("  ⚠ Error stopping expiration service: {}", e.getMessage());
            }
        } else {
            notConfigured++;
        }
        
        // Stop RocksDB Compaction Service
        if (compactionService != null) {
            try {
                compactionService.shutdown();
                log.info("  ✓ RocksDB compaction service stopped");
                stopped++;
            } catch (Exception e) {
                log.warn("  ⚠ Error stopping compaction service: {}", e.getMessage());
            }
        } else {
            notConfigured++;
        }
        
        log.info("  Scheduled services: {} stopped, {} not configured", stopped, notConfigured);
    }
    
    /**
     * Sleep between shutdown phases.
     */
    private void sleepBetweenPhases() {
        int delaySeconds = getPhaseDelaySeconds();
        try {
            log.info("Waiting {} seconds before next phase...", delaySeconds);
            Thread.sleep(delaySeconds * 1000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Shutdown sleep interrupted");
        }
    }
    
    /**
     * Check if shutdown has been initiated.
     */
    public boolean isShutdownStarted() {
        return shutdownStarted.get();
    }
    
    /**
     * Check if shutdown is complete.
     */
    public boolean isShutdownComplete() {
        return shutdownComplete.get();
    }
}
