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
import com.kuber.server.backup.BackupRestoreService;
import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.messaging.RequestResponseService;
import com.kuber.server.network.RedisProtocolServer;
import com.kuber.server.persistence.PersistenceMaintenanceService;
import com.kuber.server.publishing.RegionEventPublishingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Orchestrates the startup sequence to prevent race conditions.
 * 
 * <p>Startup Sequence:
 * <ol>
 *   <li>Spring context fully loads (ApplicationReadyEvent)</li>
 *   <li>Wait 10 seconds for system stabilization</li>
 *   <li>Persistence maintenance (RocksDB compaction / SQLite vacuum)</li>
 *   <li>Wait 2 seconds</li>
 *   <li>Initialize CacheService (recover data from persistence)</li>
 *   <li>Wait 2 seconds</li>
 *   <li>Create Kafka topics for event publishing (if configured)</li>
 *   <li>Wait 2 seconds</li>
 *   <li>Start Redis Protocol Server (accept client connections)</li>
 *   <li>Wait 2 seconds</li>
 *   <li>Start AutoloadService (process inbox files)</li>
 *   <li>Wait 2 seconds</li>
 *   <li>Start BackupRestoreService (periodic backup, restore monitoring)</li>
 *   <li>Wait 2 seconds</li>
 *   <li>Start Request/Response Messaging Service (message broker subscriptions)</li>
 *   <li>Wait 2 seconds</li>
 *   <li>Final system ready announcement</li>
 * </ol>
 * 
 * <p>This prevents race conditions where:
 * <ul>
 *   <li>Compaction runs concurrently with data recovery</li>
 *   <li>Data recovery starts before Spring is fully loaded</li>
 *   <li>Kafka topics don't exist when publishing starts</li>
 *   <li>Redis server accepts connections before data is recovered</li>
 *   <li>Autoload files are processed before persistence recovery completes</li>
 *   <li>Backups run before data is fully loaded</li>
 *   <li>Message brokers receive requests before cache is ready</li>
 * </ul>
 * 
 * @version 2.6.0
 */
@Service
@Slf4j
public class StartupOrchestrator {
    
    private static final int INITIAL_DELAY_SECONDS = 10;
    private static final int PHASE_DELAY_SECONDS = 2;
    
    private final KuberProperties properties;
    private final PersistenceMaintenanceService persistenceMaintenanceService;
    private final CacheService cacheService;
    private final AutoloadService autoloadService;
    private final RedisProtocolServer redisProtocolServer;
    private final BackupRestoreService backupRestoreService;
    private final Environment environment;
    
    @Autowired(required = false)
    private RegionEventPublishingService publishingService;
    
    @Autowired(required = false)
    private RequestResponseService requestResponseService;
    
    private final AtomicBoolean startupComplete = new AtomicBoolean(false);
    private final AtomicBoolean cacheReady = new AtomicBoolean(false);
    
    public StartupOrchestrator(KuberProperties properties,
                               PersistenceMaintenanceService persistenceMaintenanceService,
                               CacheService cacheService,
                               AutoloadService autoloadService,
                               RedisProtocolServer redisProtocolServer,
                               BackupRestoreService backupRestoreService,
                               Environment environment) {
        this.properties = properties;
        this.persistenceMaintenanceService = persistenceMaintenanceService;
        this.cacheService = cacheService;
        this.autoloadService = autoloadService;
        this.redisProtocolServer = redisProtocolServer;
        this.backupRestoreService = backupRestoreService;
        this.environment = environment;
    }
    
    /**
     * Listen for ApplicationReadyEvent and orchestrate startup.
     * This runs after Spring context is fully initialized.
     */
    @EventListener(ApplicationReadyEvent.class)
    @Order(1)
    public void onApplicationReady(ApplicationReadyEvent event) {
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  Spring context ready - starting initialization sequence...        ║");
        log.info("╚════════════════════════════════════════════════════════════════════╝");
        
        // Run initialization in a separate thread to not block the event
        Thread initThread = new Thread(this::runStartupSequence, "kuber-startup-orchestrator");
        initThread.setDaemon(false);
        initThread.start();
    }
    
    /**
     * Execute the startup sequence in order.
     */
    private void runStartupSequence() {
        try {
            // Phase 0: Wait for system stabilization
            log.info("Waiting {} seconds for Spring context stabilization...", INITIAL_DELAY_SECONDS);
            Thread.sleep(INITIAL_DELAY_SECONDS * 1000L);
            
            // Phase 1: Persistence maintenance (compaction/vacuum)
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 1: Persistence Maintenance                                  ║");
            log.info("║           Running database compaction/vacuum...                    ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            long maintenanceStart = System.currentTimeMillis();
            boolean maintenanceSuccess = persistenceMaintenanceService.executeMaintenance();
            long maintenanceElapsed = System.currentTimeMillis() - maintenanceStart;
            
            if (maintenanceSuccess) {
                log.info("Persistence maintenance completed successfully in {} ms", maintenanceElapsed);
            } else {
                log.warn("Persistence maintenance completed with warnings in {} ms", maintenanceElapsed);
            }
            
            // Wait between phases
            log.info("Waiting {} seconds before cache initialization...", PHASE_DELAY_SECONDS);
            Thread.sleep(PHASE_DELAY_SECONDS * 1000L);
            
            // Phase 2: Initialize CacheService (recover from persistence)
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 2: Cache Service Initialization                             ║");
            log.info("║           Recovering data from persistence store...                ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            long cacheStartTime = System.currentTimeMillis();
            cacheService.initialize();
            long cacheElapsed = System.currentTimeMillis() - cacheStartTime;
            
            // Mark cache as ready
            cacheReady.set(true);
            
            log.info("Cache service initialization completed in {} ms", cacheElapsed);
            
            // Wait between phases
            log.info("Waiting {} seconds before event publishing setup...", PHASE_DELAY_SECONDS);
            Thread.sleep(PHASE_DELAY_SECONDS * 1000L);
            
            // Phase 3: Create Kafka topics (if publishing is configured)
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 3: Event Publishing Setup                                   ║");
            log.info("║           Initializing publishers and creating topics/queues...    ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            if (publishingService != null) {
                try {
                    publishingService.executeStartupOrchestration();
                    log.info("Event publishing setup completed");
                } catch (Exception e) {
                    log.warn("Event publishing setup completed with warnings: {}", e.getMessage());
                }
            } else {
                log.info("Event publishing service not configured - skipping");
            }
            
            // Wait between phases
            log.info("Waiting {} seconds before starting Redis server...", PHASE_DELAY_SECONDS);
            Thread.sleep(PHASE_DELAY_SECONDS * 1000L);
            
            // Phase 4: Start Redis Protocol Server
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 4: Redis Protocol Server                                    ║");
            log.info("║           Starting server to accept client connections...          ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            redisProtocolServer.startServer();
            
            // Wait between phases
            log.info("Waiting {} seconds before starting autoload service...", PHASE_DELAY_SECONDS);
            Thread.sleep(PHASE_DELAY_SECONDS * 1000L);
            
            // Phase 5: Start AutoloadService
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 5: Autoload Service                                         ║");
            log.info("║           Starting file monitoring and processing...               ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            autoloadService.startAfterRecovery();
            
            // Wait between phases
            log.info("Waiting {} seconds before starting backup/restore service...", PHASE_DELAY_SECONDS);
            Thread.sleep(PHASE_DELAY_SECONDS * 1000L);
            
            // Phase 6: Start BackupRestoreService
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 6: Backup/Restore Service                                   ║");
            log.info("║           Starting backup scheduler and restore watcher...         ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            // Wire BackupRestoreService to CacheService for region lock checking
            cacheService.setBackupRestoreService(backupRestoreService);
            backupRestoreService.start();
            
            // Wait between phases
            log.info("Waiting {} seconds before starting messaging service...", PHASE_DELAY_SECONDS);
            Thread.sleep(PHASE_DELAY_SECONDS * 1000L);
            
            // Phase 7: Start Request/Response Messaging Service
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 7: Request/Response Messaging Service                        ║");
            log.info("║           Starting message broker subscriptions...                  ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            if (requestResponseService != null) {
                try {
                    requestResponseService.start();
                    log.info("Request/Response messaging service started");
                } catch (Exception e) {
                    log.warn("Request/Response messaging setup completed with warnings: {}", e.getMessage());
                }
            } else {
                log.info("Request/Response messaging service not configured - skipping");
            }
            
            // Wait before final announcement
            log.info("Waiting {} seconds before final announcement...", PHASE_DELAY_SECONDS);
            Thread.sleep(PHASE_DELAY_SECONDS * 1000L);
            
            // Mark startup complete
            startupComplete.set(true);
            
            // Final announcement
            String versionLine = String.format("║   SYSTEM READY - Version %-37s║", properties.getVersion());
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║                                                                    ║");
            log.info("║   ██╗  ██╗██╗   ██╗██████╗ ███████╗██████╗                         ║");
            log.info("║   ██║ ██╔╝██║   ██║██╔══██╗██╔════╝██╔══██╗                        ║");
            log.info("║   █████╔╝ ██║   ██║██████╔╝█████╗  ██████╔╝                        ║");
            log.info("║   ██╔═██╗ ██║   ██║██╔══██╗██╔══╝  ██╔══██╗                        ║");
            log.info("║   ██║  ██╗╚██████╔╝██████╔╝███████╗██║  ██║                        ║");
            log.info("║   ╚═╝  ╚═╝ ╚═════╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝                        ║");
            log.info("║                                                                    ║");
            log.info(versionLine);
            log.info("║                                                                    ║");
            log.info("║   ✓ Persistence maintenance: complete                              ║");
            log.info("║   ✓ Cache service: initialized                                     ║");
            log.info("║   ✓ Event publishing: {}                              ║", 
                    publishingService != null ? "configured    " : "not configured");
            log.info("║   ✓ Redis server: accepting connections                            ║");
            log.info("║   ✓ Autoload service: running                                      ║");
            log.info("║   ✓ Backup/restore: running                                        ║");
            log.info("║   ✓ Request/Response: {}                              ║",
                    requestResponseService != null && requestResponseService.isEnabled() ? "running       " : "not configured");
            log.info("║                                                                    ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            // Log listening ports for easy reference
            String httpPort = environment.getProperty("server.port", "8080");
            int redisPort = properties.getNetwork().getPort();
            String persistenceType = properties.getPersistence().getType();
            
            log.info("");
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  LISTENING ENDPOINTS                                               ║");
            log.info("╠════════════════════════════════════════════════════════════════════╣");
            log.info("║                                                                    ║");
            log.info(String.format("║   HTTP / Web UI      :  http://localhost:%-24s║", httpPort));
            log.info(String.format("║   REST API           :  http://localhost:%-24s║", httpPort + "/api"));
            log.info(String.format("║   Redis Protocol     :  redis://localhost:%-23s║", String.valueOf(redisPort)));
            log.info("║                                                                    ║");
            log.info(String.format("║   Persistence        :  %-39s║", persistenceType.toUpperCase()));
            log.info("║                                                                    ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Startup sequence interrupted", e);
        } catch (Exception e) {
            log.error("Startup sequence failed", e);
        }
    }
    
    /**
     * Check if startup sequence has completed.
     */
    public boolean isStartupComplete() {
        return startupComplete.get();
    }
    
    /**
     * Check if cache service has been initialized and is ready.
     * Used by other services to wait for recovery to complete.
     */
    public boolean isCacheReady() {
        return cacheReady.get();
    }
}
