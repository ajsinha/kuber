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
import com.kuber.server.cache.CacheService;
import com.kuber.server.network.RedisProtocolServer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Orchestrates the startup sequence to prevent race conditions.
 * 
 * <p>Startup Order:
 * <ol>
 *   <li>Spring context fully loads (ApplicationReadyEvent)</li>
 *   <li>Wait 10 seconds for system stabilization</li>
 *   <li>Initialize CacheService (recover data from persistence)</li>
 *   <li>Start Redis Protocol Server (accept client connections)</li>
 *   <li>Start AutoloadService (process inbox files)</li>
 * </ol>
 * 
 * <p>This prevents race conditions where:
 * <ul>
 *   <li>Data recovery starts before Spring is fully loaded</li>
 *   <li>Redis server accepts connections before data is recovered</li>
 *   <li>Autoload files are processed before persistence recovery completes</li>
 * </ul>
 * 
 * @version 1.2.4
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class StartupOrchestrator {
    
    private static final int STARTUP_DELAY_SECONDS = 10;
    
    private final CacheService cacheService;
    private final AutoloadService autoloadService;
    private final RedisProtocolServer redisProtocolServer;
    
    private final AtomicBoolean startupComplete = new AtomicBoolean(false);
    private final AtomicBoolean cacheReady = new AtomicBoolean(false);
    
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
            // Step 1: Wait for system stabilization
            log.info("Waiting {} seconds for Spring context stabilization before data recovery...", STARTUP_DELAY_SECONDS);
            Thread.sleep(STARTUP_DELAY_SECONDS * 1000L);
            
            // Step 2: Initialize CacheService (recover from persistence)
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 1: Starting cache service initialization...                 ║");
            log.info("║           Recovering data from persistence store...                ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            long cacheStartTime = System.currentTimeMillis();
            cacheService.initialize();
            long cacheElapsed = System.currentTimeMillis() - cacheStartTime;
            
            // Mark cache as ready
            cacheReady.set(true);
            
            log.info("Cache service initialization completed in {} ms", cacheElapsed);
            
            // Step 3: Start Redis Protocol Server (now safe to accept connections)
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 2: Starting Redis protocol server...                        ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            redisProtocolServer.startServer();
            
            // Step 4: Start AutoloadService
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Phase 3: Starting autoload service...                             ║");
            log.info("╚════════════════════════════════════════════════════════════════════╝");
            
            autoloadService.startAfterRecovery();
            
            // Mark startup complete
            startupComplete.set(true);
            
            log.info("╔════════════════════════════════════════════════════════════════════╗");
            log.info("║  Startup sequence completed successfully!                          ║");
            log.info("║  - Cache service: initialized (data recovered)                     ║");
            log.info("║  - Redis server: accepting connections                             ║");
            log.info("║  - Autoload service: started                                       ║");
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
