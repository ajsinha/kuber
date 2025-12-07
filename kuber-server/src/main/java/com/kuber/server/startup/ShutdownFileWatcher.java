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

import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Watches for a shutdown signal file and initiates graceful shutdown when detected.
 * 
 * <p>This provides a clean way to shutdown Kuber without using SIGTERM/SIGKILL:
 * <ul>
 *   <li>Create a file named {@code kuber.shutdown} in the application's working directory</li>
 *   <li>The watcher detects the file within 5 seconds</li>
 *   <li>Graceful shutdown is initiated via ShutdownOrchestrator</li>
 *   <li>The shutdown file is deleted after processing</li>
 * </ul>
 * 
 * <p>Configuration:
 * <pre>
 * kuber:
 *   shutdown:
 *     file-enabled: true              # Enable file-based shutdown (default: true)
 *     file-path: ./kuber.shutdown     # Path to shutdown signal file
 *     check-interval-ms: 5000         # How often to check for file (default: 5s)
 * </pre>
 * 
 * <p>Usage:
 * <pre>
 * # Linux/Mac:
 * touch kuber.shutdown
 * 
 * # Windows:
 * echo. > kuber.shutdown
 * 
 * # Or use the provided scripts:
 * ./kuber-shutdown.sh
 * kuber-shutdown.bat
 * </pre>
 * 
 * @version 1.3.10
 */
@Service
@Slf4j
public class ShutdownFileWatcher {
    
    private static final String DEFAULT_SHUTDOWN_FILE = "kuber.shutdown";
    
    private final ApplicationContext applicationContext;
    private final KuberProperties properties;
    
    private final AtomicBoolean shutdownTriggered = new AtomicBoolean(false);
    private final AtomicBoolean enabled = new AtomicBoolean(true);
    
    private Path shutdownFilePath;
    private Instant startTime;
    
    public ShutdownFileWatcher(ApplicationContext applicationContext, 
                                KuberProperties properties) {
        this.applicationContext = applicationContext;
        this.properties = properties;
    }
    
    @PostConstruct
    public void initialize() {
        // Get shutdown file path from configuration or use default
        String configuredPath = properties.getShutdown().getFilePath();
        if (configuredPath != null && !configuredPath.isBlank()) {
            shutdownFilePath = Paths.get(configuredPath).toAbsolutePath();
        } else {
            shutdownFilePath = Paths.get(DEFAULT_SHUTDOWN_FILE).toAbsolutePath();
        }
        
        enabled.set(properties.getShutdown().isFileEnabled());
        startTime = Instant.now();
        
        // Clean up any stale shutdown file from previous run
        cleanupShutdownFile();
        
        if (enabled.get()) {
            log.info("Shutdown file watcher initialized");
            log.info("  - Watching for: {}", shutdownFilePath);
            log.info("  - Check interval: {}ms", properties.getShutdown().getCheckIntervalMs());
            log.info("  - To shutdown: touch {} (or use kuber-shutdown.sh)", shutdownFilePath.getFileName());
        } else {
            log.info("Shutdown file watcher is disabled");
        }
    }
    
    /**
     * Periodically check for shutdown file.
     * Default interval is 5 seconds.
     */
    @Scheduled(fixedDelayString = "${kuber.shutdown.check-interval-ms:5000}")
    public void checkForShutdownFile() {
        if (!enabled.get() || shutdownTriggered.get()) {
            return;
        }
        
        // Don't check during the first 30 seconds after startup
        // This prevents accidental shutdown from stale files
        if (Instant.now().isBefore(startTime.plusSeconds(30))) {
            return;
        }
        
        if (Files.exists(shutdownFilePath)) {
            if (shutdownTriggered.compareAndSet(false, true)) {
                log.info("╔════════════════════════════════════════════════════════════════════╗");
                log.info("║  SHUTDOWN FILE DETECTED                                            ║");
                log.info("║  File: {}",  String.format("%-55s║", shutdownFilePath.getFileName()));
                log.info("║  Initiating graceful shutdown...                                   ║");
                log.info("╚════════════════════════════════════════════════════════════════════╝");
                
                // Delete the shutdown file
                cleanupShutdownFile();
                
                // Initiate graceful shutdown
                initiateShutdown("Shutdown file detected: " + shutdownFilePath);
            }
        }
    }
    
    /**
     * Initiate graceful shutdown of the application.
     */
    public void initiateShutdown(String reason) {
        if (shutdownTriggered.compareAndSet(false, true)) {
            log.info("Initiating shutdown: {}", reason);
        }
        
        // Use a separate thread to avoid blocking
        Thread shutdownThread = new Thread(() -> {
            try {
                // Small delay to allow response to be sent if triggered via API
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Exit the Spring application - this triggers ContextClosedEvent
            // which is handled by ShutdownOrchestrator
            int exitCode = SpringApplication.exit(applicationContext, () -> 0);
            System.exit(exitCode);
        }, "kuber-shutdown-initiator");
        
        shutdownThread.setDaemon(false);
        shutdownThread.start();
    }
    
    /**
     * Clean up shutdown file if it exists.
     */
    private void cleanupShutdownFile() {
        try {
            if (Files.exists(shutdownFilePath)) {
                Files.delete(shutdownFilePath);
                log.debug("Deleted shutdown file: {}", shutdownFilePath);
            }
        } catch (IOException e) {
            log.warn("Could not delete shutdown file: {}", e.getMessage());
        }
    }
    
    /**
     * Check if shutdown has been triggered.
     */
    public boolean isShutdownTriggered() {
        return shutdownTriggered.get();
    }
    
    /**
     * Get the path to the shutdown file.
     */
    public Path getShutdownFilePath() {
        return shutdownFilePath;
    }
    
    /**
     * Check if file watcher is enabled.
     */
    public boolean isEnabled() {
        return enabled.get();
    }
    
    /**
     * Enable or disable the file watcher.
     */
    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
        log.info("Shutdown file watcher {}", enabled ? "enabled" : "disabled");
    }
    
    @PreDestroy
    public void cleanup() {
        // Clean up shutdown file on exit
        cleanupShutdownFile();
    }
}
