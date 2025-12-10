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
package com.kuber.server.security;

import com.kuber.server.config.KuberProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Watches security configuration files (users.json, apikeys.json) for changes.
 * 
 * Features:
 * - Auto-reload every 5 minutes
 * - Detect file modifications and reload immediately
 * - Create empty files if deleted with bold warnings
 * 
 * @version 1.6.5
 */
@Slf4j
@Service
@EnableScheduling
@RequiredArgsConstructor
public class SecurityFileWatcher {
    
    private final KuberProperties properties;
    private final JsonUserDetailsService userService;
    private final ApiKeyService apiKeyService;
    
    private long usersFileLastModified = 0;
    private long apiKeysFileLastModified = 0;
    
    private Path usersFilePath;
    private Path apiKeysFilePath;
    
    @PostConstruct
    public void initialize() {
        usersFilePath = Paths.get(properties.getSecurity().getUsersFile());
        apiKeysFilePath = Paths.get(properties.getSecurity().getApiKeysFile());
        
        // Record initial modification times
        File usersFile = usersFilePath.toFile();
        File apiKeysFile = apiKeysFilePath.toFile();
        
        if (usersFile.exists()) {
            usersFileLastModified = usersFile.lastModified();
        }
        if (apiKeysFile.exists()) {
            apiKeysFileLastModified = apiKeysFile.lastModified();
        }
        
        log.info("SecurityFileWatcher initialized - monitoring: {}, {}", usersFilePath, apiKeysFilePath);
    }
    
    /**
     * Check for file changes every 30 seconds.
     * Full reload every 5 minutes (10 checks).
     */
    @Scheduled(fixedDelay = 30000) // 30 seconds
    public void checkForChanges() {
        checkUsersFile();
        checkApiKeysFile();
    }
    
    /**
     * Force full reload every 5 minutes.
     */
    @Scheduled(fixedDelay = 300000) // 5 minutes
    public void periodicReload() {
        log.debug("Periodic security files reload triggered");
        reloadAllSecurityFiles();
    }
    
    /**
     * Check users.json for changes or deletion.
     */
    private void checkUsersFile() {
        File file = usersFilePath.toFile();
        
        if (!file.exists()) {
            // File was deleted! Log big warning
            logBigWarning("USERS.JSON DELETED", usersFilePath.toString(), 
                "Web UI login will fail! Please restore users.json immediately.");
            
            // Cannot create empty users.json - it would break login
            // Just keep warning
            return;
        }
        
        long currentModified = file.lastModified();
        if (currentModified != usersFileLastModified) {
            log.info("Detected change in users.json - reloading...");
            try {
                userService.reloadUsers();
                usersFileLastModified = currentModified;
                log.info("Successfully reloaded users.json");
            } catch (Exception e) {
                log.error("Failed to reload users.json: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Check apikeys.json for changes or deletion.
     */
    private void checkApiKeysFile() {
        File file = apiKeysFilePath.toFile();
        
        if (!file.exists()) {
            // File was deleted! Log big warning and recreate
            logBigWarning("APIKEYS.JSON DELETED", apiKeysFilePath.toString(),
                "All API key authentication will fail! Creating empty file...");
            
            // Trigger reload which will create empty file
            try {
                apiKeyService.reloadKeys();
                apiKeysFileLastModified = file.exists() ? file.lastModified() : 0;
            } catch (Exception e) {
                log.error("Failed to handle apikeys.json deletion: {}", e.getMessage());
            }
            return;
        }
        
        long currentModified = file.lastModified();
        if (currentModified != apiKeysFileLastModified) {
            log.info("Detected change in apikeys.json - reloading...");
            try {
                apiKeyService.reloadKeys();
                apiKeysFileLastModified = currentModified;
                log.info("Successfully reloaded apikeys.json");
            } catch (Exception e) {
                log.error("Failed to reload apikeys.json: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Reload all security files.
     */
    public void reloadAllSecurityFiles() {
        log.info("Reloading all security configuration files...");
        
        try {
            File usersFile = usersFilePath.toFile();
            if (usersFile.exists()) {
                userService.reloadUsers();
                usersFileLastModified = usersFile.lastModified();
                log.info("Reloaded users.json: {} users", userService.getAllUsers().size());
            }
        } catch (Exception e) {
            log.error("Failed to reload users.json: {}", e.getMessage());
        }
        
        try {
            apiKeyService.reloadKeys();
            File apiKeysFile = apiKeysFilePath.toFile();
            if (apiKeysFile.exists()) {
                apiKeysFileLastModified = apiKeysFile.lastModified();
            }
            log.info("Reloaded apikeys.json: {} keys", apiKeyService.getAllKeys().size());
        } catch (Exception e) {
            log.error("Failed to reload apikeys.json: {}", e.getMessage());
        }
    }
    
    /**
     * Log a big, bold, unmistakable warning.
     */
    private void logBigWarning(String title, String filePath, String message) {
        String paddedPath = filePath + " ".repeat(Math.max(1, 70 - filePath.length()));
        String paddedMessage = message + " ".repeat(Math.max(1, 70 - message.length()));
        
        log.error("");
        log.error("╔════════════════════════════════════════════════════════════════════════════════════╗");
        log.error("║                                                                                    ║");
        log.error("║   ██████╗ ██████╗ ██╗████████╗██╗ ██████╗ █████╗ ██╗         ██╗                    ║");
        log.error("║  ██╔════╝██╔══██╗██║╚══██╔══╝██║██╔════╝██╔══██╗██║         ██║                    ║");
        log.error("║  ██║     ██████╔╝██║   ██║   ██║██║     ███████║██║         ██║                    ║");
        log.error("║  ██║     ██╔══██╗██║   ██║   ██║██║     ██╔══██║██║         ╚═╝                    ║");
        log.error("║  ╚██████╗██║  ██║██║   ██║   ██║╚██████╗██║  ██║███████╗    ██╗                    ║");
        log.error("║   ╚═════╝╚═╝  ╚═╝╚═╝   ╚═╝   ╚═╝ ╚═════╝╚═╝  ╚═╝╚══════╝    ╚═╝                    ║");
        log.error("║                                                                                    ║");
        log.error("║  ⚠️  {}  ⚠️                                        ║", String.format("%-40s", title));
        log.error("║                                                                                    ║");
        log.error("║  File: {}║", paddedPath.substring(0, Math.min(70, paddedPath.length())));
        log.error("║                                                                                    ║");
        log.error("║  {}║", paddedMessage.substring(0, Math.min(70, paddedMessage.length())));
        log.error("║                                                                                    ║");
        log.error("╚════════════════════════════════════════════════════════════════════════════════════╝");
        log.error("");
    }
}
