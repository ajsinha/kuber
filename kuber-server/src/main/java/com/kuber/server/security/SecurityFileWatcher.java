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
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Watches security configuration files (users.json, roles.json, apikeys.json) for changes.
 * 
 * Features:
 * - Auto-reload every 5 minutes
 * - Detect file modifications and reload immediately
 * - When users.json OR roles.json changes, BOTH are reloaded together
 * - Create default files if missing (admin user + admin role)
 * 
 * @version 2.4.0
 */
@Slf4j
@Service
@EnableScheduling
public class SecurityFileWatcher {
    
    private final KuberProperties properties;
    private final KuberUserService userService;
    private final KuberRoleService roleService;
    private final ApiKeyService apiKeyService;
    
    private long usersFileLastModified = 0;
    private long rolesFileLastModified = 0;
    private long apiKeysFileLastModified = 0;
    
    private Path usersFilePath;
    private Path rolesFilePath;
    private Path apiKeysFilePath;
    
    public SecurityFileWatcher(KuberProperties properties,
                               KuberUserService userService,
                               KuberRoleService roleService,
                               ApiKeyService apiKeyService) {
        this.properties = properties;
        this.userService = userService;
        this.roleService = roleService;
        this.apiKeyService = apiKeyService;
    }
    
    @PostConstruct
    public void initialize() {
        usersFilePath = Paths.get(properties.getSecurity().getUsersFile());
        rolesFilePath = Paths.get(properties.getSecurity().getRolesFile());
        apiKeysFilePath = Paths.get(properties.getSecurity().getApiKeysFile());
        
        // Record initial modification times
        File usersFile = usersFilePath.toFile();
        File rolesFile = rolesFilePath.toFile();
        File apiKeysFile = apiKeysFilePath.toFile();
        
        if (usersFile.exists()) {
            usersFileLastModified = usersFile.lastModified();
        }
        if (rolesFile.exists()) {
            rolesFileLastModified = rolesFile.lastModified();
        }
        if (apiKeysFile.exists()) {
            apiKeysFileLastModified = apiKeysFile.lastModified();
        }
        
        log.info("SecurityFileWatcher initialized - monitoring:");
        log.info("  - Users:   {}", usersFilePath);
        log.info("  - Roles:   {}", rolesFilePath);
        log.info("  - APIKeys: {}", apiKeysFilePath);
    }
    
    /**
     * Check for file changes every 30 seconds.
     */
    @Scheduled(fixedDelay = 30000) // 30 seconds
    public void checkForChanges() {
        boolean usersChanged = checkUsersFileChanged();
        boolean rolesChanged = checkRolesFileChanged();
        
        // If EITHER users.json OR roles.json changed, reload BOTH
        if (usersChanged || rolesChanged) {
            reloadUsersAndRoles(usersChanged ? "users.json" : "roles.json");
        }
        
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
     * Check if users.json has changed.
     */
    private boolean checkUsersFileChanged() {
        File file = usersFilePath.toFile();
        
        if (!file.exists()) {
            return false; // Will be handled by reloadUsersAndRoles
        }
        
        long currentModified = file.lastModified();
        if (currentModified != usersFileLastModified) {
            usersFileLastModified = currentModified;
            return true;
        }
        return false;
    }
    
    /**
     * Check if roles.json has changed.
     */
    private boolean checkRolesFileChanged() {
        File file = rolesFilePath.toFile();
        
        if (!file.exists()) {
            return false; // Will be handled by reloadUsersAndRoles
        }
        
        long currentModified = file.lastModified();
        if (currentModified != rolesFileLastModified) {
            rolesFileLastModified = currentModified;
            return true;
        }
        return false;
    }
    
    /**
     * Reload both users and roles together.
     * This ensures consistency when role assignments change.
     */
    private void reloadUsersAndRoles(String triggerFile) {
        log.info("Detected change in {} - reloading users AND roles...", triggerFile);
        
        try {
            // Always reload roles first (users reference roles)
            roleService.reloadRoles();
            File rolesFile = rolesFilePath.toFile();
            if (rolesFile.exists()) {
                rolesFileLastModified = rolesFile.lastModified();
            }
            log.info("Reloaded roles.json: {} roles", roleService.getAllRoles().size());
        } catch (Exception e) {
            log.error("Failed to reload roles.json: {}", e.getMessage());
        }
        
        try {
            userService.reloadUsers();
            File usersFile = usersFilePath.toFile();
            if (usersFile.exists()) {
                usersFileLastModified = usersFile.lastModified();
            }
            log.info("Reloaded users.json: {} users", userService.getAllUsers().size());
        } catch (Exception e) {
            log.error("Failed to reload users.json: {}", e.getMessage());
        }
        
        log.info("Security files reload complete - changes are now active");
    }
    
    /**
     * Check apikeys.json for changes or deletion.
     */
    private void checkApiKeysFile() {
        File file = apiKeysFilePath.toFile();
        
        if (!file.exists()) {
            // File was deleted! Log warning and recreate
            logBigWarning("APIKEYS.JSON DELETED", apiKeysFilePath.toString(),
                "All API key authentication will fail! Creating empty file...");
            
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
                log.info("Successfully reloaded apikeys.json: {} keys", apiKeyService.getAllKeys().size());
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
        
        // Reload roles first
        try {
            roleService.reloadRoles();
            File rolesFile = rolesFilePath.toFile();
            if (rolesFile.exists()) {
                rolesFileLastModified = rolesFile.lastModified();
            }
            log.info("Reloaded roles.json: {} roles", roleService.getAllRoles().size());
        } catch (Exception e) {
            log.error("Failed to reload roles.json: {}", e.getMessage());
        }
        
        // Then reload users
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
        
        // Finally reload API keys
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
        
        log.info("All security files reloaded - changes are now active");
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
