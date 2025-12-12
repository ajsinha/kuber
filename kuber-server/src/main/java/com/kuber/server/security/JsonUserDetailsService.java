/*
 * Copyright (c) 2025-2030, All Rights Reserved
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kuber.server.config.KuberProperties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ResourceLoader;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Simple user details service that reads users from a JSON file.
 * Uses cleartext password comparison for simplicity.
 * 
 * @version 1.6.5 - Added secure folder creation and required users.json validation
 * @deprecated As of v1.7.3, replaced by {@link KuberUserService} which provides
 *             enterprise RBAC with region-specific permissions. This class is
 *             retained for backward compatibility but will be removed in a future version.
 */
@Deprecated(since = "1.7.3", forRemoval = true)
@Slf4j
@Service("legacyUserDetailsService")
public class JsonUserDetailsService implements UserDetailsService {
    
    private final KuberProperties properties;
    private final ResourceLoader resourceLoader;
    private final ObjectMapper objectMapper;
    
    // Cache of users loaded from JSON file
    private final Map<String, JsonUser> usersCache = new ConcurrentHashMap<>();
    
    public JsonUserDetailsService(KuberProperties properties, ResourceLoader resourceLoader) {
        this.properties = properties;
        this.resourceLoader = resourceLoader;
        this.objectMapper = new ObjectMapper();
    }
    
    @PostConstruct
    public void loadUsers() {
        // Step 1: Ensure secure folder exists (v1.6.4)
        ensureSecureFolderExists();
        
        // Step 2: Load users from file
        loadUsersFromFile();
    }
    
    /**
     * Ensure the secure folder exists, create if necessary.
     * @since 1.6.5
     */
    private void ensureSecureFolderExists() {
        String secureFolder = properties.getSecure().getFolder();
        Path securePath = Paths.get(secureFolder);
        
        if (!Files.exists(securePath)) {
            try {
                Files.createDirectories(securePath);
                log.info("Created secure folder: {}", securePath.toAbsolutePath());
            } catch (Exception e) {
                log.error("Failed to create secure folder '{}': {}", secureFolder, e.getMessage());
                throw new IllegalStateException("Cannot create secure folder: " + secureFolder, e);
            }
        } else {
            log.info("Secure folder exists: {}", securePath.toAbsolutePath());
        }
    }
    
    /**
     * Load users from JSON file. Fails if file is missing.
     * @since 1.6.5 - Now fails startup if users.json is missing
     */
    private void loadUsersFromFile() {
        String usersFilePath = properties.getSecurity().getUsersFile();
        log.info("Loading users from: {}", usersFilePath);
        
        try {
            InputStream inputStream;
            
            // Check if it's a file path or classpath resource
            if (usersFilePath.startsWith("classpath:")) {
                inputStream = resourceLoader.getResource(usersFilePath).getInputStream();
            } else {
                // File system path
                File usersFile = new File(usersFilePath);
                if (!usersFile.exists()) {
                    // CRITICAL: users.json is REQUIRED - fail startup (v1.6.4)
                    logMissingUsersFileWarning(usersFilePath, usersFile.getAbsolutePath());
                    throw new IllegalStateException("Required file missing: " + usersFilePath);
                }
                inputStream = new FileInputStream(usersFile);
            }
            
            JsonNode root = objectMapper.readTree(inputStream);
            JsonNode usersNode = root.get("users");
            
            if (usersNode != null && usersNode.isArray()) {
                for (JsonNode userNode : usersNode) {
                    JsonUser user = new JsonUser();
                    user.setUserId(userNode.get("userId").asText());
                    user.setPassword(userNode.get("password").asText());
                    user.setFullName(userNode.get("fullName").asText());
                    
                    List<String> roles = new ArrayList<>();
                    JsonNode rolesNode = userNode.get("roles");
                    if (rolesNode != null && rolesNode.isArray()) {
                        for (JsonNode roleNode : rolesNode) {
                            roles.add(roleNode.asText());
                        }
                    }
                    user.setRoles(roles);
                    
                    usersCache.put(user.getUserId().toLowerCase(), user);
                    log.debug("Loaded user: {} with roles: {}", user.getUserId(), user.getRoles());
                }
            }
            
            if (usersCache.isEmpty()) {
                throw new IllegalStateException("No users found in " + usersFilePath + ". At least one user is required.");
            }
            
            log.info("Loaded {} users from {}", usersCache.size(), usersFilePath);
            inputStream.close();
            
        } catch (IllegalStateException e) {
            // Re-throw startup failures
            throw e;
        } catch (Exception e) {
            log.error("Failed to load users from file '{}': {}", usersFilePath, e.getMessage());
            throw new IllegalStateException("Failed to load users from " + usersFilePath + ": " + e.getMessage(), e);
        }
    }
    
    /**
     * Log a big, bold, unmistakable warning about missing users.json.
     */
    private void logMissingUsersFileWarning(String configuredPath, String absolutePath) {
        log.error("");
        log.error("╔═══════════════════════════════════════════════════════════════════════════════════════════════╗");
        log.error("║                                                                                               ║");
        log.error("║    ██╗   ██╗███████╗███████╗██████╗ ███████╗         ██╗███████╗ ██████╗ ███╗   ██╗           ║");
        log.error("║    ██║   ██║██╔════╝██╔════╝██╔══██╗██╔════╝         ██║██╔════╝██╔═══██╗████╗  ██║           ║");
        log.error("║    ██║   ██║███████╗█████╗  ██████╔╝███████╗         ██║███████╗██║   ██║██╔██╗ ██║           ║");
        log.error("║    ██║   ██║╚════██║██╔══╝  ██╔══██╗╚════██║    ██   ██║╚════██║██║   ██║██║╚██╗██║           ║");
        log.error("║    ╚██████╔╝███████║███████╗██║  ██║███████║    ╚█████╔╝███████║╚██████╔╝██║ ╚████║           ║");
        log.error("║     ╚═════╝ ╚══════╝╚══════╝╚═╝  ╚═╝╚══════╝     ╚════╝ ╚══════╝ ╚═════╝ ╚═╝  ╚═══╝           ║");
        log.error("║                                                                                               ║");
        log.error("║          ███╗   ███╗██╗███████╗███████╗██╗███╗   ██╗ ██████╗ ██╗                              ║");
        log.error("║          ████╗ ████║██║██╔════╝██╔════╝██║████╗  ██║██╔════╝ ██║                              ║");
        log.error("║          ██╔████╔██║██║███████╗███████╗██║██╔██╗ ██║██║  ███╗██║                              ║");
        log.error("║          ██║╚██╔╝██║██║╚════██║╚════██║██║██║╚██╗██║██║   ██║╚═╝                              ║");
        log.error("║          ██║ ╚═╝ ██║██║███████║███████║██║██║ ╚████║╚██████╔╝██╗                              ║");
        log.error("║          ╚═╝     ╚═╝╚═╝╚══════╝╚══════╝╚═╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝                              ║");
        log.error("║                                                                                               ║");
        log.error("║    ⚠️  CRITICAL ERROR: USERS.JSON NOT FOUND! APPLICATION CANNOT START!  ⚠️                    ║");
        log.error("║                                                                                               ║");
        log.error("║    Configured: {}", String.format("%-78s║", configuredPath));
        log.error("║    Absolute:   {}", String.format("%-78s║", absolutePath));
        log.error("║                                                                                               ║");
        log.error("║    The users.json file is REQUIRED for Kuber to start.                                       ║");
        log.error("║    Please create this file with at least one user.                                           ║");
        log.error("║                                                                                               ║");
        log.error("║    Example users.json:                                                                        ║");
        log.error("║    {                                                                                          ║");
        log.error("║      \"users\": [                                                                              ║");
        log.error("║        {                                                                                      ║");
        log.error("║          \"userId\": \"admin\",                                                                  ║");
        log.error("║          \"password\": \"your-secure-password\",                                                 ║");
        log.error("║          \"fullName\": \"Administrator\",                                                        ║");
        log.error("║          \"roles\": [\"ADMIN\", \"OPERATOR\", \"USER\"]                                             ║");
        log.error("║        }                                                                                      ║");
        log.error("║      ]                                                                                        ║");
        log.error("║    }                                                                                          ║");
        log.error("║                                                                                               ║");
        log.error("╚═══════════════════════════════════════════════════════════════════════════════════════════════╝");
        log.error("");
    }
    
    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        JsonUser jsonUser = usersCache.get(username.toLowerCase());
        
        if (jsonUser == null) {
            throw new UsernameNotFoundException("User not found: " + username);
        }
        
        List<SimpleGrantedAuthority> authorities = jsonUser.getRoles().stream()
                .map(role -> new SimpleGrantedAuthority("ROLE_" + role))
                .collect(Collectors.toList());
        
        // Use {noop} prefix to indicate cleartext password (no encoding)
        return User.builder()
                .username(jsonUser.getUserId())
                .password("{noop}" + jsonUser.getPassword())
                .authorities(authorities)
                .build();
    }
    
    /**
     * Get user by ID
     */
    public JsonUser getUser(String userId) {
        return usersCache.get(userId.toLowerCase());
    }
    
    /**
     * Get all users
     */
    public Collection<JsonUser> getAllUsers() {
        return new ArrayList<>(usersCache.values());
    }
    
    /**
     * Reload users from file.
     * Does not throw exceptions - logs errors instead.
     */
    public void reloadUsers() {
        String usersFilePath = properties.getSecurity().getUsersFile();
        File usersFile = new File(usersFilePath);
        
        if (!usersFile.exists()) {
            log.error("Cannot reload users - file not found: {}", usersFilePath);
            logMissingUsersFileWarning(usersFilePath, usersFile.getAbsolutePath());
            return;
        }
        
        Map<String, JsonUser> newUsersCache = new ConcurrentHashMap<>();
        
        try (InputStream inputStream = new FileInputStream(usersFile)) {
            JsonNode root = objectMapper.readTree(inputStream);
            JsonNode usersNode = root.get("users");
            
            if (usersNode != null && usersNode.isArray()) {
                for (JsonNode userNode : usersNode) {
                    JsonUser user = new JsonUser();
                    user.setUserId(userNode.get("userId").asText());
                    user.setPassword(userNode.get("password").asText());
                    user.setFullName(userNode.get("fullName").asText());
                    
                    List<String> roles = new ArrayList<>();
                    JsonNode rolesNode = userNode.get("roles");
                    if (rolesNode != null && rolesNode.isArray()) {
                        for (JsonNode roleNode : rolesNode) {
                            roles.add(roleNode.asText());
                        }
                    }
                    user.setRoles(roles);
                    
                    newUsersCache.put(user.getUserId().toLowerCase(), user);
                }
            }
            
            if (newUsersCache.isEmpty()) {
                log.error("No users found in {}. Keeping existing users.", usersFilePath);
                return;
            }
            
            // Only replace cache if we successfully loaded users
            usersCache.clear();
            usersCache.putAll(newUsersCache);
            log.info("Reloaded {} users from {}", usersCache.size(), usersFilePath);
            
        } catch (Exception e) {
            log.error("Failed to reload users from '{}': {}. Keeping existing users.", usersFilePath, e.getMessage());
        }
    }
    
    /**
     * Simple user data class
     */
    @Data
    public static class JsonUser {
        private String userId;
        private String password;
        private String fullName;
        private List<String> roles = new ArrayList<>();
    }
}
