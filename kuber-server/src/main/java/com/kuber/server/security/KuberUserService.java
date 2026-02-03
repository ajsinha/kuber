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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Service for managing Kuber users.
 * Implements Spring Security's UserDetailsService for authentication.
 * Loads users from JSON file and provides user management functionality.
 * 
 * @version 1.8.3
 * @since 1.7.3
 */
@Slf4j
@Service
public class KuberUserService implements UserDetailsService {
    
    private final KuberProperties properties;
    private final ObjectMapper objectMapper;
    
    // Cache: userId (lowercase) -> KuberUser
    private final Map<String, KuberUser> userCache = new ConcurrentHashMap<>();
    
    private Path usersFilePath;
    
    public KuberUserService(KuberProperties properties) {
        this.properties = properties;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
    
    @PostConstruct
    public void initialize() {
        // Use same file as before for backward compatibility
        String usersFile = properties.getSecurity().getUsersFile();
        usersFilePath = Paths.get(usersFile);
        
        log.info("Users file path: {}", usersFilePath);
        
        // Ensure secure folder exists
        ensureSecureFolderExists();
        
        // Load existing users
        loadUsers();
        
        // Ensure admin user exists
        ensureAdminUserExists();
    }
    
    /**
     * Ensure the secure folder exists
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
        }
    }
    
    /**
     * Load users from JSON file.
     * If file doesn't exist, creates default with admin user only.
     */
    private void loadUsers() {
        userCache.clear();
        
        File file = usersFilePath.toFile();
        if (!file.exists()) {
            log.warn("Users file not found: {}. Creating default with admin user.", usersFilePath);
            createDefaultUsersFile();
        }
        
        try {
            Map<String, List<Map<String, Object>>> wrapper = objectMapper.readValue(file, 
                    new TypeReference<Map<String, List<Map<String, Object>>>>() {});
            List<Map<String, Object>> users = wrapper.get("users");
            
            if (users != null) {
                for (Map<String, Object> userMap : users) {
                    KuberUser user = parseUserFromMap(userMap);
                    userCache.put(user.getUserId().toLowerCase(), user);
                    log.debug("Loaded user: {} with roles: {}", user.getUserId(), user.getRoles());
                }
            }
            
            if (userCache.isEmpty()) {
                log.warn("No users found in {}. Creating default admin user.", usersFilePath);
                createDefaultUsersFile();
                // Reload after creating default
                Map<String, List<Map<String, Object>>> reloadWrapper = objectMapper.readValue(file, 
                        new TypeReference<Map<String, List<Map<String, Object>>>>() {});
                List<Map<String, Object>> reloadUsers = reloadWrapper.get("users");
                if (reloadUsers != null) {
                    for (Map<String, Object> userMap : reloadUsers) {
                        KuberUser user = parseUserFromMap(userMap);
                        userCache.put(user.getUserId().toLowerCase(), user);
                    }
                }
            }
            
            log.info("Loaded {} users from {}", userCache.size(), usersFilePath);
        } catch (IOException e) {
            log.error("Failed to load users: {}", e.getMessage());
            throw new IllegalStateException("Failed to load users from " + usersFilePath + ": " + e.getMessage(), e);
        }
    }
    
    /**
     * Create default users.json with only admin user.
     */
    private void createDefaultUsersFile() {
        try {
            KuberUser adminUser = KuberUser.createAdminUser("admin123");
            
            List<Map<String, Object>> usersList = new ArrayList<>();
            usersList.add(userToMap(adminUser));
            
            Map<String, Object> wrapper = new LinkedHashMap<>();
            wrapper.put("users", usersList);
            
            objectMapper.writeValue(usersFilePath.toFile(), wrapper);
            
            log.info("Created default users.json with admin user at: {}", usersFilePath);
            logDefaultFileCreatedWarning("users.json", usersFilePath.toString());
        } catch (IOException e) {
            log.error("Failed to create default users file: {}", e.getMessage());
            throw new IllegalStateException("Failed to create default users.json: " + e.getMessage(), e);
        }
    }
    
    /**
     * Log warning about default file creation.
     */
    private void logDefaultFileCreatedWarning(String fileName, String path) {
        log.warn("");
        log.warn("╔═══════════════════════════════════════════════════════════════════════════════════════════════╗");
        log.warn("║                                                                                               ║");
        log.warn("║    ⚠️  DEFAULT {} CREATED - PLEASE CHANGE DEFAULT PASSWORD!  ⚠️             ║", String.format("%-12s", fileName.toUpperCase()));
        log.warn("║                                                                                               ║");
        log.warn("║    Location: {}", String.format("%-80s║", path));
        log.warn("║                                                                                               ║");
        log.warn("║    Default admin password is 'admin123' - CHANGE IT IMMEDIATELY!                              ║");
        log.warn("║                                                                                               ║");
        log.warn("╚═══════════════════════════════════════════════════════════════════════════════════════════════╝");
        log.warn("");
    }
    
    /**
     * Parse user from map (JSON object)
     */
    @SuppressWarnings("unchecked")
    private KuberUser parseUserFromMap(Map<String, Object> userMap) {
        KuberUser.KuberUserBuilder builder = KuberUser.builder();
        
        builder.userId((String) userMap.get("userId"));
        builder.password((String) userMap.get("password"));
        builder.fullName((String) userMap.getOrDefault("fullName", userMap.get("userId")));
        builder.email((String) userMap.get("email"));
        
        // Parse roles (can be list or set)
        Object rolesObj = userMap.get("roles");
        Set<String> roles = new HashSet<>();
        if (rolesObj instanceof List) {
            roles.addAll((List<String>) rolesObj);
        } else if (rolesObj instanceof Set) {
            roles.addAll((Set<String>) rolesObj);
        }
        builder.roles(roles);
        
        builder.enabled(Boolean.TRUE.equals(userMap.getOrDefault("enabled", true)));
        builder.locked(Boolean.TRUE.equals(userMap.getOrDefault("locked", false)));
        builder.systemUser(Boolean.TRUE.equals(userMap.getOrDefault("systemUser", false)));
        
        // Parse timestamps if present
        if (userMap.containsKey("createdAt")) {
            builder.createdAt(Instant.parse((String) userMap.get("createdAt")));
        }
        if (userMap.containsKey("createdBy")) {
            builder.createdBy((String) userMap.get("createdBy"));
        }
        
        return builder.build();
    }
    
    /**
     * Ensure admin user always exists
     */
    private void ensureAdminUserExists() {
        if (!userCache.containsKey(KuberUser.ADMIN_USER)) {
            log.warn("Admin user not found in users file. Admin user is required for system operation.");
            // Don't auto-create - require explicit creation in users.json
        } else {
            // Ensure admin user has admin role
            KuberUser admin = userCache.get(KuberUser.ADMIN_USER);
            if (!admin.hasRole(KuberRole.ADMIN_ROLE)) {
                admin.addRole(KuberRole.ADMIN_ROLE);
                saveUsers();
                log.info("Added admin role to admin user");
            }
        }
    }
    
    /**
     * Convert KuberUser to Map for JSON serialization.
     */
    private Map<String, Object> userToMap(KuberUser user) {
        Map<String, Object> userMap = new LinkedHashMap<>();
        userMap.put("userId", user.getUserId());
        userMap.put("password", user.getPassword());
        userMap.put("fullName", user.getFullName());
        if (user.getEmail() != null) {
            userMap.put("email", user.getEmail());
        }
        userMap.put("roles", new ArrayList<>(user.getRoles()));
        userMap.put("enabled", user.isEnabled());
        userMap.put("locked", user.isLocked());
        userMap.put("systemUser", user.isSystemUser());
        if (user.getCreatedAt() != null) {
            userMap.put("createdAt", user.getCreatedAt().toString());
        }
        if (user.getCreatedBy() != null) {
            userMap.put("createdBy", user.getCreatedBy());
        }
        if (user.getModifiedAt() != null) {
            userMap.put("modifiedAt", user.getModifiedAt().toString());
        }
        if (user.getModifiedBy() != null) {
            userMap.put("modifiedBy", user.getModifiedBy());
        }
        return userMap;
    }
    
    /**
     * Save users to file
     */
    private synchronized void saveUsers() {
        try {
            // Convert users to list of maps for backward-compatible JSON format
            List<Map<String, Object>> usersList = new ArrayList<>();
            for (KuberUser user : userCache.values()) {
                usersList.add(userToMap(user));
            }
            
            Map<String, List<Map<String, Object>>> wrapper = new LinkedHashMap<>();
            wrapper.put("users", usersList);
            objectMapper.writeValue(usersFilePath.toFile(), wrapper);
            log.debug("Saved {} users to {}", usersList.size(), usersFilePath);
        } catch (IOException e) {
            log.error("Failed to save users: {}", e.getMessage());
        }
    }
    
    // ==================== UserDetailsService Implementation ====================
    
    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        KuberUser user = userCache.get(username.toLowerCase());
        
        if (user == null) {
            throw new UsernameNotFoundException("User not found: " + username);
        }
        
        if (!user.isEnabled()) {
            throw new UsernameNotFoundException("User is disabled: " + username);
        }
        
        if (user.isLocked()) {
            throw new UsernameNotFoundException("User is locked: " + username);
        }
        
        // Return a copy for Spring Security authentication
        // Using NoOpPasswordEncoder for cleartext password comparison
        return KuberUser.builder()
                .userId(user.getUserId())
                .password(user.getPassword())  // Cleartext - NoOpPasswordEncoder compares directly
                .fullName(user.getFullName())
                .email(user.getEmail())
                .roles(new HashSet<>(user.getRoles()))
                .enabled(user.isEnabled())
                .locked(user.isLocked())
                .systemUser(user.isSystemUser())
                .build();
    }
    
    // ==================== User Management ====================
    
    /**
     * Get user by ID
     */
    public Optional<KuberUser> getUser(String userId) {
        if (userId == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(userCache.get(userId.toLowerCase()));
    }
    
    /**
     * Get all users (safe copies without passwords)
     */
    public Collection<KuberUser> getAllUsers() {
        return userCache.values().stream()
                .map(KuberUser::toSafeUser)
                .collect(Collectors.toList());
    }
    
    /**
     * Create a new user
     */
    public KuberUser createUser(KuberUser user, String createdBy) {
        if (user.getUserId() == null || user.getUserId().isEmpty()) {
            throw new IllegalArgumentException("User ID is required");
        }
        
        if (user.getPassword() == null || user.getPassword().isEmpty()) {
            throw new IllegalArgumentException("Password is required");
        }
        
        // Check if user already exists
        if (userCache.containsKey(user.getUserId().toLowerCase())) {
            throw new IllegalArgumentException("User already exists: " + user.getUserId());
        }
        
        // Cannot create admin user through this method
        if (KuberUser.ADMIN_USER.equalsIgnoreCase(user.getUserId())) {
            throw new IllegalArgumentException("Cannot create reserved admin user");
        }
        
        user.setCreatedAt(Instant.now());
        user.setCreatedBy(createdBy);
        user.setEnabled(true);
        user.setLocked(false);
        
        // Ensure roles set exists
        if (user.getRoles() == null) {
            user.setRoles(new HashSet<>());
        }
        
        userCache.put(user.getUserId().toLowerCase(), user);
        saveUsers();
        
        log.info("Created user '{}' by admin '{}'", user.getUserId(), createdBy);
        return user.toSafeUser();
    }
    
    /**
     * Update an existing user
     */
    public KuberUser updateUser(String userId, KuberUser updates, String modifiedBy) {
        KuberUser existing = userCache.get(userId.toLowerCase());
        if (existing == null) {
            throw new IllegalArgumentException("User not found: " + userId);
        }
        
        // Cannot modify admin user's userId or systemUser status
        if (existing.isAdmin() && updates.getUserId() != null && 
            !KuberUser.ADMIN_USER.equalsIgnoreCase(updates.getUserId())) {
            throw new IllegalArgumentException("Cannot change admin user ID");
        }
        
        // Update allowed fields
        if (updates.getFullName() != null) {
            existing.setFullName(updates.getFullName());
        }
        if (updates.getEmail() != null) {
            existing.setEmail(updates.getEmail());
        }
        if (updates.getPassword() != null && !updates.getPassword().isEmpty()) {
            existing.setPassword(updates.getPassword());
        }
        
        existing.setModifiedAt(Instant.now());
        existing.setModifiedBy(modifiedBy);
        
        saveUsers();
        
        log.info("Updated user '{}' by admin '{}'", userId, modifiedBy);
        return existing.toSafeUser();
    }
    
    /**
     * Delete a user
     */
    public boolean deleteUser(String userId) {
        KuberUser user = userCache.get(userId.toLowerCase());
        if (user == null) {
            return false;
        }
        
        // Cannot delete admin user or system users
        if (user.isAdmin() || user.isSystemUser()) {
            throw new IllegalArgumentException("Cannot delete system user: " + userId);
        }
        
        userCache.remove(userId.toLowerCase());
        saveUsers();
        
        log.info("Deleted user '{}'", userId);
        return true;
    }
    
    /**
     * Enable a user
     */
    public boolean enableUser(String userId) {
        KuberUser user = userCache.get(userId.toLowerCase());
        if (user == null) {
            return false;
        }
        
        user.setEnabled(true);
        saveUsers();
        
        log.info("Enabled user '{}'", userId);
        return true;
    }
    
    /**
     * Disable a user
     */
    public boolean disableUser(String userId) {
        KuberUser user = userCache.get(userId.toLowerCase());
        if (user == null) {
            return false;
        }
        
        // Cannot disable admin user
        if (user.isAdmin()) {
            throw new IllegalArgumentException("Cannot disable admin user");
        }
        
        user.setEnabled(false);
        saveUsers();
        
        log.info("Disabled user '{}'", userId);
        return true;
    }
    
    /**
     * Lock a user account
     */
    public boolean lockUser(String userId) {
        KuberUser user = userCache.get(userId.toLowerCase());
        if (user == null) {
            return false;
        }
        
        // Cannot lock admin user
        if (user.isAdmin()) {
            throw new IllegalArgumentException("Cannot lock admin user");
        }
        
        user.setLocked(true);
        saveUsers();
        
        log.info("Locked user '{}'", userId);
        return true;
    }
    
    /**
     * Unlock a user account
     */
    public boolean unlockUser(String userId) {
        KuberUser user = userCache.get(userId.toLowerCase());
        if (user == null) {
            return false;
        }
        
        user.setLocked(false);
        user.setFailedLoginAttempts(0);
        saveUsers();
        
        log.info("Unlocked user '{}'", userId);
        return true;
    }
    
    /**
     * Assign a role to a user
     */
    public boolean assignRole(String userId, String roleName, String modifiedBy) {
        KuberUser user = userCache.get(userId.toLowerCase());
        if (user == null) {
            return false;
        }
        
        user.addRole(roleName);
        user.setModifiedAt(Instant.now());
        user.setModifiedBy(modifiedBy);
        saveUsers();
        
        log.info("Assigned role '{}' to user '{}' by admin '{}'", roleName, userId, modifiedBy);
        return true;
    }
    
    /**
     * Remove a role from a user
     */
    public boolean removeRole(String userId, String roleName, String modifiedBy) {
        KuberUser user = userCache.get(userId.toLowerCase());
        if (user == null) {
            return false;
        }
        
        // Cannot remove admin role from admin user
        if (user.isAdmin() && KuberRole.ADMIN_ROLE.equalsIgnoreCase(roleName)) {
            throw new IllegalArgumentException("Cannot remove admin role from admin user");
        }
        
        user.removeRole(roleName);
        user.setModifiedAt(Instant.now());
        user.setModifiedBy(modifiedBy);
        saveUsers();
        
        log.info("Removed role '{}' from user '{}' by admin '{}'", roleName, userId, modifiedBy);
        return true;
    }
    
    /**
     * Set user's roles (replace all)
     */
    public boolean setRoles(String userId, Set<String> roles, String modifiedBy) {
        KuberUser user = userCache.get(userId.toLowerCase());
        if (user == null) {
            return false;
        }
        
        // Ensure admin user keeps admin role
        if (user.isAdmin()) {
            roles = new HashSet<>(roles);
            roles.add(KuberRole.ADMIN_ROLE);
        }
        
        user.setRoles(roles);
        user.setModifiedAt(Instant.now());
        user.setModifiedBy(modifiedBy);
        saveUsers();
        
        log.info("Set roles {} for user '{}' by admin '{}'", roles, userId, modifiedBy);
        return true;
    }
    
    /**
     * Change user's password
     */
    public boolean changePassword(String userId, String newPassword, String modifiedBy) {
        KuberUser user = userCache.get(userId.toLowerCase());
        if (user == null) {
            return false;
        }
        
        user.setPassword(newPassword);
        user.setModifiedAt(Instant.now());
        user.setModifiedBy(modifiedBy);
        saveUsers();
        
        log.info("Changed password for user '{}' by admin '{}'", userId, modifiedBy);
        return true;
    }
    
    /**
     * Record successful login
     */
    public void recordSuccessfulLogin(String userId) {
        KuberUser user = userCache.get(userId.toLowerCase());
        if (user != null) {
            user.setLastLoginAt(Instant.now());
            user.setFailedLoginAttempts(0);
            saveUsers();
        }
    }
    
    /**
     * Record failed login attempt
     */
    public void recordFailedLogin(String userId) {
        KuberUser user = userCache.get(userId.toLowerCase());
        if (user != null) {
            user.setFailedLoginAttempts(user.getFailedLoginAttempts() + 1);
            
            // Auto-lock after 5 failed attempts (except admin)
            if (!user.isAdmin() && user.getFailedLoginAttempts() >= 5) {
                user.setLocked(true);
                log.warn("User '{}' locked after {} failed login attempts", userId, user.getFailedLoginAttempts());
            }
            
            saveUsers();
        }
    }
    
    /**
     * Reload users from file
     */
    public void reloadUsers() {
        loadUsers();
        ensureAdminUserExists();
    }
    
    /**
     * Check if user exists
     */
    public boolean userExists(String userId) {
        return userId != null && userCache.containsKey(userId.toLowerCase());
    }
    
    /**
     * Get users with a specific role
     */
    public List<KuberUser> getUsersWithRole(String roleName) {
        return userCache.values().stream()
                .filter(u -> u.hasRole(roleName))
                .map(KuberUser::toSafeUser)
                .collect(Collectors.toList());
    }
    
    /**
     * Get statistics about users
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("totalUsers", userCache.size());
        stats.put("enabledUsers", userCache.values().stream().filter(KuberUser::isEnabled).count());
        stats.put("lockedUsers", userCache.values().stream().filter(KuberUser::isLocked).count());
        stats.put("adminUsers", userCache.values().stream().filter(KuberUser::isAdmin).count());
        stats.put("filePath", usersFilePath.toString());
        return stats;
    }
}
