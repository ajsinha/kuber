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
 * Service for managing Kuber roles.
 * Loads roles from JSON file and provides role lookup and management.
 * 
 * @version 1.8.3
 * @since 1.7.3
 */
@Slf4j
@Service
public class KuberRoleService {
    
    private final KuberProperties properties;
    private final ObjectMapper objectMapper;
    
    // Cache: roleName -> KuberRole
    private final Map<String, KuberRole> roleCache = new ConcurrentHashMap<>();
    
    private Path rolesFilePath;
    
    public KuberRoleService(KuberProperties properties) {
        this.properties = properties;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
    
    @PostConstruct
    public void initialize() {
        String rolesFile = properties.getSecurity().getRolesFile();
        rolesFilePath = Paths.get(rolesFile);
        
        log.info("Roles file path: {}", rolesFilePath);
        
        // Ensure parent directory exists
        try {
            Files.createDirectories(rolesFilePath.getParent());
        } catch (IOException e) {
            log.warn("Could not create roles directory: {}", e.getMessage());
        }
        
        // Load existing roles
        loadRoles();
        
        // Ensure admin role exists
        ensureAdminRoleExists();
    }
    
    /**
     * Load roles from JSON file
     */
    private void loadRoles() {
        roleCache.clear();
        
        File file = rolesFilePath.toFile();
        if (!file.exists()) {
            log.warn("Roles file not found: {}. Creating with default roles.", rolesFilePath);
            createDefaultRolesFile(file);
            return;
        }
        
        try {
            Map<String, List<KuberRole>> wrapper = objectMapper.readValue(file, 
                    new TypeReference<Map<String, List<KuberRole>>>() {});
            List<KuberRole> roles = wrapper.get("roles");
            
            if (roles != null) {
                for (KuberRole role : roles) {
                    roleCache.put(role.getName().toLowerCase(), role);
                }
            }
            
            log.info("Loaded {} roles from {}", roleCache.size(), rolesFilePath);
        } catch (IOException e) {
            log.error("Failed to load roles: {}. Creating default roles.", e.getMessage());
            createDefaultRolesFile(file);
        }
    }
    
    /**
     * Create default roles file with admin role only.
     */
    private void createDefaultRolesFile(File file) {
        try {
            KuberRole adminRole = KuberRole.createAdminRole();
            roleCache.put(adminRole.getName().toLowerCase(), adminRole);
            
            saveRoles();
            
            log.info("Created default roles file at: {}", rolesFilePath);
            logDefaultFileCreatedWarning("roles.json", rolesFilePath.toString());
        } catch (Exception e) {
            log.error("Failed to create default roles file: {}", e.getMessage());
        }
    }
    
    /**
     * Log warning about default file creation.
     */
    private void logDefaultFileCreatedWarning(String fileName, String path) {
        log.warn("");
        log.warn("╔═══════════════════════════════════════════════════════════════════════════════════════════════╗");
        log.warn("║                                                                                               ║");
        log.warn("║    ⚠️  DEFAULT {} CREATED WITH ADMIN ROLE ONLY  ⚠️                          ║", String.format("%-12s", fileName.toUpperCase()));
        log.warn("║                                                                                               ║");
        log.warn("║    Location: {}", String.format("%-80s║", path));
        log.warn("║                                                                                               ║");
        log.warn("║    Add additional roles for region-specific access control.                                   ║");
        log.warn("║    See secure-sample/roles.json.sample for examples.                                          ║");
        log.warn("║                                                                                               ║");
        log.warn("╚═══════════════════════════════════════════════════════════════════════════════════════════════╝");
        log.warn("");
    }
    
    /**
     * Ensure admin role always exists
     */
    private void ensureAdminRoleExists() {
        if (!roleCache.containsKey(KuberRole.ADMIN_ROLE)) {
            KuberRole adminRole = KuberRole.createAdminRole();
            roleCache.put(adminRole.getName().toLowerCase(), adminRole);
            saveRoles();
            log.info("Created admin role");
        }
    }
    
    /**
     * Save roles to file
     */
    private synchronized void saveRoles() {
        try {
            List<KuberRole> roles = new ArrayList<>(roleCache.values());
            Map<String, List<KuberRole>> wrapper = new LinkedHashMap<>();
            wrapper.put("roles", roles);
            objectMapper.writeValue(rolesFilePath.toFile(), wrapper);
            log.debug("Saved {} roles to {}", roles.size(), rolesFilePath);
        } catch (IOException e) {
            log.error("Failed to save roles: {}", e.getMessage());
        }
    }
    
    /**
     * Get a role by name
     */
    public Optional<KuberRole> getRole(String roleName) {
        if (roleName == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(roleCache.get(roleName.toLowerCase()));
    }
    
    /**
     * Get all roles
     */
    public Collection<KuberRole> getAllRoles() {
        return new ArrayList<>(roleCache.values());
    }
    
    /**
     * Get roles for a specific region
     */
    public List<KuberRole> getRolesForRegion(String regionName) {
        return roleCache.values().stream()
                .filter(r -> r.appliesToRegion(regionName))
                .collect(Collectors.toList());
    }
    
    /**
     * Create a new role
     */
    public KuberRole createRole(KuberRole role, String createdBy) {
        if (role.getName() == null || role.getName().isEmpty()) {
            throw new IllegalArgumentException("Role name is required");
        }
        
        // Check if role already exists
        if (roleCache.containsKey(role.getName().toLowerCase())) {
            throw new IllegalArgumentException("Role already exists: " + role.getName());
        }
        
        // Cannot create admin role
        if (KuberRole.ADMIN_ROLE.equalsIgnoreCase(role.getName())) {
            throw new IllegalArgumentException("Cannot create reserved admin role");
        }
        
        role.setCreatedAt(Instant.now());
        role.setCreatedBy(createdBy);
        role.setActive(true);
        
        roleCache.put(role.getName().toLowerCase(), role);
        saveRoles();
        
        log.info("Created role '{}' by user '{}'", role.getName(), createdBy);
        return role;
    }
    
    /**
     * Update an existing role
     */
    public KuberRole updateRole(String roleName, KuberRole updates, String modifiedBy) {
        KuberRole existing = roleCache.get(roleName.toLowerCase());
        if (existing == null) {
            throw new IllegalArgumentException("Role not found: " + roleName);
        }
        
        // Cannot modify admin role
        if (existing.isAdmin()) {
            throw new IllegalArgumentException("Cannot modify admin role");
        }
        
        // Update allowed fields
        if (updates.getDisplayName() != null) {
            existing.setDisplayName(updates.getDisplayName());
        }
        if (updates.getDescription() != null) {
            existing.setDescription(updates.getDescription());
        }
        if (updates.getRegion() != null) {
            existing.setRegion(updates.getRegion());
        }
        if (updates.getPermissions() != null && !updates.getPermissions().isEmpty()) {
            existing.setPermissions(updates.getPermissions());
        }
        
        existing.setModifiedAt(Instant.now());
        existing.setModifiedBy(modifiedBy);
        
        saveRoles();
        
        log.info("Updated role '{}' by user '{}'", roleName, modifiedBy);
        return existing;
    }
    
    /**
     * Delete a role
     */
    public boolean deleteRole(String roleName) {
        KuberRole role = roleCache.get(roleName.toLowerCase());
        if (role == null) {
            return false;
        }
        
        // Cannot delete admin role or system roles
        if (role.isAdmin() || role.isSystemRole()) {
            throw new IllegalArgumentException("Cannot delete system role: " + roleName);
        }
        
        roleCache.remove(roleName.toLowerCase());
        saveRoles();
        
        log.info("Deleted role '{}'", roleName);
        return true;
    }
    
    /**
     * Activate a role
     */
    public boolean activateRole(String roleName) {
        KuberRole role = roleCache.get(roleName.toLowerCase());
        if (role == null) {
            return false;
        }
        
        role.setActive(true);
        saveRoles();
        
        log.info("Activated role '{}'", roleName);
        return true;
    }
    
    /**
     * Deactivate a role
     */
    public boolean deactivateRole(String roleName) {
        KuberRole role = roleCache.get(roleName.toLowerCase());
        if (role == null) {
            return false;
        }
        
        // Cannot deactivate admin role
        if (role.isAdmin()) {
            throw new IllegalArgumentException("Cannot deactivate admin role");
        }
        
        role.setActive(false);
        saveRoles();
        
        log.info("Deactivated role '{}'", roleName);
        return true;
    }
    
    /**
     * Create roles for a new region
     */
    public void createRegionRoles(String regionName, String createdBy) {
        // Only create if they don't exist
        String readOnlyName = regionName + "_readonly";
        String readWriteName = regionName + "_readwrite";
        String fullName = regionName + "_full";
        
        if (!roleCache.containsKey(readOnlyName.toLowerCase())) {
            KuberRole readOnly = KuberRole.createReadOnlyRole(regionName);
            readOnly.setCreatedBy(createdBy);
            roleCache.put(readOnlyName.toLowerCase(), readOnly);
        }
        
        if (!roleCache.containsKey(readWriteName.toLowerCase())) {
            KuberRole readWrite = KuberRole.createReadWriteRole(regionName);
            readWrite.setCreatedBy(createdBy);
            roleCache.put(readWriteName.toLowerCase(), readWrite);
        }
        
        if (!roleCache.containsKey(fullName.toLowerCase())) {
            KuberRole full = KuberRole.createFullAccessRole(regionName);
            full.setCreatedBy(createdBy);
            roleCache.put(fullName.toLowerCase(), full);
        }
        
        saveRoles();
        log.info("Created roles for region '{}' by user '{}'", regionName, createdBy);
    }
    
    /**
     * Delete roles for a region
     */
    public void deleteRegionRoles(String regionName) {
        List<String> rolesToDelete = roleCache.values().stream()
                .filter(r -> !r.isSystemRole() && regionName.equalsIgnoreCase(r.getRegion()))
                .map(KuberRole::getName)
                .collect(Collectors.toList());
        
        for (String roleName : rolesToDelete) {
            roleCache.remove(roleName.toLowerCase());
        }
        
        if (!rolesToDelete.isEmpty()) {
            saveRoles();
            log.info("Deleted {} roles for region '{}'", rolesToDelete.size(), regionName);
        }
    }
    
    /**
     * Reload roles from file
     */
    public void reloadRoles() {
        loadRoles();
        ensureAdminRoleExists();
    }
    
    /**
     * Check if a role exists
     */
    public boolean roleExists(String roleName) {
        return roleName != null && roleCache.containsKey(roleName.toLowerCase());
    }
    
    /**
     * Get statistics about roles
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("totalRoles", roleCache.size());
        stats.put("activeRoles", roleCache.values().stream().filter(KuberRole::isActive).count());
        stats.put("systemRoles", roleCache.values().stream().filter(KuberRole::isSystemRole).count());
        stats.put("filePath", rolesFilePath.toString());
        return stats;
    }
}
