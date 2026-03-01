/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.server.security;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a role in the Kuber RBAC system.
 * 
 * A role defines permissions for a specific region or system-wide (admin).
 * 
 * Role naming convention:
 * - "admin" - Reserved system admin role with full access
 * - "{region}_readonly" - Read-only access to a specific region
 * - "{region}_readwrite" - Read and write access to a specific region  
 * - "{region}_full" - Full access (read, write, delete) to a specific region
 * - Custom roles can have any name
 * 
 * @version 2.6.3
 * @since 1.7.3
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KuberRole {
    
    /**
     * Reserved admin role name
     */
    public static final String ADMIN_ROLE = "admin";
    
    /**
     * Wildcard for all regions
     */
    public static final String ALL_REGIONS = "*";
    
    /**
     * Unique role identifier/name
     */
    private String name;
    
    /**
     * Display name for the role
     */
    private String displayName;
    
    /**
     * Description of what this role grants
     */
    private String description;
    
    /**
     * Region this role applies to.
     * Use "*" for all regions (admin role only).
     * Use specific region name for region-scoped roles.
     */
    private String region;
    
    /**
     * Permissions granted by this role
     */
    @Builder.Default
    private Set<KuberPermission> permissions = new HashSet<>();
    
    /**
     * Whether this role is a system role (cannot be deleted)
     */
    @Builder.Default
    private boolean systemRole = false;
    
    /**
     * Whether this role is currently active
     */
    @Builder.Default
    private boolean active = true;
    
    /**
     * When the role was created
     */
    private Instant createdAt;
    
    /**
     * Who created this role
     */
    private String createdBy;
    
    /**
     * When the role was last modified
     */
    private Instant modifiedAt;
    
    /**
     * Who last modified this role
     */
    private String modifiedBy;
    
    /**
     * Check if this is the admin role
     */
    @JsonIgnore
    public boolean isAdmin() {
        return ADMIN_ROLE.equalsIgnoreCase(name);
    }
    
    /**
     * Check if this role applies to all regions
     */
    @JsonIgnore
    public boolean isGlobalRole() {
        return ALL_REGIONS.equals(region) || isAdmin();
    }
    
    /**
     * Check if this role applies to a specific region
     */
    public boolean appliesToRegion(String regionName) {
        if (isGlobalRole()) {
            return true;
        }
        return region != null && region.equalsIgnoreCase(regionName);
    }
    
    /**
     * Check if this role has a specific permission
     */
    public boolean hasPermission(KuberPermission permission) {
        if (isAdmin()) {
            return true; // Admin has all permissions
        }
        return permissions != null && permissions.contains(permission);
    }
    
    /**
     * Check if this role has a specific permission for a specific region
     */
    public boolean hasPermission(String regionName, KuberPermission permission) {
        if (!appliesToRegion(regionName)) {
            return false;
        }
        return hasPermission(permission);
    }
    
    /**
     * Create the admin role
     */
    public static KuberRole createAdminRole() {
        Set<KuberPermission> allPermissions = new HashSet<>();
        allPermissions.add(KuberPermission.READ);
        allPermissions.add(KuberPermission.WRITE);
        allPermissions.add(KuberPermission.DELETE);
        allPermissions.add(KuberPermission.ADMIN);
        
        return KuberRole.builder()
                .name(ADMIN_ROLE)
                .displayName("System Administrator")
                .description("Full system access including region management, user management, and all cache operations")
                .region(ALL_REGIONS)
                .permissions(allPermissions)
                .systemRole(true)
                .active(true)
                .createdAt(Instant.now())
                .createdBy("system")
                .build();
    }
    
    /**
     * Create a read-only role for a region
     */
    public static KuberRole createReadOnlyRole(String regionName) {
        Set<KuberPermission> readPermissions = new HashSet<>();
        readPermissions.add(KuberPermission.READ);
        
        return KuberRole.builder()
                .name(regionName + "_readonly")
                .displayName(regionName + " Read Only")
                .description("Read-only access to region: " + regionName)
                .region(regionName)
                .permissions(readPermissions)
                .systemRole(false)
                .active(true)
                .createdAt(Instant.now())
                .createdBy("system")
                .build();
    }
    
    /**
     * Create a read-write role for a region
     */
    public static KuberRole createReadWriteRole(String regionName) {
        Set<KuberPermission> rwPermissions = new HashSet<>();
        rwPermissions.add(KuberPermission.READ);
        rwPermissions.add(KuberPermission.WRITE);
        
        return KuberRole.builder()
                .name(regionName + "_readwrite")
                .displayName(regionName + " Read/Write")
                .description("Read and write access to region: " + regionName)
                .region(regionName)
                .permissions(rwPermissions)
                .systemRole(false)
                .active(true)
                .createdAt(Instant.now())
                .createdBy("system")
                .build();
    }
    
    /**
     * Create a full access role for a region
     */
    public static KuberRole createFullAccessRole(String regionName) {
        Set<KuberPermission> fullPermissions = new HashSet<>();
        fullPermissions.add(KuberPermission.READ);
        fullPermissions.add(KuberPermission.WRITE);
        fullPermissions.add(KuberPermission.DELETE);
        
        return KuberRole.builder()
                .name(regionName + "_full")
                .displayName(regionName + " Full Access")
                .description("Full access (read, write, delete) to region: " + regionName)
                .region(regionName)
                .permissions(fullPermissions)
                .systemRole(false)
                .active(true)
                .createdAt(Instant.now())
                .createdBy("system")
                .build();
    }
}
