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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Set;

/**
 * Service for authorization checks in Kuber.
 * Implements fine-grained Role-Based Access Control (RBAC).
 * 
 * Authorization rules:
 * - Admin users have full access to everything
 * - Other users need specific role permissions for each region
 * - Region creation/deletion requires admin permission
 * - User/role management requires admin permission
 * 
 * @version 1.7.4
 * @since 1.7.3
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AuthorizationService {
    
    private final KuberUserService userService;
    private final KuberRoleService roleService;
    
    /**
     * Check if the current user has admin permission
     */
    public boolean isAdmin() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated()) {
            return false;
        }
        
        String userId = auth.getName();
        return isAdmin(userId);
    }
    
    /**
     * Check if a specific user has admin permission
     */
    public boolean isAdmin(String userId) {
        Optional<KuberUser> userOpt = userService.getUser(userId);
        return userOpt.map(KuberUser::isAdmin).orElse(false);
    }
    
    /**
     * Check if the current user can read from a region
     */
    public boolean canRead(String regionName) {
        return hasPermission(regionName, KuberPermission.READ);
    }
    
    /**
     * Check if the current user can write to a region
     */
    public boolean canWrite(String regionName) {
        return hasPermission(regionName, KuberPermission.WRITE);
    }
    
    /**
     * Check if the current user can delete from a region
     */
    public boolean canDelete(String regionName) {
        return hasPermission(regionName, KuberPermission.DELETE);
    }
    
    /**
     * Check if the current user has a specific permission for a region
     */
    public boolean hasPermission(String regionName, KuberPermission permission) {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated()) {
            log.debug("No authenticated user for permission check");
            return false;
        }
        
        String userId = auth.getName();
        return hasPermission(userId, regionName, permission);
    }
    
    /**
     * Check if a specific user has a specific permission for a region
     */
    public boolean hasPermission(String userId, String regionName, KuberPermission permission) {
        Optional<KuberUser> userOpt = userService.getUser(userId);
        if (userOpt.isEmpty()) {
            log.debug("User not found: {}", userId);
            return false;
        }
        
        KuberUser user = userOpt.get();
        
        // Admin has all permissions
        if (user.isAdmin()) {
            return true;
        }
        
        // Check user's roles for the permission
        Set<String> userRoles = user.getRoles();
        for (String roleName : userRoles) {
            Optional<KuberRole> roleOpt = roleService.getRole(roleName);
            if (roleOpt.isPresent()) {
                KuberRole role = roleOpt.get();
                if (role.isActive() && role.hasPermission(regionName, permission)) {
                    return true;
                }
            }
        }
        
        log.debug("User '{}' does not have {} permission for region '{}'", userId, permission, regionName);
        return false;
    }
    
    /**
     * Check if the current user can create regions
     */
    public boolean canCreateRegion() {
        return isAdmin();
    }
    
    /**
     * Check if the current user can delete regions
     */
    public boolean canDeleteRegion() {
        return isAdmin();
    }
    
    /**
     * Check if the current user can manage users
     */
    public boolean canManageUsers() {
        return isAdmin();
    }
    
    /**
     * Check if the current user can manage roles
     */
    public boolean canManageRoles() {
        return isAdmin();
    }
    
    /**
     * Check if the current user can manage API keys
     */
    public boolean canManageApiKeys() {
        return isAdmin();
    }
    
    /**
     * Check if the current user can access admin dashboard
     */
    public boolean canAccessAdminDashboard() {
        return isAdmin();
    }
    
    /**
     * Check read permission and throw exception if denied
     */
    public void requireRead(String regionName) {
        if (!canRead(regionName)) {
            throw new AuthorizationException("Read access denied for region: " + regionName);
        }
    }
    
    /**
     * Check write permission and throw exception if denied
     */
    public void requireWrite(String regionName) {
        if (!canWrite(regionName)) {
            throw new AuthorizationException("Write access denied for region: " + regionName);
        }
    }
    
    /**
     * Check delete permission and throw exception if denied
     */
    public void requireDelete(String regionName) {
        if (!canDelete(regionName)) {
            throw new AuthorizationException("Delete access denied for region: " + regionName);
        }
    }
    
    /**
     * Check admin permission and throw exception if denied
     */
    public void requireAdmin() {
        if (!isAdmin()) {
            throw new AuthorizationException("Admin access required");
        }
    }
    
    /**
     * Get the current authenticated user ID
     */
    public String getCurrentUserId() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated()) {
            return null;
        }
        return auth.getName();
    }
    
    /**
     * Get the current authenticated user
     */
    public Optional<KuberUser> getCurrentUser() {
        String userId = getCurrentUserId();
        if (userId == null) {
            return Optional.empty();
        }
        return userService.getUser(userId);
    }
    
    /**
     * Check permission for API key authentication
     */
    public boolean hasPermissionForApiKey(ApiKey apiKey, String regionName, KuberPermission permission) {
        if (apiKey == null) {
            return false;
        }
        
        // Check if API key's user has permission
        String userId = apiKey.getUserId();
        return hasPermission(userId, regionName, permission);
    }
    
    /**
     * Get effective permissions for a user on a region
     */
    public Set<KuberPermission> getEffectivePermissions(String userId, String regionName) {
        Set<KuberPermission> permissions = new java.util.HashSet<>();
        
        Optional<KuberUser> userOpt = userService.getUser(userId);
        if (userOpt.isEmpty()) {
            return permissions;
        }
        
        KuberUser user = userOpt.get();
        
        // Admin has all permissions
        if (user.isAdmin()) {
            permissions.add(KuberPermission.READ);
            permissions.add(KuberPermission.WRITE);
            permissions.add(KuberPermission.DELETE);
            permissions.add(KuberPermission.ADMIN);
            return permissions;
        }
        
        // Collect permissions from all roles
        for (String roleName : user.getRoles()) {
            Optional<KuberRole> roleOpt = roleService.getRole(roleName);
            if (roleOpt.isPresent()) {
                KuberRole role = roleOpt.get();
                if (role.isActive() && role.appliesToRegion(regionName)) {
                    permissions.addAll(role.getPermissions());
                }
            }
        }
        
        return permissions;
    }
    
    /**
     * Authorization exception for permission denied
     */
    public static class AuthorizationException extends RuntimeException {
        public AuthorizationException(String message) {
            super(message);
        }
    }
}
