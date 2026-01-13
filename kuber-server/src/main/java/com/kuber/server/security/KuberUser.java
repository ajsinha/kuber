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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents a user in the Kuber system.
 * Implements Spring Security's UserDetails for authentication.
 * 
 * @version 1.7.8
 * @since 1.7.3
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KuberUser implements UserDetails {
    
    /**
     * Reserved admin user ID
     */
    public static final String ADMIN_USER = "admin";
    
    /**
     * Unique user identifier (username)
     */
    private String userId;
    
    /**
     * Encoded password
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private String password;
    
    /**
     * User's full name for display
     */
    private String fullName;
    
    /**
     * User's email address
     */
    private String email;
    
    /**
     * Role names assigned to this user
     */
    @Builder.Default
    private Set<String> roles = new HashSet<>();
    
    /**
     * Whether the user account is enabled
     */
    @Builder.Default
    private boolean enabled = true;
    
    /**
     * Whether the account is locked
     */
    @Builder.Default
    private boolean locked = false;
    
    /**
     * Whether this is a system user (cannot be deleted)
     */
    @Builder.Default
    private boolean systemUser = false;
    
    /**
     * When the user was created
     */
    private Instant createdAt;
    
    /**
     * Who created this user
     */
    private String createdBy;
    
    /**
     * When the user was last modified
     */
    private Instant modifiedAt;
    
    /**
     * Who last modified this user
     */
    private String modifiedBy;
    
    /**
     * When the user last logged in
     */
    private Instant lastLoginAt;
    
    /**
     * Number of failed login attempts
     */
    @Builder.Default
    private int failedLoginAttempts = 0;
    
    // ==================== UserDetails Implementation ====================
    
    @Override
    @JsonIgnore
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return roles.stream()
                .map(role -> new SimpleGrantedAuthority("ROLE_" + role.toUpperCase()))
                .collect(Collectors.toList());
    }
    
    @Override
    @JsonIgnore
    public String getUsername() {
        return userId;
    }
    
    @Override
    @JsonIgnore
    public boolean isAccountNonExpired() {
        return true; // No account expiration implemented
    }
    
    @Override
    @JsonIgnore
    public boolean isAccountNonLocked() {
        return !locked;
    }
    
    @Override
    @JsonIgnore
    public boolean isCredentialsNonExpired() {
        return true; // No credential expiration implemented
    }
    
    @Override
    public boolean isEnabled() {
        return enabled;
    }
    
    // ==================== RBAC Methods ====================
    
    /**
     * Check if this is the admin user
     */
    @JsonIgnore
    public boolean isAdmin() {
        return ADMIN_USER.equalsIgnoreCase(userId) || 
               roles.stream().anyMatch(r -> KuberRole.ADMIN_ROLE.equalsIgnoreCase(r));
    }
    
    /**
     * Check if user has a specific role
     */
    public boolean hasRole(String roleName) {
        if (isAdmin()) {
            return true; // Admin has all roles implicitly
        }
        return roles.stream().anyMatch(r -> r.equalsIgnoreCase(roleName));
    }
    
    /**
     * Add a role to this user
     */
    public void addRole(String roleName) {
        if (roles == null) {
            roles = new HashSet<>();
        }
        roles.add(roleName);
    }
    
    /**
     * Remove a role from this user
     */
    public void removeRole(String roleName) {
        if (roles != null) {
            roles.removeIf(r -> r.equalsIgnoreCase(roleName));
        }
    }
    
    /**
     * Get roles as a list (for JSON serialization compatibility)
     */
    @JsonIgnore
    public List<String> getRolesList() {
        return new ArrayList<>(roles != null ? roles : Collections.emptySet());
    }
    
    /**
     * Set roles from a list
     */
    public void setRolesList(List<String> rolesList) {
        this.roles = rolesList != null ? new HashSet<>(rolesList) : new HashSet<>();
    }
    
    /**
     * Create the admin user
     */
    public static KuberUser createAdminUser(String password) {
        Set<String> adminRoles = new HashSet<>();
        adminRoles.add(KuberRole.ADMIN_ROLE);
        
        return KuberUser.builder()
                .userId(ADMIN_USER)
                .password(password)
                .fullName("System Administrator")
                .email("admin@localhost")
                .roles(adminRoles)
                .enabled(true)
                .locked(false)
                .systemUser(true)
                .createdAt(Instant.now())
                .createdBy("system")
                .build();
    }
    
    /**
     * Create a safe copy without password for display
     */
    @JsonIgnore
    public KuberUser toSafeUser() {
        return KuberUser.builder()
                .userId(this.userId)
                .fullName(this.fullName)
                .email(this.email)
                .roles(new HashSet<>(this.roles))
                .enabled(this.enabled)
                .locked(this.locked)
                .systemUser(this.systemUser)
                .createdAt(this.createdAt)
                .createdBy(this.createdBy)
                .modifiedAt(this.modifiedAt)
                .modifiedBy(this.modifiedBy)
                .lastLoginAt(this.lastLoginAt)
                .failedLoginAttempts(this.failedLoginAttempts)
                .build();
    }
}
