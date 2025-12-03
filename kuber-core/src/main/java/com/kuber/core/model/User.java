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
package com.kuber.core.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a user in the Kuber system.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class User implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Unique user ID
     */
    private String id;
    
    /**
     * Username for login
     */
    private String username;
    
    /**
     * Password hash (never exposed in JSON)
     */
    @JsonIgnore
    private String passwordHash;
    
    /**
     * Email address
     */
    private String email;
    
    /**
     * Display name
     */
    private String displayName;
    
    /**
     * User roles
     */
    @Builder.Default
    private Set<Role> roles = new HashSet<>();
    
    /**
     * Whether the user is active
     */
    @Builder.Default
    private boolean active = true;
    
    /**
     * Whether the user is locked out
     */
    @Builder.Default
    private boolean locked = false;
    
    /**
     * Number of failed login attempts
     */
    @Builder.Default
    private int failedLoginAttempts = 0;
    
    /**
     * Last login timestamp
     */
    private Instant lastLogin;
    
    /**
     * Account creation timestamp
     */
    private Instant createdAt;
    
    /**
     * Last update timestamp
     */
    private Instant updatedAt;
    
    /**
     * Regions this user has access to (empty means all)
     */
    @Builder.Default
    private Set<String> allowedRegions = new HashSet<>();
    
    /**
     * API key for programmatic access
     */
    @JsonIgnore
    private String apiKey;
    
    /**
     * User roles
     */
    public enum Role {
        ADMIN,      // Full access
        OPERATOR,   // Can manage regions and entries
        USER,       // Can read and write entries
        READONLY    // Can only read entries
    }
    
    /**
     * Check if user has admin role
     */
    public boolean isAdmin() {
        return roles.contains(Role.ADMIN);
    }
    
    /**
     * Check if user has operator role
     */
    public boolean isOperator() {
        return roles.contains(Role.ADMIN) || roles.contains(Role.OPERATOR);
    }
    
    /**
     * Check if user can write
     */
    public boolean canWrite() {
        return roles.contains(Role.ADMIN) || 
               roles.contains(Role.OPERATOR) || 
               roles.contains(Role.USER);
    }
    
    /**
     * Check if user has access to a region
     */
    public boolean hasAccessToRegion(String region) {
        if (isAdmin()) {
            return true;
        }
        if (allowedRegions.isEmpty()) {
            return true;
        }
        return allowedRegions.contains(region);
    }
    
    /**
     * Record a failed login attempt
     */
    public void recordFailedLogin() {
        this.failedLoginAttempts++;
        if (this.failedLoginAttempts >= 5) {
            this.locked = true;
        }
    }
    
    /**
     * Record a successful login
     */
    public void recordSuccessfulLogin() {
        this.failedLoginAttempts = 0;
        this.locked = false;
        this.lastLogin = Instant.now();
    }
    
    /**
     * Create a default admin user
     */
    public static User createDefaultAdmin(String passwordHash) {
        return User.builder()
                .id("admin")
                .username("admin")
                .passwordHash(passwordHash)
                .email("admin@kuber.local")
                .displayName("Administrator")
                .roles(Set.of(Role.ADMIN))
                .active(true)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
    }
}
