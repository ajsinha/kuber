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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * Represents an API Key for programmatic access to Kuber.
 * API keys can be used by REST clients, Python clients, and Java clients
 * as an alternative to username/password authentication.
 *
 * @version 2.4.0
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ApiKey {
    
    /**
     * Unique identifier for the API key (for display/management)
     */
    private String keyId;
    
    /**
     * The actual API key value (secret)
     */
    private String keyValue;
    
    /**
     * Human-readable name/description for the key
     */
    private String name;
    
    /**
     * User ID this key is associated with (determines permissions)
     */
    private String userId;
    
    /**
     * Roles assigned to this API key
     */
    private List<String> roles;
    
    /**
     * When the key was created
     */
    private Instant createdAt;
    
    /**
     * When the key was last used (null if never used)
     */
    private Instant lastUsedAt;
    
    /**
     * Optional expiration time (null means no expiration)
     */
    private Instant expiresAt;
    
    /**
     * Whether the key is currently active
     */
    private boolean active;
    
    /**
     * Check if the key has expired.
     * Not serialized to JSON - computed property.
     */
    @JsonIgnore
    public boolean isExpired() {
        if (expiresAt == null) {
            return false;
        }
        return Instant.now().isAfter(expiresAt);
    }
    
    /**
     * Check if the key is valid (active and not expired).
     * Not serialized to JSON - computed property.
     */
    @JsonIgnore
    public boolean isValid() {
        return active && !isExpired();
    }
}
