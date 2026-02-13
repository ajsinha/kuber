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

/**
 * Permission types for Kuber RBAC.
 * 
 * @version 2.4.0
 * @since 1.7.3
 */
public enum KuberPermission {
    /**
     * Read permission - allows GET, EXISTS, KEYS, SCAN, SEARCH operations
     */
    READ,
    
    /**
     * Write permission - allows SET, UPDATE, INCR, APPEND operations
     */
    WRITE,
    
    /**
     * Delete permission - allows DEL, FLUSH operations
     */
    DELETE,
    
    /**
     * Admin permission - full access including region creation/deletion
     */
    ADMIN;
    
    /**
     * Parse permission from string (case-insensitive)
     */
    public static KuberPermission fromString(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            return KuberPermission.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
