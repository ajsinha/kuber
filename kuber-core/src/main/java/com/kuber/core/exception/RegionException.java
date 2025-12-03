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
package com.kuber.core.exception;

/**
 * Exception thrown when a region-related operation fails.
 */
public class RegionException extends KuberException {
    
    public RegionException(String message) {
        super(ErrorCode.NO_SUCH_REGION, message);
    }
    
    public RegionException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }
    
    public static RegionException notFound(String region) {
        return new RegionException("Region not found: " + region);
    }
    
    public static RegionException alreadyExists(String region) {
        return new RegionException(ErrorCode.REGION_EXISTS, "Region already exists: " + region);
    }
    
    public static RegionException captive(String region) {
        return new RegionException(ErrorCode.REGION_IS_CAPTIVE, 
                "Cannot delete captive region: " + region);
    }
    
    public static RegionException notEmpty(String region) {
        return new RegionException(ErrorCode.REGION_NOT_EMPTY, 
                "Region is not empty: " + region);
    }
}
