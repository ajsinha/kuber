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
 * Exception thrown when a write operation is attempted on a secondary/read-only node.
 */
public class ReadOnlyException extends KuberException {
    
    public ReadOnlyException() {
        super(ErrorCode.READ_ONLY, "READONLY You can't write against a read only replica");
    }
    
    public ReadOnlyException(String message) {
        super(ErrorCode.READ_ONLY, message);
    }
}
