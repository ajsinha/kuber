/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.core.exception;

/**
 * Base exception for all Kuber-related errors.
 */
public class KuberException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;
    
    private final ErrorCode errorCode;
    
    public KuberException(String message) {
        super(message);
        this.errorCode = ErrorCode.GENERAL_ERROR;
    }
    
    public KuberException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = ErrorCode.GENERAL_ERROR;
    }
    
    public KuberException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }
    
    public KuberException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }
    
    public ErrorCode getErrorCode() {
        return errorCode;
    }
    
    /**
     * Error codes for Kuber operations
     */
    public enum ErrorCode {
        GENERAL_ERROR("ERR"),
        WRONG_TYPE("WRONGTYPE"),
        NO_SUCH_KEY("NOKEY"),
        NO_SUCH_REGION("NOREGION"),
        REGION_EXISTS("REGIONEXISTS"),
        REGION_NOT_EMPTY("REGIONNOTEMPTY"),
        REGION_IS_CAPTIVE("REGIONCAPTIVE"),
        INVALID_COMMAND("INVALIDCMD"),
        INVALID_ARGUMENT("INVALIDARG"),
        NOT_AUTHORIZED("NOAUTH"),
        READ_ONLY("READONLY"),
        SYNC_ERROR("SYNCERR"),
        PERSISTENCE_ERROR("PERSISTENCEERR"),
        TIMEOUT("TIMEOUT"),
        CONNECTION_ERROR("CONNERR"),
        PROTOCOL_ERROR("PROTOERR"),
        JSON_PARSE_ERROR("JSONERR"),
        JSON_PATH_ERROR("JSONPATHERR"),
        TTL_ERROR("TTLERR"),
        MEMORY_LIMIT("MEMLIMIT"),
        REPLICATION_ERROR("REPLERR"),
        ZOOKEEPER_ERROR("ZKERR");
        
        private final String prefix;
        
        ErrorCode(String prefix) {
            this.prefix = prefix;
        }
        
        public String getPrefix() {
            return prefix;
        }
    }
    
    /**
     * Get Redis-compatible error message
     */
    public String getRedisErrorMessage() {
        return errorCode.getPrefix() + " " + getMessage();
    }
}
