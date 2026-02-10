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
package com.kuber.server.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Represents a cache response sent via message broker.
 * 
 * <p>Response format:
 * <pre>
 * {
 *   "request_receive_timestamp": "ISO timestamp when request was received",
 *   "response_time": "ISO timestamp when response is sent",
 *   "processing_time_ms": 123,
 *   "request": { original request JSON },
 *   "response": {
 *     "success": true/false,
 *     "result": operation result,
 *     "error": error message if failed
 *   }
 * }
 * </pre>
 * 
 * @version 2.1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class CacheResponse {
    
    private static final DateTimeFormatter ISO_FORMATTER = 
        DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("UTC"));
    
    /**
     * ISO timestamp when request was received from broker.
     */
    @JsonProperty("request_receive_timestamp")
    private String requestReceiveTimestamp;
    
    /**
     * ISO timestamp when response is placed on response topic.
     */
    @JsonProperty("response_time")
    private String responseTime;
    
    /**
     * Processing time in milliseconds.
     */
    @JsonProperty("processing_time_ms")
    private long processingTimeMs;
    
    /**
     * Original request as received (for correlation).
     */
    private CacheRequest request;
    
    /**
     * Response payload.
     */
    private ResponsePayload response;
    
    /**
     * Response payload containing result or error.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ResponsePayload {
        
        /**
         * Whether the operation succeeded.
         */
        private boolean success;
        
        /**
         * Result of the operation (type depends on operation).
         */
        private Object result;
        
        /**
         * Error message if operation failed (empty string by default).
         */
        @Builder.Default
        private String error = "";
        
        /**
         * Error code if operation failed.
         */
        @JsonProperty("error_code")
        private String errorCode;
        
        /**
         * Server message for informational purposes (e.g., result truncation warnings).
         * Empty string by default.
         */
        @JsonProperty("server_message")
        @Builder.Default
        private String serverMessage = "";
        
        /**
         * Total count of matching items (for search operations where results are truncated).
         */
        @JsonProperty("total_count")
        private Integer totalCount;
        
        /**
         * Count of items returned in this response.
         */
        @JsonProperty("returned_count")
        private Integer returnedCount;
        
        /**
         * Create a success response.
         */
        public static ResponsePayload success(Object result) {
            return ResponsePayload.builder()
                .success(true)
                .result(result)
                .error("")
                .serverMessage("")
                .build();
        }
        
        /**
         * Create a success response with server message.
         */
        public static ResponsePayload successWithMessage(Object result, String serverMessage) {
            return ResponsePayload.builder()
                .success(true)
                .result(result)
                .error("")
                .serverMessage(serverMessage != null ? serverMessage : "")
                .build();
        }
        
        /**
         * Create a success response with count information (for search results).
         */
        public static ResponsePayload successWithCounts(Object result, int totalCount, int returnedCount, String serverMessage) {
            return ResponsePayload.builder()
                .success(true)
                .result(result)
                .error("")
                .serverMessage(serverMessage != null ? serverMessage : "")
                .totalCount(totalCount)
                .returnedCount(returnedCount)
                .build();
        }
        
        /**
         * Create an error response.
         */
        public static ResponsePayload error(String errorCode, String error) {
            return ResponsePayload.builder()
                .success(false)
                .errorCode(errorCode)
                .error(error != null ? error : "")
                .serverMessage("")
                .build();
        }
    }
    
    /**
     * Create a response for a successful operation.
     */
    public static CacheResponse success(CacheRequest request, Object result, 
                                         Instant receiveTime, Instant sendTime) {
        return CacheResponse.builder()
            .requestReceiveTimestamp(ISO_FORMATTER.format(receiveTime))
            .responseTime(ISO_FORMATTER.format(sendTime))
            .processingTimeMs(sendTime.toEpochMilli() - receiveTime.toEpochMilli())
            .request(request)
            .response(ResponsePayload.success(result))
            .build();
    }
    
    /**
     * Create a response for a successful operation with server message.
     */
    public static CacheResponse successWithMessage(CacheRequest request, Object result, 
                                                    String serverMessage,
                                                    Instant receiveTime, Instant sendTime) {
        return CacheResponse.builder()
            .requestReceiveTimestamp(ISO_FORMATTER.format(receiveTime))
            .responseTime(ISO_FORMATTER.format(sendTime))
            .processingTimeMs(sendTime.toEpochMilli() - receiveTime.toEpochMilli())
            .request(request)
            .response(ResponsePayload.successWithMessage(result, serverMessage))
            .build();
    }
    
    /**
     * Create a response for search results with count information.
     */
    public static CacheResponse successWithCounts(CacheRequest request, Object result,
                                                   int totalCount, int returnedCount,
                                                   String serverMessage,
                                                   Instant receiveTime, Instant sendTime) {
        return CacheResponse.builder()
            .requestReceiveTimestamp(ISO_FORMATTER.format(receiveTime))
            .responseTime(ISO_FORMATTER.format(sendTime))
            .processingTimeMs(sendTime.toEpochMilli() - receiveTime.toEpochMilli())
            .request(request)
            .response(ResponsePayload.successWithCounts(result, totalCount, returnedCount, serverMessage))
            .build();
    }
    
    /**
     * Create a response for a failed operation.
     */
    public static CacheResponse error(CacheRequest request, String errorCode, String error,
                                       Instant receiveTime, Instant sendTime) {
        return CacheResponse.builder()
            .requestReceiveTimestamp(ISO_FORMATTER.format(receiveTime))
            .responseTime(ISO_FORMATTER.format(sendTime))
            .processingTimeMs(sendTime.toEpochMilli() - receiveTime.toEpochMilli())
            .request(request)
            .response(ResponsePayload.error(errorCode, error))
            .build();
    }
    
    /**
     * Error codes.
     */
    public static class ErrorCode {
        public static final String INVALID_REQUEST = "INVALID_REQUEST";
        public static final String AUTHENTICATION_FAILED = "AUTHENTICATION_FAILED";
        public static final String OPERATION_NOT_SUPPORTED = "OPERATION_NOT_SUPPORTED";
        public static final String KEY_NOT_FOUND = "KEY_NOT_FOUND";
        public static final String REGION_NOT_FOUND = "REGION_NOT_FOUND";
        public static final String INTERNAL_ERROR = "INTERNAL_ERROR";
        public static final String PERMISSION_DENIED = "PERMISSION_DENIED";
        public static final String PARSE_ERROR = "PARSE_ERROR";
        public static final String BATCH_LIMIT_EXCEEDED = "BATCH_LIMIT_EXCEEDED";
        public static final String SEARCH_LIMIT_EXCEEDED = "SEARCH_LIMIT_EXCEEDED";
    }
}
