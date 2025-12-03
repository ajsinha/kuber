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
package com.kuber.core.protocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a Redis protocol response.
 * Follows RESP (Redis Serialization Protocol) format.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedisResponse implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * Response type following RESP protocol
     */
    private ResponseType type;
    
    /**
     * Simple string or error message
     */
    private String message;
    
    /**
     * Integer value for integer responses
     */
    private Long intValue;
    
    /**
     * Bulk string value
     */
    private String bulkString;
    
    /**
     * Raw bulk bytes
     */
    private byte[] bulkBytes;
    
    /**
     * Array elements for array responses
     */
    @Builder.Default
    private List<RedisResponse> arrayElements = new ArrayList<>();
    
    /**
     * Whether this is a null response
     */
    @Builder.Default
    private boolean nullValue = false;
    
    /**
     * Request ID for tracking
     */
    private String requestId;
    
    /**
     * Response type enumeration
     */
    public enum ResponseType {
        SIMPLE_STRING,  // +
        ERROR,          // -
        INTEGER,        // :
        BULK_STRING,    // $
        ARRAY,          // *
        NULL            // Null bulk string or null array
    }
    
    // Factory methods for common responses
    
    /**
     * Create an OK response
     */
    public static RedisResponse ok() {
        return RedisResponse.builder()
                .type(ResponseType.SIMPLE_STRING)
                .message("OK")
                .build();
    }
    
    /**
     * Create a PONG response
     */
    public static RedisResponse pong() {
        return RedisResponse.builder()
                .type(ResponseType.SIMPLE_STRING)
                .message("PONG")
                .build();
    }
    
    /**
     * Create a simple string response
     */
    public static RedisResponse simpleString(String message) {
        return RedisResponse.builder()
                .type(ResponseType.SIMPLE_STRING)
                .message(message)
                .build();
    }
    
    /**
     * Create an error response
     */
    public static RedisResponse error(String message) {
        return RedisResponse.builder()
                .type(ResponseType.ERROR)
                .message(message)
                .build();
    }
    
    /**
     * Create an integer response
     */
    public static RedisResponse integer(long value) {
        return RedisResponse.builder()
                .type(ResponseType.INTEGER)
                .intValue(value)
                .build();
    }
    
    /**
     * Create a bulk string response
     */
    public static RedisResponse bulkString(String value) {
        if (value == null) {
            return nullBulkString();
        }
        return RedisResponse.builder()
                .type(ResponseType.BULK_STRING)
                .bulkString(value)
                .build();
    }
    
    /**
     * Create a bulk bytes response
     */
    public static RedisResponse bulkBytes(byte[] bytes) {
        if (bytes == null) {
            return nullBulkString();
        }
        return RedisResponse.builder()
                .type(ResponseType.BULK_STRING)
                .bulkBytes(bytes)
                .build();
    }
    
    /**
     * Create a null bulk string response
     */
    public static RedisResponse nullBulkString() {
        return RedisResponse.builder()
                .type(ResponseType.NULL)
                .nullValue(true)
                .build();
    }
    
    /**
     * Create an array response
     */
    public static RedisResponse array(List<RedisResponse> elements) {
        if (elements == null) {
            return nullArray();
        }
        return RedisResponse.builder()
                .type(ResponseType.ARRAY)
                .arrayElements(elements)
                .build();
    }
    
    /**
     * Create an array from strings
     */
    public static RedisResponse arrayFromStrings(List<String> strings) {
        if (strings == null) {
            return nullArray();
        }
        List<RedisResponse> elements = new ArrayList<>();
        for (String s : strings) {
            elements.add(bulkString(s));
        }
        return array(elements);
    }
    
    /**
     * Create a null array response
     */
    public static RedisResponse nullArray() {
        return RedisResponse.builder()
                .type(ResponseType.NULL)
                .nullValue(true)
                .build();
    }
    
    /**
     * Create a QUEUED response for transactions
     */
    public static RedisResponse queued() {
        return simpleString("QUEUED");
    }
    
    /**
     * Encode this response to RESP format
     */
    public String encode() {
        StringBuilder sb = new StringBuilder();
        encode(sb);
        return sb.toString();
    }
    
    /**
     * Encode this response to RESP format into a StringBuilder
     */
    public void encode(StringBuilder sb) {
        switch (type) {
            case SIMPLE_STRING:
                sb.append('+').append(message).append("\r\n");
                break;
                
            case ERROR:
                sb.append('-').append(message).append("\r\n");
                break;
                
            case INTEGER:
                sb.append(':').append(intValue).append("\r\n");
                break;
                
            case BULK_STRING:
                if (bulkString != null) {
                    sb.append('$').append(bulkString.length()).append("\r\n");
                    sb.append(bulkString).append("\r\n");
                } else if (bulkBytes != null) {
                    sb.append('$').append(bulkBytes.length).append("\r\n");
                    sb.append(new String(bulkBytes)).append("\r\n");
                } else {
                    sb.append("$-1\r\n");
                }
                break;
                
            case ARRAY:
                if (arrayElements != null) {
                    sb.append('*').append(arrayElements.size()).append("\r\n");
                    for (RedisResponse element : arrayElements) {
                        element.encode(sb);
                    }
                } else {
                    sb.append("*-1\r\n");
                }
                break;
                
            case NULL:
                sb.append("$-1\r\n");
                break;
        }
    }
    
    /**
     * Encode bulk string with proper handling for strings containing newlines, tabs, etc.
     */
    public static String encodeBulkStringSafe(String value) {
        if (value == null) {
            return "$-1\r\n";
        }
        byte[] bytes = value.getBytes();
        return "$" + bytes.length + "\r\n" + value + "\r\n";
    }
}
