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
 * Represents a Redis protocol command with Kuber extensions.
 * Supports standard Redis commands plus Kuber-specific operations.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedisCommand implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * The command name (e.g., GET, SET, HGET, etc.)
     */
    private String command;
    
    /**
     * Command arguments
     */
    @Builder.Default
    private List<String> arguments = new ArrayList<>();
    
    /**
     * Raw binary arguments (for binary-safe operations)
     */
    @Builder.Default
    private List<byte[]> rawArguments = new ArrayList<>();
    
    /**
     * Target region (Kuber extension)
     */
    @Builder.Default
    private String region = "default";
    
    /**
     * Whether this command is a write operation
     */
    private boolean writeOperation;
    
    /**
     * Request ID for tracking
     */
    private String requestId;
    
    /**
     * Client ID that issued this command
     */
    private String clientId;
    
    /**
     * Timestamp when command was received
     */
    private long timestamp;
    
    /**
     * Get the key (first argument for most commands)
     */
    public String getKey() {
        return arguments.isEmpty() ? null : arguments.get(0);
    }
    
    /**
     * Get the value (second argument for SET-like commands)
     */
    public String getValue() {
        return arguments.size() < 2 ? null : arguments.get(1);
    }
    
    /**
     * Get argument at specified index
     */
    public String getArgument(int index) {
        return index < arguments.size() ? arguments.get(index) : null;
    }
    
    /**
     * Get argument count
     */
    public int getArgumentCount() {
        return arguments.size();
    }
    
    /**
     * Check if this is a read-only command
     */
    public boolean isReadOnly() {
        return !writeOperation;
    }
    
    /**
     * Standard Redis commands supported
     */
    public enum CommandType {
        // String commands
        GET, SET, SETNX, SETEX, PSETEX, MGET, MSET, MSETNX,
        APPEND, GETRANGE, SETRANGE, STRLEN, INCR, INCRBY, INCRBYFLOAT,
        DECR, DECRBY, GETSET, GETEX, GETDEL,
        
        // Key commands
        DEL, EXISTS, EXPIRE, EXPIREAT, PEXPIRE, PEXPIREAT,
        TTL, PTTL, PERSIST, KEYS, SCAN, TYPE, RENAME, RENAMENX,
        TOUCH, UNLINK, COPY, DUMP, RESTORE,
        
        // Hash commands
        HGET, HSET, HSETNX, HMGET, HMSET, HDEL, HEXISTS,
        HGETALL, HKEYS, HVALS, HLEN, HINCRBY, HINCRBYFLOAT, HSCAN,
        
        // List commands
        LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX,
        LSET, LINSERT, LREM, LTRIM, LPOS, LMOVE, BLPOP, BRPOP,
        
        // Set commands
        SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER,
        SDIFF, SDIFFSTORE, SINTER, SINTERSTORE, SUNION, SUNIONSTORE,
        SMOVE, SSCAN,
        
        // Sorted Set commands
        ZADD, ZREM, ZSCORE, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE,
        ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZCARD, ZCOUNT, ZINCRBY,
        ZRANGEBYLEX, ZREVRANGEBYLEX, ZLEXCOUNT, ZPOPMIN, ZPOPMAX,
        BZPOPMIN, BZPOPMAX, ZRANGESTORE, ZUNION, ZUNIONSTORE,
        ZINTER, ZINTERSTORE, ZDIFF, ZDIFFSTORE, ZSCAN, ZMSCORE,
        
        // Server commands
        PING, ECHO, INFO, DBSIZE, FLUSHDB, FLUSHALL, TIME,
        CONFIG, CLIENT, COMMAND, DEBUG, MEMORY, SLOWLOG,
        
        // Connection commands
        AUTH, SELECT, QUIT, RESET,
        
        // Pub/Sub commands
        SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE,
        PUBSUB,
        
        // Transaction commands
        MULTI, EXEC, DISCARD, WATCH, UNWATCH,
        
        // Scripting commands
        EVAL, EVALSHA, SCRIPT,
        
        // Cluster commands (for future use)
        CLUSTER, READONLY, READWRITE,
        
        // Kuber extension commands
        JSET,       // Set JSON value
        JGET,       // Get JSON value
        JQUERY,     // Query JSON using path
        JSEARCH,    // Search JSON values
        JDEL,       // Delete JSON path
        
        REGION,     // Region management
        REGIONS,    // List all regions
        RCREATE,    // Create region
        RDROP,      // Drop region
        RPURGE,     // Purge region
        RINFO,      // Region info
        RSELECT,    // Select region
        
        SYNC,       // Sync with primary
        STATUS,     // Node status
        REPLINFO,   // Replication info
        
        KSEARCH,    // Search keys by regex and return key-value pairs
        
        SUBSCRIBE_EVENT,    // Subscribe to events
        UNSUBSCRIBE_EVENT,  // Unsubscribe from events
        
        UNKNOWN     // Unknown command
    }
    
    /**
     * Parse command type from string
     */
    public static CommandType parseCommandType(String cmd) {
        if (cmd == null || cmd.isEmpty()) {
            return CommandType.UNKNOWN;
        }
        try {
            return CommandType.valueOf(cmd.toUpperCase());
        } catch (IllegalArgumentException e) {
            return CommandType.UNKNOWN;
        }
    }
    
    /**
     * Check if command type is a write operation
     */
    public static boolean isWriteCommand(CommandType type) {
        switch (type) {
            case GET, MGET, HGET, HMGET, HGETALL, HKEYS, HVALS, HEXISTS, HLEN,
                 LLEN, LRANGE, LINDEX, SMEMBERS, SISMEMBER, SCARD, SRANDMEMBER,
                 ZSCORE, ZRANK, ZREVRANK, ZRANGE, ZREVRANGE, ZCARD, ZCOUNT,
                 PING, ECHO, INFO, DBSIZE, TTL, PTTL, TYPE, EXISTS, KEYS,
                 SCAN, HSCAN, SSCAN, ZSCAN, TIME, CONFIG, CLIENT, COMMAND,
                 MEMORY, SLOWLOG, JGET, JQUERY, JSEARCH, REGIONS, RINFO,
                 STATUS, REPLINFO, STRLEN, GETRANGE, LPOS, DEBUG, KSEARCH:
                return false;
            default:
                return true;
        }
    }
    
    /**
     * Create a simple command
     */
    public static RedisCommand of(String command, String... args) {
        RedisCommand cmd = new RedisCommand();
        cmd.setCommand(command);
        cmd.setArguments(new ArrayList<>(List.of(args)));
        cmd.setWriteOperation(isWriteCommand(parseCommandType(command)));
        cmd.setTimestamp(System.currentTimeMillis());
        return cmd;
    }
}
