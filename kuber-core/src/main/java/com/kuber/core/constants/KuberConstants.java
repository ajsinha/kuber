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
package com.kuber.core.constants;

/**
 * Constants used throughout the Kuber system.
 */
public final class KuberConstants {
    
    private KuberConstants() {
        // Prevent instantiation
    }
    
    // Default configuration values
    public static final String DEFAULT_REGION = "default";
    public static final int DEFAULT_PORT = 6380;
    public static final int DEFAULT_HTTP_PORT = 8080;
    public static final int DEFAULT_MAX_MEMORY_ENTRIES = 100000;
    public static final int DEFAULT_DECODER_MAX_LINE_LENGTH = 1048576; // 1MB
    public static final int DEFAULT_TTL_SECONDS = -1; // No expiration
    public static final int DEFAULT_SYNC_BATCH_SIZE = 1000;
    public static final int DEFAULT_PERSISTENCE_BATCH_SIZE = 100;
    public static final int DEFAULT_PERSISTENCE_INTERVAL_MS = 1000;
    
    // ZooKeeper paths
    public static final String ZK_ROOT_PATH = "/kuber";
    public static final String ZK_LEADER_PATH = "/kuber/leader";
    public static final String ZK_NODES_PATH = "/kuber/nodes";
    public static final String ZK_CONFIG_PATH = "/kuber/config";
    
    // MongoDB collection prefixes
    public static final String MONGO_COLLECTION_PREFIX = "kuber_";
    public static final String MONGO_REGIONS_COLLECTION = "kuber_regions";
    public static final String MONGO_USERS_COLLECTION = "kuber_users";
    public static final String MONGO_CONFIG_COLLECTION = "kuber_config";
    public static final String MONGO_AUDIT_COLLECTION = "kuber_audit";
    
    // Event channels
    public static final String EVENT_CHANNEL_PREFIX = "__kuber__:";
    public static final String EVENT_CHANNEL_REGION = "__kuber__:region:";
    public static final String EVENT_CHANNEL_ENTRY = "__kuber__:entry:";
    public static final String EVENT_CHANNEL_KEY = "__kuber__:key:";
    public static final String EVENT_CHANNEL_TTL = "__kuber__:ttl:";
    public static final String EVENT_CHANNEL_SYNC = "__kuber__:sync";
    public static final String EVENT_CHANNEL_NODE = "__kuber__:node";
    
    // Protocol markers
    public static final String CRLF = "\r\n";
    public static final byte SIMPLE_STRING_PREFIX = '+';
    public static final byte ERROR_PREFIX = '-';
    public static final byte INTEGER_PREFIX = ':';
    public static final byte BULK_STRING_PREFIX = '$';
    public static final byte ARRAY_PREFIX = '*';
    
    // Session attributes
    public static final String SESSION_USER = "KUBER_USER";
    public static final String SESSION_REGION = "KUBER_CURRENT_REGION";
    public static final String SESSION_AUTHENTICATED = "KUBER_AUTHENTICATED";
    
    // HTTP headers
    public static final String HEADER_API_KEY = "X-Kuber-Api-Key";
    public static final String HEADER_REGION = "X-Kuber-Region";
    public static final String HEADER_NODE_ID = "X-Kuber-Node-Id";
    public static final String HEADER_IS_PRIMARY = "X-Kuber-Is-Primary";
    
    // Error messages
    public static final String ERR_UNKNOWN_COMMAND = "unknown command '%s'";
    public static final String ERR_WRONG_NUMBER_OF_ARGS = "wrong number of arguments for '%s' command";
    public static final String ERR_WRONG_TYPE = "WRONGTYPE Operation against a key holding the wrong kind of value";
    public static final String ERR_NOT_AUTHENTICATED = "NOAUTH Authentication required";
    public static final String ERR_READONLY = "READONLY You can't write against a read only replica";
    public static final String ERR_INVALID_TTL = "invalid expire time in '%s'";
    public static final String ERR_NO_SUCH_KEY = "no such key";
    
    // JSON path prefixes
    public static final String JSON_PATH_ROOT = "$";
    public static final String JSON_PATH_SEPARATOR = ".";
    
    // Replication states
    public static final String REPL_STATE_PRIMARY = "PRIMARY";
    public static final String REPL_STATE_SECONDARY = "SECONDARY";
    public static final String REPL_STATE_SYNCING = "SYNCING";
    public static final String REPL_STATE_STANDALONE = "STANDALONE";
    
    // Cache statistics keys
    public static final String STAT_HITS = "hits";
    public static final String STAT_MISSES = "misses";
    public static final String STAT_SETS = "sets";
    public static final String STAT_DELETES = "deletes";
    public static final String STAT_EXPIRED = "expired";
    public static final String STAT_EVICTED = "evicted";
}
