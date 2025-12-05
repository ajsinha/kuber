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
package com.kuber.server.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Configuration properties for Kuber distributed cache.
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "kuber")
@Validated
public class KuberProperties {
    
    /**
     * Unique node identifier
     */
    private String nodeId = java.util.UUID.randomUUID().toString().substring(0, 8);
    
    /**
     * Network configuration
     */
    private Network network = new Network();
    
    /**
     * Cache configuration
     */
    private Cache cache = new Cache();
    
    /**
     * MongoDB configuration
     */
    private Mongo mongo = new Mongo();
    
    /**
     * Persistence configuration
     */
    private Persistence persistence = new Persistence();
    
    /**
     * ZooKeeper configuration
     */
    private Zookeeper zookeeper = new Zookeeper();
    
    /**
     * Replication configuration
     */
    private Replication replication = new Replication();
    
    /**
     * Security configuration
     */
    private Security security = new Security();
    
    /**
     * Autoload configuration
     */
    private Autoload autoload = new Autoload();
    
    @Data
    public static class Network {
        /**
         * Redis protocol port
         */
        @Min(1)
        private int port = 6380;
        
        /**
         * Bind address
         */
        private String bindAddress = "0.0.0.0";
        
        /**
         * Maximum line length for text decoder
         */
        @Min(1024)
        private int decoderMaxLineLength = 1048576; // 1MB
        
        /**
         * Connection timeout in milliseconds
         */
        @Min(1000)
        private int connectionTimeoutMs = 30000;
        
        /**
         * Read timeout in milliseconds
         */
        @Min(1000)
        private int readTimeoutMs = 30000;
        
        /**
         * Write buffer size
         */
        @Min(1024)
        private int writeBufferSize = 65536;
        
        /**
         * Read buffer size
         */
        @Min(1024)
        private int readBufferSize = 65536;
        
        /**
         * Maximum number of concurrent connections
         */
        @Min(1)
        private int maxConnections = 10000;
        
        /**
         * IO processor count (0 = auto-detect)
         */
        @Min(0)
        private int ioProcessorCount = 0;
    }
    
    @Data
    public static class Cache {
        /**
         * Default maximum number of entries to keep in memory per region.
         * This is used when no region-specific limit is configured.
         */
        @Min(1000)
        private int maxMemoryEntries = 100000;
        
        /**
         * Global maximum number of entries across ALL regions combined.
         * When set (> 0), the total in-memory entries across all regions
         * will not exceed this value. Memory is allocated proportionally
         * based on each region's data size and configured limits.
         * Set to 0 to disable global limiting (each region uses its own limit).
         */
        @Min(0)
        private int globalMaxMemoryEntries = 0;
        
        /**
         * Per-region memory limits. Keys are region names, values are max entries.
         * If a region is not specified here, maxMemoryEntries is used as default.
         * Example: { "customers": 50000, "products": 200000 }
         */
        private java.util.Map<String, Integer> regionMemoryLimits = new java.util.HashMap<>();
        
        /**
         * Whether to use persistent mode (sync writes to MongoDB)
         */
        private boolean persistentMode = false;
        
        /**
         * Batch size for async persistence
         */
        @Min(1)
        private int persistenceBatchSize = 100;
        
        /**
         * Interval for async persistence in milliseconds
         */
        @Min(100)
        private int persistenceIntervalMs = 1000;
        
        /**
         * Default TTL in seconds (-1 for no expiration)
         */
        private long defaultTtlSeconds = -1;
        
        /**
         * Eviction policy: LRU, LFU, FIFO
         */
        private String evictionPolicy = "LRU";
        
        /**
         * TTL cleanup interval in seconds
         */
        @Min(1)
        private int ttlCleanupIntervalSeconds = 60;
        
        /**
         * Enable statistics collection
         */
        private boolean enableStatistics = true;
        
        /**
         * Get the memory limit for a specific region.
         * Returns the region-specific limit if configured, otherwise the default.
         */
        public int getMemoryLimitForRegion(String regionName) {
            return regionMemoryLimits.getOrDefault(regionName, maxMemoryEntries);
        }
        
        // ==================== Memory Management ====================
        
        /**
         * Enable automated memory management watcher.
         * When enabled, heap usage is monitored and cache entries are
         * evicted to persistence when memory exceeds the high watermark.
         */
        private boolean memoryWatcherEnabled = true;
        
        /**
         * High watermark percentage for heap memory.
         * When heap usage exceeds this threshold, eviction begins.
         */
        @Min(50)
        @Max(95)
        private int memoryHighWatermarkPercent = 85;
        
        /**
         * Low watermark percentage for heap memory.
         * Eviction continues until heap usage drops below this threshold.
         */
        @Min(20)
        @Max(80)
        private int memoryLowWatermarkPercent = 50;
        
        /**
         * Number of entries to evict per batch during memory pressure.
         */
        @Min(100)
        private int memoryEvictionBatchSize = 1000;
        
        /**
         * Interval in milliseconds for memory watcher checks.
         */
        @Min(1000)
        private int memoryWatcherIntervalMs = 5000;
    }
    
    @Data
    public static class Mongo {
        /**
         * MongoDB connection URI
         */
        @NotBlank
        private String uri = "mongodb://localhost:27017";
        
        /**
         * Database name
         */
        @NotBlank
        private String database = "kuber";
        
        /**
         * Connection pool size
         */
        @Min(1)
        private int connectionPoolSize = 50;
        
        /**
         * Connection timeout in milliseconds
         */
        @Min(1000)
        private int connectionTimeoutMs = 10000;
        
        /**
         * Socket timeout in milliseconds
         */
        @Min(1000)
        private int socketTimeoutMs = 30000;
        
        /**
         * Enable write concern acknowledgment
         */
        private boolean writeConcernAcknowledged = true;
    }
    
    @Data
    public static class Zookeeper {
        /**
         * Whether ZooKeeper is enabled
         */
        private boolean enabled = false;
        
        /**
         * ZooKeeper connection string
         */
        private String connectString = "localhost:2181";
        
        /**
         * Session timeout in milliseconds
         */
        @Min(1000)
        private int sessionTimeoutMs = 30000;
        
        /**
         * Connection timeout in milliseconds
         */
        @Min(1000)
        private int connectionTimeoutMs = 15000;
        
        /**
         * Base path for Kuber nodes
         */
        private String basePath = "/kuber";
        
        /**
         * Retry policy - initial sleep time
         */
        @Min(100)
        private int retryBaseSleepMs = 1000;
        
        /**
         * Retry policy - max retries
         */
        @Min(1)
        private int retryMaxAttempts = 3;
    }
    
    @Data
    public static class Replication {
        /**
         * Batch size for sync operations
         */
        @Min(1)
        private int syncBatchSize = 1000;
        
        /**
         * Sync timeout in milliseconds
         */
        @Min(1000)
        private int syncTimeoutMs = 60000;
        
        /**
         * Heartbeat interval in milliseconds
         */
        @Min(1000)
        private int heartbeatIntervalMs = 5000;
        
        /**
         * Primary check interval in milliseconds
         */
        @Min(1000)
        private int primaryCheckIntervalMs = 10000;
    }
    
    @Data
    public static class Security {
        /**
         * Path to users.json file for authentication
         */
        private String usersFile = "classpath:users.json";
        
        /**
         * Session timeout in minutes
         */
        @Min(1)
        private int sessionTimeoutMinutes = 30;
        
        /**
         * Password for Redis AUTH command (empty = no auth)
         */
        private String redisPassword = null;
    }
    
    @Data
    public static class Autoload {
        /**
         * Whether autoload is enabled
         */
        private boolean enabled = true;
        
        /**
         * Base directory for autoload (contains inbox and outbox subfolders)
         */
        private String directory = "./autoload";
        
        /**
         * Scan interval in seconds
         */
        @Min(10)
        private int scanIntervalSeconds = 60;
        
        /**
         * Maximum records to process per file (0 = unlimited)
         */
        @Min(0)
        private int maxRecordsPerFile = 0;
        
        /**
         * Whether to create directories if they don't exist
         */
        private boolean createDirectories = true;
        
        /**
         * File encoding
         */
        private String fileEncoding = "UTF-8";
    }
    
    @Data
    public static class Persistence {
        /**
         * Persistence store type: mongodb, sqlite, postgresql, rocksdb, lmdb, memory
         * Default: rocksdb (no external dependencies required)
         */
        @NotBlank
        private String type = "rocksdb";
        
        /**
         * SQLite configuration
         */
        private Sqlite sqlite = new Sqlite();
        
        /**
         * PostgreSQL configuration
         */
        private Postgresql postgresql = new Postgresql();
        
        /**
         * RocksDB configuration
         */
        private Rocksdb rocksdb = new Rocksdb();
        
        /**
         * LMDB configuration
         */
        private Lmdb lmdb = new Lmdb();
    }
    
    @Data
    public static class Sqlite {
        /**
         * Path to SQLite database file
         */
        private String path = "./data/kuber.db";
    }
    
    @Data
    public static class Postgresql {
        /**
         * PostgreSQL JDBC URL
         */
        private String url = "jdbc:postgresql://localhost:5432/kuber";
        
        /**
         * Database username
         */
        private String username = "kuber";
        
        /**
         * Database password
         */
        private String password = "kuber";
        
        /**
         * Connection pool size
         */
        @Min(1)
        private int poolSize = 10;
        
        /**
         * Minimum idle connections
         */
        @Min(1)
        private int minIdle = 2;
        
        /**
         * Connection timeout in milliseconds
         */
        @Min(1000)
        private long connectionTimeoutMs = 30000;
        
        /**
         * Idle timeout in milliseconds
         */
        @Min(10000)
        private long idleTimeoutMs = 600000;
        
        /**
         * Maximum connection lifetime in milliseconds
         */
        @Min(30000)
        private long maxLifetimeMs = 1800000;
    }
    
    @Data
    public static class Rocksdb {
        /**
         * Path to RocksDB database directory
         */
        private String path = "./data/rocksdb";
        
        /**
         * Enable automatic compaction to reclaim disk space
         */
        private boolean compactionEnabled = true;
        
        /**
         * Cron expression for scheduled compaction.
         * Default: "0 0 2 * * ?" runs at 2:00 AM daily.
         * 
         * Format: second minute hour day-of-month month day-of-week
         * Examples:
         *   "0 0 2 * * ?"     - 2:00 AM daily
         *   "0 0 3 * * SUN"   - 3:00 AM every Sunday
         *   "0 0 1,13 * * ?"  - 1:00 AM and 1:00 PM daily
         *   "0 0 * * * ?"     - Every hour
         */
        private String compactionCron = "0 0 2 * * ?";
    }
    
    @Data
    public static class Lmdb {
        /**
         * Path to LMDB database directory
         */
        private String path = "./data/lmdb";
        
        /**
         * Maximum database size (map size) in bytes.
         * LMDB memory-maps the entire database, so this sets the upper limit.
         * Default: 1GB. Increase for larger datasets.
         * 
         * Common values:
         *   1GB  = 1073741824
         *   2GB  = 2147483648
         *   4GB  = 4294967296
         *   8GB  = 8589934592
         *   16GB = 17179869184
         */
        private long mapSize = 1073741824L; // 1GB
        
        /**
         * Whether to sync writes immediately.
         * true = safer but slower (MDB_NOSYNC disabled)
         * false = faster but risk of data loss on crash (MDB_NOSYNC enabled)
         */
        private boolean syncWrites = true;
    }
}
