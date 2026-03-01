/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration properties for Kuber distributed cache.
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "kuber")
@Validated
public class KuberProperties {
    
    /**
     * Base data directory for all Kuber data files.
     * All relative paths for persistence, autoload, backup, etc. are resolved from this directory.
     * Default: ./kuberdata
     * Override with -Dkuber.base.datadir=/path/to/data or environment variable KUBER_BASE_DATADIR
     */
    private Base base = new Base();
    
    /**
     * Secure folder for sensitive configuration files (users.json, apikeys.json).
     * This folder will be created automatically if it doesn't exist.
     * Default: ./secure
     * @since 1.6.5
     */
    private Secure secure = new Secure();
    
    /**
     * Request/Response messaging configuration.
     * @since 2.2.0
     */
    private Messaging messaging = new Messaging();
    
    /**
     * Application version.
     * Read from application.properties: kuber.version
     * Used for logging, API responses, and UI display.
     * @since 1.6.1
     */
    private String version = "2.6.3";
    
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
    
    /**
     * Backup and restore configuration
     */
    private Backup backup = new Backup();
    
    /**
     * Shutdown configuration
     */
    private Shutdown shutdown = new Shutdown();
    
    /**
     * Secondary indexing configuration including file watcher settings.
     * @since 1.9.0
     */
    private Indexing indexing = new Indexing();
    
    /**
     * Prometheus metrics configuration
     * @since 1.7.9
     */
    private Prometheus prometheus = new Prometheus();

    /**
     * JSON search configuration including parallel search settings.
     * @since 1.7.9
     */
    private Search search = new Search();
    
    @Data
    public static class Base {
        /**
         * Base data directory for all Kuber data files.
         * All relative paths for persistence, autoload, backup, etc. are resolved from this directory.
         * Default: ./kuberdata
         */
        private String datadir = "./kuberdata";
    }
    
    /**
     * Secure folder configuration for sensitive files.
     * @since 1.6.5
     */
    @Data
    public static class Secure {
        /**
         * Folder for sensitive configuration files (users.json, apikeys.json).
         * This folder will be created automatically if it doesn't exist.
         * Default: ./secure
         */
        private String folder = "./secure";
    }
    
    /**
     * Request/Response Messaging configuration.
     * @since 2.2.0
     */
    @Data
    public static class Messaging {
        /**
         * Path to the request/response messaging configuration JSON file.
         * Contains broker definitions, topic subscriptions, and service settings.
         * The file is hot-reloaded when modified — changes take effect without restart.
         * Default: config/request_response.json
         * @since 2.2.0
         */
        private String requestResponseConfigFile = "config/request_response.json";
    }
    
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
         * Connection/session idle timeout in milliseconds.
         * Sessions are closed after this period of inactivity.
         * Can be adjusted per-session using CLIENT SETTIMEOUT command.
         * Set to 0 to disable timeout (not recommended).
         */
        @Min(0)
        private int connectionTimeoutMs = 300000; // 5 minutes
        
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
         * Maximum allowed key length in bytes.
         * Keys exceeding this length will be rejected with an error.
         * The key and value will be logged for debugging purposes.
         * 
         * This limit applies to ALL persistence stores (LMDB, RocksDB, MongoDB, etc.)
         * and prevents issues with database-specific key length limits.
         * 
         * Default: 1024 bytes (sufficient for most use cases including longer keys)
         * Maximum recommended: 4096 bytes (for compatibility with all backends)
         */
        @Min(1)
        @Max(65536)
        private int maxKeyLengthBytes = 1024;
        
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
         * Use off-heap (direct memory) for key index storage.
         * When enabled, keys are stored in DRAM outside the Java heap, providing:
         * - Zero GC pressure for key storage
         * - Better scalability for millions of keys
         * - No GC pauses affecting key lookups
         * 
         * Default: false (use on-heap ConcurrentHashMap)
         */
        private boolean offHeapKeyIndex = false;
        
        /**
         * Initial size in MB for off-heap key index per region.
         * Only used when offHeapKeyIndex is enabled.
         * The buffer grows automatically up to offHeapKeyIndexMaxSizeMb.
         */
        @Min(1)
        private int offHeapKeyIndexInitialSizeMb = 16;
        
        /**
         * Maximum size in MB for off-heap key index per region.
         * Only used when offHeapKeyIndex is enabled.
         */
        @Min(16)
        private int offHeapKeyIndexMaxSizeMb = 1024;
        
        // ==================== Factory Configuration ====================
        
        /**
         * Cache implementation to use for value caches.
         * Supports: CAFFEINE (default), GUAVA, EHCACHE.
         * The cache implementation can be changed without modifying code.
         * 
         * @since 1.5.0
         */
        private String cacheImplementation = "CAFFEINE";
        
        /**
         * Collections implementation to use for internal collections (Map, List, Set, Queue, Deque).
         * Supports: DEFAULT (uses Java concurrent collections).
         * The collections implementation can be changed without modifying code.
         * 
         * @since 1.5.0
         */
        private String collectionsImplementation = "DEFAULT";
        
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
        
        // ==================== Count-Based Value Cache Limiting ====================
        
        /**
         * Enable count-based value cache limiting per region.
         * When enabled, limits the number of values kept in memory based on
         * percentage of total keys or absolute count (whichever is lower).
         * This runs independently of memory pressure-based eviction.
         */
        private boolean valueCacheLimitEnabled = true;
        
        /**
         * Maximum percentage of total keys in a region to keep in value cache.
         * For example, 20 means only 20% of keys can have their values in memory.
         * Used in conjunction with valueCacheMaxEntries (whichever is lower wins).
         */
        @Min(1)
        @Max(100)
        private int valueCacheMaxPercent = 20;
        
        /**
         * Maximum absolute number of values to keep in memory per region.
         * Used in conjunction with valueCacheMaxPercent (whichever is lower wins).
         * For example, if a region has 100,000 keys and valueCacheMaxPercent=20,
         * the limit would be 20,000, but if valueCacheMaxEntries=10,000,
         * then only 10,000 values will be kept in memory.
         */
        @Min(100)
        private int valueCacheMaxEntries = 10000;
        
        /**
         * Interval in milliseconds for count-based cache limit checks.
         * This runs separately from memory watcher.
         */
        @Min(1000)
        private int valueCacheLimitCheckIntervalMs = 30000;
        
        // ==================== Warm Object Configuration (v1.7.9) ====================
        
        /**
         * Enable warm object maintenance per region.
         * When enabled, the system proactively maintains a minimum number of
         * "warm" (in-memory) objects per region, loading from disk if necessary.
         * This ensures frequently accessed data remains in memory for fast access.
         * 
         * @since 1.7.9
         */
        private boolean warmObjectsEnabled = true;
        
        /**
         * Per-region warm object counts. Keys are region names, values are minimum
         * number of objects to keep warm (in-memory) for that region.
         * 
         * Example configuration:
         *   kuber.cache.region-warm-object-counts.trade=100000
         *   kuber.cache.region-warm-object-counts.reference=50000
         *   kuber.cache.region-warm-object-counts.session=10000
         * 
         * If a region is not specified here, it falls back to default behavior
         * (no guaranteed minimum, subject to normal eviction policies).
         * 
         * Note: The warm object count cannot exceed the total keys in the region.
         * If configured higher, it will be capped at the region's key count.
         * 
         * @since 1.7.9
         */
        private java.util.Map<String, Integer> regionWarmObjectCounts = new java.util.HashMap<>();
        
        /**
         * Interval in milliseconds for warm object maintenance checks.
         * The warm object service runs at this interval to ensure regions
         * have their configured minimum warm objects in memory.
         * 
         * @since 1.7.9
         */
        @Min(1000)
        private int warmObjectCheckIntervalMs = 60000;
        
        /**
         * Batch size for loading warm objects from disk.
         * When the warm object service needs to load objects from disk
         * to meet the warm object target, it loads them in batches of this size.
         * 
         * @since 1.7.9
         */
        @Min(100)
        private int warmObjectLoadBatchSize = 1000;
        
        /**
         * Get the warm object count for a specific region.
         * Returns the region-specific count if configured, otherwise 0 (no minimum).
         * 
         * @since 1.7.9
         */
        public int getWarmObjectCountForRegion(String regionName) {
            return regionWarmObjectCounts.getOrDefault(regionName, 0);
        }
        
        /**
         * Check if a region has a configured warm object count.
         * 
         * @since 1.7.9
         */
        public boolean hasWarmObjectConfig(String regionName) {
            return regionWarmObjectCounts.containsKey(regionName) 
                    && regionWarmObjectCounts.get(regionName) > 0;
        }
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
         * Batch size for sync operations (snapshot pagination and oplog fetch).
         * Larger batches reduce HTTP round-trips but increase memory per batch.
         */
        @Min(1)
        private int syncBatchSize = 5000;
        
        /**
         * Sync timeout in milliseconds (overall full-sync timeout)
         */
        @Min(1000)
        private int syncTimeoutMs = 60000;
        
        /**
         * Heartbeat interval in milliseconds (ZooKeeper node re-registration)
         */
        @Min(1000)
        private int heartbeatIntervalMs = 5000;
        
        /**
         * Primary check interval in milliseconds
         */
        @Min(1000)
        private int primaryCheckIntervalMs = 10000;
        
        /**
         * OpLog circular buffer capacity.
         * Determines how many write operations are retained for delta replication.
         * If a SECONDARY falls more than this many operations behind, it must
         * perform a full resync.
         * 
         * Memory usage: ~500 bytes per entry (varies with value size).
         * Default 100,000 entries ≈ 50–100 MB.
         * 
         * Increase for high write-rate deployments or slow SECONDARY networks.
         */
        @Min(1000)
        private int oplogCapacity = 100000;
        
        /**
         * Polling interval in milliseconds for the continuous oplog tail.
         * The SECONDARY polls the PRIMARY's oplog at this interval when no
         * new entries are available.
         * 
         * Lower values reduce replication lag but increase HTTP traffic.
         * Default: 500ms (max replication lag ~500ms when idle).
         */
        @Min(100)
        private int syncIntervalMs = 500;
        
        /**
         * HTTP connect timeout in milliseconds for replication client.
         */
        @Min(1000)
        private int connectTimeoutMs = 5000;
        
        /**
         * HTTP read timeout in milliseconds for replication client.
         * Must be large enough for full-sync snapshot pages.
         */
        @Min(5000)
        private int readTimeoutMs = 30000;
        
        /**
         * Shared authentication token for replication API.
         * Both PRIMARY and SECONDARY must use the same token.
         * If blank, replication endpoints are unauthenticated.
         * 
         * STRONGLY recommended to set this in production.
         */
        private String authToken;
        
        /**
         * Advertised hostname or IP address that other nodes use to reach
         * this node's HTTP server.
         * 
         * Default: "localhost" (single-machine development).
         * In production, set to this node's actual IP or DNS name.
         * 
         * Example: "10.0.1.50" or "kuber-node-a.internal"
         */
        private String advertisedAddress = "localhost";
    }
    
    @Data
    public static class Security {
        /**
         * Path to users.json file for authentication.
         * Default: config/secure/users.json
         * @since 1.7.3 - Moved to secure folder, supports RBAC roles
         */
        private String usersFile = "config/secure/users.json";
        
        /**
         * Path to roles.json file for RBAC role definitions.
         * Default: config/secure/roles.json
         * @since 1.7.3
         */
        private String rolesFile = "config/secure/roles.json";
        
        /**
         * Path to API keys JSON file.
         * Default: config/secure/apikeys.json
         * @since 1.7.3 - Moved to secure folder
         */
        private String apiKeysFile = "config/secure/apikeys.json";
        
        /**
         * Session timeout in minutes
         */
        @Min(1)
        private int sessionTimeoutMinutes = 30;
        
        /**
         * Password for Redis AUTH command (empty = no auth)
         */
        private String redisPassword = null;
        
        /**
         * Whether to enforce RBAC authorization on cache operations.
         * When enabled, users must have appropriate roles to read/write/delete from regions.
         * Default: true
         * @since 1.7.3
         */
        private boolean rbacEnabled = true;
        
        /**
         * Whether to auto-create region roles when a new region is created.
         * When enabled, creates {region}_readonly, {region}_readwrite, {region}_full roles.
         * Default: true
         * @since 1.7.3
         */
        private boolean autoCreateRegionRoles = true;
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
        
        /**
         * Batch size for bulk writes to persistence store.
         * Records are accumulated and written in batches for better performance.
         * Batch writes use async mode (WAL written but not synced per batch).
         * Default: 32768 records per batch.
         * Set to 1 to disable batching (write each record individually).
         */
        @Min(1)
        private int batchSize = 32768;
        
        /**
         * Percentage of cache capacity to warm after autoload completes.
         * This proactively loads data into memory in a background thread.
         * Set to 0 to disable automatic warming (rely on lazy loading via GET).
         * Default: 10 (load 10% of capacity).
         * 
         * This setting is also used for:
         * - Startup loading from existing RocksDB database
         * - Restore from backup
         * - Manual reload from persistence
         * 
         * @since 1.6.1
         */
        @Min(0)
        @Max(100)
        private int warmPercentage = 10;
        
        /**
         * Whether to normalize all text values to US-ASCII during autoload.
         * 
         * When enabled:
         * - Accented characters are converted to base forms (é → e, ü → u, ñ → n)
         * - Special characters are transliterated (ß → ss, æ → ae, € → EUR)
         * - Ligatures are expanded (ﬁ → fi, ﬂ → fl)
         * - Typographic quotes/dashes are converted to ASCII equivalents
         * - Characters that cannot be transliterated are removed
         * 
         * This ensures all cached data contains only ASCII characters (0-127),
         * which is useful for:
         * - Legacy system compatibility
         * - Search optimization (searching "cafe" finds "café")
         * - URL-safe key generation
         * - Cross-platform consistency
         * 
         * Default: true (normalize to ASCII)
         * 
         * Can be overridden per-file in metadata: ascii_normalize:false
         * 
         * @since 1.7.9
         */
        private boolean asciiNormalize = true;
        
        /**
         * Whether to also normalize cache keys to ASCII.
         * Only applies when asciiNormalize is true.
         * 
         * Default: true
         * 
         * @since 1.7.9
         */
        private boolean asciiNormalizeKeys = true;
    }
    
    // ==================== BACKUP AND RESTORE CONFIGURATION ====================
    
    /**
     * Backup and restore configuration for RocksDB and LMDB persistence stores.
     * SQL databases (SQLite, PostgreSQL) have their own backup mechanisms.
     */
    @Data
    public static class Backup {
        /**
         * Whether backup is enabled.
         * When enabled, regions are periodically backed up to the backup directory.
         */
        private boolean enabled = true;
        
        /**
         * Directory where backup files are stored.
         * Each backup file is named: &lt;region&gt;.&lt;timestamp&gt;.backup
         */
        private String backupDirectory = "./backup";
        
        /**
         * Directory to monitor for restore files.
         * Place backup files here to trigger automatic restore.
         * Files are moved to backupDirectory after restore completes.
         */
        private String restoreDirectory = "./restore";
        
        /**
         * Cron expression for scheduled backups.
         * Default: "0 0 23 * * *" (11:00 PM daily)
         * Format: second minute hour day-of-month month day-of-week
         * Examples:
         *   "0 0 23 * * *" - 11:00 PM daily
         *   "0 0 2 * * *"  - 2:00 AM daily
         *   "0 0 0/6 * * *" - Every 6 hours (starting at midnight)
         *   "0 30 1 * * SUN" - 1:30 AM every Sunday
         */
        private String cron = "0 0 23 * * *";
        
        /**
         * Maximum number of backup files to keep per region.
         * Older backups are deleted when this limit is exceeded.
         * Set to 0 to keep all backups (no automatic cleanup).
         */
        @Min(0)
        private int maxBackupsPerRegion = 10;
        
        /**
         * Whether to create directories if they don't exist.
         */
        private boolean createDirectories = true;
        
        /**
         * Batch size for reading/writing entries during backup/restore.
         */
        @Min(100)
        private int batchSize = 10000;
        
        /**
         * File encoding for backup files.
         */
        private String fileEncoding = "UTF-8";
        
        /**
         * Whether to compress backup files (gzip).
         */
        private boolean compress = true;
    }
    
    // ==================== EVENT PUBLISHING CONFIGURATION ====================
    
    /**
     * Centralized broker/destination definitions.
     * Regions reference these by name.
     */
    @Data
    public static class BrokerDefinition {
        /**
         * Whether this broker is enabled.
         * Broker connections are only initialized if enabled=true.
         */
        private boolean enabled = false;
        
        /**
         * Broker type: kafka, confluent-kafka, activemq, rabbitmq, ibmmq, file
         */
        private String type;
        
        // ---- Kafka settings ----
        private String bootstrapServers = "localhost:9092";
        private int partitions = 3;
        private short replicationFactor = 1;
        private int retentionHours = 168;
        private String acks = "1";
        private int batchSize = 16384;
        private int lingerMs = 5;
        
        // ---- Confluent Kafka settings (v2.6.3) ----
        private String apiKey = "";
        private String apiSecret = "";
        private String schemaRegistryUrl = "";
        private String schemaRegistryApiKey = "";
        private String schemaRegistryApiSecret = "";
        
        // ---- ActiveMQ settings ----
        private String brokerUrl = "tcp://localhost:61616";
        
        // ---- RabbitMQ settings ----
        private String host = "localhost";
        private int port = 5672;
        private String virtualHost = "/";
        private String exchangeType = "topic";
        private boolean durable = true;
        
        // ---- IBM MQ settings ----
        private String queueManager = "QM1";
        private String channel = "DEV.APP.SVRCONN";
        private int ccsid = 0;
        private String sslCipherSuite = "";
        
        // ---- File settings ----
        private String directory = "./events";
        private int maxFileSizeMb = 100;
        private String rotationPolicy = "daily";
        private String format = "jsonl";
        private boolean compress = false;
        private int retentionDays = 30;
        
        // ---- Common settings ----
        private String username = "";
        private String password = "";
        private int ttlSeconds = 86400;
        private boolean persistent = true;
        private boolean useTopic = false;
        
        // ---- SSL/TLS settings (v2.2.0) ----
        private BrokerSsl ssl = new BrokerSsl();
    }
    
    /**
     * SSL/TLS configuration for message brokers.
     * Used by Kafka, ActiveMQ, RabbitMQ, and IBM MQ publishers.
     * @since 2.2.0
     */
    @Data
    public static class BrokerSsl {
        /**
         * Enable SSL/TLS for this broker connection.
         */
        private boolean enabled = false;
        
        /**
         * SSL mode hint — guides which fields are used:
         * <ul>
         *   <li>{@code jks} — JKS/PKCS12 trust store (and optionally key store)</li>
         *   <li>{@code pem} — PEM certificate files for trust</li>
         *   <li>{@code sasl_ssl} — SASL authentication over SSL (Kafka)</li>
         *   <li>{@code mtls_pem} — Mutual TLS with PEM client certificate + key</li>
         *   <li>{@code mtls_jks} — Mutual TLS with JKS/PKCS12 key store</li>
         * </ul>
         * Optional — if omitted, fields are used as-is.
         */
        private String mode = "";
        
        /**
         * SSL/TLS protocol version (e.g., TLSv1.2, TLSv1.3).
         */
        private String protocol = "TLSv1.3";
        
        // ---- JKS / PKCS12 Trust Store ----
        
        /**
         * Path to the trust store file (JKS, PKCS12, or PEM).
         */
        private String trustStorePath = "";
        
        /**
         * Trust store password.
         */
        private String trustStorePassword = "";
        
        /**
         * Trust store type: JKS, PKCS12, PEM.
         */
        private String trustStoreType = "JKS";
        
        // ---- JKS / PKCS12 Key Store (mTLS) ----
        
        /**
         * Path to the key store file (for mutual TLS / client certificate auth).
         */
        private String keyStorePath = "";
        
        /**
         * Key store password.
         */
        private String keyStorePassword = "";
        
        /**
         * Key store type: JKS, PKCS12, PEM.
         */
        private String keyStoreType = "JKS";
        
        /**
         * Key password (if different from key store password).
         */
        private String keyPassword = "";
        
        // ---- PEM Certificate Files ----
        
        /**
         * Path to the CA / trust certificate PEM file.
         * Used when mode is "pem" or "mtls_pem" instead of a JKS trust store.
         */
        private String trustCertPath = "";
        
        /**
         * Path to the client certificate PEM file (for mTLS PEM mode).
         */
        private String keyCertPath = "";
        
        /**
         * Path to the client private key PEM file (for mTLS PEM mode).
         */
        private String keyPath = "";
        
        // ---- SASL (Kafka) ----
        
        /**
         * SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER.
         * Used when mode is "sasl_ssl".
         */
        private String saslMechanism = "";
        
        /**
         * Full JAAS configuration string for Kafka SASL.
         * Example: {@code org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";}
         */
        private String saslJaasConfig = "";
        
        // ---- General ----
        
        /**
         * Whether to verify the broker's hostname against its certificate.
         */
        private boolean hostnameVerification = true;
        
        /**
         * SSL cipher suite override (primarily for IBM MQ).
         * Leave empty to use JVM defaults.
         */
        private String cipherSuite = "";
    }
    
    /**
     * Region publishing configuration - references centralized brokers.
     */
    @Data
    public static class RegionPublishingConfig {
        /**
         * Whether publishing is enabled for this region
         */
        private boolean enabled = false;
        
        /**
         * List of destinations for this region.
         * Each destination references a broker and specifies topic/queue.
         */
        private List<DestinationConfig> destinations = new ArrayList<>();
        
        // Legacy support - direct configuration (deprecated but still works)
        private KafkaConfig kafka = new KafkaConfig();
        private ActiveMqConfig activemq = new ActiveMqConfig();
        private RabbitMqConfig rabbitmq = new RabbitMqConfig();
        private IbmMqConfig ibmmq = new IbmMqConfig();
        private FilePublisherConfig file = new FilePublisherConfig();
    }
    
    /**
     * Destination configuration - links a region to a broker with specific topic/queue.
     */
    @Data
    public static class DestinationConfig {
        /**
         * Reference to a broker defined in kuber.publishing.brokers
         */
        private String broker;
        
        /**
         * Topic/queue/exchange name for this destination
         */
        private String topic = "";
        
        /**
         * Routing key (for RabbitMQ topic exchanges)
         */
        private String routingKey = "";
        
        /**
         * Queue name (for RabbitMQ - binds to exchange)
         */
        private String queue = "";
        
        /**
         * Override TTL for this specific destination (0 = use broker default)
         */
        private int ttlSeconds = 0;
        
        /**
         * Override persistence for this destination
         */
        private Boolean persistent = null;
    }
    
    @Data
    public static class KafkaConfig {
        /**
         * Whether Kafka publishing is enabled
         */
        private boolean enabled = false;
        
        /**
         * Kafka bootstrap servers (comma-separated)
         */
        private String bootstrapServers = "localhost:9092";
        
        /**
         * Topic name for this region's events
         */
        private String topic = "";
        
        /**
         * Number of partitions for auto-created topic
         */
        @Min(1)
        private int partitions = 3;
        
        /**
         * Replication factor for auto-created topic
         */
        @Min(1)
        private short replicationFactor = 1;
        
        /**
         * Retention period in hours for the topic
         */
        @Min(1)
        private int retentionHours = 168; // 7 days
        
        /**
         * Producer acks configuration: all, 1, or 0
         */
        private String acks = "1";
        
        /**
         * Producer batch size in bytes
         */
        @Min(1)
        private int batchSize = 16384;
        
        /**
         * Producer linger time in milliseconds
         */
        @Min(0)
        private int lingerMs = 5;
    }
    
    @Data
    public static class ActiveMqConfig {
        /**
         * Whether ActiveMQ publishing is enabled
         */
        private boolean enabled = false;
        
        /**
         * ActiveMQ broker URL
         */
        private String brokerUrl = "tcp://localhost:61616";
        
        /**
         * Username for ActiveMQ connection (optional)
         */
        private String username = "";
        
        /**
         * Password for ActiveMQ connection (optional)
         */
        private String password = "";
        
        /**
         * Queue or topic name for this region's events
         */
        private String destination = "";
        
        /**
         * Whether destination is a topic (true) or queue (false)
         */
        private boolean useTopic = false;
        
        /**
         * Time-to-live for messages in seconds
         */
        @Min(0)
        private int ttlSeconds = 86400; // 24 hours
        
        /**
         * Delivery mode: persistent (true) or non-persistent (false)
         */
        private boolean persistent = true;
    }
    
    @Data
    public static class RabbitMqConfig {
        /**
         * Whether RabbitMQ publishing is enabled
         */
        private boolean enabled = false;
        
        /**
         * RabbitMQ host
         */
        private String host = "localhost";
        
        /**
         * RabbitMQ port
         */
        @Min(1)
        private int port = 5672;
        
        /**
         * Virtual host
         */
        private String virtualHost = "/";
        
        /**
         * Username for RabbitMQ connection
         */
        private String username = "guest";
        
        /**
         * Password for RabbitMQ connection
         */
        private String password = "guest";
        
        /**
         * Exchange name
         */
        private String exchange = "";
        
        /**
         * Exchange type: direct, fanout, topic, headers
         */
        private String exchangeType = "topic";
        
        /**
         * Queue name (optional - if set, will be declared and bound)
         */
        private String queue = "";
        
        /**
         * Routing key pattern
         */
        private String routingKey = "";
        
        /**
         * Whether exchange/queue should be durable
         */
        private boolean durable = true;
        
        /**
         * Whether messages should be persistent
         */
        private boolean persistent = true;
        
        /**
         * Message TTL in seconds (0 = no expiration)
         */
        @Min(0)
        private int ttlSeconds = 86400; // 24 hours
    }
    
    @Data
    public static class IbmMqConfig {
        /**
         * Whether IBM MQ publishing is enabled
         */
        private boolean enabled = false;
        
        /**
         * IBM MQ host
         */
        private String host = "localhost";
        
        /**
         * IBM MQ port
         */
        @Min(1)
        private int port = 1414;
        
        /**
         * Queue manager name
         */
        private String queueManager = "QM1";
        
        /**
         * Channel name
         */
        private String channel = "DEV.APP.SVRCONN";
        
        /**
         * Queue name for events
         */
        private String queue = "";
        
        /**
         * Whether destination is a topic (true) or queue (false)
         */
        private boolean useTopic = false;
        
        /**
         * Username for connection (optional)
         */
        private String username = "";
        
        /**
         * Password for connection (optional)
         */
        private String password = "";
        
        /**
         * CCSID (Coded Character Set Identifier). 0 = use default
         */
        @Min(0)
        private int ccsid = 0;
        
        /**
         * SSL cipher suite for encrypted connections (optional)
         */
        private String sslCipherSuite = "";
        
        /**
         * Message TTL in seconds (0 = no expiration)
         */
        @Min(0)
        private int ttlSeconds = 86400; // 24 hours
        
        /**
         * Whether messages should be persistent
         */
        private boolean persistent = true;
    }
    
    @Data
    public static class FilePublisherConfig {
        /**
         * Whether file publishing is enabled
         */
        private boolean enabled = false;
        
        /**
         * Output directory for event files
         */
        private String directory = "";
        
        /**
         * Maximum file size in MB before rotation
         */
        @Min(1)
        private int maxFileSizeMb = 100;
        
        /**
         * Rotation policy: size, daily, hourly
         */
        private String rotationPolicy = "daily";
        
        /**
         * File format: jsonl (JSON Lines)
         */
        private String format = "jsonl";
        
        /**
         * Whether to compress rotated files
         */
        private boolean compress = false;
        
        /**
         * Number of days to retain files (0 = forever)
         */
        @Min(0)
        private int retentionDays = 30;
    }
    
    @Data
    public static class Publishing {
        /**
         * Thread pool size for async publishing
         */
        @Min(1)
        private int threadPoolSize = 4;
        
        /**
         * Queue capacity for pending events
         */
        @Min(100)
        private int queueCapacity = 10000;
        
        /**
         * Path to external JSON file for region event publishing configuration.
         * Regions defined in this file are merged into the publishing config,
         * overriding any same-named regions from application.properties.
         * Default: config/event_publishing.json
         * @since 2.2.0
         */
        private String regionConfigFile = "config/event_publishing.json";
        
        /**
         * Path to external JSON file for message broker definitions.
         * Brokers defined in this file are merged into the publishing config,
         * overriding any same-named brokers from application.properties.
         * Default: config/message_brokers.json
         * @since 2.2.0
         */
        private String brokerConfigFile = "config/message_brokers.json";
        
        /**
         * Centralized broker definitions.
         * Key is broker name, value is broker configuration.
         */
        private Map<String, BrokerDefinition> brokers = new HashMap<>();
        
        /**
         * Region-specific publishing configuration.
         * Key is region name, value is publishing config.
         */
        private Map<String, RegionPublishingConfig> regions = new HashMap<>();
    }
    
    /**
     * Publishing configuration
     */
    private Publishing publishing = new Publishing();
    
    @Data
    public static class Persistence {
        /**
         * Persistence store type: lmdb, rocksdb, mongodb, sqlite, postgresql, aerospike, memory
         * Default: lmdb (fastest reads via memory-mapping, no external dependencies)
         * 
         * LMDB is recommended for most use cases due to:
         * - Zero-copy reads via memory mapping
         * - Excellent read performance
         * - ACID transactions
         * - No recovery needed after crash
         * - Simple deployment (no external services)
         */
        @NotBlank
        private String type = "lmdb";
        
        /**
         * Whether to synchronously write individual PUT/SET operations to disk.
         * 
         * When false (default - async mode):
         * - Individual writes are saved to memory immediately (value cache + key index)
         * - Disk write happens asynchronously in background
         * - Better performance (10-100x faster for individual writes)
         * - Data is eventually consistent with disk
         * - Risk: If crash occurs before async write completes, that entry is lost
         * 
         * When true (sync mode):
         * - Each write waits for disk confirmation before returning
         * - Maximum durability - data survives power loss
         * - Slower performance (~1-5ms per write due to fsync)
         * 
         * Batch operations (autoload) always use async mode regardless of this setting.
         * 
         * Default: false (async for better performance)
         */
        private boolean syncIndividualWrites = false;
        
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
        
        /**
         * Aerospike configuration
         * @since 1.9.0
         */
        private Aerospike aerospike = new Aerospike();
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
         * LMDB memory-maps the database using virtual address space.
         * Default: 1TB (1024GB). This is safe on 64-bit systems.
         * 
         * IMPORTANT NOTES:
         * - This reserves VIRTUAL address space, NOT physical disk or RAM
         * - Actual disk usage grows only as data is written (sparse file)
         * - Physical RAM usage is managed by OS page cache (only active pages)
         * - 64-bit systems have 128TB+ virtual space, so 1TB is trivial
         * - Can be set much larger than available RAM or disk without issues
         * 
         * If you see "Environment mapsize reached" error, increase this value
         * and restart. The map size cannot be changed while database is running.
         * 
         * Common values:
         *   100GB = 107374182400
         *   500GB = 536870912000
         *   1TB   = 1099511627776 (default)
         *   2TB   = 2199023255552
         *   4TB   = 4398046511104
         */
        private long mapSize = 1099511627776L; // 1TB (1024GB)
        
        /**
         * Whether to sync writes immediately.
         * true = safer but slower (MDB_NOSYNC disabled)
         * false = faster but risk of data loss on crash (MDB_NOSYNC enabled)
         */
        private boolean syncWrites = true;
    }
    
    /**
     * Aerospike persistence configuration.
     * 
     * <p>Aerospike is a high-performance, distributed NoSQL database that provides:
     * <ul>
     *   <li>Sub-millisecond read/write latency</li>
     *   <li>Native TTL support per record (automatic expiration)</li>
     *   <li>Horizontal scalability with automatic sharding</li>
     *   <li>Strong consistency with flash-optimized storage</li>
     * </ul>
     * 
     * <p>Prerequisites:
     * <ol>
     *   <li>Install and start Aerospike server (Community or Enterprise)</li>
     *   <li>Create a namespace in aerospike.conf (e.g., "kuber")</li>
     *   <li>Configure the namespace with appropriate storage (memory, SSD, or hybrid)</li>
     * </ol>
     * 
     * @since 1.9.0
     */
    @Data
    public static class Aerospike {
        /**
         * Aerospike cluster hosts.
         * Format: host1:port1,host2:port2
         * Default port is 3000 if not specified.
         * 
         * Example: "localhost:3000" or "aero1:3000,aero2:3000,aero3:3000"
         */
        private String hosts = "localhost:3000";
        
        /**
         * Aerospike namespace to use.
         * The namespace must be pre-configured in aerospike.conf on the server.
         * Each namespace can have different storage configurations.
         */
        private String namespace = "kuber";
        
        /**
         * Username for authentication (if security is enabled on Aerospike).
         * Leave empty for Aerospike Community Edition without security.
         */
        private String username;
        
        /**
         * Password for authentication (if security is enabled on Aerospike).
         */
        private String password;
        
        /**
         * Connection timeout in milliseconds.
         * Time to wait for a new connection to be established.
         */
        private int connectionTimeoutMs = 5000;
        
        /**
         * Socket timeout in milliseconds.
         * Maximum time to wait for a socket operation (read/write).
         */
        private int socketTimeoutMs = 30000;
        
        /**
         * Maximum connections per node.
         * Aerospike client maintains a connection pool per node.
         */
        private int maxConnsPerNode = 300;
        
        /**
         * Connection pools per node.
         * Multiple pools can reduce lock contention under high load.
         */
        private int connPoolsPerNode = 1;
        
        /**
         * Whether to store the user key with the record.
         * When true, the key can be retrieved via record metadata.
         * Recommended: true (required for scan operations to return keys).
         */
        private boolean sendKey = true;
        
        /**
         * Write commit level.
         * COMMIT_ALL - Wait for write to be committed on all replicas
         * COMMIT_MASTER - Wait for write to be committed on master only (faster)
         */
        private String commitLevel = "COMMIT_ALL";
        
        /**
         * Read replica policy.
         * SEQUENCE - Read from nodes in sequence (prefer master)
         * MASTER - Read only from master
         * MASTER_PROLES - Read from master or proles randomly
         * RANDOM - Read from random node
         */
        private String replica = "SEQUENCE";
        
        /**
         * Default TTL for records in seconds.
         * -1 = Use namespace default TTL
         * -2 = Never expire
         * 0 = Use namespace default TTL
         * >0 = Specific TTL in seconds
         */
        private int defaultTtl = -1;
    }
    
    /**
     * Shutdown configuration for graceful application termination.
     * 
     * <p>Kuber supports multiple shutdown mechanisms:
     * <ul>
     *   <li><b>File-based</b>: Touch a shutdown file (default: kuber.shutdown)</li>
     *   <li><b>REST API</b>: POST /api/admin/shutdown with API key</li>
     *   <li><b>Signal</b>: SIGTERM/SIGINT (standard Spring Boot handling)</li>
     * </ul>
     */
    @Data
    public static class Shutdown {
        /**
         * Enable file-based shutdown monitoring.
         * When enabled, Kuber watches for a shutdown file and initiates
         * graceful shutdown when the file is detected.
         */
        private boolean fileEnabled = true;
        
        /**
         * Path to the shutdown signal file.
         * Create this file to trigger graceful shutdown.
         * The file is deleted after shutdown is initiated.
         * 
         * Default: kuber.shutdown (in working directory)
         */
        private String filePath = "kuber.shutdown";
        
        /**
         * How often to check for shutdown file (in milliseconds).
         * Default: 5000ms (5 seconds)
         */
        @Min(1000)
        private long checkIntervalMs = 5000;
        
        /**
         * Enable REST API shutdown endpoint.
         * When enabled, POST /api/admin/shutdown triggers graceful shutdown.
         * Requires valid API key authentication.
         */
        private boolean apiEnabled = true;
        
        /**
         * Delay between shutdown phases in seconds.
         * Each phase waits this long before proceeding to ensure
         * clean resource release.
         * Default: 5 seconds
         */
        @Min(1)
        private int phaseDelaySeconds = 5;
    }
    
    /**
     * Secondary indexing configuration.
     * 
     * <p>Controls file-based index operations for automation and scripting.
     * 
     * <h2>File-Based Triggers:</h2>
     * <ul>
     *   <li>{@code kuber.index.<region>.rebuild} - Rebuild all indexes for region</li>
     *   <li>{@code kuber.index.<region>.drop} - Drop all indexes for region</li>
     *   <li>{@code kuber.index.<region>.<field>.rebuild} - Rebuild specific index</li>
     *   <li>{@code kuber.index.<region>.<field>.drop} - Drop specific index</li>
     *   <li>{@code kuber.index.all.rebuild} - Rebuild ALL indexes</li>
     *   <li>{@code kuber.index.<region>.<field>.create.<type>} - Create new index</li>
     * </ul>
     * 
     * @since 1.9.0
     */
    @Data
    public static class Indexing {
        /**
         * Enable file-based index operation triggers.
         * When enabled, Kuber watches for trigger files like kuber.index.*.rebuild
         * and executes the corresponding index operations.
         * 
         * Default: true
         */
        private boolean fileWatcherEnabled = true;
        
        /**
         * How often to check for index trigger files (in milliseconds).
         * Default: 5000ms (5 seconds)
         */
        @Min(1000)
        private long fileWatcherIntervalMs = 5000;
        
        /**
         * Directory to watch for index trigger files.
         * Default: . (current working directory)
         */
        private String fileWatcherDirectory = ".";
        
        // ==================== Index Rebuild Optimization (v2.1.0) ====================
        
        /**
         * Batch size for index rebuild operations.
         * Documents are loaded and indexed in batches of this size to reduce
         * GC pressure and improve rebuild performance.
         * 
         * <p>Larger batches are more efficient but use more memory during rebuild.
         * Recommended values: 500-2000 for most workloads.
         * 
         * Default: 1000
         * 
         * @since 2.0.0
         */
        @Min(100)
        private int rebuildBatchSize = 1000;
        
        /**
         * Number of parallel batches to process during index rebuild.
         * Higher values speed up rebuild but use more CPU and memory.
         * 
         * Default: 4 (usually matches available cores)
         * 
         * @since 2.0.0
         */
        @Min(1)
        private int rebuildParallelBatches = 4;
        
        /**
         * Maximum memory budget for secondary indexes in megabytes.
         * When indexes exceed this budget, least-recently-used entries are evicted.
         * Set to 0 to disable memory budget (unlimited).
         * 
         * Default: 0 (unlimited)
         * 
         * @since 2.0.0
         */
        private int maxMemoryMb = 0;
        
        /**
         * Intern threshold for string value deduplication.
         * Field values that appear more than this many times are interned
         * to reduce memory usage.
         * 
         * Default: 100
         * 
         * @since 2.0.0
         */
        @Min(10)
        private int internThreshold = 100;
        
        /**
         * Stream entries directly from persistence during index rebuild.
         * When true, bypasses the in-memory cache to avoid eviction pressure.
         * When false, reads from cache (may cause eviction cascade on large datasets).
         * 
         * <p>Recommended: true for regions with more than 100K entries.
         * 
         * Default: true
         * 
         * @since 2.0.0
         */
        private boolean rebuildFromPersistence = true;
        
        /**
         * Default storage mode for secondary indexes.
         * HEAP: Fast but uses JVM heap memory (may cause GC pressure)
         * OFFHEAP: Slower but uses direct memory (no GC pressure, better for large indexes)
         * DISK: Slowest but minimal RAM usage, survives restarts
         * Default: HEAP
         */
        private String defaultStorage = "HEAP";
        
        /**
         * Storage mode for HASH indexes.
         * HEAP | OFFHEAP | DISK | DEFAULT (use defaultStorage)
         */
        private String hashStorage = "DEFAULT";
        
        /**
         * Storage mode for BTREE indexes.
         * HEAP | OFFHEAP | DISK | DEFAULT (use defaultStorage)
         */
        private String btreeStorage = "DEFAULT";
        
        /**
         * Storage mode for TRIGRAM indexes.
         * Recommended: OFFHEAP (trigram indexes consume significant memory)
         * HEAP | OFFHEAP | DISK | DEFAULT (use defaultStorage)
         */
        private String trigramStorage = "OFFHEAP";
        
        /**
         * Storage mode for PREFIX indexes.
         * HEAP | OFFHEAP | DISK | DEFAULT (use defaultStorage)
         */
        private String prefixStorage = "DEFAULT";
        
        /**
         * Initial off-heap buffer size per index in bytes.
         * Default: 16MB
         */
        private long offheapInitialSize = 16L * 1024L * 1024L;
        
        /**
         * Maximum off-heap buffer size per index in bytes.
         * Default: 1GB
         */
        private long offheapMaxSize = 1024L * 1024L * 1024L;
        
        // ==================== Disk-Based Index Configuration (v1.9.0) ====================
        
        /**
         * Backend engine for disk-based indexes.
         * Options: rocksdb, lmdb, sqlite
         * Default: rocksdb (best write performance)
         * 
         * <p>This is independent of the main persistence backend. You can use
         * PostgreSQL for data persistence and RocksDB for index storage.
         * 
         * @since 1.9.0
         */
        private String diskBackend = "rocksdb";
        
        /**
         * Directory for disk-based index storage.
         * Default: ./kuberdata/indexes
         * 
         * @since 1.9.0
         */
        private String diskDirectory = "./kuberdata/indexes";
        
        /**
         * Enable write-ahead logging for disk indexes.
         * Improves durability but reduces write performance.
         * Default: false (indexes can be rebuilt from data)
         * 
         * @since 1.9.0
         */
        private boolean diskWalEnabled = false;
        
        /**
         * Sync writes to disk immediately.
         * true: Maximum durability, slower writes
         * false: Buffered writes, faster but may lose recent index updates on crash
         * Default: false (indexes can be rebuilt)
         * 
         * @since 1.9.0
         */
        private boolean diskSyncWrites = false;
        
        /**
         * Cache size for disk-based indexes in MB.
         * Higher values improve read performance but use more RAM.
         * Default: 64MB
         * 
         * @since 1.9.0
         */
        private int diskCacheSizeMb = 64;
        
        /**
         * Enable bloom filters for disk-based indexes.
         * Improves lookup performance for non-existent keys.
         * Default: true
         * 
         * @since 1.9.0
         */
        private boolean diskBloomFilterEnabled = true;
        
        /**
         * Skip index rebuild on startup if disk indexes exist.
         * When true and using DISK storage, existing index data is reused.
         * When false, indexes are always rebuilt from persistence on startup.
         * Default: true
         * 
         * @since 1.9.0
         */
        private boolean diskReuseOnStartup = true;
        
        // ==================== Hybrid Query Strategy (v1.9.0) ====================
        
        /**
         * Enable hybrid query strategy.
         * When a Kuber secondary index doesn't exist for a query field,
         * fall back to native database queries (PostgreSQL GIN, MongoDB, etc.)
         * instead of full table scan.
         * 
         * <p>Supported backends for native query fallback:
         * <ul>
         *   <li>PostgreSQL - Uses GIN JSONB index and @&gt; operator</li>
         *   <li>MongoDB - Uses native document queries</li>
         *   <li>SQLite - Uses JSON1 extension (json_extract)</li>
         * </ul>
         * 
         * <p>Backends without native JSON query support (RocksDB, LMDB, Memory)
         * will fall back to full scan when no Kuber index exists.
         * 
         * Default: true
         * 
         * @since 1.9.0
         */
        private boolean hybridQueryEnabled = true;
        
        /**
         * Threshold for falling back to native database query.
         * If the estimated number of entries to scan exceeds this threshold
         * AND a native query is available, use native database query.
         * Default: 10000
         * 
         * @since 1.9.0
         */
        private int hybridQueryThreshold = 10000;
    }
    
    /**
     * Prometheus metrics configuration.
     * @since 1.7.9
     */
    @Data
    public static class Prometheus {
        /**
         * Enable Prometheus metrics endpoint.
         * When enabled, metrics are exposed at /actuator/prometheus
         */
        private boolean enabled = true;
        
        /**
         * Metrics update interval in milliseconds.
         * How often to refresh cache metrics.
         * Default: 5000ms (5 seconds)
         */
        @Min(1000)
        private long updateIntervalMs = 5000;
        
        /**
         * Include JVM metrics.
         * When enabled, JVM memory, GC, and thread metrics are exported.
         */
        private boolean includeJvmMetrics = true;
        
        /**
         * Include per-region metrics.
         * When enabled, individual region statistics are exported.
         */
        private boolean includeRegionMetrics = true;
        
        /**
         * Custom metric prefix.
         * All Kuber metrics will use this prefix.
         * Default: kuber
         */
        private String metricPrefix = "kuber";
        
        /**
         * Include operation latency histograms.
         * When enabled, detailed latency distribution is tracked.
         * Note: May increase memory usage.
         */
        private boolean includeLatencyHistograms = false;
        
        /**
         * Histogram bucket boundaries in microseconds for latency metrics.
         * Only used when includeLatencyHistograms is true.
         */
        private double[] latencyBuckets = {100, 500, 1000, 5000, 10000, 50000, 100000};
    }
    
    /**
     * JSON search configuration including parallel search settings.
     * @since 1.7.9
     */
    @Data
    public static class Search {
        /**
         * Enable parallel JSON search.
         * When enabled, large searches are executed using multiple threads.
         * Default: true
         */
        private boolean parallelEnabled = true;
        
        /**
         * Number of threads for parallel search.
         * Default: 8
         * Set to 0 to use number of available processors.
         */
        @Min(0)
        @Max(64)
        private int threadCount = 8;
        
        /**
         * Minimum number of keys to trigger parallel search.
         * For datasets smaller than this threshold, sequential search is used.
         * Default: 1000
         */
        @Min(100)
        private int parallelThreshold = 1000;
        
        /**
         * Search timeout in seconds.
         * If a search takes longer than this, partial results are returned.
         * Default: 60 seconds
         */
        @Min(1)
        private int timeoutSeconds = 60;
        
        /**
         * Cache compiled regex patterns for performance.
         * Default: true
         */
        private boolean cacheRegexPatterns = true;
        
        /**
         * Maximum number of cached regex patterns.
         * Oldest patterns are evicted when this limit is reached.
         * Default: 1000
         */
        @Min(10)
        private int maxCachedPatterns = 1000;
    }
}
