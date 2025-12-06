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
         * Path to API keys JSON file.
         * Default: config/apikeys.json
         */
        private String apiKeysFile = "config/apikeys.json";
        
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
         * Broker type: kafka, activemq, rabbitmq, ibmmq, file
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
