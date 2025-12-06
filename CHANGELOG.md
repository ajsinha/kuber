# Kuber Distributed Cache - Changelog

All notable changes to this project are documented in this file.

## [1.2.8] - 2025-12-06 - CENTRALIZED EVENT PUBLISHING CONFIGURATION

### Added
- **Centralized Broker Definitions**: Define brokers once, reference from multiple regions
  - New `kuber.publishing.brokers.<name>.*` configuration section
  - Supports Kafka, ActiveMQ, RabbitMQ, IBM MQ, and File destinations
  - Each broker defined once with full connection details
  - Regions reference brokers by name with specific topics/queues

- **Multi-Destination Publishing**: Each region can publish to multiple destinations
  - New `destinations[]` array in region configuration
  - Example: orders region → Kafka + RabbitMQ + File simultaneously
  - Per-destination TTL and persistence overrides

- **BrokerDefinition Class**: Centralized broker configuration
  - Kafka: bootstrapServers, partitions, replicationFactor, retentionHours, acks, batchSize, lingerMs
  - ActiveMQ: brokerUrl, username, password, useTopic, ttlSeconds, persistent
  - RabbitMQ: host, port, virtualHost, exchangeType, durable, username, password
  - IBM MQ: host, port, queueManager, channel, ccsid, sslCipherSuite, username, password
  - File: directory, maxFileSizeMb, rotationPolicy, format, compress, retentionDays

- **DestinationConfig Class**: Region-to-broker linking
  - broker: Reference to centralized broker definition
  - topic: Topic/queue/exchange name
  - routingKey: For RabbitMQ topic exchanges
  - queue: For RabbitMQ queue binding
  - ttlSeconds/persistent: Per-destination overrides

### Changed
- **All Event Publishers Refactored**: Support both centralized and legacy configuration
  - KafkaEventPublisher: Multi-destination with producer pooling by bootstrap server
  - ActiveMqEventPublisher: Multi-destination with connection pooling by broker URL
  - RabbitMqEventPublisher: Multi-destination with channel pooling by exchange
  - IbmMqEventPublisher: Multi-destination with connection pooling by queue manager
  - FileEventPublisher: Multi-destination with writer pooling by directory

### Fixed
- **JMS Compatibility**: ActiveMQ and IBM MQ now use javax.jms (not jakarta.jms)
  - ActiveMQ 5.x requires javax.jms namespace
  - IBM MQ client requires javax.jms namespace
  - Updated imports in ActiveMqEventPublisher and IbmMqEventPublisher

### Documentation
- Updated publishing.html with centralized configuration examples
- New tabbed interface for broker type properties
- Legacy configuration moved to collapsible section
- Comprehensive multi-destination examples

## [1.2.7] - 2025-12-06 - EVENT PUBLISHING FOUNDATION

### Added
- **Event Publishing Framework**: Pluggable publisher architecture
  - EventPublisher interface with lifecycle methods
  - PublisherRegistry for automatic discovery
  - RegionEventPublishingService for async publishing

- **Multiple Publisher Support**:
  - KafkaEventPublisher: Apache Kafka integration
  - ActiveMqEventPublisher: Apache ActiveMQ integration  
  - RabbitMqEventPublisher: RabbitMQ AMQP integration
  - IbmMqEventPublisher: IBM MQ integration
  - FileEventPublisher: Local/network file publishing

- **Publishing Documentation**: New /help/publishing page
  - Configuration reference for all publishers
  - Architecture diagrams
  - Example configurations

## [1.2.6] - 2025-12-06 - STARTUP ORCHESTRATION & CONFIGURABLE API KEYS PATH

### Added
- **Configurable API Keys Path**: API keys file location now configurable via properties
  - New property: `kuber.security.api-keys-file` (default: `config/apikeys.json`)
  - Supports any file path relative to working directory or absolute path

- **PersistenceMaintenanceService**: New Spring-managed service for database maintenance
  - Runs within Spring context after ApplicationReadyEvent
  - Supports RocksDB compaction and SQLite vacuum
  - Replaces pre-Spring PreStartupCompaction approach

### Changed
- **Startup Orchestration**: Complete restructuring of startup sequence
  - Phase 0: Spring context initialization
  - Phase 1: 10-second stabilization wait
  - Phase 2: Persistence maintenance (compaction/vacuum)
  - Phase 3: 2-second wait + Cache service initialization
  - Phase 4: 2-second wait + Redis protocol server start
  - Phase 5: 2-second wait + Autoload service start
  - Phase 6: 2-second wait + Final system ready announcement

- **Removed Pre-Startup Compaction**: Database maintenance now runs after Spring context loads
  - PreStartupCompaction.run() removed from KuberApplication.main()
  - PreStartupCompaction class marked as @Deprecated (retained for standalone testing)
  - Compaction/vacuum now managed by StartupOrchestrator via PersistenceMaintenanceService

- **ApiKeyService**: Now uses configurable path from properties
  - Falls back to `config/apikeys.json` if not specified

### Fixed
- Startup timing improved with explicit delays between phases
- Better isolation of database maintenance from cache initialization

## [1.2.5] - 2025-12-05 - API KEY AUTHENTICATION

### Added
- **API Key Authentication**: Full support for API key-based authentication
  - Generate API keys from Admin UI (/admin/apikeys)
  - API keys can be used by REST, Python, Java, and Redis clients
  - Keys associated with users and inherit their roles
  - Optional expiration dates for keys
  - Key revocation and reactivation
  - Last-used tracking for audit purposes

- **API Key Service**: New `ApiKeyService` for key management
  - Secure key generation (kub_ prefix + 64 hex characters)
  - JSON file storage (config/apikeys.json)
  - Key validation with expiration checking
  - Statistics and audit tracking

- **API Key Authentication Filter**: Spring Security filter for REST API
  - X-API-Key header support
  - Authorization: ApiKey scheme support
  - Query parameter (api_key) support

- **Redis Protocol API Key Support**: 
  - `AUTH APIKEY kub_xxx...` command
  - `AUTH kub_xxx...` direct key authentication

- **Admin API Keys Page**: Full management UI
  - Generate new keys with roles and expiration
  - View all keys with masked values
  - Revoke/activate/delete keys
  - Usage instructions for all client types

- **Documentation Updates**:
  - API Key section in architecture documentation
  - Help pages for API key usage
  - Client examples for all supported languages

### Changed
- SecurityConfig updated to support API key filter
- AdminController extended with API key endpoints
- RedisProtocolHandler supports API key authentication
- Layout updated with API Keys menu item
- Version bumped to 1.2.5 across all modules

## [1.2.4] - 2025-12-05 - STARTUP RACE CONDITION FIX

### Fixed
- **Startup Race Condition**: Fixed critical race condition where data recovery and autoload could start before Spring context was fully loaded
- **Redis Server Delayed Start**: Redis protocol server now starts AFTER cache service initialization and data recovery
  - Clients cannot connect until cache is ready with recovered data
  - Prevents "cache not initialized" errors during client connections
- **Memory Watcher Guard**: Memory watcher service now checks if cache is initialized before performing operations
- **Scheduled Task Protection**: All @Scheduled methods now check for cache initialization before executing
- **Ordered Startup Sequence**: Guaranteed startup order:
  1. Spring context fully loads (ApplicationReadyEvent)
  2. Wait 10 seconds for system stabilization
  3. Initialize CacheService (recover data from persistence store)
  4. Start Redis Protocol Server (now accepts client connections)
  5. Start Autoload Service (process inbox files)

### Added
- **Startup Orchestration Documentation**: Comprehensive documentation of startup sequence
  - Added Section 3 "Startup Orchestration" to ARCHITECTURE.md with diagrams and tables
  - Added Section 3 "Startup Orchestration" to architecture.html with visual diagrams
  - Added "Startup Sequence" card to help index page linking to architecture#startup-orchestration
  - Documents race condition prevention strategies
  - Documents scheduled task protection mechanisms
  - Includes startup logging examples

### Changed
- `StartupOrchestrator` now controls Redis server startup in addition to cache and autoload
- `RedisProtocolServer` no longer uses @PostConstruct - started explicitly by orchestrator
- `MemoryWatcherService` skips operations until cache is initialized
- `PersistenceExpirationService` skips operations until cache is initialized
- `CacheService.cleanupExpiredEntries()` skips until initialized
- Added `isCacheReady()` method to StartupOrchestrator for service coordination
- Renumbered architecture documentation sections (Core Components now Section 4, etc.)

## [1.2.3] - 2025-12-05 - ARCHITECTURE DOCUMENTATION

### Added
- **Architecture Help Page**: Comprehensive system architecture documentation accessible from Help menu
  - High-level architecture diagrams
  - Hybrid memory architecture explanation
  - Core components description
  - Persistence layer comparison
  - Protocol design documentation
  - Replication architecture
  - Security architecture
  - Data flow diagrams
  - Deployment patterns
  - Accessible to non-authenticated users

### Changed
- Help index now includes Architecture card in Getting Started section
- Version bumped to 1.2.3 across all modules and documentation

## [1.2.2] - 2025-12-05 - OFF-HEAP KEY INDEX

### Added
- **Off-Heap Key Index**: Optional DRAM-based key storage outside Java heap
  - Keys stored in direct ByteBuffer memory, not subject to GC
  - Zero GC pressure for key storage - no heap scans during garbage collection
  - Automatic buffer growth and compaction
  - Configurable initial and maximum buffer sizes
  - Proper memory cleanup on shutdown

- **Configuration Options**:
  ```yaml
  kuber:
    cache:
      off-heap-key-index: true  # Enable off-heap key storage
      off-heap-key-index-initial-size-mb: 16  # Initial buffer per region
      off-heap-key-index-max-size-mb: 1024  # Max buffer per region
  ```

- **KeyIndexInterface**: Abstraction layer for pluggable key storage implementations
  - `KeyIndex`: On-heap implementation (default, uses ConcurrentHashMap)
  - `OffHeapKeyIndex`: Off-heap implementation (uses direct ByteBuffer)

- **Extended Autoload File Format Support**:
  - **TXT files**: Now supported with `.txt` extension (processed identically to CSV)

- **Composite Key Support in Autoload**: Support for composite keys in data file loading
  - Use "/" separator in `key_field` to specify multiple fields: `key_field:country/state/city`
  - Values are extracted and joined with "/" delimiter: `US/CA/Los Angeles`
  - Optional `key_delimiter` to customize the join character (default: "/")
  - Works for CSV, TXT, and JSON file formats

  Example metadata file:
  ```
  region:locations
  ttl:3600
  key_field:country/state/city
  key_delimiter:/
  ```

### Performance Benefits
| Scenario | On-Heap | Off-Heap | Benefit |
|----------|---------|----------|---------|
| 1M keys | ~500MB heap | ~50MB heap + 80MB DRAM | 90% less GC pressure |
| GC pauses | May affect key ops | No impact on keys | More predictable latency |
| Max keys | Limited by heap | Limited by DRAM | Can scale further |

### Changed
- CacheService now uses `KeyIndexInterface` for flexibility
- Server info includes off-heap memory usage when enabled
- Shutdown properly releases off-heap memory

## [1.2.1] - 2025-12-05 - HYBRID MEMORY ARCHITECTURE

### Added
- **Hybrid Memory Architecture**: Aerospike-inspired design where all keys are always in memory
  - KeyIndex per region: Stores all key metadata in memory for O(1) lookups
  - Value cache: Hot values in memory (Caffeine), cold values on disk only
  - EXISTS operation: O(1) pure memory lookup - NEVER hits disk
  - KEYS operation: O(n) memory scan - NEVER hits disk
  - DBSIZE: O(1) from KeyIndex.size() - instant accurate count
  - Negative lookups: Instant fail (key not in index = doesn't exist)

- **KeyIndex Class**: In-memory index for each region
  - Tracks: key, valueType, valueLocation (MEMORY/DISK/BOTH), valueSize, TTL, timestamps
  - LRU/LFU support for value eviction decisions
  - Statistics: index hits, misses, hit rate
  - ~104 bytes per key overhead (vs ~500+ for full entry)

- **Value Location Tracking**: When values are evicted from memory due to size constraints
  - Key stays in KeyIndex (always accessible)
  - Value marked as DISK only
  - Next GET loads value back to memory

### Performance Impact
| Operation | Before v1.2.1 | After v1.2.1 | Improvement |
|-----------|---------------|--------------|-------------|
| EXISTS | May hit disk | O(1) memory | 10-100x faster |
| KEYS * | Scans disk | Memory scan | 100x+ faster |
| Missing key | Negative cache | Instant fail | Always instant |
| DBSIZE | O(n) estimate | O(1) exact | 1000x faster |

### Changed
- CacheService now uses KeyIndex + value cache architecture
- Removed negative cache (KeyIndex replaces it more efficiently)
- Memory limits now apply only to value cache (keys are always retained)
- Region stats now include: keysInMemory, valuesInMemory, valuesOnDiskOnly

## [1.2.0] - 2025-12-05

### Added
- **LMDB Persistence Store**: Lightning Memory-Mapped Database support
  - Zero-copy reads via memory-mapped I/O
  - ACID transactions with MVCC (multiple readers, single writer)
  - No recovery needed after crash (copy-on-write B+ tree)
  - Separate LMDB environment per region (like RocksDB architecture)
  - Configurable map size (default 1GB, supports up to 16GB+)
  - Thread-safe environment creation with double-checked locking

- **Six Persistence Backends**: Now supports RocksDB (default), LMDB, MongoDB, PostgreSQL, SQLite, and in-memory

### Configuration
```yaml
kuber:
  persistence:
    type: lmdb  # or: rocksdb, mongodb, postgresql, sqlite, memory
    lmdb:
      path: ./data/lmdb
      map-size: 1073741824  # 1GB
```

### Persistence Store Comparison
| Store | Speed | Durability | Best For |
|-------|-------|------------|----------|
| RocksDB | Very Fast | Excellent | Default - production workloads |
| LMDB | Extremely Fast | Excellent | Read-heavy workloads |
| SQLite | Fast | Good | Simple deployments |
| MongoDB | Fast | Excellent | Document-native queries |
| PostgreSQL | Fast | Excellent | JSONB support, SQL queries |

## [1.1.18] - 2025-12-05

### Fixed
- **RocksDB Lock Error**: Fixed race condition during concurrent autoload operations that caused "lock hold by current process" errors
- **SQLite Connection Race**: Fixed similar race condition in SQLite persistence store during concurrent database creation

### Changed
- Replaced `computeIfAbsent` with proper double-checked locking pattern for region database/connection creation
- Thread-safe region database initialization for all file-based stores (RocksDB, SQLite)

### Technical Details
- New `dbCreationLock` object in RocksDbPersistenceStore for synchronized database creation
- New `connCreationLock` object in SqlitePersistenceStore for synchronized connection creation
- Fast path checks existing databases without lock; slow path uses synchronized block with double-check

## [1.1.17] - 2025-12-04

### Added
- **Negative Cache**: Caffeine cache tracking keys known to NOT exist (30s TTL, 100K max entries)
- **Batch Loading Interface**: `loadEntriesByKeys()` method in PersistenceStore interface
- **RocksDB Batch Loading**: Uses `multiGetAsList()` for single native call batch retrieval
- **SQLite Batch Loading**: Uses SQL `IN` clause for batch queries
- **MongoDB Batch Loading**: Uses `Filters.in()` for batch queries
- **PostgreSQL Batch Loading**: Uses SQL `IN` clause for batch queries

### Changed
- **Optimized getEntry()**: Fast path for memory cache hits (2 method calls vs 5)
- **Optimized mget()**: Single batch call instead of N individual queries (100x faster for 100 keys)
- **Optimized exists()**: Fast path checks memory and negative cache before full getEntry()

### Fixed
- **Thymeleaf Template Errors**: Fixed intermittent parsing errors due to invalid syntax
  - Replaced chained Elvis operators with separate th:if blocks
  - Replaced `>=` with `ge`, `<` with `lt` (XML-safe operators)
  - Added null checks before `#temporals.format()` calls

## [1.1.16] - 2025-12-03

### Added
- **Custom Error Pages**: Detailed error information with status-specific icons and suggestions
- Exception details and collapsible stack traces for debugging
- System status display for server errors (500)
- Request ID tracking for debugging
- JSON error responses for API requests (Accept: application/json)

## [1.1.15] - 2025-12-02

### Added
- **Pre-Startup Compaction**: Database optimization runs BEFORE Spring context loads
- Standalone PreStartupCompaction class (non-Spring managed)
- RocksDB compaction runs before web server starts
- SQLite VACUUM runs before web server starts

### Changed
- No resource contention during startup - databases fully optimized before any requests
- Progress output with size savings displayed

## [1.1.14] - 2025-12-01

### Fixed
- TTL Expiration Service card not loading on admin dashboard
- Removed duplicate expiration cards causing JavaScript conflicts
- Consolidated maintenance cards (Compaction + Expiration) in single row

## [1.1.13] - 2025-11-30

### Added
- **Fast Entry Counts**: O(1) entry estimation using RocksDB native properties
- Dashboard and login pages now load instantly with large datasets (1M+ entries)

### Changed
- Entry counting uses RocksDB `rocksdb.estimate-num-keys` property instead of iteration

## [1.1.12] - 2025-11-29

### Added
- **Scheduled Compaction**: Cron-based RocksDB compaction scheduling
- Default schedule: 2:00 AM daily
- Configurable cron expression via `kuber.persistence.rocksdb.compaction-cron`
- Manual compaction trigger via API (`POST /api/admin/compaction/trigger`) and UI

## [1.1.11] - 2025-11-28

### Added
- **Global Memory Limit**: `global-max-memory-entries` caps total entries across all regions
- **Per-Region Memory Limits**: `region-memory-limits` map for individual region configuration
- **Smart Memory Allocation**: Proportional distribution based on 50% configured limits + 50% actual data size
- **Smart Cache Priming**: Loads most recently accessed entries first on restart

### Changed
- Removed RocksDB metadata database - relies solely on folder discovery
- Region metadata stored as JSON file in each region directory

### Fixed
- Critical bug: cache now properly reloads from disk on restart

## [1.1.10] - 2025-11-27

### Added
- Region isolation for RocksDB and SQLite (separate database per region)
- Independent compaction per region
- Better concurrency with parallel I/O across regions

## [1.1.9] - 2025-11-26

### Added
- Autoload service for bulk CSV and JSON import
- Metadata file format for configuring imports
- Automatic file processing with inbox/outbox directories

## [1.1.8] - 2025-11-25

### Added
- CSV export functionality for regions and query results
- REST API endpoints for export

## [1.1.7] - 2025-11-24

### Added
- Primary/Secondary replication with ZooKeeper coordination
- Automatic failover on primary failure
- Read scaling via secondary nodes

## [1.1.6] - 2025-11-23

### Added
- Python client libraries (Redis protocol and REST API)
- Java client libraries (Redis protocol and REST API)

## [1.1.5] - 2025-11-22

### Added
- PostgreSQL persistence backend with JSONB support
- HikariCP connection pooling

## [1.1.4] - 2025-11-21

### Added
- SQLite persistence backend
- WAL mode for better concurrency

## [1.1.3] - 2025-11-20

### Added
- RocksDB persistence backend
- LZ4 compression support

## [1.1.2] - 2025-11-19

### Added
- JSON document storage (JSET, JGET)
- JSONPath queries (JSEARCH)
- Deep search with multiple operators

## [1.1.1] - 2025-11-18

### Added
- Region support (RCREATE, RSELECT, RDELETE, REGIONS)
- Region-based key isolation
- Default region handling

## [1.1.0] - 2025-11-17

### Added
- Initial release
- Redis protocol support (RESP)
- Core commands: GET, SET, DEL, EXISTS, KEYS, EXPIRE, TTL
- Hash operations: HSET, HGET, HGETALL, HDEL
- Batch operations: MGET, MSET
- Web management UI
- REST API
- User authentication with roles
- Caffeine in-memory cache
- MongoDB persistence backend

---

Copyright © 2025-2030, All Rights Reserved  
Ashutosh Sinha | Email: ajsinha@gmail.com

Patent Pending
