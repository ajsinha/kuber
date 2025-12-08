# Kuber Distributed Cache - Changelog

All notable changes to this project are documented in this file.

## [1.5.0] - 2025-12-08 - FACTORY PATTERN FOR PLUGGABLE CACHE & COLLECTIONS

### Added
- **Cache Factory Pattern**: Pluggable cache implementation via Factory + Proxy pattern
  - `CacheProxy<K,V>` interface: Abstraction for cache operations
  - `CacheFactory` interface: Factory for creating cache instances
  - `CaffeineCacheFactory`: Default implementation using Caffeine
  - `CaffeineCacheProxy`: Caffeine-specific proxy implementation
  - `CacheConfig`: Configuration object for cache creation

- **Collections Factory Pattern**: Pluggable collections implementation
  - `CollectionsFactory` interface: Factory for creating Map, List, Set, Queue, Deque, Stack
  - `DefaultCollectionsFactory`: Default implementation using Java concurrent collections
  - `CollectionsConfig`: Configuration for collection creation
  - Supports: ConcurrentHashMap, CopyOnWriteArrayList, ConcurrentSkipListSet, ConcurrentLinkedQueue, ConcurrentLinkedDeque
  - Thread-safe, ordered, and sorted variants supported via configuration

- **Factory Provider**: Central access point for factories
  - `FactoryProvider` component: Selects appropriate factory based on configuration
  - Supports runtime selection of implementations

- **New Configuration Options**:
  ```yaml
  kuber:
    cache:
      cache-implementation: CAFFEINE  # CAFFEINE (default)
      collections-implementation: DEFAULT  # DEFAULT (default)
  ```

### Changed
- **CacheService**: Now uses factory pattern for value caches
  - Injected `FactoryProvider` for cache creation
  - Uses `CacheProxy<String, CacheEntry>` instead of direct `Cache<String, CacheEntry>`
  - Cache implementation can be swapped without code changes

- **Server Info**: Now includes cache and collections implementation types
  - `cacheImplementation`: Current cache type (e.g., "CAFFEINE")
  - `collectionsImplementation`: Current collections type (e.g., "DEFAULT")

- **Autoload Operation Blocking**: Write operations and stats now wait during autoload
  - All write operations (SET, DELETE, EXPIRE, PERSIST, HSET, JSONSET) wait for autoload to complete
  - Stats collection (getRegion, dbSize) waits for accurate counts
  - Region management (deleteRegion, purgeRegion, clearRegionCaches) waits for safety
  - Read operations can proceed during autoload (may return stale data)
  - Added `waitForRegionLoadingIfNeeded()` helper method
  - 60-second timeout with IllegalStateException on timeout

### Architecture
The Factory Pattern enables:
- **Abstraction**: Code doesn't depend on specific cache/collection implementations
- **Extensibility**: New providers can be added (Guava, EhCache, etc.)
- **Testability**: Mock implementations can be injected for testing
- **Configuration**: Implementation can be changed via configuration

```
┌─────────────────┐     ┌─────────────────┐
│  CacheService   │────>│  FactoryProvider│
└─────────────────┘     └─────────────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
            ┌───────▼───────┐     ┌───────▼───────────┐
            │  CacheFactory │     │CollectionsFactory │
            └───────────────┘     └───────────────────┘
                    │                     │
            ┌───────▼───────────┐ ┌───────▼───────────────┐
            │CaffeineCacheFactory│ │DefaultCollectionsFactory│
            └───────────────────┘ └───────────────────────┘
                    │                     │
            ┌───────▼───────┐     ┌───────┴───────────────┐
            │ CacheProxy<K,V>│     │ Map, List, Set,       │
            └───────────────┘     │ Queue, Deque, Stack   │
                    │             └───────────────────────┘
            ┌───────▼───────────┐
            │CaffeineCacheProxy │
            └───────────────────┘
```

- **Autoload Composite Keys**: Allow empty key components
  - One or more components in a composite key can now be empty/null
  - Records are still processed with empty components (e.g., "US//LA" for empty state)
  - Records only skipped when ALL key components are empty
  - Updated `buildCompositeKeyFromCsvRecord` and `buildCompositeKeyFromJsonNode`
  - Added `isKeyEffectivelyEmpty` helper method

- **Autoload Persistence-Only Mode**: Skip value cache during bulk loading
  - Autoload now writes ONLY to KeyIndex and persistence store
  - Value cache is NOT populated during autoload (no eviction interference)
  - Memory stays within configured limits during bulk loads
  - Cache warms naturally through GET operations (lazy loading)
  - No automatic warming after autoload - prevents OOM with large datasets
  - Updated `putEntriesBatch(entries, skipValueCache)` with new boolean parameter
  - Default batch size increased to 32768 records

- **Cache Warming & Reload (Fixed in v1.5.0)**
  - Fixed OOM issues with large datasets during warming/reload
  - Added background warming after autoload (25% of cache capacity)
  - Background warming runs in low-priority thread, doesn't block operations
  - `warmRegionCache()` now uses batched loading (1000 entries per batch)
  - `warmRegionCache()` respects `kuber.cache.max-memory-entries` config limit
  - `warmRegionCacheInBackground()` - new method for async warming
  - `reloadRegionFromPersistence()` simplified - no longer iterates all KeyIndex entries
  - For large datasets, lazy loading via GET is the recommended approach
  - API endpoints still available:
    - `POST /api/regions/{name}/warm` - Warm cache up to config limit
    - `POST /api/regions/{name}/reload` - Clear cache and warm
  - New UI buttons on Regions page and Region Detail page:
    - "Reload from Persistence" - Evict and reload
    - "Warm Cache" - Load additional entries (detail page only)

- **Query & Search Performance (Fixed in v1.5.0)**
  - Fixed slow `searchKeysByRegex()` - now uses KeyIndex (in-memory) for key lookup
  - Fixed slow `jsonSearch()` - now uses KeyIndex and batch loading
  - Removed slow `getNonExpiredKeys()` calls that scanned entire persistence
  - Added batch loading from persistence (`loadEntriesByKeys`) instead of individual gets
  - Search results are cached for future access

- **Base Data Directory Configuration**
  - New `kuber.base.datadir` property (default: `./kuberdata`)
  - All data paths now relative to base directory:
    - `${kuber.base.datadir}/data/kuber.db` - SQLite database
    - `${kuber.base.datadir}/data/rocksdb` - RocksDB data
    - `${kuber.base.datadir}/data/lmdb` - LMDB data
    - `${kuber.base.datadir}/autoload/inbox` - Autoload inbox
    - `${kuber.base.datadir}/autoload/outbox` - Autoload outbox
    - `${kuber.base.datadir}/backup` - Backup files
    - `${kuber.base.datadir}/restore` - Restore files
    - `${kuber.base.datadir}/log/kuber/audit` - Audit logs
    - `${kuber.base.datadir}/archive/kuber-events` - Event archive
    - `${kuber.base.datadir}/kuber.shutdown` - Shutdown signal file
  - Override via command line: `-Dkuber.base.datadir=/your/path`
  - Override via environment: `KUBER_BASE_DATADIR=/your/path`
  - New `DataDirectoryInitializer` component creates directories at startup
  - Updated shutdown scripts to use `./kuberdata` as default directory

### New Files
- `com.kuber.server.startup.DataDirectoryInitializer` - Creates data directories at startup
- `com.kuber.server.factory.CacheProxy` - Cache abstraction interface
- `com.kuber.server.factory.CacheFactory` - Factory interface
- `com.kuber.server.factory.CacheConfig` - Cache configuration
- `com.kuber.server.factory.CaffeineCacheProxy` - Caffeine implementation
- `com.kuber.server.factory.CaffeineCacheFactory` - Caffeine factory
- `com.kuber.server.factory.CollectionsFactory` - Collections factory interface
- `com.kuber.server.factory.CollectionsConfig` - Collections configuration
- `com.kuber.server.factory.DefaultCollectionsFactory` - Default collections factory
- `com.kuber.server.factory.FactoryProvider` - Central factory provider

### Documentation
- **How to Start Kuber Server**: Comprehensive guide added
  - `docs/HOW_TO_START_KUBER_SERVER.md` - Full markdown documentation
  - `templates/help/server-startup.html` - Web UI help page
  - Covers: Prerequisites, building, running methods, configuration, command-line overrides
  - Includes: Environment variables, common examples, troubleshooting

---

## [1.4.2] - 2025-12-07 - BUG FIXES & STATS REFRESH

### Fixed
- **Memory Leak**: Fixed TypeReference anonymous class instantiation inside iteration loops
  - RocksDbPersistenceStore: Added static `MAP_TYPE_REF` constant
  - LmdbPersistenceStore: Added static `MAP_TYPE_REF` constant
  - Reduces heap pressure during backup of large regions
  - Added periodic flush every 50,000 entries during backup

- **RocksDB Restore Lock Error**: Fixed "lock hold by current process" error during restore
  - purgeRegion() now properly stores reopened database handle in regionDatabases map
  - Caused by openRegionDatabase() not returning handle to map after purge

- **Empty Backup Prevention**: Backup now validates region state before proceeding
  - Checks if cache service is initialized
  - Checks if region is still being loaded during startup
  - Checks if region exists
  - Deletes empty backup files and throws error if no entries found

- **CacheMetricsService**: Skip metrics rotation during cache loading and shutdown
  - Added check for cacheService.isInitialized()
  - Added shuttingDown flag check with proper final rotation

- **Metrics Tracking**: All regions now properly registered for metrics tracking
  - Regions registered during initialization and when created
  - Added `initializeRegionMetrics()` to ensure all regions tracked during rotation
  - Fixed "Rotated metrics for 0 regions" issue

- **Thymeleaf Template**: Fixed regions.html data attribute rendering
  - Changed `th:data-region` to `th:attr="data-region=..."` to fix template error
  - Thymeleaf security restrictions prevented string expressions in data attributes

### Added
- **Refresh Stats Button**: Region cards now include "Refresh Stats" button
  - Recalculates entry counts from KeyIndex (source of truth)
  - Updates CacheRegion object with accurate counts
  - Shows warning if KeyIndex is empty but persistence has entries

- **Stats Refresh API Endpoints**:
  - `POST /api/monitoring/stats/refresh/{region}` - Refresh single region
  - `POST /api/monitoring/stats/refresh` - Refresh all regions

- **CacheService.isRegionLoading()**: Exposed method to check if region is loading

- **CacheMetricsService.registerRegion()**: Register region for metrics tracking

- **Diagnostic Logging**: Added warnings in forEachEntry when database not found

### Changed
- Scheduled backup now skips regions that are still loading
- Scheduled backup logs count of skipped regions

---

## [1.4.1] - 2025-12-07 - CRON-BASED BACKUP SCHEDULING

### Changed
- **Cron-Based Backup Scheduling**: Replaced fixed interval with cron expressions
  - Default schedule: 11:00 PM daily (`0 0 23 * * *`)
  - Supports standard cron format (second minute hour day-of-month month day-of-week)
  - Examples: daily at 2 AM, every 6 hours, weekly on Sunday

### Added
- **Admin Dashboard Region Selector**: Backup individual regions or all regions
  - Dropdown to select specific region or "All Regions"
  - Immediate feedback on backup completion
  - Region count displayed for "All Regions" backup

### Configuration
```yaml
kuber:
  backup:
    enabled: true
    backup-directory: ./backup
    restore-directory: ./restore
    cron: "0 0 23 * * *"           # 11:00 PM daily (default)
    max-backups-per-region: 10
    compress: true
    batch-size: 10000
```

### Cron Expression Examples
| Expression | Description |
|------------|-------------|
| `0 0 23 * * *` | 11:00 PM daily (default) |
| `0 0 2 * * *` | 2:00 AM daily |
| `0 0 */6 * * *` | Every 6 hours |
| `0 30 1 * * SUN` | 1:30 AM every Sunday |

---

## [1.4.0] - 2025-12-07 - BACKUP AND RESTORE

### Added
- **Backup Service**: Automatic scheduled backup of all regions
  - Backup files: `<region>.<timestamp>.backup.gz`
  - Optional gzip compression (enabled by default)
  - Automatic cleanup of old backups (configurable retention)
  - Works with RocksDB and LMDB persistence stores

- **Restore Service**: Automatic restore from backup files
  - Place backup file in `./restore` directory
  - Region name inferred from file name
  - Region locked during restore (no read/write operations)
  - Processed files moved to backup directory

- **Region Locking**: During restore, all operations on the region are blocked
  - Prevents data corruption during restore
  - Clear error message to clients

- **BackupRestoreService**: New service class
  - Scheduled backup using Spring TaskScheduler
  - Watches restore directory for files
  - Statistics tracking (total backups, restores, bytes)

- **Admin Dashboard Backup Card**: Manual backup trigger from UI
  - Backup & Restore card with statistics
  - Shows backup statistics and recent backup list
  - Real-time status updates

- **Region-Partitioned Async Executors**: Improved async write architecture
  - 4 single-thread executors for async saves
  - Region name hash determines which executor handles writes
  - All writes for same region are sequential (no race conditions)
  - Different regions can write in parallel (up to 4 concurrent)

### Backup File Format
- JSONL format (one CacheEntry per line)
- Header line with metadata (version, region, timestamp)
- Optional gzip compression

### Restore Process
1. Place backup file in `./restore` directory
2. Service detects file and parses region name
3. Region is locked (operations blocked)
4. Existing data purged
5. Backup data restored in batches
6. Region unlocked
7. Processed file moved to backup directory

### Startup Sequence
Added Phase 6 for BackupRestoreService after AutoloadService.

### New Methods
- `CacheService.getRegionNames()` - Get all region names
- `CacheService.clearRegionCaches(region)` - Clear memory caches for restore
- `CacheService.loadEntriesIntoCache(region, entries)` - Load entries into memory
- `CacheService.isRegionBeingRestored(region)` - Check if region is locked

### Notes
- Only RocksDB and LMDB are supported (SQL databases have their own backup mechanisms)
- MongoDB uses mongodump/mongorestore instead

## [1.3.10] - 2025-12-07 - ASYNC INDIVIDUAL WRITES (DEFAULT)

### Added
- **Configurable Individual Write Mode**: New `kuber.persistence.sync-individual-writes` setting
  - `false` (default): **ASYNC mode** - Memory updated first, disk write in background
  - `true`: **SYNC mode** - Wait for disk write before returning

### Performance Impact

| Mode | Latency | Throughput | Durability |
|------|---------|------------|------------|
| ASYNC (default) | ~0.01-0.1ms | 10,000-100,000 ops/sec | Eventually consistent |
| SYNC | ~1-5ms | 200-1,000 ops/sec | Immediate |

### How ASYNC Mode Works

```
PUT key=X
    │
    ├─► Update KeyIndex (key → BOTH)     ◄── Immediate
    ├─► Update ValueCache (key → entry)  ◄── Immediate (readable now)
    └─► saveEntryAsync(entry)            ◄── Background thread
              │
              └─► Eventually written to disk
```

**Trade-off**: If crash occurs before async write completes, that specific entry is lost.
This is acceptable for most use cases where performance is critical.

### Configuration

```yaml
kuber:
  persistence:
    sync-individual-writes: false  # ASYNC (default, fast)
    # sync-individual-writes: true  # SYNC (durable)
```

### Startup Log
```
Cache service initialized with 5 regions (HYBRID MODE, Individual writes: ASYNC (fast, eventually consistent))
```

### Changed
- **putEntry() method**: Now checks `syncIndividualWrites` configuration
  - ASYNC: Updates memory first, then async disk write
  - SYNC: Disk write first, then memory update (v1.3.8 behavior)

### Notes
- Batch operations (autoload) always use async mode regardless of this setting
- Batch writes already optimized in v1.3.9 with configurable batch size
- This change makes individual interactive PUT/SET operations much faster

## [1.3.9] - 2025-12-06 - BATCH WRITES FOR AUTOLOAD

### Added
- **Batch Writes for Autoload**: Records are now written in batches during autoload for significantly better performance
  - New `kuber.autoload.batch-size` configuration (default: 8192)
  - Records accumulated in memory and flushed to persistence store in batches
  - Works with ALL persistence stores (RocksDB, SQLite, LMDB, PostgreSQL, MongoDB)
  - RocksDB uses native WriteBatch for atomic batch writes
  - SQLite uses batched INSERT statements
  - Typical 10-50x performance improvement for bulk data loading

- **CacheService.putEntriesBatch()**: New public method for batch writes
  - Groups entries by region automatically
  - Saves to persistence store first (data consistency)
  - Updates KeyIndex and value cache for all entries
  - Records batch statistics

- **CacheMetricsService.recordSets()**: New method to record bulk SET operations

- **CacheService.ensureRegionExistsPublic()**: Public method to pre-create regions

### Changed
- **AutoloadService**: Completely rewritten to use batch writes
  - CSV processing: Accumulates entries, flushes every N records
  - JSON processing: Same batch pattern
  - Creates CacheEntry objects directly instead of using jsonSet()
  - Shows batch size in statistics

### Configuration
```yaml
kuber:
  autoload:
    batch-size: 8192  # Records per batch (default)
```

### Performance Impact
| Records | Before (per-record) | After (batched) | Improvement |
|---------|---------------------|-----------------|-------------|
| 10,000  | ~30 seconds        | ~3 seconds      | 10x faster  |
| 100,000 | ~5 minutes         | ~15 seconds     | 20x faster  |
| 1,000,000 | ~50 minutes      | ~2-3 minutes    | 20x faster  |

*Actual performance varies by persistence type and hardware.*

## [1.3.8] - 2025-12-06 - SYNCHRONOUS SAVES & PARALLELISM REMOVED

### Critical Fix
- **Key Index vs Disk Mismatch**: Fixed race condition where keys appeared in index but not on disk
  - Root cause: Keys were added to KeyIndex BEFORE async save completed to RocksDB
  - If read occurred before async save finished, key existed in index but not on disk
  - Fix: Now saves to disk SYNCHRONOUSLY first, then updates KeyIndex
  
### Removed
- **All Parallelism Features Removed**: Parallel processing completely removed for stability
  - Removed `kuber.autoload.parallelism` configuration
  - Removed `kuber.persistence.rocksdb.parallelism` configuration
  - Removed `kuber.persistence.sqlite.parallelism` configuration
  - Removed parallel file processing from AutoloadService
  - Removed parallel region loading from CacheService
  - All operations now run sequentially for data consistency

### Changed
- **putEntry() Save Order**: Critical fix for data consistency
  - Before: keyIndex.put() → cache.put() → persistenceStore.saveEntryAsync()
  - After: persistenceStore.saveEntry() → keyIndex.put() → cache.put()
  - Disk write is now SYNCHRONOUS and happens FIRST
  - KeyIndex only updated AFTER disk write succeeds
  
- **AutoloadService**: Simplified to sequential-only file processing
  - Removed fileProcessorPool executor
  - Files processed one at a time in scheduler thread
  - More predictable and stable behavior

- **CacheService.primeCacheHybrid()**: Simplified to sequential-only
  - Removed getLoadParallelism() method
  - Removed primeCacheHybridConcurrent() method
  - Regions loaded one at a time

### Why This Matters
The async save pattern was:
```java
keyIndex.put(key, BOTH);           // 1. Key marked as "on disk" 
cache.put(key, entry);              // 2. Entry in memory
persistenceStore.saveEntryAsync();  // 3. Async - might not complete!
```

If a GET happened between steps 1 and 3 completing:
- KeyIndex said: "Key exists on disk"
- RocksDB said: "Key not found"
- Result: "Key was in index but not on disk" warning

Now the pattern is:
```java
persistenceStore.saveEntry();       // 1. SYNCHRONOUS disk write
keyIndex.put(key, BOTH);           // 2. Only after disk confirmed
cache.put(key, entry);              // 3. Then memory cache
```

## [1.3.7] - 2025-12-06 - ROCKSDB SHUTDOWN FIX, SEQUENTIAL LOADING & DOUBLE SHUTDOWN GUARD

### Fixed
- **ShutdownInProgress Exception**: Fixed RocksDB `flush()` failing with "ShutdownInProgress" error
  - Root cause: `cancelAllBackgroundWork(true)` was called BEFORE `flush()`, putting RocksDB in shutdown mode
  - Fix: Reordered to flush memtables and sync WAL BEFORE canceling background work
  
- **Async Executor Shutdown Order**: Fixed pending async saves not completing before database close
  - Root cause: `shutdownAsyncExecutor()` was called AFTER database close in all persistence stores
  - Fix: Now called FIRST, before any database operations begin
  
- **Async Save Timeout**: Increased async executor shutdown timeout from 5s to 30s
  - Allows large pending save queues to complete before database close
  - Logs dropped tasks if force shutdown is required

- **Double Shutdown Prevention**: Added `alreadyShutdown` guard to prevent duplicate shutdown calls
  - RocksDB, LMDB, SQLite, PostgreSQL, MongoDB, Memory persistence stores
  - CacheService
  - Spring's @PreDestroy was calling shutdown twice (once from ShutdownOrchestrator, once from bean destruction)

### Changed
- **gracefulCloseDatabase Sequence**: Corrected order of operations
  - Step 1: Flush memtables to SST files (while RocksDB is active)
  - Step 2: Sync WAL
  - Step 3: Final WAL flush with sync
  - Step 4: Wait 500ms for I/O
  - Step 5: Cancel background work (AFTER all data is on disk)
  - Step 6: Wait 500ms for OS buffers
  - Step 7: Close database

- **Sequential Loading by Default**: Changed default parallelism from 4 to 1
  - Note: Parallelism feature completely removed in v1.3.8 for stability
  - All operations now run sequentially

- **AbstractPersistenceStore**: Added `asyncShuttingDown` flag
  - Rejects new async saves during shutdown
  - Prevents race conditions with late-arriving saves

- **All Persistence Stores**: Updated shutdown sequence
  - Step 1: Guard check (alreadyShutdown)
  - Step 2: Set unavailable flag
  - Step 3: Shutdown async executor (wait for pending saves)
  - Step 4: Wait for in-flight operations
  - Step 5: Close databases/connections

### Affected Files
- `RocksDbPersistenceStore.java` - Fixed gracefulCloseDatabase order, added alreadyShutdown guard
- `AbstractPersistenceStore.java` - Added asyncShuttingDown flag, increased timeout
- `LmdbPersistenceStore.java` - Shutdown async executor first, added alreadyShutdown guard
- `SqlitePersistenceStore.java` - Shutdown async executor first, added alreadyShutdown guard
- `PostgresPersistenceStore.java` - Shutdown async executor first, added alreadyShutdown guard
- `MongoPersistenceStore.java` - Shutdown async executor first, added alreadyShutdown guard
- `MemoryPersistenceStore.java` - Shutdown async executor first, added alreadyShutdown guard
- `CacheService.java` - Added alreadyShutdown guard

## [1.3.6] - 2025-12-06 - COMPLETE SHUTDOWN ORCHESTRATION & ROCKSDB DURABILITY

### Added
- **SchedulerConfig**: New configuration class providing controllable ThreadPoolTaskScheduler
  - Custom thread pool with 4 threads
  - Can be shut down programmatically to halt all @Scheduled methods
  - Configurable thread name prefix for debugging

- **Task Scheduler Shutdown**: ShutdownOrchestrator now shuts down Spring TaskScheduler immediately in Phase 0
  - This is the most effective way to stop ALL scheduled tasks at once
  - Prevents tasks like CacheMetricsService from running during shutdown

- **Shutdown Flags**: Added shuttingDown flag to all scheduled services
  - CacheService: cleanupExpiredEntries() checks shutdown flag
  - CacheMetricsService: rotateMetrics() checks shutdown flag
  - MemoryWatcherService: checkMemoryUsage() checks shutdown flag
  - PersistenceExpirationService: cleanupExpiredEntries() checks shutdown flag
  - RocksDbCompactionService: scheduledCompaction() checks shutdown flag
  - ReplicationManager: healthCheck() checks shutdown flag

- **RocksDB Write Protection**: ReadWriteLock prevents writes during shutdown
  - All write operations (saveEntry, saveEntries, deleteEntry, deleteEntries) acquire read lock
  - Shutdown acquires write lock, blocking until all reads complete
  - Prevents concurrent writes during database close

- **RocksDB WriteOptions Sync**: Critical writes now use WriteOptions.setSync(true)
  - Single entry saves are synced immediately
  - Batch operations use WAL (synced at shutdown)
  - Delete operations are synced for durability

### Changed
- **Shutdown Phases Renumbered**: Simplified from 8 phases to 7
  - Phase 0: Signal Shutdown & Stop Scheduler (immediate)
  - Phase 1: Stop Autoload Service
  - Phase 2: Stop Redis Protocol Server
  - Phase 3: Stop Replication Manager
  - Phase 4: Stop Event Publishing
  - Phase 5a/b/c: Pre-Sync, Persist Cache, Post-Sync
  - Phase 6: Close Persistence Store

- **RocksDB Options Enhanced**:
  - Added `setWalTtlSeconds(0)` - Keep WAL until explicitly cleaned
  - Added `setWalSizeLimitMB(0)` - No size limit on WAL
  - Added `setManualWalFlush(false)` - Let RocksDB manage WAL
  - Added `setAvoidFlushDuringShutdown(false)` - Flush during shutdown
  - Added `setAvoidFlushDuringRecovery(false)` - Flush during recovery

- **gracefulCloseDatabase Enhanced**:
  - Added `pauseBackgroundWork()` before canceling
  - Extended OS buffer wait from 500ms to 1000ms
  - Added `continueBackgroundWork()` before close
  - Better error handling for version compatibility

### Fixed
- **CacheMetricsService not stopping**: Now properly stopped by shutting down TaskScheduler
- **Scheduled tasks running during shutdown**: All tasks now check shutdown flag before executing
- **RocksDB corruption on shutdown**: Write lock ensures no concurrent writes during close
- **Missing sync on writes**: WriteOptions now configured for durability
- **ShutdownFileWatcher.disable()**: Fixed to use setEnabled(false)
- **ForkJoinPool.commonPool activity during shutdown**: 
  - Caffeine caches now use synchronous executor (`.executor(Runnable::run)`)
  - Removal listeners check `shuttingDown` flag before processing
  - AbstractPersistenceStore uses dedicated async executor instead of commonPool
  - All persistence stores now shut down the async executor properly
  - CacheService.shutdown() explicitly invalidates and cleans up all Caffeine caches

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
