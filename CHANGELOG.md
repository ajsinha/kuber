# Kuber Distributed Cache - Changelog

All notable changes to this project are documented in this file.

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
