# Kuber Distributed Cache

**High-Performance Distributed Cache with Redis Protocol Support**

Version 1.6.2

Copyright (c) 2025-2030, All Rights Reserved  
Ashutosh Sinha | Email: ajsinha@gmail.com

---

## Overview

Kuber is a powerful, enterprise-grade distributed caching system that provides:

- **Off-Heap Key Index (v1.3.2 - segmented, >2GB)**: Optional DRAM-based key storage outside Java heap - zero GC pressure
- **Hybrid Memory Architecture (v1.2.1)**: All keys always in memory, values can overflow to disk (Aerospike-like)
- **Redis Protocol Compatibility**: Connect using any Redis client
- **Region-Based Organization**: Logical isolation with dedicated database per region
- **JSON Document Support**: Store and query JSON documents with JSONPath
- **Multi-Backend Persistence**: RocksDB (default), LMDB, MongoDB, SQLite, PostgreSQL
- **Event Publishing (v1.2.8)**: Stream cache events to Kafka, RabbitMQ, IBM MQ, ActiveMQ, or files
- **Concurrent Region Processing (v1.3.2)**: Parallel startup compaction and data loading
- **Region Isolation**: Each region gets its own database instance for better concurrency
- **Smart Memory Management**: Global and per-region memory limits with intelligent allocation
- **Pre-Startup Compaction**: RocksDB/SQLite optimized BEFORE Spring context loads
- **Scheduled Compaction**: Additional cron-based compaction (default: 2 AM daily)
- **Primary/Secondary Replication**: Automatic failover via ZooKeeper
- **Autoload**: Bulk data import from CSV, TXT, and JSON files
- **API Key Authentication (v1.2.5)**: Secure programmatic access with revocable keys
- **Web Management UI**: Browser-based administration interface
- **REST API**: Programmatic access for all operations
- **CSV Export**: Export cache data to CSV files

## Features

### Hybrid Memory Architecture (v1.2.1)

Kuber uses an Aerospike-inspired hybrid storage model where **all keys are always kept in memory** while values can overflow to disk:

```
┌─────────────────────────────────────────────────────────────────┐
│              KeyIndex (Always In Memory - Per Region)           │
│  ┌─────────┬──────────────┬─────────┬────────────┬───────────┐ │
│  │   Key   │ ValueInMem?  │  Size   │  ExpiresAt │  Type     │ │
│  │ user:1  │     true     │  1.2KB  │  16:00:00  │  JSON     │ │
│  │ user:2  │     false    │  2.1KB  │     -1     │  STRING   │ │
│  │ user:3  │     true     │  0.5KB  │  17:30:00  │  JSON     │ │
│  └─────────┴──────────────┴─────────┴────────────┴───────────┘ │
├─────────────────────────────────────────────────────────────────┤
│         Value Cache (Memory - Hot Values)                       │
│         Persistence Store (Disk - All Values)                   │
└─────────────────────────────────────────────────────────────────┘
```

| Component | Storage | Purpose |
|-----------|---------|---------|
| KeyIndex | Always in Memory | O(1) existence checks, pattern matching, TTL tracking |
| Hot Values | Memory (Caffeine) | Frequently accessed data |
| Cold Values | Disk Only | Overflow when memory is constrained |
| All Values | Disk | Durability (RocksDB/LMDB/SQLite/MongoDB/PostgreSQL) |

**Performance Benefits:**

| Operation | Before v1.2.1 | After v1.2.1 | Improvement |
|-----------|---------------|--------------|-------------|
| EXISTS | May hit disk | O(1) pure memory | **10-100x faster** |
| KEYS * | Scans disk | O(n) memory scan | **100x+ faster** |
| GET (missing key) | Negative cache (30s TTL) | O(1) instant fail | **Always instant** |
| DBSIZE | O(n) disk scan | O(1) index.size() | **1000x faster** |
| Entry count | Estimate from disk | Exact from index | **Instant & accurate** |

### Core Features

| Feature | Description |
|---------|-------------|
| Redis Protocol | Full RESP protocol support for standard Redis commands |
| Regions | Logical namespaces for cache entries |
| JSON Queries | JSONPath-based search and retrieval |
| TTL Support | Automatic expiration of cache entries |
| Pub/Sub | Event notifications for cache operations |
| Transactions | MULTI/EXEC transaction support |

### Enterprise Features

| Feature | Description |
|---------|-------------|
| Multi-Backend Persistence | RocksDB (default), LMDB, MongoDB, SQLite, PostgreSQL, or in-memory |
| LMDB Support (v1.2.0) | Lightning Memory-Mapped Database with zero-copy reads |
| Region Isolation | Separate database instance per region (RocksDB/LMDB/SQLite) |
| Concurrent Processing (v1.3.0) | Parallel startup compaction and data loading across regions |
| Event Publishing (v1.2.8) | Stream to Kafka, RabbitMQ, IBM MQ, ActiveMQ, or files |
| API Key Auth (v1.2.5) | Secure programmatic access with revocable keys |
| Smart Memory Management | Global cap and per-region limits with proportional allocation |
| Automatic Compaction | Pre-startup compaction before Spring + cron schedule (default: 2 AM daily) |
| Smart Cache Priming | Loads most recently accessed entries first on restart |
| Fast Entry Counts | O(1) entry estimation for dashboard - instant with millions of entries |
| Custom Error Pages | Detailed error information with status-specific suggestions and stack traces |
| SQLite Auto-Vacuum | Runs VACUUM on all SQLite databases at startup |
| ZooKeeper Replication | Automatic primary/secondary failover |
| Autoload | Bulk CSV/TXT/JSON import with metadata |
| CSV Export | Export regions and query results |
| Web UI | Bootstrap-based management dashboard |
| REST API | Full HTTP/JSON API for all operations |
| Authentication | User management and API keys with role-based access |
| Statistics | Comprehensive metrics and monitoring |

## Quick Start

### Prerequisites

- Java 17 or higher
- One of: MongoDB 5.0+, PostgreSQL 14+, or local file system for SQLite/RocksDB/LMDB
- Maven 3.8 or higher
- ZooKeeper 3.8+ (optional, for replication)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/ashutosh/kuber.git
   cd kuber
   ```

2. **Build the project**
   ```bash
   mvn clean package -DskipTests
   ```

3. **Start MongoDB** (if not running)
   ```bash
   mongod --dbpath /data/db
   ```

4. **Run Kuber**
   ```bash
   java -jar kuber-server/target/kuber-server-1.6.1-SNAPSHOT.jar
   ```

5. **Access the Web UI**
   
   Open http://localhost:8080 in your browser
   
   Default credentials: `admin` / `admin123`

6. **Connect via Redis CLI**
   ```bash
   redis-cli -p 6380
   127.0.0.1:6380> PING
   PONG
   127.0.0.1:6380> SET hello world
   OK
   127.0.0.1:6380> GET hello
   "world"
   ```

## Client Libraries

Kuber provides standalone client libraries for Python and Java, each supporting both Redis protocol and REST API.

### Python Clients

**Redis Protocol Client** (`kuber_redis_standalone.py`):
```python
from kuber_redis_standalone import KuberRedisClient

with KuberRedisClient('localhost', 6380) as client:
    # Basic operations
    client.set('key', 'value')
    value = client.get('key')
    
    # Store JSON in specific region
    client.json_set('user:1', {'name': 'Alice', 'age': 30}, region='users')
    
    # Deep JSON search
    results = client.json_search('$.age>25', region='users')
```

**REST API Client** (`kuber_rest_standalone.py`):
```python
from kuber_rest_standalone import KuberRestClient

with KuberRestClient('localhost', 8080, username='admin', password='secret') as client:
    # Basic operations  
    client.set('key', 'value')
    value = client.get('key')
    
    # JSON operations with specific region
    client.json_set('product:1', {'name': 'Laptop', 'price': 999}, region='products')
    
    # Search across regions
    results = client.json_search('$.price>500', region='products')
```

### Java Clients

**Redis Protocol Client** (`KuberClient.java`):
```java
try (KuberClient client = new KuberClient("localhost", 6380)) {
    // Basic operations
    client.set("key", "value");
    String value = client.get("key");
    
    // Region-based JSON storage
    client.selectRegion("products");
    client.jsonSet("prod:1", "{\"name\": \"Widget\", \"price\": 29.99}");
    
    // JSON search
    List<JsonNode> results = client.jsonSearch("$.price<50");
}
```

**REST API Client** (`KuberRestClient.java`):
```java
try (KuberRestClient client = new KuberRestClient("localhost", 8080, "admin", "secret")) {
    // Basic operations
    client.set("key", "value");
    String value = client.get("key");
    
    // JSON with specific region and TTL
    client.jsonSet("order:1", orderObject, "orders", Duration.ofDays(30));
    
    // Cross-region search
    List<JsonNode> results = client.jsonSearch("$.status=shipped", "orders");
}
```

### Client Features

| Feature | Python Redis | Python REST | Java Redis | Java REST |
|---------|:------------:|:-----------:|:----------:|:---------:|
| GET/SET/MGET/MSET | ✓ | ✓ | ✓ | ✓ |
| Key Pattern Search | ✓ | ✓ | ✓ | ✓ |
| Hash Operations | ✓ | ✓ | ✓ | ✓ |
| Region Management | ✓ | ✓ | ✓ | ✓ |
| JSON Storage | ✓ | ✓ | ✓ | ✓ |
| JSON Deep Search | ✓ | ✓ | ✓ | ✓ |
| Cross-Region Search | ✓ | ✓ | ✓ | ✓ |
| TTL Support | ✓ | ✓ | ✓ | ✓ |
| Bulk Operations | - | ✓ | - | ✓ |
| No Dependencies | ✓ | ✓ | - | - |

## Configuration

Configuration is done via `application.yml`:

```yaml
kuber:
  # Network settings
  network:
    port: 6380
    bind-address: 0.0.0.0
    max-connections: 10000
  
  # Cache settings
  cache:
    max-memory-entries: 100000          # Default per-region limit (for value cache)
    global-max-memory-entries: 500000   # Global cap across all regions (0=unlimited)
    region-memory-limits:               # Per-region overrides
      customers: 50000
      products: 200000
    persistent-mode: false
    eviction-policy: LRU
    
    # Factory configuration (v1.6.1)
    cache-implementation: CAFFEINE       # Cache provider: CAFFEINE (default)
    collections-implementation: DEFAULT  # Collections provider: DEFAULT (default)
  
  # Persistence (rocksdb, lmdb, mongodb, postgresql, sqlite, memory)
  persistence:
    type: rocksdb                       # Options: rocksdb, lmdb, mongodb, postgresql, sqlite, memory
    sync-individual-writes: false       # false=ASYNC (fast), true=SYNC (durable) - v1.3.10
    rocksdb:
      path: ./data/rocksdb
      compaction-enabled: true
      compaction-cron: "0 0 2 * * ?"    # 2 AM daily
    lmdb:
      path: ./data/lmdb
      map-size: 1073741824              # 1GB (increase for larger datasets)
  
  # MongoDB settings (if persistence.type=mongodb)
  mongo:
    uri: mongodb://localhost:27017
    database: kuber
  
  # ZooKeeper settings (optional)
  zookeeper:
    enabled: false
    connect-string: localhost:2181
```

### Factory Pattern (v1.6.1)

Kuber uses the Factory Pattern to allow pluggable cache and collections implementations. This enables changing the underlying cache provider or collection types without code changes:

| Property | Default | Description |
|----------|---------|-------------|
| `cache-implementation` | `CAFFEINE` | Cache provider for value caches |
| `collections-implementation` | `DEFAULT` | Collections provider for Map, List, Set, Queue, Deque |

Current implementations:
- **Cache**: CAFFEINE (default) - High-performance Java caching library
- **Collections**: DEFAULT (default) - Uses Java concurrent collections (ConcurrentHashMap, CopyOnWriteArrayList, ConcurrentLinkedQueue, etc.)

### Individual Write Modes (v1.3.10)

Kuber supports two write modes for individual PUT/SET operations:

| Mode | Config Value | Latency | Durability | Use Case |
|------|--------------|---------|------------|----------|
| **ASYNC** (default) | `false` | ~0.01-0.1ms | Eventually consistent | High throughput, performance critical |
| **SYNC** | `true` | ~1-5ms | Immediate | Maximum durability required |

**ASYNC Mode (default)**:
```
PUT key=X → Update Memory → Return Success → Background Disk Write
```
- Memory (KeyIndex + ValueCache) updated immediately
- Disk write happens asynchronously in background
- 10-100x faster than sync mode
- Trade-off: If crash before async write completes, that entry is lost

**SYNC Mode**:
```
PUT key=X → Write to Disk → Wait for fsync → Update Memory → Return Success
```
- Disk write completes before returning
- Maximum durability - survives power loss
- Slower due to fsync overhead

### Persistence Store Comparison

| Store | Speed | Durability | Use Case |
|-------|-------|------------|----------|
| RocksDB | Very Fast | Excellent | Default - production workloads |
| LMDB | Extremely Fast | Excellent | Read-heavy workloads, memory-mapped |
| SQLite | Fast | Good | Simple deployments |
| MongoDB | Fast | Excellent | Document-native, flexible queries |
| PostgreSQL | Fast | Excellent | JSONB support, SQL queries |
| Memory | Fastest | None | Testing, ephemeral data |

### Memory Management (v1.2.0)

Kuber provides flexible memory management:

- **Per-Region Limits**: Configure memory for individual regions via `region-memory-limits`
- **Global Cap**: Set `global-max-memory-entries` to limit total memory across all regions
- **Smart Allocation**: When global cap is exceeded, memory is allocated proportionally based on:
  - 50% configured limits
  - 50% actual data size (persisted entry count)
- **Smart Priming**: On restart, most recently accessed entries are loaded first

## Usage

### Redis Protocol Commands

Connect using any Redis client:

```bash
redis-cli -p 6380
```

#### Standard Commands

```redis
# String operations
SET key value
GET key
MSET k1 v1 k2 v2
MGET k1 k2
INCR counter
DECR counter

# Key operations
DEL key
EXISTS key
EXPIRE key 60
TTL key
KEYS pattern*

# Hash operations
HSET hash field value
HGET hash field
HGETALL hash
```

#### Kuber Extensions

```redis
# Region operations
REGIONS              # List all regions
RCREATE region desc  # Create a region
RSELECT region       # Select a region
RDROP region         # Delete a region
RPURGE region        # Purge a region

# JSON operations
JSET key {"name":"John","age":30}
JGET key
JGET key $.name
JSEARCH $.age>25
JSEARCH $.status=active,$.type=user
```

### REST API

```bash
# Server info
curl http://localhost:8080/api/info

# List regions
curl http://localhost:8080/api/regions

# Get a value
curl http://localhost:8080/api/cache/default/mykey

# Set a value
curl -X PUT http://localhost:8080/api/cache/default/mykey \
  -H "Content-Type: application/json" \
  -d '{"value": "hello world", "ttl": 3600}'

# JSON search
curl -X POST http://localhost:8080/api/cache/default/search \
  -H "Content-Type: application/json" \
  -d '{"query": "$.status=active"}'
```

### Java Client

```java
try (KuberClient client = new KuberClient("localhost", 6380)) {
    // String operations
    client.set("user:1001", "John Doe");
    String name = client.get("user:1001");
    
    // JSON operations
    client.jsonSet("user:1002", "{\"name\": \"Jane\", \"age\": 30}");
    JsonNode user = client.jsonGet("user:1002");
    
    // Region operations
    client.selectRegion("sessions");
    client.set("session:abc", "data", Duration.ofMinutes(30));
}
```

### Python Client

```python
from kuber import KuberClient

with KuberClient('localhost', 6380) as client:
    # String operations
    client.set('user:1001', 'John Doe')
    name = client.get('user:1001')
    
    # JSON operations
    client.json_set('user:1002', {'name': 'Jane', 'age': 30})
    user = client.json_get('user:1002')
    
    # Region operations
    client.select_region('sessions')
    client.set('session:abc', 'data', ttl=timedelta(minutes=30))
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Kuber Server                            │
├──────────────┬──────────────┬──────────────┬────────────────┤
│  Redis       │    REST      │    Web       │  Replication   │
│  Protocol    │    API       │    UI        │  Manager       │
│  (MINA)      │  (Spring)    │ (Thymeleaf)  │  (ZooKeeper)   │
├──────────────┴──────────────┴──────────────┴────────────────┤
│                     Cache Service                            │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────┐  │
│  │   Caffeine  │  │    Event     │  │   JSON Utilities   │  │
│  │   (Memory)  │  │   Publisher  │  │   (JSONPath)       │  │
│  └─────────────┘  └──────────────┘  └────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                   Persistence Layer                          │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              MongoDB Repository                       │   │
│  │   (Regions → Collections, Entries → Documents)        │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Regions

Regions provide logical isolation for cache entries:

- **RocksDB/SQLite**: Each region gets its own dedicated database instance
  - Better concurrency - parallel I/O across regions
  - Better isolation - issues in one region don't affect others
  - Independent compaction - compact individual regions
- **MongoDB**: Each region maps to a separate collection
- The `default` region is captive (cannot be deleted)
- Regions have independent statistics and TTL settings

```
Directory Structure (RocksDB):
./data/rocksdb/
  ├── _metadata/     # Region metadata
  ├── default/       # Default region database
  ├── users/         # Users region database
  └── sessions/      # Sessions region database
```

```redis
RCREATE sessions "User session data"
RSELECT sessions
SET session:abc "{...}"
```

## JSON Queries

Kuber supports JSONPath-like queries for searching JSON documents:

```redis
# Set JSON data
JSET user:1 {"name":"John","age":30,"status":"active"}
JSET user:2 {"name":"Jane","age":25,"status":"inactive"}
JSET user:3 {"name":"Bob","age":35,"status":"active"}

# Search queries
JSEARCH $.status=active           # Find active users
JSEARCH $.age>25                  # Find users over 25
JSEARCH $.name~=J.*               # Regex match on name
JSEARCH $.status=active,$.age>25  # Multiple conditions (AND)
```

Supported operators:
- `=` Equal
- `!=` Not equal
- `>` `<` `>=` `<=` Comparison
- `~=` Regex match
- `contains` String contains
- `startsWith` String starts with
- `endsWith` String ends with
- `exists` Field exists

## Replication

Kuber supports primary/secondary replication using ZooKeeper for leader election:

1. **Enable ZooKeeper** in configuration:
   ```yaml
   kuber:
     zookeeper:
       enabled: true
       connect-string: zk1:2181,zk2:2181,zk3:2181
   ```

2. **Start multiple nodes** - they will automatically elect a primary

3. **Automatic failover** - if primary fails, a secondary is promoted

4. **Read scaling** - secondary nodes handle read requests

## Event Publishing (v1.2.8)

Kuber can stream cache events to external messaging systems for real-time integrations:

### Supported Publishers

| Publisher | Type | Features |
|-----------|------|----------|
| Apache Kafka | `kafka` | High throughput, auto topic creation, producer pooling |
| RabbitMQ | `rabbitmq` | AMQP messaging, exchange routing, channel pooling |
| IBM MQ | `ibmmq` | Enterprise messaging, SSL/TLS, queue manager integration |
| Apache ActiveMQ | `activemq` | JMS messaging, connection pooling |
| File System | `file` | JSON Lines format, date/size rotation |

### Configuration

```yaml
kuber:
  publishing:
    thread-pool-size: 4
    queue-capacity: 10000
    
    # Define reusable brokers
    brokers:
      prod-kafka:
        type: kafka
        kafka:
          bootstrap-servers: kafka1:9092,kafka2:9092
      
      event-files:
        type: file
        file:
          directory: ./log/kuber/events
          rotation-policy: daily
    
    # Configure per-region publishing
    regions:
      customers:
        destinations:
          - broker: prod-kafka
            topic: customer-events
          - broker: event-files
            prefix: customers
```

### Event Format

```json
{
  "key": "user:1001",
  "action": "inserted",
  "region": "customers",
  "payload": {"name": "John", "email": "john@example.com"},
  "timestamp": "2025-12-06T12:00:00Z",
  "nodeId": "kuber-01"
}
```

## Concurrency Safety & Graceful Shutdown (v1.3.3)

Kuber v1.3.3 introduces comprehensive concurrency controls and graceful shutdown orchestration to prevent RocksDB corruption and ensure data integrity.

### Persistence Operation Locking

A centralized `PersistenceOperationLock` prevents concurrent operations that could corrupt the persistence store:

| Operation | Lock Type | Blocks |
|-----------|-----------|--------|
| **Compaction** | Exclusive Global | All other operations |
| **Shutdown** | Exclusive Global | All other operations |
| **Cleanup/Expiration** | Shared Global | Only during exclusive operations |
| **Autoload** | Per-Region | Only same-region operations |
| **Region Loading** | Per-Region | Queries to that region |

### Shutdown Orchestrator

The `ShutdownOrchestrator` ensures orderly shutdown in the **exact reverse order** of startup with configurable delays between phases:

```
Startup Order:                    Shutdown Order (Reverse):
1. Persistence Maintenance  →     5. Close Persistence Store
2. Cache Service           →     4. Persist Cache Data
3. Event Publishing        →     3. Stop Event Publishing
4. Redis Server            →     2. Stop Redis Server
5. Autoload Service        →     1. Stop Autoload Service
```

Each phase has a configurable delay (default: 5 seconds) to ensure:
- In-flight operations complete
- Buffers are flushed
- File handles are properly closed
- No corruption from concurrent shutdown

### Region Loading Guards

During startup, queries to a region that's still loading will **wait** until loading completes (30s timeout). This prevents:
- Queries returning incomplete data
- Race conditions during recovery

## Management Scripts (v1.3.5)

Kuber v1.3.5 provides comprehensive management scripts in the `scripts/` folder for startup, shutdown, and status monitoring.

### Available Scripts

| Script | Platform | Description |
|--------|----------|-------------|
| `kuber-start.sh` | Linux/Mac | Start Kuber server |
| `kuber-start.bat` | Windows | Start Kuber server |
| `kuber-shutdown.sh` | Linux/Mac | Graceful shutdown |
| `kuber-shutdown.bat` | Windows | Graceful shutdown |
| `kuber-status.sh` | Linux/Mac | Check server status |

### Startup Scripts

```bash
# Basic startup
./scripts/kuber-start.sh

# With options
./scripts/kuber-start.sh -m 4g -p prod              # 4GB heap, prod profile
./scripts/kuber-start.sh -d -l /var/log/kuber       # Daemon mode with log dir
./scripts/kuber-start.sh --redis-port 6381          # Custom Redis port
./scripts/kuber-start.sh --debug                     # Enable remote debugging

# Windows
scripts\kuber-start.bat
scripts\kuber-start.bat /memory:4g /profile:prod
```

### Shutdown Methods

| Method | Command | Use Case |
|--------|---------|----------|
| **File-based** | `touch kuber.shutdown` | Local shutdown, scripted |
| **REST API** | `POST /api/admin/shutdown` | Remote shutdown, automation |
| **Signal** | `SIGTERM` / `Ctrl+C` | Container orchestration |
| **Script** | `./scripts/kuber-shutdown.sh` | Convenient wrapper |

### File-Based Shutdown (Recommended)

The simplest way to shutdown Kuber cleanly:

```bash
# Linux/Mac
touch kuber.shutdown

# Windows
echo. > kuber.shutdown

# Or use the provided script
./scripts/kuber-shutdown.sh
```

Kuber watches for this file every 5 seconds. When detected:
1. Initiates graceful shutdown sequence
2. Deletes the shutdown file
3. Completes all phases in order

### REST API Shutdown

For remote or programmatic shutdown:

```bash
# Trigger shutdown
curl -X POST http://localhost:8080/api/admin/shutdown \
  -H "X-API-Key: your-api-key" \
  -H "Content-Type: application/json"

# Check shutdown status
curl http://localhost:8080/api/admin/shutdown/status \
  -H "X-API-Key: your-api-key"

# View shutdown configuration
curl http://localhost:8080/api/admin/shutdown/config \
  -H "X-API-Key: your-api-key"
```

### Shutdown Scripts

Convenient wrapper scripts are provided:

```bash
# Linux/Mac
./scripts/kuber-shutdown.sh                        # File-based (default)
./scripts/kuber-shutdown.sh -a -k myapikey         # API-based
./scripts/kuber-shutdown.sh -r "Maintenance"       # With reason

# Windows
scripts\kuber-shutdown.bat                         # File-based (default)
scripts\kuber-shutdown.bat /api /key:myapikey      # API-based
scripts\kuber-shutdown.bat /reason:"Maintenance"   # With reason
```

### Startup Sequence

```
Application Start
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 1: Infrastructure Setup                               │
│  - Initialize logging & configuration                        │
│  - Connect to ZooKeeper (if replication enabled)            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 2: Persistence Maintenance (Concurrent)               │
│  - Initialize RocksDB/SQLite stores per region              │
│  - Run compaction in parallel                                │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 3: Cache Recovery (Concurrent)                        │
│  - Load persisted entries into memory                        │
│  - Build off-heap key indexes                                │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 4: Service Activation                                 │
│  - Start Event Publishing, Redis Server, HTTP Server        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 5: Background Services - System Ready!                │
└─────────────────────────────────────────────────────────────┘
```

### Shutdown Sequence

```
Shutdown Triggered (file/API/signal)
       │
       ▼
1. Signal Shutdown (block new operations) → Wait 5s
       │
2. Stop Autoload Service → Wait 5s
       │
3. Stop Redis Protocol Server → Wait 5s
       │
4. Stop Event Publishing → Wait 5s
       │
5. Persist Cache Data → Wait 5s
       │
6. Close Persistence Store → Wait 5s
       │
       └── Exit (total ~30 seconds)
```

### Configuration

```yaml
kuber:
  shutdown:
    file-enabled: true              # Enable file-based shutdown
    file-path: kuber.shutdown       # Path to shutdown signal file
    check-interval-ms: 5000         # Check interval (5 seconds)
    api-enabled: true               # Enable REST API shutdown
    phase-delay-seconds: 5          # Delay between shutdown phases
```

## Complete Shutdown Orchestration & RocksDB Durability (v1.3.6)

Kuber v1.3.6 provides comprehensive shutdown orchestration and fixes RocksDB corruption issues.

### Task Scheduler Shutdown

All `@Scheduled` methods are now properly stopped during shutdown:

- **TaskScheduler.shutdown()**: Immediately halts Spring's task scheduler
- **Shutdown flags**: Each service checks `shuttingDown` before execution
- **No more stray executions**: CacheMetricsService, MemoryWatcher, Expiration, Compaction all stop cleanly

### RocksDB Write Protection

Writes are now protected during shutdown using a ReadWriteLock:

```
Write Operation                    Shutdown Operation
     │                                   │
     ▼                                   ▼
┌──────────────────┐            ┌──────────────────┐
│ readLock.lock()  │            │ shuttingDown=true│
│     │            │            │      │           │
│     ▼            │            │      ▼           │
│ checkWriteAllowed│            │ writeLock.lock() │ ← Blocks until all
│     │            │            │      │           │   reads complete
│     ▼            │            │      ▼           │
│ db.put(...)      │            │ gracefulClose()  │
│     │            │            │      │           │
│     ▼            │            │      ▼           │
│ readLock.unlock()│            │ db.close()       │
└──────────────────┘            └──────────────────┘
```

### Enhanced WAL Configuration

RocksDB now uses optimal WAL settings for durability:

```java
Options options = new Options()
    .setWalTtlSeconds(0)              // Keep WAL until cleaned
    .setWalSizeLimitMB(0)             // No size limit
    .setManualWalFlush(false)         // Auto WAL management
    .setAvoidFlushDuringShutdown(false) // Flush on shutdown
    .setAvoidFlushDuringRecovery(false); // Flush on recovery
```

### WriteOptions Sync

Critical writes now use `WriteOptions.setSync(true)`:

```java
try (WriteOptions writeOptions = new WriteOptions()) {
    writeOptions.setSync(true);  // Sync to disk immediately
    regionDb.put(writeOptions, key, value);
}
```

## RocksDB Durability Improvements (v1.3.5)

Kuber v1.3.5 includes critical improvements to RocksDB persistence to prevent data corruption and ensure reliable recovery.

### Graceful Shutdown Sequence

The shutdown process now properly handles RocksDB's background operations:

1. **Cancel Background Work**: Stop all compaction and flush threads
2. **Flush Memtables**: Write in-memory data to SST files
3. **Sync WAL**: Ensure Write-Ahead Log is durable on disk
4. **Final Sync**: Double-sync to ensure OS buffers are flushed
5. **Close Database**: Release file handles

```
┌─────────────────────────────────────────────────────────────┐
│  RocksDB Graceful Close Sequence                             │
├─────────────────────────────────────────────────────────────┤
│  1. cancelAllBackgroundWork(true)  ← Wait for completion    │
│  2. flush(waitForFlush=true)       ← Memtable → SST         │
│  3. syncWal()                      ← WAL to disk            │
│  4. flushWal(sync=true)            ← Final WAL sync         │
│  5. wait(500ms)                    ← OS buffer flush        │
│  6. close()                        ← Release handles        │
└─────────────────────────────────────────────────────────────┘
```

### Corruption Detection and Recovery

If corruption is detected on startup, Kuber automatically attempts recovery:

1. **Backup**: Create backup of corrupted data
2. **Tolerant Open**: Attempt opening with relaxed checks (may recover partial data)
3. **Fresh Start**: As last resort, create fresh database (corrupted data backed up)

```
Corruption Detected on Startup
         │
         ▼
┌─────────────────────────────────────────┐
│  1. Create backup of corrupted data      │
│     → {region}_corrupted_{timestamp}     │
├─────────────────────────────────────────┤
│  2. Open with paranoidChecks=false       │
│     → May recover partial data           │
├─────────────────────────────────────────┤
│  3. Start fresh (data loss)              │
│     → Corrupted data preserved in backup │
└─────────────────────────────────────────┘
```

### All Persistence Stores - Graceful Shutdown

v1.3.5 adds graceful shutdown to ALL persistence stores:

| Store | Shutdown Sequence |
|-------|-------------------|
| **RocksDB** | Cancel background work → Flush memtables → Sync WAL → Close |
| **LMDB** | Sync environments → Close DBI handles → Close environments |
| **SQLite** | WAL checkpoint (TRUNCATE) → Close connections |
| **PostgreSQL** | Wait for active connections → Close HikariCP pool |
| **MongoDB** | Wait for in-flight ops → Spring manages MongoClient |
| **Memory** | Clear maps (data is volatile) |

### Pre/Post Sync During Shutdown

The shutdown orchestrator now includes sync phases:

- **Pre-Sync**: Sync existing data before cache persist
- **Cache Persist**: Save all cache entries
- **Post-Sync**: Sync newly written data
- **Extended Wait**: Double delay before closing

This ensures all data is safely written to disk before the database handles are closed.


## Concurrent Region Processing (v1.3.2)

Kuber v1.3.2 introduces parallel processing for startup operations, significantly reducing startup time for systems with multiple regions.

### Off-Heap Key Index - Segmented Buffers (v1.3.2)

The off-heap key index now supports buffer sizes **exceeding 2GB** using multiple 1GB segments:

| Feature | v1.3.0 | v1.3.2 |
|---------|--------|--------|
| Max Size Per Region | 2047MB (capped) | **8GB+** (configurable) |
| Buffer Architecture | Single ByteBuffer | Multiple 1GB segments |
| Offset Type | int | **long** |

Configuration:
```yaml
kuber:
  cache:
    off-heap-key-index: true
    off-heap-key-index-initial-size-mb: 16
    off-heap-key-index-max-size-mb: 8192  # Now supports >2GB!
```

Segments are allocated dynamically as data grows. Statistics now include `segmentCount` to track allocation.

### What's Processed Concurrently

| Operation | Description | When |
|-----------|-------------|------|
| **Startup Compaction** | RocksDB compactRange() or SQLite VACUUM | Before cache initialization |
| **Data Loading** | Loading keys/values from persistence into memory | During cache priming |

### Why It's Safe

Each region has its own **isolated database instance**:
- **RocksDB**: Separate directory per region (e.g., `./data/rocksdb/customers/`, `./data/rocksdb/orders/`)
- **SQLite**: Separate file per region (e.g., `customers.db`, `orders.db`)

No lock contention between regions - they're completely independent databases.

### Configuration

```yaml
kuber:
  persistence:
    type: rocksdb
    rocksdb:
      path: ./data/rocksdb
      compaction-enabled: true
    
    # Or for SQLite:
    sqlite:
      path: ./data
```

## Autoload - Bulk Data Import

Kuber can automatically load data from CSV and JSON files placed in a watched directory.

### Setup

The autoload service watches a configurable directory (default: `./autoload`) with two subfolders:
- `inbox/` - Place data files here with metadata files
- `outbox/` - Processed files are moved here

### Configuration

```yaml
kuber:
  autoload:
    enabled: true
    directory: ./autoload
    scan-interval-seconds: 60
    batch-size: 8192          # Records per batch for bulk writes (v1.3.9)
    max-records-per-file: 0   # 0 = unlimited
    create-directories: true
    file-encoding: UTF-8
```

Files are processed sequentially with batch writes for optimal performance.
Batch writes provide 10-50x faster loading compared to per-record writes.

### Metadata File Format

Each data file requires a metadata file with the same name plus `.metadata`:

```properties
# users.csv.metadata
region:users
ttl:3600
key_field:user_id
delimiter:,
```

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| region | No | default | Target cache region |
| ttl | No | -1 | Time-to-live in seconds (-1 = no expiration) |
| key_field | **Yes** | - | Column/field name to use as cache key |
| delimiter | No | , | CSV delimiter character |

### CSV File Example

**users.csv**
```csv
user_id,name,email,age,active
1001,John Doe,john@example.com,30,true
1002,Jane Smith,jane@example.com,25,true
1003,Bob Wilson,bob@example.com,35,false
```

**users.csv.metadata**
```properties
region:users
ttl:7200
key_field:user_id
```

Result: Three JSON entries created in "users" region with keys "1001", "1002", "1003".

### JSON File Example (JSONL)

**products.json** (one JSON object per line)
```json
{"sku":"PROD001","name":"Widget","price":29.99}
{"sku":"PROD002","name":"Gadget","price":49.99}
{"sku":"PROD003","name":"Gizmo","price":19.99}
```

**products.json.metadata**
```properties
region:products
ttl:-1
key_field:sku
```

### REST API

```bash
# Get autoload status
curl -u admin:admin123 http://localhost:8080/api/autoload/status

# Trigger immediate scan (Admin only)
curl -X POST -u admin:admin123 http://localhost:8080/api/autoload/trigger
```

### Processed Files

After processing, files are moved to outbox with timestamp and status:
```
20250101_120000_SUCCESS_users.csv
20250101_120000_SUCCESS_users.csv.metadata
20250101_120000_ERROR_NO_KEY_FIELD_data.csv
```

### Replication

Data loaded via autoload is automatically replicated to secondary nodes when replication is enabled.

## Backup and Restore (v1.4.0)

Kuber provides automatic scheduled backup of all regions and automatic restore when backup files are placed in the restore directory. This feature supports RocksDB and LMDB persistence stores only (SQL databases have their own backup mechanisms).

### Configuration

```yaml
kuber:
  backup:
    enabled: true                    # Enable backup/restore service
    backup-directory: ./backup       # Where backup files are stored
    restore-directory: ./restore     # Monitor this for restore files
    cron: "0 0 23 * * *"            # Schedule: 11:00 PM daily (default)
    max-backups-per-region: 10       # Keep last 10 backups per region
    compress: true                   # Gzip compression (recommended)
    batch-size: 10000               # Entries per batch during backup/restore
```

### Cron Expression Examples

| Expression | Description |
|------------|-------------|
| `0 0 23 * * *` | 11:00 PM daily (default) |
| `0 0 2 * * *` | 2:00 AM daily |
| `0 0 */6 * * *` | Every 6 hours |
| `0 30 1 * * SUN` | 1:30 AM every Sunday |

### Backup

Backups run automatically according to the cron schedule. Each region is backed up to a separate file:

```
./backup/
├── customers.20251207_230000.backup.gz
├── products.20251207_230003.backup.gz
└── default.20251207_230005.backup.gz
```

File format is JSONL (one CacheEntry per line) with optional gzip compression.

#### Manual Backup

Trigger immediate backup via:

1. **Admin Dashboard**: Select a region (or "All Regions") and click the "Backup" button
2. **REST API**:
```bash
# Backup all regions
curl -X POST http://localhost:8080/api/admin/backup -H "X-API-Key: your-key"

# Backup single region
curl -X POST http://localhost:8080/api/admin/backup/customers -H "X-API-Key: your-key"
```

### Restore

To restore a region:

1. Place backup file in `./restore` directory
2. Service detects file within 30 seconds
3. Region name inferred from file name
4. **Region is LOCKED** - all operations blocked
5. Existing data purged
6. Backup data restored in batches
7. Region unlocked
8. Processed file moved to backup directory

```bash
# Restore a region
cp ./backup/customers.20251207_143022.backup.gz ./restore/

# Watch logs for progress
tail -f logs/kuber.log
```

### Region Locking During Restore

During restore, all GET, SET, DELETE operations on the region return an error:
```
"Region 'customers' is currently being restored. Please wait for restore to complete."
```

### Supported Stores

| Store | Supported | Notes |
|-------|-----------|-------|
| RocksDB | ✓ | Full support |
| LMDB | ✓ | Full support |
| SQLite | ✗ | Use `.backup` command |
| PostgreSQL | ✗ | Use `pg_dump` |
| MongoDB | ✗ | Use `mongodump` |

## Security

### Authentication

Users are configured in `users.json` file with cleartext credentials for simplicity:

```json
{
  "users": [
    {
      "userId": "admin",
      "password": "admin123",
      "fullName": "System Administrator",
      "roles": ["ADMIN", "OPERATOR", "USER"]
    }
  ]
}
```

Default users:
- `admin / admin123` - Full system access
- `operator / operator123` - Region and cache management
- `user / user123` - Read/write cache entries
- `readonly / readonly123` - Read-only access

### Roles

| Role | Permissions |
|------|-------------|
| ADMIN | Full access, user management |
| OPERATOR | Create/delete regions, purge |
| USER | Read/write cache entries |
| READONLY | Read-only access |

## Monitoring

### Actuator Endpoints

- `/actuator/health` - Health status
- `/actuator/info` - Application info
- `/actuator/metrics` - Metrics

### Cache Statistics

```redis
INFO                    # Server info
STATUS                  # Node status
REPLINFO               # Replication info
DBSIZE                 # Entry count
```

## Project Structure

```
kuber/
├── kuber-core/           # Core models and protocols
├── kuber-server/         # Spring Boot server
├── kuber-client-java/    # Java client library
├── kuber-client-python/  # Python client library
├── pom.xml               # Parent POM
└── README.md             # This file
```

## Building

```bash
# Build all modules
mvn clean package

# Run tests
mvn test

# Create distribution archive
mvn package -Pdist
```

## License

Copyright © 2025-2030, All Rights Reserved

This software is proprietary and confidential. Unauthorized copying, distribution, modification, or use is strictly prohibited without explicit written permission from the copyright holder.

**Patent Pending**: Certain architectural patterns and implementations are subject to patent applications.

## Support

For support inquiries, please contact: ajsinha@gmail.com
