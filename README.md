# Kuber Distributed Cache

**High-Performance Distributed Cache with Redis Protocol Support**

Version 1.7.6

Copyright (c) 2025-2030, All Rights Reserved  
Ashutosh Sinha | Email: ajsinha@gmail.com

---

## Overview

Kuber is a powerful, enterprise-grade distributed caching system that provides:

- **Role-Based Access Control (v1.7.3)**: Enterprise RBAC with region-specific permissions (READ/WRITE/DELETE)
- **Off-Heap Key Index (v1.3.2 - segmented, >2GB)**: Optional DRAM-based key storage outside Java heap - zero GC pressure
- **Hybrid Memory Architecture (v1.2.1)**: All keys always in memory, values can overflow to disk (Aerospike-like)
- **Redis Protocol Compatibility**: Connect using any Redis client
- **Region-Based Organization**: Logical isolation with dedicated database per region
- **JSON Document Support**: Store and query JSON documents with JSONPath
- **Multi-Backend Persistence**: LMDB (default), RocksDB, MongoDB, SQLite, PostgreSQL
- **Event Publishing (v1.2.8)**: Stream cache events to Kafka, RabbitMQ, IBM MQ, ActiveMQ, or files
- **Request/Response Messaging (v1.7.1)**: Access cache via message brokers with async processing, backpressure, and broker controls
- **Concurrent Region Processing (v1.3.2)**: Parallel startup compaction and data loading
- **Region Isolation**: Each region gets its own database instance for better concurrency
- **Smart Memory Management**: Global and per-region memory limits with intelligent allocation
- **Pre-Startup Compaction**: RocksDB/SQLite optimized BEFORE Spring context loads
- **Scheduled Compaction**: Additional cron-based compaction (default: 2 AM daily)
- **Primary/Secondary Replication**: Automatic failover via ZooKeeper
- **Autoload**: Bulk data import from CSV, TXT, and JSON files
- **API Key Authentication (v1.2.5)**: Secure programmatic access with revocable keys
- **Web Management UI**: Browser-based administration interface with comprehensive help system
- **REST API**: Programmatic access for all operations
- **CSV Export**: Export cache data to CSV files
- **Multi-Language Clients (v1.7.1)**: Python, Java, and C# client libraries

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
| Multi-Backend Persistence | LMDB (default), RocksDB, MongoDB, SQLite, PostgreSQL, or in-memory |
| LMDB Support (v1.2.0) | Lightning Memory-Mapped Database with zero-copy reads |
| Region Isolation | Separate database instance per region (RocksDB/LMDB/SQLite) |
| Concurrent Processing (v1.3.0) | Parallel startup compaction and data loading across regions |
| Key Length Validation | Configurable max key length (default: 256 bytes) with logging |
| Event Publishing (v1.2.8) | Stream to Kafka, RabbitMQ, IBM MQ, ActiveMQ, or files |
| Request/Response (v1.7.1) | Cache access via message brokers with backpressure and broker controls |
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
| Web UI | Bootstrap-based management dashboard with comprehensive help |
| REST API | Full HTTP/JSON API for all operations |
| Authentication | User management and API keys with role-based access |
| Statistics | Comprehensive metrics and monitoring |

### Memory Management (v1.7.4)

Kuber provides dual eviction strategies for optimal memory management:

| Strategy | Trigger | Default | Description |
|----------|---------|---------|-------------|
| **Memory Pressure** | Heap > 85% | Enabled | Reactive - evicts when JVM heap is constrained |
| **Count-Based** | Values > limit | Enabled | Proactive - limits values per region |

**Count-Based Limiting (v1.7.4):**

The effective limit per region is the **LOWER** of:
- `valueCacheMaxPercent` % of total keys (default: 20%)
- `valueCacheMaxEntries` absolute max (default: 10,000)

| Region Keys | 20% Limit | Max Entries | Effective Limit |
|-------------|-----------|-------------|-----------------|
| 100,000 | 20,000 | 10,000 | **10,000** |
| 1,000 | 200 | 10,000 | **200** |

**Configuration:**
```properties
# Memory Pressure Eviction
kuber.cache.memory-watcher-enabled=true
kuber.cache.memory-high-watermark-percent=85
kuber.cache.memory-low-watermark-percent=50

# Count-Based Eviction (v1.7.4)
kuber.cache.value-cache-limit-enabled=true
kuber.cache.value-cache-max-percent=20
kuber.cache.value-cache-max-entries=10000
```

### Warm Objects (v1.7.6)

Kuber can maintain a minimum number of "warm" (in-memory) objects per region, ensuring frequently accessed data stays in memory for optimal read performance:

| Component | Purpose |
|-----------|---------|
| **WarmObjectService** | Loads values to meet minimum threshold (floor) |
| **ValueCacheLimitService** | Evicts values to stay under max limit (ceiling) |
| **MemoryWatcherService** | Evicts during heap pressure (takes priority) |

**Configuration:**
```properties
# Enable warm object maintenance
kuber.cache.warm-objects-enabled=true
kuber.cache.warm-object-check-interval-ms=60000
kuber.cache.warm-object-load-batch-size=1000

# Per-region warm object counts
kuber.cache.region-warm-object-counts.trade=100000
kuber.cache.region-warm-object-counts.reference=50000
kuber.cache.region-warm-object-counts.session=10000
```

**Benefits:**
- Frequently accessed data stays warm in memory
- Different workloads get appropriate cache sizes
- Background service handles loading automatically
- Works with eviction services (respects memory limits)
- Falls back to default behavior if not configured

### Prometheus Monitoring (v1.7.6)

Kuber integrates with Prometheus for comprehensive metrics monitoring:

```properties
# Enable Prometheus endpoint
kuber.prometheus.enabled=true
kuber.prometheus.update-interval-ms=5000

# Expose actuator endpoints
management.endpoints.web.exposure.include=health,info,metrics,prometheus
```

**Key Metrics:**

| Metric | Description |
|--------|-------------|
| `kuber_cache_hit_rate` | Cache hit rate (0.0 to 1.0) |
| `kuber_cache_gets_total` | Total GET operations |
| `kuber_cache_sets_total` | Total SET operations |
| `kuber_heap_usage_ratio` | JVM heap usage ratio |
| `kuber_region_keys{region}` | Keys per region |
| `kuber_total_keys` | Total keys all regions |

**Prometheus scrape config:**
```yaml
scrape_configs:
  - job_name: 'kuber-cache'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets: ['kuber-server:8080']
```

See [docs/PROMETHEUS.md](docs/PROMETHEUS.md) for full documentation including Grafana dashboards and alerting rules.

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

3. **Run Kuber**
   ```bash
   # Required JVM options for LMDB persistence support on Java 9+
   java --add-opens=java.base/java.nio=ALL-UNNAMED \
        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
        -jar kuber-server/target/kuber-server-1.7.3-SNAPSHOT.jar
   ```
   
   Or use the startup script which includes all required JVM options:
   ```bash
   ./scripts/kuber-start.sh
   ```

4. **Access the Web UI**
   
   Open http://localhost:8080 in your browser
   
   Default credentials: `admin` / `admin123`

5. **Connect via Redis CLI**
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

Kuber provides client libraries for **Python**, **Java**, and **C#**, each supporting Redis protocol, REST API, and messaging patterns.

### Python Client

**Redis Protocol** (`kuber_redis_standalone.py`):
```python
from kuber_redis_standalone import KuberRedisClient

# Connect with API key authentication
with KuberRedisClient('localhost', 6380, api_key='kub_your_key') as client:
    # Basic operations
    client.set('key', 'value')
    value = client.get('key')
    
    # Store JSON in specific region
    client.json_set('user:1', {'name': 'Alice', 'age': 30, 'temp': 'x'}, region='users')
    
    # Update/merge JSON (new in v1.7.2)
    client.json_update('user:1', {'age': 31, 'city': 'NYC'}, region='users')
    
    # Remove attributes from JSON (new in v1.7.2)
    client.json_remove('user:1', ['temp'], region='users')
    
    # Deep JSON search
    results = client.json_search('$.age>25', region='users')
```

**REST API** (`kuber_rest_standalone.py`):
```python
from kuber_rest_standalone import KuberRestClient

# Using API key (recommended)
with KuberRestClient('localhost', 8080, api_key='kub_your_key') as client:
    # Basic operations  
    client.set('key', 'value')
    value = client.get('key')
    
    # JSON operations with specific region
    client.json_set('product:1', {'name': 'Laptop', 'price': 999}, region='products')
```

**Messaging** (`examples/messaging_example.py`):
```python
from kuber.messaging import KuberMessagingClient

client = KuberMessagingClient(api_key='your-api-key')

# Build request message
request = client.build_get_request('user:123', region='users')

# Send via Kafka/RabbitMQ/ActiveMQ
producer.send('ccs_cache_request', json.dumps(request))
```

### Java Client

**Redis Protocol** (`KuberClient.java`):
```java
// Connect with API key authentication
try (KuberClient client = new KuberClient("localhost", 6380, "kub_your_key")) {
    // Basic operations
    client.set("key", "value");
    String value = client.get("key");
    
    // Region-based JSON storage
    client.selectRegion("products");
    client.jsonSet("prod:1", "{\"name\": \"Widget\", \"price\": 29.99, \"temp\": \"x\"}");
    
    // Update/merge JSON (new in v1.7.2)
    client.jsonUpdate("prod:1", "{\"price\": 39.99, \"stock\": 100}");
    
    // Remove attributes from JSON (new in v1.7.2)
    client.jsonRemove("prod:1", "temp");
    
    // JSON search
    List<JsonNode> results = client.jsonSearch("$.price<50");
}
```

**REST API** (`KuberRestClient.java`):
```java
try (KuberRestClient client = new KuberRestClient("localhost", 8080, "admin", "secret")) {
    // Basic operations
    client.set("key", "value");
    String value = client.get("key");
    
    // JSON with specific region and TTL
    client.jsonSet("order:1", orderObject, "orders", Duration.ofDays(30));
}
```

**Messaging** (`KuberMessagingExample.java`):
```java
RequestBuilder builder = new RequestBuilder("your-api-key");

// Build request
CacheRequest request = builder.get("user:123").inRegion("users").build();

// Send via Kafka
producer.send("ccs_cache_request", objectMapper.writeValueAsString(request));
```

### C# / .NET Client (New in v1.7.1)

**Redis Protocol** (`KuberClient.cs`):
```csharp
using Kuber.Client;

using var client = new KuberClient("localhost", 6380);

// Basic operations
await client.SetAsync("key", "value");
var value = await client.GetAsync("key");

// JSON operations with region
await client.JsonSetAsync("user:1", new { Name = "Alice", Age = 30 }, "users");
var results = await client.JsonSearchAsync<User>("$.age>25", "users");
```

**REST API** (`KuberRestClient.cs`):
```csharp
using var client = new KuberRestClient("localhost", 8080, apiKey: "your-api-key");

// Basic operations
await client.SetAsync("key", "value");
var value = await client.GetAsync("key");

// JSON operations
await client.JsonSetAsync("product:1", product, "products");
```

**Messaging** (`KuberMessagingClient.cs`):
```csharp
var messagingClient = new KuberMessagingClient("your-api-key");

// Build request
var request = messagingClient.BuildGetRequest("user:123", "users");

// Send via message broker
await producer.SendAsync("ccs_cache_request", JsonSerializer.Serialize(request));
```

### Client Features Matrix

| Feature | Python Redis | Python REST | Java Redis | Java REST | C# Redis | C# REST |
|---------|:------------:|:-----------:|:----------:|:---------:|:--------:|:-------:|
| GET/SET/MGET/MSET | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Key Pattern Search | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Hash Operations | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Region Management | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| JSON Storage | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| JSON Deep Search | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| TTL Support | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Messaging Support | ✓ | - | ✓ | - | ✓ | - |
| Async/Await | - | - | - | - | ✓ | ✓ |
| No Dependencies | ✓ | ✓ | - | - | - | - |

## Request/Response Messaging (v1.7.1)

Access cache operations via message brokers for decoupled, asynchronous architectures.

### Supported Brokers

| Broker | Status | Features |
|--------|--------|----------|
| Apache Kafka | ✓ | High throughput, partitioning, consumer groups |
| Apache ActiveMQ | ✓ | JMS support, durable subscriptions |
| RabbitMQ | ✓ | AMQP, flexible routing, acknowledgments |
| IBM MQ | ✓ | Enterprise messaging, SSL/TLS |

### Message Format

**Request:**
```json
{
  "api_key": "your-api-key",
  "message_id": "uuid-12345",
  "operation": "GET",
  "region": "users",
  "key": "user:123"
}
```

**Response:**
```json
{
  "request_receive_timestamp": "2025-12-10T10:00:00Z",
  "response_time": "2025-12-10T10:00:00.005Z",
  "processing_time_ms": 5,
  "request": { ... },
  "response": {
    "success": true,
    "result": "{\"name\": \"Alice\", \"age\": 30}"
  }
}
```

### Supported Operations

| Category | Operations |
|----------|------------|
| Strings | GET, SET, DELETE |
| Batch | MGET, MSET |
| Keys | KEYS, EXISTS, TTL, EXPIRE |
| Hashes | HGET, HSET, HGETALL, HMSET |
| JSON | JSET, JGET, JSEARCH |
| Admin | PING, INFO, REGIONS |

### Global Service Control (v1.7.1)

Enable or disable the entire Request/Response Messaging feature:

| Method | How | Persisted |
|--------|-----|-----------|
| Admin UI | Admin → Messaging → Enable/Disable Service | Yes |
| REST API | `POST /api/v1/messaging/enable` or `/disable` | Yes |
| JSON Config | `request_response.json`: `"enabled": true/false` | Yes |

When disabled globally, all brokers disconnect and messages queue at the broker until re-enabled.

### Broker Control (v1.7.1)

Manage broker connections dynamically from the Admin UI:

| Action | Description |
|--------|-------------|
| **Enable** | Connect to a disabled broker and start consuming |
| **Disable** | Disconnect and stop consuming (updates config) |
| **Pause** | Stop consuming but keep connection open |
| **Resume** | Resume consuming after pause |
| **Reconnect** | Retry connection for failed brokers |

### Configuration

Create `secure/request_response.json`:

```json
{
  "enabled": true,
  "max_queue_depth": 100,
  "thread_pool_size": 10,
  "logging_enabled": true,
  "max_log_messages": 1000,
  "brokers": {
    "kafka_primary": {
      "enabled": true,
      "type": "kafka",
      "display_name": "Primary Kafka",
      "connection": {
        "bootstrap_servers": "kafka:9092",
        "group_id": "kuber-processor"
      },
      "request_topics": ["ccs_cache_request"]
    }
  }
}
```

### Test Clients (v1.7.6)

Ready-to-use test clients are provided for all brokers in Python, Java, and C#:

| Broker | Python | Java | C# |
|--------|--------|------|-----|
| **Kafka** | `kafka_request_response_test.py` | `KafkaRequestResponseTest.java` | `KafkaRequestResponseTest.cs` |
| **ActiveMQ** | `activemq_request_response_test.py` | `ActiveMqRequestResponseTest.java` | `ActiveMqRequestResponseTest.cs` |
| **RabbitMQ** | `rabbitmq_request_response_test.py` | `RabbitMqRequestResponseTest.java` | `RabbitMqRequestResponseTest.cs` |
| **IBM MQ** | `ibmmq_request_response_test.py` | `IbmMqRequestResponseTest.java` | `IbmMqRequestResponseTest.cs` |

**Location:**
- Python: `kuber-client-python/examples/`
- Java: `kuber-client-java/examples/` (standalone, not compiled with main library)
- C#: `kuber-client-csharp/examples-standalone/` (standalone, not compiled with main library)

**Run Python Test Clients:**
```bash
# Install dependencies
pip install kafka-python stomp.py pika pymqi

# Run tests
cd kuber-client-python/examples
python kafka_request_response_test.py
python activemq_request_response_test.py    # STOMP on port 61613
python rabbitmq_request_response_test.py
python ibmmq_request_response_test.py       # Requires MQ client
```

**Kafka Diagnostics Tool:**
```bash
# Check Kafka connectivity and message flow
python kafka_diagnostics.py                 # Basic diagnostics
python kafka_diagnostics.py --from-beginning --count 20  # Read messages
python kafka_diagnostics.py --live --watch-only          # Live watch mode
```

> **Note:** ActiveMQ Python client uses STOMP protocol on port 61613, not OpenWire on port 61616.

### Request/Response Logging (v1.7.6)

All request/response pairs can be logged to files for debugging and auditing:

| Setting | Default | Description |
|---------|---------|-------------|
| `logging_enabled` | true | Enable/disable request/response logging |
| `max_log_messages` | 1000 | Messages per file before rolling |

**Features:**
- **Async Writing**: Non-blocking file writes to avoid impacting message processing
- **Rolling Files**: Up to 10 file versions per broker/topic (oldest deleted automatically)
- **JSON Format**: Easy to parse and analyze
- **Web UI**: View logs at `/admin/messaging/logs` with broker/topic filters

**Log Location:**
```
<secure_folder>/request_response/<broker_name>/<topic>_YYYYMMDD_HHMMSS.json
```

**API Endpoints:**
```bash
GET /api/v1/messaging/logs/brokers                     # List brokers with logs
GET /api/v1/messaging/logs/brokers/{broker}/topics     # List topics for broker
GET /api/v1/messaging/logs/brokers/{broker}/topics/{topic}/messages  # Get messages
GET /api/v1/messaging/logs/stats                       # Get logging statistics
```

## JVM Requirements

Kuber requires specific JVM options for certain features:

### Required for LMDB Persistence

When using LMDB as the persistence store (Java 9+), the following JVM arguments are **required**:

```bash
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

These allow the LMDB Java library to access internal ByteBuffer fields for direct memory operations.

### Complete JVM Options

For production deployments, use the startup scripts which include all required options:

```bash
# Linux/macOS
./scripts/kuber-start.sh -m 4g -d

# Windows
kuber-start.bat /memory:4g
```

Or specify options manually:

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -Xms4g -Xmx4g \
     -XX:+UseG1GC \
     -jar kuber-server.jar
```

| Option | Purpose |
|--------|---------|
| `--add-opens=java.base/java.nio=ALL-UNNAMED` | Required for LMDB persistence (Java 9+) |
| `--add-opens=java.base/sun.nio.ch=ALL-UNNAMED` | Required for LMDB persistence (Java 9+) |
| `-Xms4g -Xmx4g` | Heap size (adjust based on data size) |
| `-XX:+UseG1GC` | Recommended garbage collector |

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
    max-memory-entries: 100000
    global-max-memory-entries: 500000
    off-heap-key-index: false
  
  # Persistence (LMDB is default and recommended)
  persistence:
    type: lmdb
    lmdb:
      path: ./data/lmdb
      map-size: 1099511627776  # 1TB (virtual address space, not disk)
  
  # Security
  secure:
    folder: ./secure
  
  # Backup
  backup:
    enabled: true
    cron: "0 0 23 * * *"
    backup-directory: ./backup
    restore-directory: ./restore
  
  # Compaction
  compaction:
    pre-startup-enabled: true
    scheduled-enabled: true
    cron: "0 0 2 * * *"
```

## Web Management UI

Kuber provides a comprehensive web-based administration interface:

### Dashboard Features
- Real-time cache statistics
- Region management
- Entry browsing and editing
- JSON query interface
- Backup/restore controls

### Admin Panel
- API key management
- User administration
- Messaging broker management with live controls
- Configuration viewer

### Help System (v1.7.1)
Comprehensive documentation accessible at `/help`:

| Section | Topics |
|---------|--------|
| Getting Started | Overview, Quick Start, Configuration |
| Operations | String, Hash, JSON, Key, TTL, Batch |
| Client Libraries | Python, Java, C# with examples |
| Advanced | Regions, Search, Replication, Messaging |
| Reference | REST API, Redis Protocol, Glossary |

## Event Publishing (v1.2.8)

Stream cache events to external systems for real-time integrations:

```yaml
kuber:
  publishing:
    kafka:
      enabled: true
      bootstrap-servers: kafka:9092
      regions:
        users: user-events
        products: product-events
```

### Event Format

```json
{
  "key": "user:1001",
  "action": "inserted",
  "region": "customers",
  "payload": { "name": "John", "email": "john@example.com" },
  "timestamp": "2025-12-06T12:00:00Z",
  "nodeId": "kuber-01"
}
```

## Autoload

Bulk import data from CSV, TXT, and JSON files:

### Metadata File Format

Each data file requires a metadata file with the same name plus `.metadata`:

```properties
# users.csv.metadata
region:users
ttl:3600
key_field:user_id
delimiter:,
```

### Example Files

**users.csv**
```csv
user_id,name,email,age,active
1001,John Doe,john@example.com,30,true
1002,Jane Smith,jane@example.com,25,true
```

## Backup and Restore (v1.4.0)

Automatic scheduled backup and restore for RocksDB and LMDB:

```yaml
kuber:
  backup:
    enabled: true
    backup-directory: ./backup
    restore-directory: ./restore
    cron: "0 0 23 * * *"
    max-backups-per-region: 10
    compress: true
```

## Security

### Role-Based Access Control (RBAC) - v1.7.3

Kuber implements enterprise-grade RBAC with region-specific permissions:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Authorization Flow                            │
│                                                                  │
│   User ──> Roles ──> Permissions ──> Regions                    │
│                                                                  │
│   admin ──> [admin] ──> ALL ──> *                               │
│   operator ──> [default_full, test_readwrite] ──> R/W/D, R/W    │
│   readonly ──> [default_readonly] ──> READ ──> default          │
└─────────────────────────────────────────────────────────────────┘
```

### Permission Types

| Permission | Operations | Description |
|------------|------------|-------------|
| `READ` | GET, EXISTS, KEYS, SCAN, SEARCH, JGET, JSEARCH | View and search entries |
| `WRITE` | SET, SETEX, INCR, APPEND, HSET, JSET, JUPDATE | Create and update entries |
| `DELETE` | DEL, HDEL, JDEL, JREMOVE, FLUSHDB | Remove entries |
| `ADMIN` | Region create/delete, user/role management | Full system control |

### Configuration Files

**users.json** (secure/users.json):

```json
{
  "users": [
    {
      "userId": "admin",
      "password": "admin123",
      "fullName": "System Administrator",
      "email": "admin@localhost",
      "roles": ["admin"],
      "enabled": true,
      "systemUser": true
    },
    {
      "userId": "operator",
      "password": "operator123",
      "fullName": "Cache Operator",
      "roles": ["default_full", "test_readwrite"],
      "enabled": true
    }
  ]
}
```

**roles.json** (secure/roles.json):

```json
{
  "roles": [
    {
      "name": "admin",
      "displayName": "System Administrator",
      "region": "*",
      "permissions": ["READ", "WRITE", "DELETE", "ADMIN"],
      "systemRole": true
    },
    {
      "name": "default_readonly",
      "displayName": "Default Read Only",
      "region": "default",
      "permissions": ["READ"]
    },
    {
      "name": "default_full",
      "displayName": "Default Full Access",
      "region": "default",
      "permissions": ["READ", "WRITE", "DELETE"]
    }
  ]
}
```

### Role Naming Convention

| Pattern | Example | Description |
|---------|---------|-------------|
| `admin` | `admin` | Reserved system admin role |
| `{region}_readonly` | `customers_readonly` | Read-only access to region |
| `{region}_readwrite` | `customers_readwrite` | Read and write access |
| `{region}_full` | `customers_full` | Full access (read, write, delete) |

### API Keys

Generate API keys from Admin UI for programmatic access:

```bash
# Using API key header
curl -H "X-API-Key: kub_your_api_key" http://localhost:8080/api/v1/cache/default/key

# Using Authorization header
curl -H "Authorization: ApiKey kub_your_api_key" http://localhost:8080/api/v1/cache/default/key
```

API keys inherit permissions from the associated user's roles.

### RBAC Configuration Properties

```properties
# Enable RBAC authorization (default: true)
kuber.security.rbac-enabled=true

# Auto-create region roles when new region is created (default: true)
kuber.security.auto-create-region-roles=true

# Configuration file paths
kuber.security.users-file=${kuber.secure.folder}/users.json
kuber.security.roles-file=${kuber.secure.folder}/roles.json
kuber.security.api-keys-file=${kuber.secure.folder}/apikeys.json
```

### Hot Reload

Security configuration files are automatically reloaded without server restart:

- **Change Detection**: Every 30 seconds
- **Linked Reload**: When `users.json` OR `roles.json` changes, BOTH are reloaded
- **Admin UI**: Immediate reload via "Reload" button

### Default File Creation

If configuration files are missing at startup:

| Missing File | Default Created |
|--------------|-----------------|
| `users.json` | Admin user only (password: `admin123`) |
| `roles.json` | Admin role only |

> ⚠️ **CRITICAL**: Change default admin password immediately!

### Sample Files

Comprehensive examples available in `secure-sample/`:

```bash
cp secure-sample/users.json.sample secure/users.json
cp secure-sample/roles.json.sample secure/roles.json
# Edit with your configuration
```

### Authorization Enforcement

RBAC is enforced on:

| Layer | Operations |
|-------|------------|
| **Web UI** | Cache browsing, region management, inserts/deletes |
| **REST API** | All `/api/*` endpoints |
| **Redis Protocol** | All commands after AUTH |

Users only see regions they have access to in the UI.

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
├── kuber-core/              # Core models and protocols
├── kuber-server/            # Spring Boot server
├── kuber-client-java/       # Java client library
├── kuber-client-python/     # Python client library
├── kuber-client-csharp/     # C# / .NET client library (v1.7.1)
├── docs/                    # Documentation
│   ├── ARCHITECTURE.md
│   ├── CLIENT_USAGE.md
│   └── HOW_TO_START_KUBER_SERVER.md
├── scripts/                 # Startup/shutdown scripts
├── pom.xml                  # Parent POM
└── README.md                # This file
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
