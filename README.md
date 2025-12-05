# Kuber Distributed Cache

**High-Performance Distributed Cache with Redis Protocol Support**

Version 1.1.14

Copyright (c) 2025-2030, All Rights Reserved  
Ashutosh Sinha | Email: ajsinha@gmail.com

---

## Overview

Kuber is a powerful, enterprise-grade distributed caching system that provides:

- **Redis Protocol Compatibility**: Connect using any Redis client
- **Region-Based Organization**: Logical isolation with dedicated database per region
- **JSON Document Support**: Store and query JSON documents with JSONPath
- **Multi-Backend Persistence**: RocksDB (default), MongoDB, SQLite, PostgreSQL
- **Region Isolation**: Each region gets its own database instance for better concurrency
- **Smart Memory Management**: Global and per-region memory limits with intelligent allocation
- **Automatic Compaction**: RocksDB compaction via cron schedule (default: 2 AM daily)
- **SQLite Auto-Vacuum**: VACUUM on all SQLite databases at startup
- **Primary/Secondary Replication**: Automatic failover via ZooKeeper
- **Autoload**: Bulk data import from CSV and JSON files
- **Web Management UI**: Browser-based administration interface
- **REST API**: Programmatic access for all operations
- **CSV Export**: Export cache data to CSV files

## Features

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
| Multi-Backend Persistence | RocksDB (default), MongoDB, SQLite, PostgreSQL, or in-memory |
| Region Isolation | Separate database instance per region (RocksDB/SQLite) |
| Smart Memory Management | Global cap and per-region limits with proportional allocation |
| Automatic Compaction | RocksDB compaction via cron schedule (default: 2 AM daily) or manual trigger |
| Smart Cache Priming | Loads most recently accessed entries first on restart |
| Fast Entry Counts | O(1) entry estimation for dashboard - instant with millions of entries |
| SQLite Auto-Vacuum | Runs VACUUM on all SQLite databases at startup |
| ZooKeeper Replication | Automatic primary/secondary failover |
| Autoload | Bulk CSV/JSON import with metadata |
| CSV Export | Export regions and query results |
| Web UI | Bootstrap-based management dashboard |
| REST API | Full HTTP/JSON API for all operations |
| Authentication | User management with role-based access |
| Statistics | Comprehensive metrics and monitoring |

## Quick Start

### Prerequisites

- Java 17 or higher
- One of: MongoDB 5.0+, PostgreSQL 14+, or local file system for SQLite/RocksDB
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
   java -jar kuber-server/target/kuber-server-1.0.0-SNAPSHOT.jar
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
    max-memory-entries: 100000          # Default per-region limit
    global-max-memory-entries: 500000   # Global cap across all regions (0=unlimited)
    region-memory-limits:               # Per-region overrides
      customers: 50000
      products: 200000
    persistent-mode: false
    eviction-policy: LRU
  
  # Persistence (rocksdb, mongodb, postgresql, sqlite, memory)
  persistence:
    type: rocksdb
    rocksdb:
      path: ./data/rocksdb
      compaction-enabled: true
      compaction-interval-minutes: 30
  
  # MongoDB settings (if persistence.type=mongodb)
  mongo:
    uri: mongodb://localhost:27017
    database: kuber
  
  # ZooKeeper settings (optional)
  zookeeper:
    enabled: false
    connect-string: localhost:2181
```

### Memory Management (v1.1.11)

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
    max-records-per-file: 0  # 0 = unlimited
    create-directories: true
    file-encoding: UTF-8
```

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
