# Kuber Distributed Cache - Architecture Document

**Version 1.1.0**

Copyright © 2025-2030, All Rights Reserved  
Ashutosh Sinha | Email: ajsinha@gmail.com

**Patent Pending**: Certain architectural patterns and implementations described in this document may be subject to patent applications.

---

## Table of Contents

1. [Overview](#1-overview)
2. [System Architecture](#2-system-architecture)
3. [Core Components](#3-core-components)
4. [Client Architecture](#4-client-architecture)
5. [Protocol Design](#5-protocol-design)
6. [Persistence Layer](#6-persistence-layer)
7. [Replication Architecture](#7-replication-architecture)
8. [Security Architecture](#8-security-architecture)
9. [Data Flow](#9-data-flow)
10. [Deployment Patterns](#10-deployment-patterns)

---

## 1. Overview

Kuber is an enterprise-grade distributed caching system designed for high-performance, scalability, and flexibility. It provides Redis protocol compatibility while extending functionality with advanced features like region-based partitioning, JSON document queries, and pluggable persistence.

### Key Architectural Principles

| Principle | Description |
|-----------|-------------|
| **Protocol Compatibility** | Full Redis RESP protocol support for drop-in replacement |
| **Pluggable Persistence** | Configurable backends (MongoDB, SQLite, PostgreSQL, RocksDB, Memory) |
| **Region Isolation** | Logical namespaces for multi-tenant data organization |
| **High Availability** | Primary/Secondary replication via ZooKeeper |
| **Mandatory Security** | All clients must authenticate with username and password |
| **Extensibility** | Modular design allowing custom persistence and protocol handlers |

---

## 2. System Architecture

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT LAYER                                    │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────────┤
│  Python Redis   │  Python REST    │   Java Redis    │      Java REST         │
│    Client       │    Client       │    Client       │       Client           │
│ (Auth Required) │ (Auth Required) │ (Auth Required) │   (Auth Required)      │
└────────┬────────┴────────┬────────┴────────┬────────┴──────────┬─────────────┘
         │                 │                 │                   │
         │ Redis RESP      │ HTTP/REST       │ Redis RESP        │ HTTP/REST
         ▼                 ▼                 ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PROTOCOL LAYER                                     │
├─────────────────────────────────┬───────────────────────────────────────────┤
│       Redis Protocol Server     │           REST API Server                  │
│       (Apache MINA - Port 6380) │        (Spring Boot - Port 8080)          │
└─────────────────┬───────────────┴──────────────────┬────────────────────────┘
                  │                                   │
                  ▼                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            SERVICE LAYER                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │   Cache     │  │   Region    │  │    JSON     │  │   Authentication    │ │
│  │  Service    │  │   Manager   │  │   Service   │  │      Service        │ │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘ │
│         │                │                │                     │           │
│         └────────────────┴────────┬───────┴─────────────────────┘           │
│                                   │                                          │
│                                   ▼                                          │
│                        ┌─────────────────────┐                               │
│                        │ In-Memory Cache     │                               │
│                        │    (Caffeine)       │                               │
│                        └──────────┬──────────┘                               │
└───────────────────────────────────┼─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PERSISTENCE LAYER                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                     ┌──────────────────────────┐                             │
│                     │   PersistenceStore       │                             │
│                     │      Interface           │                             │
│                     └────────────┬─────────────┘                             │
│                                  │                                           │
│    ┌─────────┬─────────┬─────────┼─────────┬─────────┐                      │
│    ▼         ▼         ▼         ▼         ▼         ▼                      │
│ ┌──────┐ ┌──────┐ ┌──────────┐ ┌──────┐ ┌────────┐                         │
│ │Mongo │ │SQLite│ │PostgreSQL│ │Rocks │ │In-Mem  │                         │
│ │ DB   │ │      │ │          │ │  DB  │ │        │                         │
│ └──────┘ └──────┘ └──────────┘ └──────┘ └────────┘                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       REPLICATION LAYER (Optional)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                        ┌─────────────────────┐                               │
│                        │     ZooKeeper       │                               │
│                        │   Coordination      │                               │
│                        └─────────┬───────────┘                               │
│                                  │                                           │
│              ┌───────────────────┼───────────────────┐                      │
│              ▼                   ▼                   ▼                      │
│         ┌─────────┐         ┌─────────┐         ┌─────────┐                 │
│         │ Primary │ ──────► │Secondary│ ──────► │Secondary│                 │
│         │  Node   │         │  Node 1 │         │  Node 2 │                 │
│         └─────────┘         └─────────┘         └─────────┘                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Module Structure

```
kuber/
├── kuber-core/                    # Core domain models and utilities
│   ├── model/
│   │   ├── CacheEntry.java       # Cache entry with TTL support
│   │   ├── CacheRegion.java      # Region configuration
│   │   └── User.java             # User model for authentication
│   ├── protocol/
│   │   ├── RedisCommand.java     # Redis command representation
│   │   └── RedisResponse.java    # Redis response builder
│   └── exception/
│       └── KuberException.java   # Custom exceptions
│
├── kuber-server/                  # Server implementation
│   ├── cache/
│   │   └── CacheService.java     # Primary cache operations
│   ├── persistence/
│   │   ├── PersistenceStore.java # Persistence interface
│   │   ├── MongoDBStore.java     # MongoDB implementation
│   │   ├── SQLiteStore.java      # SQLite implementation
│   │   ├── PostgreSQLStore.java  # PostgreSQL implementation
│   │   ├── RocksDBStore.java     # RocksDB implementation
│   │   └── InMemoryStore.java    # In-memory implementation
│   ├── protocol/
│   │   └── RedisProtocolHandler.java  # RESP protocol handler
│   ├── api/
│   │   └── RestApiController.java     # REST endpoints
│   ├── replication/
│   │   └── ReplicationService.java    # ZooKeeper replication
│   └── autoload/
│       └── AutoloadService.java       # CSV/JSON bulk import
│
├── kuber-client-java/             # Java client libraries
│   ├── KuberClient.java          # Redis protocol client (Auth required)
│   ├── KuberRestClient.java      # REST API client (Auth required)
│   └── examples/
│       ├── KuberRedisExample.java
│       └── KuberRestExample.java
│
├── kuber-client-python/           # Python client libraries
│   ├── kuber_redis_standalone.py # Redis protocol client (Auth required)
│   └── kuber_rest_standalone.py  # REST API client (Auth required)
│
└── docs/
    ├── ARCHITECTURE.md           # This document
    └── CLIENT_USAGE.md           # Client usage guide
```

---

## 3. Core Components

### 3.1 Cache Service

The `CacheService` is the central component managing all cache operations:

```
┌─────────────────────────────────────────────────────────────┐
│                      CacheService                            │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              Region Manager                           │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐    │   │
│  │  │ default │ │ users   │ │products │ │sessions │    │   │
│  │  │ region  │ │ region  │ │ region  │ │ region  │    │   │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘    │   │
│  │       └──────┬────┴─────┬─────┴──────┬────┘         │   │
│  └──────────────┼──────────┼────────────┼──────────────┘   │
│                 ▼          ▼            ▼                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │           Caffeine In-Memory Cache                    │   │
│  │  • Automatic expiration (TTL)                         │   │
│  │  • Size-based eviction (LRU/LFU)                      │   │
│  │  • Statistics collection                              │   │
│  │  • Refresh-ahead support                              │   │
│  └──────────────────────────────────────────────────────┘   │
│                           │                                  │
│                           ▼                                  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              Persistence Store                        │   │
│  │  • Write-behind for durability                        │   │
│  │  • Startup recovery                                   │   │
│  │  • Graceful shutdown persistence                      │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Region Manager

Regions provide logical isolation for cache entries:

| Feature | Description |
|---------|-------------|
| **Namespace Isolation** | Keys are scoped within regions |
| **Independent TTL** | Each region can have default TTL settings |
| **Separate Statistics** | Per-region hit/miss tracking |
| **Access Control** | Future: Region-level permissions |

### 3.3 JSON Service

Native JSON document support with JSONPath queries:

```
┌─────────────────────────────────────────────────────────────┐
│                      JSON Service                            │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Document Storage:                                           │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Key: "user:1001"                                      │  │
│  │  Value: {                                              │  │
│  │    "name": "Alice",                                    │  │
│  │    "age": 30,                                          │  │
│  │    "roles": ["admin", "developer"],                    │  │
│  │    "address": {                                        │  │
│  │      "city": "New York",                               │  │
│  │      "zip": "10001"                                    │  │
│  │    }                                                   │  │
│  │  }                                                     │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                              │
│  Query Operators:                                            │
│  • Equality:      $.name=Alice                               │
│  • Comparison:    $.age>25, $.age<=35                        │
│  • Inequality:    $.status!=inactive                         │
│  • Pattern:       $.name LIKE %Ali%                          │
│  • Contains:      $.roles CONTAINS admin                     │
│  • Nested:        $.address.city=New York                    │
│  • Combined:      $.age>25,$.roles CONTAINS admin            │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 4. Client Architecture

### 4.1 Client Overview

**IMPORTANT: All clients require authentication with username and password.**

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         CLIENT ARCHITECTURE                              │
├─────────────────────────┬───────────────────────────────────────────────┤
│    Redis Protocol       │              REST API                          │
│    (Port 6380)          │           (Port 8080)                          │
├─────────────────────────┼───────────────────────────────────────────────┤
│                         │                                                │
│  ┌─────────────────┐    │    ┌─────────────────┐                        │
│  │ Python Client   │    │    │ Python Client   │                        │
│  │                 │    │    │                 │                        │
│  │ • Pure socket   │    │    │ • Pure urllib   │                        │
│  │ • No deps       │    │    │ • No deps       │                        │
│  │ • RESP parser   │    │    │ • JSON parsing  │                        │
│  │ • Auth REQUIRED │    │    │ • Auth REQUIRED │                        │
│  └─────────────────┘    │    └─────────────────┘                        │
│                         │                                                │
│  ┌─────────────────┐    │    ┌─────────────────┐                        │
│  │ Java Client     │    │    │ Java Client     │                        │
│  │                 │    │    │                 │                        │
│  │ • Socket I/O    │    │    │ • HttpURLConn   │                        │
│  │ • Jackson JSON  │    │    │ • Jackson JSON  │                        │
│  │ • RESP parser   │    │    │ • Basic Auth    │                        │
│  │ • Auth REQUIRED │    │    │ • Auth REQUIRED │                        │
│  └─────────────────┘    │    └─────────────────┘                        │
│                         │                                                │
└─────────────────────────┴───────────────────────────────────────────────┘
```

### 4.2 Authentication Requirement

All client constructors require username and password:

**Python Redis Client:**
```python
# Both username and password are REQUIRED
client = KuberRedisClient(host, port, username='admin', password='secret')
```

**Python REST Client:**
```python
# Both username and password are REQUIRED
client = KuberRestClient(host, port, username='admin', password='secret')
```

**Java Redis Client:**
```java
// Both username and password are REQUIRED
KuberClient client = new KuberClient(host, port, username, password);
```

**Java REST Client:**
```java
// Both username and password are REQUIRED
KuberRestClient client = new KuberRestClient(host, port, username, password);
```

### 4.3 Client Connection Flow

```
┌──────────────┐     ┌───────────────────┐     ┌──────────────────┐
│   Client     │     │      Server       │     │   Persistence    │
└──────┬───────┘     └─────────┬─────────┘     └────────┬─────────┘
       │                       │                        │
       │  1. TCP Connect       │                        │
       │──────────────────────►│                        │
       │                       │                        │
       │  2. AUTH password     │                        │
       │──────────────────────►│                        │
       │                       │  3. Validate           │
       │                       │─────────────────────►  │
       │                       │  4. OK/FAIL            │
       │  5. +OK / -ERR        │◄─────────────────────  │
       │◄──────────────────────│                        │
       │                       │                        │
       │  6. SET key value     │                        │
       │──────────────────────►│                        │
       │                       │  7. Store              │
       │                       │─────────────────────►  │
       │  8. +OK               │                        │
       │◄──────────────────────│                        │
       │                       │                        │
```

---

## 5. Protocol Design

### 5.1 Redis RESP Protocol

Kuber implements the Redis Serialization Protocol (RESP):

| Type | Prefix | Example |
|------|--------|---------|
| Simple String | `+` | `+OK\r\n` |
| Error | `-` | `-ERR unknown command\r\n` |
| Integer | `:` | `:1000\r\n` |
| Bulk String | `$` | `$5\r\nhello\r\n` |
| Array | `*` | `*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n` |
| Null | `$-1` | `$-1\r\n` |

### 5.2 Kuber Protocol Extensions

Additional commands beyond standard Redis:

| Command | Description | Example |
|---------|-------------|---------|
| `RSELECT region` | Select region | `RSELECT users` |
| `RCREATE name desc` | Create region | `RCREATE orders "Order data"` |
| `RDROP region` | Delete region | `RDROP temp` |
| `RPURGE region` | Clear region | `RPURGE sessions` |
| `REGIONS` | List regions | `REGIONS` |
| `JSET key json [path] [ttl]` | Set JSON | `JSET user:1 {"name":"Alice"}` |
| `JGET key [path]` | Get JSON | `JGET user:1 $.name` |
| `JSEARCH query` | Search JSON | `JSEARCH $.age>30` |
| `STATUS` | Server status | `STATUS` |
| `REPLINFO` | Replication info | `REPLINFO` |

### 5.3 REST API Endpoints

```
Base URL: http://server:8080/api/v1

Authentication: HTTP Basic Auth (Required)
Header: Authorization: Basic base64(username:password)

Server Operations:
  GET    /ping                      - Health check
  GET    /info                      - Server information
  GET    /status                    - Server status
  GET    /stats                     - Statistics

Region Operations:
  GET    /regions                   - List all regions
  GET    /regions/{name}            - Get region info
  POST   /regions                   - Create region
  DELETE /regions/{name}            - Delete region
  POST   /regions/{name}/purge      - Purge region

Cache Operations:
  GET    /cache/{region}/{key}      - Get value
  PUT    /cache/{region}/{key}      - Set value
  DELETE /cache/{region}/{key}      - Delete key
  GET    /cache/{region}/keys       - List keys (pattern)
  POST   /cache/{region}/mget       - Multi-get
  POST   /cache/{region}/mset       - Multi-set

Hash Operations:
  GET    /cache/{region}/{key}/hash           - Get all fields
  GET    /cache/{region}/{key}/hash/{field}   - Get field
  PUT    /cache/{region}/{key}/hash/{field}   - Set field
  DELETE /cache/{region}/{key}/hash/{field}   - Delete field

JSON Operations:
  GET    /json/{region}/{key}       - Get JSON document
  PUT    /json/{region}/{key}       - Set JSON document
  DELETE /json/{region}/{key}       - Delete JSON document
  POST   /json/{region}/search      - Search JSON documents

Bulk Operations:
  POST   /cache/{region}/import     - Bulk import
  GET    /cache/{region}/export     - Bulk export
```

---

## 6. Persistence Layer

### 6.1 Pluggable Persistence Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   PersistenceStore Interface                 │
├─────────────────────────────────────────────────────────────┤
│  void save(String region, String key, CacheEntry entry)     │
│  Optional<CacheEntry> load(String region, String key)       │
│  void delete(String region, String key)                     │
│  void deleteRegion(String region)                           │
│  List<String> keys(String region, String pattern)           │
│  long count(String region)                                  │
│  Map<String, CacheEntry> loadAll(String region)             │
│  void saveAll(String region, Map entries)                   │
│  boolean isAvailable()                                      │
│  String getBackendType()                                    │
│  Map<String, Object> getBackendInfo()                       │
└─────────────────────────────────────────────────────────────┘
              │
              │ implements
              ▼
┌─────────┬─────────┬───────────┬─────────┬──────────┐
│ MongoDB │ SQLite  │PostgreSQL │ RocksDB │ InMemory │
│  Store  │  Store  │   Store   │  Store  │   Store  │
└─────────┴─────────┴───────────┴─────────┴──────────┘
```

### 6.2 Backend Comparison

| Backend | Use Case | Pros | Cons |
|---------|----------|------|------|
| **MongoDB** | Production clusters | Distributed, scalable | Requires separate server |
| **SQLite** | Development/Single-node | Zero config, file-based | Single-writer, limited scale |
| **PostgreSQL** | Enterprise | ACID, mature tooling | Requires separate server |
| **RocksDB** | High-performance | Embedded, LSM-tree | Complex tuning |
| **InMemory** | Testing/Dev | Fastest, no I/O | No persistence |

### 6.3 Configuration

```yaml
kuber:
  persistence:
    # Options: mongodb, sqlite, postgresql, rocksdb, memory
    backend: sqlite
    
    # MongoDB settings
    mongodb:
      uri: mongodb://localhost:27017
      database: kuber
    
    # SQLite settings  
    sqlite:
      path: ./data/kuber.db
    
    # PostgreSQL settings
    postgresql:
      url: jdbc:postgresql://localhost:5432/kuber
      username: kuber
      password: secret
    
    # RocksDB settings
    rocksdb:
      path: ./data/rocksdb
```

---

## 7. Replication Architecture

### 7.1 Primary/Secondary Replication

```
┌─────────────────────────────────────────────────────────────┐
│                      ZooKeeper Cluster                       │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐                    │
│  │  ZK 1   │   │  ZK 2   │   │  ZK 3   │                    │
│  └────┬────┘   └────┬────┘   └────┬────┘                    │
│       └─────────────┼─────────────┘                         │
│                     │                                        │
│           ┌─────────┴─────────┐                             │
│           │  Leader Election  │                             │
│           └─────────┬─────────┘                             │
│                     │                                        │
└─────────────────────┼────────────────────────────────────────┘
                      │
        ┌─────────────┴─────────────┐
        ▼                           ▼
┌───────────────┐           ┌───────────────┐
│   PRIMARY     │           │  SECONDARY    │
│               │ replicate │               │
│  Read/Write   │──────────►│  Read-Only    │
│               │           │               │
│  • All writes │           │ • Read replica│
│  • Failover   │           │ • Hot standby │
│    master     │           │ • Auto-promote│
└───────────────┘           └───────────────┘
```

### 7.2 Failover Process

1. **Primary Failure Detection**: ZooKeeper detects primary node is unresponsive
2. **Leader Election**: Remaining nodes participate in leader election
3. **Promotion**: Secondary with most recent data becomes new primary
4. **Client Reconnection**: Clients reconnect to new primary via service discovery
5. **Recovery**: Old primary rejoins as secondary after recovery

---

## 8. Security Architecture

### 8.1 Authentication

**All clients MUST provide credentials. This is enforced at the client level.**

```
┌─────────────────────────────────────────────────────────────┐
│                  Authentication Flow                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Redis Protocol:                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  AUTH password                                        │   │
│  │                                                       │   │
│  │  Response: +OK (success) or -ERR (failure)           │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  REST API:                                                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  HTTP Header: Authorization: Basic base64(user:pass) │   │
│  │                                                       │   │
│  │  Response: 200 OK (success) or 401 Unauthorized      │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  Client Enforcement:                                         │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  All client constructors validate that username and   │   │
│  │  password are provided. Missing credentials throw     │   │
│  │  an exception immediately.                            │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 8.2 User Configuration

Users are configured in `users.json`:

```json
{
  "users": [
    {
      "username": "admin",
      "password": "hashed_password",
      "roles": ["ADMIN", "USER"],
      "enabled": true
    },
    {
      "username": "readonly",
      "password": "hashed_password", 
      "roles": ["READER"],
      "enabled": true
    }
  ]
}
```

---

## 9. Data Flow

### 9.1 Write Operation Flow

```
┌──────────┐   ┌───────────┐   ┌─────────────┐   ┌─────────────┐   ┌──────────────┐
│  Client  │   │ Protocol  │   │   Cache     │   │  Caffeine   │   │ Persistence  │
│          │   │  Handler  │   │  Service    │   │   Cache     │   │    Store     │
└────┬─────┘   └─────┬─────┘   └──────┬──────┘   └──────┬──────┘   └──────┬───────┘
     │               │                │                 │                 │
     │ SET key val   │                │                 │                 │
     │──────────────►│                │                 │                 │
     │               │ set(region,key)│                 │                 │
     │               │───────────────►│                 │                 │
     │               │                │ put(key,entry)  │                 │
     │               │                │────────────────►│                 │
     │               │                │                 │                 │
     │               │                │ save(region,key)│                 │
     │               │                │─────────────────┼────────────────►│
     │               │                │                 │                 │
     │               │ +OK            │                 │                 │
     │◄──────────────│◄───────────────│                 │                 │
     │               │                │                 │                 │
```

### 9.2 Read Operation Flow

```
┌──────────┐   ┌───────────┐   ┌─────────────┐   ┌─────────────┐   ┌──────────────┐
│  Client  │   │ Protocol  │   │   Cache     │   │  Caffeine   │   │ Persistence  │
│          │   │  Handler  │   │  Service    │   │   Cache     │   │    Store     │
└────┬─────┘   └─────┬─────┘   └──────┬──────┘   └──────┬──────┘   └──────┬───────┘
     │               │                │                 │                 │
     │ GET key       │                │                 │                 │
     │──────────────►│                │                 │                 │
     │               │ get(region,key)│                 │                 │
     │               │───────────────►│                 │                 │
     │               │                │ get(key)        │                 │
     │               │                │────────────────►│                 │
     │               │                │                 │                 │
     │               │                │  CACHE HIT      │                 │
     │               │                │◄────────────────│                 │
     │               │                │                 │                 │
     │               │ $value         │                 │                 │
     │◄──────────────│◄───────────────│                 │                 │
     │               │                │                 │                 │
```

---

## 10. Deployment Patterns

### 10.1 Single Node (Development)

```
┌─────────────────────────────────────────┐
│            Single Server                 │
│  ┌─────────────────────────────────┐    │
│  │         Kuber Server            │    │
│  │  • Redis Protocol (6380)        │    │
│  │  • REST API (8080)              │    │
│  │  • Web UI (8080)                │    │
│  │  • SQLite Persistence           │    │
│  └─────────────────────────────────┘    │
└─────────────────────────────────────────┘
```

### 10.2 High Availability Cluster

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                      │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │                    Load Balancer                             │   │
│   │              (HAProxy / AWS ALB / etc.)                      │   │
│   └───────────────────────┬─────────────────────────────────────┘   │
│                           │                                          │
│           ┌───────────────┼───────────────┐                         │
│           ▼               ▼               ▼                         │
│   ┌───────────┐   ┌───────────┐   ┌───────────┐                    │
│   │  Kuber    │   │  Kuber    │   │  Kuber    │                    │
│   │ Primary   │   │Secondary 1│   │Secondary 2│                    │
│   │           │   │           │   │           │                    │
│   │ • R/W     │   │ • R only  │   │ • R only  │                    │
│   └─────┬─────┘   └─────┬─────┘   └─────┬─────┘                    │
│         │               │               │                           │
│         └───────────────┼───────────────┘                           │
│                         ▼                                            │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │                    ZooKeeper Cluster                         │   │
│   │              (Leader Election + Config)                      │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                         │                                            │
│                         ▼                                            │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │              MongoDB Replica Set / PostgreSQL               │   │
│   │                    (Shared Persistence)                      │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Appendix: Performance Considerations

### Memory Management

- **Caffeine Cache**: Configure maximum size based on available heap
- **TTL Policies**: Set appropriate TTLs to prevent unbounded growth
- **Eviction**: LRU/LFU eviction when capacity reached

### Network Optimization

- **Connection Pooling**: Reuse connections in client applications
- **Pipelining**: Batch multiple commands for reduced latency
- **Compression**: Consider compression for large JSON documents

### Persistence Tuning

| Backend | Optimization |
|---------|--------------|
| MongoDB | Index on region + key, connection pool sizing |
| SQLite | WAL mode, appropriate page size |
| PostgreSQL | Connection pool, prepared statements |
| RocksDB | Block cache, write buffer size |

---

*End of Architecture Document*
