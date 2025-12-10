# Kuber Distributed Cache - Architecture Document

**Version 1.7.0**

Copyright © 2025-2030, All Rights Reserved  
Ashutosh Sinha | Email: ajsinha@gmail.com

**Patent Pending**: Certain architectural patterns and implementations described in this document may be subject to patent applications.

---

## Table of Contents

1. [Overview](#1-overview)
2. [System Architecture](#2-system-architecture)
3. [Factory Pattern Architecture](#3-factory-pattern-architecture)
4. [Startup Orchestration](#4-startup-orchestration)
5. [Core Components](#5-core-components)
6. [Client Architecture](#6-client-architecture)
7. [Protocol Design](#7-protocol-design)
8. [Persistence Layer](#8-persistence-layer)
9. [Replication Architecture](#9-replication-architecture)
10. [Event Publishing](#10-event-publishing)
11. [Request/Response Messaging](#11-request-response-messaging)
12. [Security Architecture](#12-security-architecture)
13. [Data Flow](#13-data-flow)
14. [Deployment Patterns](#14-deployment-patterns)

---

## 1. Overview

Kuber is an enterprise-grade distributed caching system designed for high-performance, scalability, and flexibility. It provides Redis protocol compatibility while extending functionality with advanced features like region-based partitioning, JSON document queries, and pluggable persistence.

### Key Architectural Principles

| Principle | Description |
|-----------|-------------|
| **Protocol Compatibility** | Full Redis RESP protocol support for drop-in replacement |
| **Pluggable Persistence** | Configurable backends (MongoDB, SQLite, PostgreSQL, RocksDB, LMDB, Memory) |
| **Region Isolation** | Logical namespaces for multi-tenant data organization |
| **High Availability** | Primary/Secondary replication via ZooKeeper |
| **Mandatory Security** | All clients must authenticate with username/password or API key |
| **Event Publishing** | Stream cache events to Kafka, RabbitMQ, IBM MQ, ActiveMQ, or files |
| **Request/Response** | Access cache via message brokers with async processing and backpressure |
| **Extensibility** | Modular design allowing custom persistence, publishers, and protocol handlers |

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
│   │   ├── CacheService.java     # Primary cache operations
│   │   ├── KeyIndex.java         # In-memory key index
│   │   └── OffHeapKeyIndex.java  # Off-heap key storage
│   ├── persistence/
│   │   ├── PersistenceStore.java # Persistence interface
│   │   ├── RocksDbStore.java     # RocksDB implementation
│   │   ├── LmdbStore.java        # LMDB implementation
│   │   ├── MongoDBStore.java     # MongoDB implementation
│   │   ├── SQLiteStore.java      # SQLite implementation
│   │   ├── PostgreSQLStore.java  # PostgreSQL implementation
│   │   └── InMemoryStore.java    # In-memory implementation
│   ├── publishing/               # Event Publishing (v1.2.8)
│   │   ├── EventPublisher.java   # Publisher interface
│   │   ├── PublisherRegistry.java # Central publisher registry
│   │   ├── KafkaEventPublisher.java
│   │   ├── RabbitMqEventPublisher.java
│   │   ├── IbmMqEventPublisher.java
│   │   ├── ActiveMqEventPublisher.java
│   │   └── FileEventPublisher.java
│   ├── protocol/
│   │   └── RedisProtocolHandler.java  # RESP protocol handler
│   ├── api/
│   │   └── RestApiController.java     # REST endpoints
│   ├── replication/
│   │   └── ReplicationService.java    # ZooKeeper replication
│   ├── security/
│   │   ├── ApiKeyService.java    # API key management
│   │   └── ApiKeyFilter.java     # API key authentication
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

## 3. Factory Pattern Architecture

**Added in v1.5.0**

Kuber uses the Factory Pattern combined with the Proxy Pattern to allow pluggable cache and collection implementations. This enables changing the underlying cache provider (e.g., from Caffeine to EhCache) or collection implementation without modifying application code.

### 3.1 Design Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CacheService                                       │
│                                                                             │
│  Uses FactoryProvider to create cache and collection instances              │
└─────────────────────────────────────────────────────────┬───────────────────┘
                                                          │
                                                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FactoryProvider                                      │
│                                                                             │
│  ┌─────────────────────────┐       ┌─────────────────────────┐              │
│  │     CacheFactory        │       │  CollectionsFactory     │              │
│  │  (returns CacheProxy)   │       │(returns Map,List,Set,..)│              │
│  └────────────┬────────────┘       └───────────┬─────────────┘              │
└───────────────┼────────────────────────────────┼────────────────────────────┘
                │                                │
        ┌───────┴───────┐                ┌───────┴───────┐
        ▼               ▼                ▼               ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│   Caffeine    │ │    Guava      │ │   Default     │ │    Custom     │
│  CacheFactory │ │ CacheFactory  │ │ Collections   │ │ Collections   │
│   (default)   │ │   (future)    │ │   (default)   │ │   (future)    │
└───────┬───────┘ └───────────────┘ └───────┬───────┘ └───────────────┘
        │                                   │
        ▼                                   ▼
┌───────────────────────────┐ ┌─────────────────────────────────────────┐
│    CacheProxy<K, V>       │ │ Collection Types:                       │
│                           │ │ - Map<K,V> (ConcurrentHashMap, etc.)    │
│  - get(K key): V          │ │ - List<E> (CopyOnWriteArrayList, etc.)  │
│  - put(K key, V value)    │ │ - Set<E> (ConcurrentSkipListSet, etc.)  │
│  - invalidate(K key)      │ │ - Queue<E> (ConcurrentLinkedQueue)      │
│  - estimatedSize(): long  │ │ - Deque<E> (ConcurrentLinkedDeque)      │
│  - stats(): CacheStats    │ │ - Stack<E> (via Deque interface)        │
└───────────────────────────┘ └─────────────────────────────────────────┘
```

### 3.2 Key Interfaces

#### CacheProxy<K, V>
Abstraction for cache operations, allowing different implementations to be swapped.

```java
public interface CacheProxy<K, V> {
    V get(K key);
    V get(K key, Function<? super K, ? extends V> mappingFunction);
    void put(K key, V value);
    void invalidate(K key);
    void invalidateAll();
    long estimatedSize();
    Map<K, V> asMap();
    CacheStats stats();
    String getType();  // e.g., "CAFFEINE", "GUAVA"
}
```

#### CacheFactory
Factory for creating cache instances.

```java
public interface CacheFactory {
    <K, V> CacheProxy<K, V> createCache(String name, CacheConfig config);
    String getType();
}
```

#### CollectionsFactory
Factory for creating thread-safe collection instances.

```java
public interface CollectionsFactory {
    // Map creation
    <K, V> Map<K, V> createMap(String name);
    <K, V> Map<K, V> createMap(String name, CollectionsConfig config);
    
    // List creation
    <E> List<E> createList(String name);
    <E> List<E> createList(String name, CollectionsConfig config);
    
    // Set creation
    <E> Set<E> createSet(String name);
    <E> Set<E> createSet(String name, CollectionsConfig config);
    
    // Queue creation
    <E> Queue<E> createQueue(String name);
    <E> Queue<E> createQueue(String name, CollectionsConfig config);
    
    // Deque creation (also used for Stack)
    <E> Deque<E> createDeque(String name);
    <E> Deque<E> createDeque(String name, CollectionsConfig config);
    
    // Stack creation (returns Deque)
    <E> Deque<E> createStack(String name);
    
    String getType();
}
```

#### CollectionsConfig
Configuration for collection creation with support for various options.

```java
CollectionsConfig config = CollectionsConfig.builder()
    .initialCapacity(1000)
    .concurrencyLevel(32)
    .threadSafe(true)      // Use concurrent collections
    .ordered(false)        // Use LinkedHashMap/LinkedHashSet
    .sorted(false)         // Use TreeMap/TreeSet
    .build();
```

### 3.3 Default Collection Implementations

The `DefaultCollectionsFactory` uses Java concurrent collections:

| Collection Type | Thread-Safe Implementation | Non-Thread-Safe | Ordered | Sorted |
|-----------------|---------------------------|-----------------|---------|--------|
| **Map** | ConcurrentHashMap | HashMap | LinkedHashMap | TreeMap/ConcurrentSkipListMap |
| **List** | CopyOnWriteArrayList | ArrayList | - | - |
| **Set** | ConcurrentHashMap.newKeySet() | HashSet | LinkedHashSet | TreeSet/ConcurrentSkipListSet |
| **Queue** | ConcurrentLinkedQueue | LinkedList | - | - |
| **Deque** | ConcurrentLinkedDeque | ArrayDeque | - | - |

### 3.4 Configuration

Configure cache and collection implementations in `application.yml`:

```yaml
kuber:
  cache:
    # Cache implementation for value caches
    # Options: CAFFEINE (default)
    # Future: GUAVA, EHCACHE
    cache-implementation: CAFFEINE
    
    # Collections implementation for internal collections
    # Options: DEFAULT (default)
    # Future: CUSTOM
    collections-implementation: DEFAULT
```

### 3.5 Benefits

| Benefit | Description |
|---------|-------------|
| **Abstraction** | Code doesn't depend on specific cache/collection implementations |
| **Extensibility** | New providers can be added without changing existing code |
| **Testability** | Mock implementations can be injected for unit testing |
| **Configuration** | Implementation can be changed via configuration without code changes |
| **Consistency** | All caches and collections use the same abstraction layer |
| **Flexibility** | Collections can be configured for thread-safety, ordering, or sorting |

### 3.6 Adding a New Cache Implementation

To add a new cache implementation (e.g., EhCache):

1. Create `EhCacheCacheProxy<K,V>` implementing `CacheProxy<K,V>`
2. Create `EhCacheCacheFactory` implementing `CacheFactory`
3. Update `FactoryProvider.selectCacheFactory()` to recognize the new type
4. Add to configuration options

### 3.7 Adding a New Collections Implementation

To add a new collections implementation:

1. Create `CustomCollectionsFactory` implementing `CollectionsFactory`
2. Implement all methods (createMap, createList, createSet, createQueue, createDeque)
3. Update `FactoryProvider.selectCollectionsFactory()` to recognize the new type
4. Add to configuration options

---

## 4. Startup Orchestration

Kuber uses a `StartupOrchestrator` to ensure correct initialization order and prevent race conditions during application startup. This is critical for data integrity and system stability.

### 13.1 The Problem

Without proper orchestration, several race conditions can occur:

| Race Condition | Description | Impact |
|----------------|-------------|--------|
| **Early Data Recovery** | Persistence recovery starts before Spring context is fully loaded | Missing bean dependencies, null pointers |
| **Premature Client Connections** | Redis server accepts connections before cache is ready | Clients receive errors or stale data |
| **Autoload Race** | Files processed before persistence recovery completes | Data overwrites, inconsistent state |
| **Scheduled Task Conflicts** | @Scheduled methods run before initialization | Operations on uninitialized caches |

### 13.2 Startup Sequence

The `StartupOrchestrator` guarantees this strict initialization order:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STARTUP SEQUENCE (v1.3.2)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ PHASE 0: Spring Boot Initialization                                  │   │
│  │ • Spring context loads all beans                                     │   │
│  │ • Dependency injection completes                                     │   │
│  │ • ApplicationReadyEvent fires                                        │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ STABILIZATION: Wait 10 seconds                                       │   │
│  │ • Allows all Spring beans to fully initialize                        │   │
│  │ • Ensures async initializations complete                             │   │
│  │ • Prevents premature service access                                  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼                                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ PHASE 1: Persistence Maintenance (SEQUENTIAL - v1.3.8)              │   │
│  │ • RocksDB: Full compaction of all region databases (sequential)     │   │
│  │ • SQLite: VACUUM on all region database files (sequential)          │   │
│  │ • LMDB: Skip (B+ tree auto-balances)                                │   │
│  │ • All operations run one at a time for data consistency             │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼ (2 second wait)                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ PHASE 2: Cache Service Initialization (SEQUENTIAL - v1.3.8)         │   │
│  │ • Load regions from persistence store                               │   │
│  │ • Recover all cached data from disk/database (sequential per region)│   │
│  │ • Build KeyIndex for each region (all keys in memory)               │   │
│  │ • Prime value cache with hot entries                                │   │
│  │ • Mark CacheService as initialized                                  │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼ (2 second wait)                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ PHASE 3: Redis Protocol Server                                       │   │
│  │ • Bind to configured port (default: 6380)                            │   │
│  │ • Start accepting client connections                                 │   │
│  │ • Clients can now safely connect with full data available            │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼ (2 second wait)                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ PHASE 4: Autoload Service                                            │   │
│  │ • Initialize inbox/outbox directories                                │   │
│  │ • Start file watcher for new data files                              │   │
│  │ • Process any pending files in inbox                                 │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                    │                                         │
│                                    ▼ (2 second wait)                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ SYSTEM READY: Final Announcement                                     │   │
│  │ • Mark startup as complete                                           │   │
│  │ • Log system ready message                                           │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 13.3 Scheduled Task Protection

All `@Scheduled` methods check for cache initialization before executing:

```java
@Scheduled(fixedRate = 60000)
public void cleanupExpiredEntries() {
    // Skip if cache service not yet initialized
    if (!cacheService.isInitialized()) {
        return;
    }
    // ... perform cleanup
}
```

Protected scheduled tasks include:

| Service | Method | Protection |
|---------|--------|------------|
| CacheService | cleanupExpiredEntries() | `initialized.get()` |
| MemoryWatcherService | checkMemoryUsage() | `cacheService.isInitialized()` |
| PersistenceExpirationService | cleanupExpiredEntries() | `cacheService.isInitialized()` |
| RocksDbCompactionService | scheduledCompaction() | `enabled.get()` |

### 13.4 Startup Logging

The orchestrator provides clear visual logging of each phase:

```
╔════════════════════════════════════════════════════════════════════╗
║  Spring context ready - starting initialization sequence...        ║
╚════════════════════════════════════════════════════════════════════╝
Waiting 10 seconds for Spring context stabilization...

╔════════════════════════════════════════════════════════════════════╗
║  Phase 1: Starting cache service initialization...                 ║
║           Recovering data from persistence store...                ║
╚════════════════════════════════════════════════════════════════════╝
Cache service initialization completed in 1234 ms

╔════════════════════════════════════════════════════════════════════╗
║  Phase 2: Starting Redis protocol server...                        ║
╚════════════════════════════════════════════════════════════════════╝
Redis protocol server started on port 6380

╔════════════════════════════════════════════════════════════════════╗
║  Phase 3: Starting autoload service...                             ║
╚════════════════════════════════════════════════════════════════════╝

╔════════════════════════════════════════════════════════════════════╗
║  Startup sequence completed successfully!                          ║
║  - Cache service: initialized (data recovered)                     ║
║  - Redis server: accepting connections                             ║
║  - Autoload service: started                                       ║
╚════════════════════════════════════════════════════════════════════╝
```

### 13.5 Checking Startup Status

Services can query the orchestrator for startup status:

```java
@Autowired
private StartupOrchestrator startupOrchestrator;

// Check if cache is ready
if (startupOrchestrator.isCacheReady()) {
    // Safe to perform cache operations
}

// Check if full startup is complete
if (startupOrchestrator.isStartupComplete()) {
    // All services are ready
}
```

### 13.5 Shutdown Utility (v1.3.5)

Kuber provides multiple clean shutdown mechanisms:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SHUTDOWN MECHANISMS                              │
├─────────────────────────────────────────────────────────────────────┤
│  ┌───────────────┐    ┌───────────────┐    ┌───────────────┐        │
│  │  Shutdown     │    │   REST API    │    │    Signal     │        │
│  │    File       │    │   Endpoint    │    │  (SIGTERM)    │        │
│  │               │    │               │    │               │        │
│  │ kuberdata/    │    │ /api/admin/   │    │  kill <pid>   │        │
│  │ kuber.shutdown│    │  shutdown     │    │   Ctrl+C      │        │
│  └───────┬───────┘    └───────┬───────┘    └───────┬───────┘        │
│          │                    │                    │                │
│          └────────────────────┼────────────────────┘                │
│                               │                                      │
│                               ▼                                      │
│                    ┌─────────────────────┐                          │
│                    │  ShutdownOrchestrator│                          │
│                    └──────────┬──────────┘                          │
│                               │                                      │
│                               ▼                                      │
│          ┌─────────────────────────────────────────────┐            │
│          │  Orderly Shutdown (reverse startup order)    │            │
│          │  1. Stop Autoload         (wait 5s)         │            │
│          │  2. Stop Redis Server     (wait 5s)         │            │
│          │  3. Stop Event Publishing (wait 5s)         │            │
│          │  4. Persist Cache Data    (wait 5s)         │            │
│          │  5. Close Persistence     (wait 5s)         │            │
│          └─────────────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────────────┘
```

#### File-Based Shutdown

```bash
# Linux/Mac
touch ./kuberdata/kuber.shutdown

# Windows
echo. > kuberdata\kuber.shutdown

# Using the provided script
./kuber-shutdown.sh
./kuber-shutdown.sh -r "Maintenance window"
```

#### REST API Shutdown

```bash
curl -X POST http://localhost:8080/api/admin/shutdown \
  -H "X-API-Key: your-api-key"
```

#### Configuration

```yaml
kuber:
  base:
    datadir: ./kuberdata              # Base directory for all data
  shutdown:
    file-enabled: true                # Enable file-based shutdown
    file-path: ${kuber.base.datadir}/kuber.shutdown  # Path to shutdown signal file
    check-interval-ms: 5000           # Check interval (5 seconds)
    api-enabled: true                 # Enable REST API shutdown
    phase-delay-seconds: 5            # Delay between shutdown phases
```

---

## 5. Core Components

### 13.1 Cache Service

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

### 13.2 Region Manager

Regions provide logical isolation for cache entries:

| Feature | Description |
|---------|-------------|
| **Auto-Creation** | Regions are automatically created when data is stored to a non-existent region |
| **Namespace Isolation** | Keys are scoped within regions |
| **Independent TTL** | Each region can have default TTL settings |
| **Separate Statistics** | Per-region hit/miss tracking |
| **Attribute Mapping** | JSON attribute transformation on storage |
| **Access Control** | Future: Region-level permissions |

**Auto-Creation Behavior:**
- When data is stored to a region that doesn't exist, the region is automatically created
- Auto-created regions have the description "Auto-created region"
- No manual region creation is required before storing data
- The `default` region always exists and cannot be deleted

**Attribute Mapping:**
Regions can have optional attribute mapping configuration that transforms JSON attribute names when data is stored:
- Configure mapping: `RSETMAP region {"firstName":"first_name","lastName":"last_name"}`
- Get mapping: `RGETMAP region`
- Clear mapping: `RCLEARMAP region`
- REST API: `PUT /api/regions/{name}/attributemapping`

Example: If region has mapping `{"firstName":"first_name"}`, storing `{"firstName":"John"}` saves as `{"first_name":"John"}`

### 13.3 JSON Service

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

## 6. Client Architecture

### 13.1 Client Overview

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

### 13.2 Authentication Methods

All client constructors support both password and API key authentication:

**Python Redis Client:**
```python
# Password authentication
client = KuberRedisClient(host, port, username='admin', password='secret')

# API Key authentication (v1.2.5)
import redis
r = redis.Redis(host='localhost', port=6380)
r.execute_command('AUTH', 'APIKEY', 'kub_your_api_key_here')
```

**Python REST Client:**
```python
# Password authentication
client = KuberRestClient(host, port, username='admin', password='secret')

# API Key authentication (v1.2.5)
import requests
headers = {'X-API-Key': 'kub_your_api_key_here'}
response = requests.get('http://localhost:8080/api/cache/default/mykey', headers=headers)
```

**Java Redis Client:**
```java
// Password authentication
KuberClient client = new KuberClient(host, port, username, password);

// API Key authentication (v1.2.5)
try (Jedis jedis = new Jedis("localhost", 6380)) {
    jedis.auth("kub_your_api_key_here");
    jedis.set("key", "value");
}
```

**Java REST Client:**
```java
// Both username and password are REQUIRED
KuberRestClient client = new KuberRestClient(host, port, username, password);
```

### 13.3 Client Connection Flow

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

## 7. Protocol Design

### 13.1 Redis RESP Protocol

Kuber implements the Redis Serialization Protocol (RESP):

| Type | Prefix | Example |
|------|--------|---------|
| Simple String | `+` | `+OK\r\n` |
| Error | `-` | `-ERR unknown command\r\n` |
| Integer | `:` | `:1000\r\n` |
| Bulk String | `$` | `$5\r\nhello\r\n` |
| Array | `*` | `*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n` |
| Null | `$-1` | `$-1\r\n` |

### 13.2 Kuber Protocol Extensions

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

### 13.3 REST API Endpoints

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

Generic Search API:
  POST   /genericsearch             - Unified search with field projection
                                     - Supports: key lookup, regex pattern, JSON attribute search
                                     - Optional: fields parameter for field projection

Bulk Operations:
  POST   /cache/{region}/import     - Bulk import
  GET    /cache/{region}/export     - Bulk export
```

---

## 8. Persistence Layer

### 13.1 Pluggable Persistence Architecture

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
┌─────────┬─────────┬──────┬───────────┬─────────┬──────────┐
│ MongoDB │ SQLite  │ LMDB │PostgreSQL │ RocksDB │ InMemory │
│  Store  │  Store  │Store │   Store   │  Store  │   Store  │
└─────────┴─────────┴──────┴───────────┴─────────┴──────────┘
```

### 13.2 Backend Comparison

| Backend | Use Case | Pros | Cons |
|---------|----------|------|------|
| **RocksDB** | High-performance (Default) | Embedded, LSM-tree, per-region isolation | Complex tuning |
| **LMDB** | Zero-copy reads (v1.2.0) | ACID, crash-safe, memory-mapped, zero-copy | Write-heavy workloads |
| **MongoDB** | Production clusters | Distributed, scalable | Requires separate server |
| **SQLite** | Development/Single-node | Zero config, file-based | Single-writer, limited scale |
| **PostgreSQL** | Enterprise | ACID, mature tooling | Requires separate server |
| **InMemory** | Testing/Dev | Fastest, no I/O | No persistence |

### 13.3 Configuration

```yaml
kuber:
  persistence:
    # Options: mongodb, sqlite, postgresql, rocksdb, lmdb, memory
    # Default: rocksdb (embedded, no external dependencies)
    backend: rocksdb
    
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
    
    # RocksDB settings (default)
    rocksdb:
      path: ./data/rocksdb
    
    # LMDB settings (v1.2.0+)
    lmdb:
      path: ./data/lmdb
      map-size: 10737418240   # 10GB max database size
```

---

## 9. Replication Architecture

### 13.1 Primary/Secondary Replication

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

### 13.2 Failover Process

1. **Primary Failure Detection**: ZooKeeper detects primary node is unresponsive
2. **Leader Election**: Remaining nodes participate in leader election
3. **Promotion**: Secondary with most recent data becomes new primary
4. **Client Reconnection**: Clients reconnect to new primary via service discovery
5. **Recovery**: Old primary rejoins as secondary after recovery

---

## 10. Request/Response Messaging (v1.7.0)

Request/Response Messaging enables cache access via message brokers, allowing asynchronous, 
decoupled communication with the cache system.

### 10.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MESSAGE BROKER LAYER                                  │
├────────────────┬────────────────┬────────────────┬────────────────────────────┤
│     Kafka      │   RabbitMQ     │    ActiveMQ    │         IBM MQ            │
│  (Topics)      │   (Queues)     │   (Queues)     │        (Queues)           │
└───────┬────────┴───────┬────────┴───────┬────────┴────────────┬──────────────┘
        │                │                │                      │
        │ request_topic  │ request_queue  │ request_queue        │ REQUEST.QUEUE
        ▼                ▼                ▼                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     REQUEST RESPONSE SERVICE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐ │
│  │  Broker Adapters │  │  Blocking Queue  │  │     Thread Pool            │ │
│  │  (Multi-broker)  │  │  (Backpressure)  │  │  (Async Processing)        │ │
│  └────────┬─────────┘  └────────┬─────────┘  └─────────────┬─────────────┘ │
│           │                     │                          │               │
│           └─────────────────────┴──────────────────────────┘               │
│                                 │                                           │
│                                 ▼                                           │
│                        ┌─────────────────┐                                  │
│                        │  Cache Service  │                                  │
│                        └─────────────────┘                                  │
│                                 │                                           │
│                                 ▼                                           │
│                        ┌─────────────────┐                                  │
│                        │ Response Topic  │                                  │
│                        └─────────────────┘                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 10.2 Components

| Component | Description |
|-----------|-------------|
| **MessageBrokerAdapter** | Interface for broker-specific implementations |
| **KafkaBrokerAdapter** | Apache Kafka consumer/producer |
| **ActiveMqBrokerAdapter** | Apache ActiveMQ JMS client |
| **RabbitMqBrokerAdapter** | RabbitMQ AMQP client |
| **IbmMqBrokerAdapter** | IBM MQ JMS client |
| **RequestResponseService** | Main service managing brokers, queue, and processing |

### 10.3 Request Processing Flow

1. **Message Arrival**: Broker adapter receives message from request topic
2. **Queue Submission**: Message added to blocking queue (with backpressure)
3. **Worker Pickup**: Thread pool worker takes message from queue
4. **Authentication**: API key validated against ApiKeyService
5. **Operation Execution**: Cache operation executed via CacheService
6. **Response Composition**: Response JSON with timestamps and result
7. **Response Delivery**: Response published to inferred response topic

### 10.4 Backpressure Management

```
┌─────────────────────────────────────────────────────────────────┐
│                    BACKPRESSURE STATES                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Queue Depth: [============================-------] 80%          │
│               ↑                                                  │
│               HIGH WATER MARK - Pause Consumption                │
│                                                                  │
│  Queue Depth: [==============-------------------] 50%            │
│               ↑                                                  │
│               LOW WATER MARK - Resume Consumption                │
│                                                                  │
│  Queue Depth: [====================================] 100%        │
│               ↑                                                  │
│               QUEUE FULL - Reject Requests                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

| State | Queue % | Action |
|-------|---------|--------|
| Normal | 0-50% | Full speed consumption |
| Elevated | 50-80% | Normal consumption |
| High Water | 80%+ | Pause message consumption |
| Queue Full | 100% | Reject incoming requests |

### 10.5 Configuration

Configuration stored in `request_response.json` in the secure folder:

```json
{
  "enabled": true,
  "max_queue_depth": 100,
  "thread_pool_size": 10,
  "brokers": {
    "kafka_prod": {
      "enabled": true,
      "type": "kafka",
      "display_name": "Production Kafka",
      "connection": {
        "bootstrap_servers": "kafka1:9092,kafka2:9092",
        "group_id": "kuber-request-processor"
      },
      "request_topics": ["ccs_cache_request"]
    }
  }
}
```

### 10.6 Startup/Shutdown Integration

| Phase | Startup | Shutdown |
|-------|---------|----------|
| Messaging | Phase 7 (Last) | Phase 0.5 (First) |

This ensures:
- Cache is fully initialized before accepting broker requests
- All in-flight requests complete before cache shutdown

---

## 11. Security Architecture

### 13.1 Authentication Methods

Kuber supports multiple authentication methods for different use cases (v1.2.5):

| Method | Use Case | Protocol |
|--------|----------|----------|
| **Username/Password** | Web UI, interactive use | Form login, HTTP Basic |
| **API Key** | Programmatic access, CI/CD, services | Header, query param |
| **Redis AUTH** | Redis protocol clients | AUTH command |

### 13.2 API Key Authentication (v1.2.5)

API keys provide secure, revocable authentication for programmatic access:

```
┌─────────────────────────────────────────────────────────────┐
│                  API Key Authentication                      │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Key Format: kub_[64 hexadecimal characters]                │
│  Example: kub_a1b2c3d4e5f6...                               │
│                                                              │
│  REST API Methods:                                           │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  1. X-API-Key Header (Recommended)                    │   │
│  │     curl -H "X-API-Key: kub_xxx..." /api/cache/key   │   │
│  │                                                       │   │
│  │  2. Authorization Header                              │   │
│  │     curl -H "Authorization: ApiKey kub_xxx..." ...   │   │
│  │                                                       │   │
│  │  3. Query Parameter                                   │   │
│  │     curl "/api/cache/key?api_key=kub_xxx..."         │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  Redis Protocol:                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  AUTH APIKEY kub_xxx...                              │   │
│  │  AUTH kub_xxx...                                      │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  Key Features:                                               │
│  • Secure 64-character cryptographically random keys        │
│  • Role-based access (USER, OPERATOR, ADMIN)                │
│  • Optional expiration dates                                 │
│  • Revocation and reactivation                              │
│  • Last-used tracking for audit                              │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 13.3 Password Authentication

**All clients MUST provide credentials. This is enforced at the client level.**

```
┌─────────────────────────────────────────────────────────────┐
│                  Password Authentication                     │
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

### 13.4 User Configuration

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

## 11. Data Flow

### 13.1 Write Operation Flow (v1.3.10)

Kuber supports two write modes configured via `kuber.persistence.sync-individual-writes`:

#### ASYNC Mode (Default) - `sync-individual-writes: false`

Memory is updated first, disk write happens in background. Faster but eventual consistency.

```
┌──────────┐   ┌───────────┐   ┌─────────────┐   ┌─────────────┐   ┌──────────────┐
│  Client  │   │ Protocol  │   │   Cache     │   │  KeyIndex   │   │ Persistence  │
│          │   │  Handler  │   │  Service    │   │ + ValueCache│   │    Store     │
└────┬─────┘   └─────┬─────┘   └──────┬──────┘   └──────┬──────┘   └──────┬───────┘
     │               │                │                 │                 │
     │ SET key val   │                │                 │                 │
     │──────────────►│                │                 │                 │
     │               │ set(region,key)│                 │                 │
     │               │───────────────►│                 │                 │
     │               │                │                 │                 │
     │               │                │ 1. keyIndex.put │                 │
     │               │                │────────────────►│ (IMMEDIATE)     │
     │               │                │                 │                 │
     │               │                │ 2. valueCache   │                 │
     │               │                │────────────────►│ (IMMEDIATE)     │
     │               │                │                 │                 │
     │               │                │ 3. saveEntryAsync ─ ─ ─ ─ ─ ─ ─ ─►│
     │               │                │    (NON-BLOCKING)                 │ (BACKGROUND)
     │               │ +OK            │                 │                 │
     │◄──────────────│◄───────────────│                 │                 │
     │               │                │                 │      ┌──────────┴──────────┐
     │               │                │                 │      │ Eventually written  │
     │               │                │                 │      │ to disk             │
     │               │                │                 │      └─────────────────────┘
```

**Timing**: ~0.01-0.1ms (returns after memory update)

#### SYNC Mode - `sync-individual-writes: true`

Disk write completes before returning. Maximum durability.

```
┌──────────┐   ┌───────────┐   ┌─────────────┐   ┌─────────────┐   ┌──────────────┐
│  Client  │   │ Protocol  │   │   Cache     │   │  KeyIndex   │   │ Persistence  │
│          │   │  Handler  │   │  Service    │   │ + ValueCache│   │    Store     │
└────┬─────┘   └─────┬─────┘   └──────┬──────┘   └──────┬──────┘   └──────┬───────┘
     │               │                │                 │                 │
     │ SET key val   │                │                 │                 │
     │──────────────►│                │                 │                 │
     │               │ set(region,key)│                 │                 │
     │               │───────────────►│                 │                 │
     │               │                │                 │                 │
     │               │                │ 1. saveEntry    │                 │
     │               │                │─────────────────┼────────────────►│
     │               │                │                 │                 │ (SYNC WRITE)
     │               │                │                 │                 │ fsync()
     │               │                │                 │  WRITE CONFIRMED│
     │               │                │◄────────────────┼─────────────────│
     │               │                │                 │                 │
     │               │                │ 2. keyIndex.put │                 │
     │               │                │────────────────►│                 │
     │               │                │                 │                 │
     │               │                │ 3. valueCache   │                 │
     │               │                │────────────────►│                 │
     │               │                │                 │                 │
     │               │ +OK            │                 │                 │
     │◄──────────────│◄───────────────│                 │                 │
```

**Timing**: ~1-5ms (includes fsync)

#### Write Mode Comparison

| Aspect | ASYNC (default) | SYNC |
|--------|-----------------|------|
| Latency | ~0.01-0.1ms | ~1-5ms |
| Throughput | 10,000-100,000 ops/sec | 200-1,000 ops/sec |
| Durability | Eventually consistent | Immediate |
| Crash Risk | Entry may be lost | No data loss |
| Use Case | Performance critical | Durability critical |

### 13.2 Read Operation Flow

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

## 12. Deployment Patterns

### 13.1 Single Node (Development)

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

### 13.2 High Availability Cluster

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

## 13. Backup and Restore (v1.4.0)

Kuber provides automatic periodic backup of all regions and automatic restore when backup files are placed in the restore directory.

### 13.1 Supported Persistence Stores

| Store | Supported | Reason |
|-------|-----------|--------|
| RocksDB | ✓ | Full support via BackupRestoreService |
| LMDB | ✓ | Full support via BackupRestoreService |
| SQLite | ✗ | Use SQLite's `.backup` command |
| PostgreSQL | ✗ | Use `pg_dump` / `pg_restore` |
| MongoDB | ✗ | Use `mongodump` / `mongorestore` |

### 13.2 Backup Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         BackupRestoreService                            │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌───────────────────────┐    ┌───────────────────────┐                 │
│  │   Backup Scheduler    │    │   Restore Watcher     │                 │
│  │   (cron: 11 PM daily) │    │   (every 30 sec)      │                 │
│  └──────────┬────────────┘    └──────────┬────────────┘                 │
│             │                            │                              │
│             ▼                            ▼                              │
│  ┌───────────────────────┐    ┌───────────────────────┐                 │
│  │  For each region:     │    │  For each file in     │                 │
│  │  - loadEntries()      │    │  ./restore:           │                 │
│  │  - Write to JSONL     │    │  - Parse region name  │                 │
│  │  - Gzip compress      │    │  - Lock region        │                 │
│  │  - Save to ./backup   │    │  - Purge existing     │                 │
│  └───────────────────────┘    │  - Restore in batches │                 │
│                               │  - Unlock region      │                 │
│                               │  - Move to ./backup   │                 │
│                               └───────────────────────┘                 │
└─────────────────────────────────────────────────────────────────────────┘
```

### 13.3 Backup File Format

```
# KUBER_BACKUP_HEADER: {"version":"1.4.0","region":"customers","timestamp":"2025-12-07T14:30:22Z"}
{"id":"uuid1","key":"cust001","region":"customers","valueType":"JSON","jsonValue":{...}}
{"id":"uuid2","key":"cust002","region":"customers","valueType":"JSON","jsonValue":{...}}
...
```

- **Format**: JSONL (one CacheEntry per line)
- **Compression**: Gzip (optional, enabled by default)
- **Naming**: `<region>.<timestamp>.backup[.gz]`
- **Timestamp**: `yyyyMMdd_HHmmss`

### 13.4 Region Locking During Restore

```
┌────────────────────────────────────────────────────────────────┐
│                      Region Lock State                          │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Normal State:          Restore in Progress:                   │
│  ┌──────────────┐       ┌──────────────┐                       │
│  │   Region X   │       │   Region X   │                       │
│  │  ┌────────┐  │       │  ┌────────┐  │                       │
│  │  │ OPEN   │  │  ──►  │  │ LOCKED │  │                       │
│  │  └────────┘  │       │  └────────┘  │                       │
│  │              │       │              │                       │
│  │  GET ✓       │       │  GET ✗       │                       │
│  │  SET ✓       │       │  SET ✗       │                       │
│  │  DEL ✓       │       │  DEL ✗       │                       │
│  └──────────────┘       └──────────────┘                       │
│                                                                 │
│  Error Response: "Region 'X' is currently being restored.      │
│                   Please wait for restore to complete."         │
│                                                                 │
└────────────────────────────────────────────────────────────────┘
```

### 13.5 Restore Data Flow

```
1. File Detection
   ./restore/customers.20251207_143022.backup.gz
        │
        ▼
2. Parse Region Name
   "customers" extracted from filename
        │
        ▼
3. Lock Region
   regionsBeingRestored.put("customers", true)
        │
        ▼
4. Purge Existing Data
   persistenceStore.purgeRegion("customers")
   cacheService.clearRegionCaches("customers")
        │
        ▼
5. Restore in Batches (batch-size: 10000)
   ┌─────────────────────────────────────────┐
   │  Read JSONL  ──►  saveEntries()         │
   │              ──►  loadEntriesIntoCache()│
   │  (repeat until EOF)                     │
   └─────────────────────────────────────────┘
        │
        ▼
6. Unlock Region
   regionsBeingRestored.put("customers", false)
        │
        ▼
7. Move Processed File
   ./restore/... ──► ./backup/restored_<timestamp>_...
```

### 13.6 Configuration

```yaml
kuber:
  backup:
    enabled: true                    # Enable backup/restore service
    backup-directory: ./backup       # Where backup files are stored
    restore-directory: ./restore     # Monitor this for restore files
    cron: "0 0 23 * * *"            # Schedule: 11:00 PM daily (default)
    max-backups-per-region: 10       # Retention policy (0 = keep all)
    compress: true                   # Gzip compression
    batch-size: 10000               # Entries per batch
    file-encoding: UTF-8
    create-directories: true
```

#### Cron Expression Examples

| Expression | Description |
|------------|-------------|
| `0 0 23 * * *` | 11:00 PM daily (default) |
| `0 0 2 * * *` | 2:00 AM daily |
| `0 0 */6 * * *` | Every 6 hours |
| `0 30 1 * * SUN` | 1:30 AM every Sunday |

### 13.7 Integration with Startup Sequence

BackupRestoreService is started in Phase 6 of the startup sequence:

```
Phase 1: Wait 10 seconds for stabilization
Phase 2: Persistence maintenance (compaction)
Phase 3: Cache service initialization (recovery)
Phase 4: Redis protocol server
Phase 5: Autoload service
Phase 6: Backup/Restore service  ← NEW
Phase 7: System ready
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

## Appendix: System Internals

### Memory Management in Kuber

Kuber employs a **Hybrid Memory Architecture** inspired by Aerospike:

1. **KeyIndex (Always in Memory)**
   - Every key ever written is tracked in the KeyIndex
   - ~100-150 bytes per key (key string, metadata, pointers)
   - Never evicted under memory pressure
   - Enables O(1) EXISTS and O(n) KEYS operations without disk I/O

2. **Value Cache (Caffeine, LRU Eviction)**
   - Hot values kept in memory
   - Configurable per-region size limits
   - When full, LRU eviction to disk

3. **Memory Watcher Service**
   - Monitors JVM heap every 5 seconds
   - When heap > 85% (high watermark), starts eviction
   - Evicts values (not keys) in batches of 1000
   - Continues until heap < 50% (low watermark)
   - **Guarantee**: No OOM crashes in 24x7 operation

### Off-Heap Key Index Architecture (v1.3.2)

For systems with millions of keys, the on-heap KeyIndex can cause GC pressure. The **Off-Heap Key Index** stores keys in direct memory (DRAM) outside the Java heap.

#### Segmented Buffer Design

Since Java's `ByteBuffer.allocateDirect()` is limited to ~2GB per buffer, Kuber uses **multiple 1GB segments**:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    OFF-HEAP KEY INDEX                                │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │  Segment 0  │  │  Segment 1  │  │  Segment 2  │  ...             │
│  │   (1 GB)    │  │   (1 GB)    │  │   (1 GB)    │                  │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                  │
│         │                │                │                          │
│         └────────────────┴────────────────┴──────────────────────────│
│                             │                                        │
│              Global Offset (long): position across segments          │
├─────────────────────────────────────────────────────────────────────┤
│  On-Heap Index:  ConcurrentHashMap<String, Long>  (key → offset)    │
└─────────────────────────────────────────────────────────────────────┘
```

#### Memory Layout Per Entry

Each key entry in off-heap memory uses this compact format:

```
┌──────────┬─────────────┬───────────────────────────────────────────────┐
│ Bytes    │ Field       │ Description                                   │
├──────────┼─────────────┼───────────────────────────────────────────────┤
│ 2        │ keyLength   │ Length of key in bytes (short)                │
│ variable │ keyBytes    │ UTF-8 encoded key string                      │
│ 8        │ expiresAt   │ Expiration timestamp (-1 = no expiration)     │
│ 1        │ location    │ Value location (MEMORY=0, DISK=1, BOTH=2)     │
│ 4        │ valueSize   │ Size of value in bytes                        │
│ 8        │ lastAccess  │ Last access timestamp                         │
│ 4        │ accessCount │ Number of accesses                            │
└──────────┴─────────────┴───────────────────────────────────────────────┘
Total metadata: 25 bytes + 2 bytes (key length) + key bytes
```

#### Configuration

```yaml
kuber:
  cache:
    off-heap-key-index: true
    off-heap-key-index-initial-size-mb: 16    # Initial segment size
    off-heap-key-index-max-size-mb: 8192      # Max total size (8GB)
```

#### Key Features

| Feature | Description |
|---------|-------------|
| **Zero GC Pressure** | Keys stored outside Java heap |
| **>2GB Support** | Segmented architecture allows 8GB+ per region |
| **Auto-Growth** | New segments allocated as data grows |
| **Compaction** | Automatic defragmentation when >30% deleted |
| **Thread-Safe** | ReentrantReadWriteLock for buffer access |

#### When to Use Off-Heap

| Scenario | Recommendation |
|----------|----------------|
| < 1 million keys | On-heap (default) is simpler |
| 1-10 million keys | Off-heap recommended |
| > 10 million keys | Off-heap required |
| GC tuning problems | Off-heap helps |
| Large key strings | Off-heap more efficient |

**Value Retrieval Flow (when value not in memory):**
```
GET key
  └── Check KeyIndex (O(1), always in memory)
        ├── Key NOT found → Return NULL immediately (no disk I/O)
        └── Key found
              └── Check Value Cache
                    ├── Value in cache → Return immediately
                    └── Value NOT in cache (cold)
                          └── Load from persistence store
                                ├── Found → Promote to cache, return value
                                └── Not found → Cleanup index, return NULL
```

### Thread Model

| Thread Pool | Purpose |
|-------------|---------|
| main | Spring Boot startup |
| kuber-startup-orchestrator | Startup sequence coordination |
| NioProcessor-[N] | MINA I/O processing |
| pool-[N]-thread-[M] | Redis command handlers |
| scheduling-1 | @Scheduled tasks (TTL, memory, compaction) |
| kuber-autoload | File scanning/processing |
| kuber-backup | Backup scheduler |
| kuber-restore-watcher | Restore file monitoring |
| persistence-async-save-[0-3] | Region-partitioned async writes (v1.4.0) |
| http-nio-8080-exec-[N] | REST API handlers |

### Region-Partitioned Async Executors (v1.4.0)

Async writes use 4 single-thread executors with hash-based partitioning:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Region-Partitioned Async Writes                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  Region "customers"  ──► hash % 4 = 2 ──► persistence-async-save-2      │
│  Region "products"   ──► hash % 4 = 0 ──► persistence-async-save-0      │
│  Region "orders"     ──► hash % 4 = 1 ──► persistence-async-save-1      │
│  Region "inventory"  ──► hash % 4 = 3 ──► persistence-async-save-3      │
│                                                                          │
│  Benefits:                                                               │
│  • All writes for same region are SEQUENTIAL (no race conditions)       │
│  • Different regions can write in PARALLEL (up to 4 concurrent)         │
│  • Consistent mapping: same region always uses same executor            │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Safety Guarantees

1. **Write Durability**
   - KeyIndex update: Synchronous
   - Value Cache update: Synchronous
   - Persistence write: Asynchronous (queued immediately)

2. **Crash Recovery**
   - RocksDB: WAL (Write-Ahead Log) for recovery
   - LMDB: Full ACID, copy-on-write
   - SQLite: Journal mode (WAL or rollback)

3. **Startup Recovery**
   - All data recovered from persistence
   - Keys loaded into KeyIndex
   - Hot values loaded into cache (up to memory limit)
   - Redis server starts AFTER recovery complete

---

## Appendix B: Event Publishing (v1.2.8)

Kuber can publish cache events to external messaging systems for real-time integrations using a pluggable publisher architecture.

### Supported Publishers

| Publisher | Type | Features |
|-----------|------|----------|
| **Apache Kafka** | kafka | High throughput streaming, configurable retention, auto topic creation |
| **Apache ActiveMQ** | activemq | Enterprise JMS messaging, configurable TTL, queue/topic support |
| **RabbitMQ** | rabbitmq | AMQP messaging, flexible routing, exchange types, auto recovery |
| **IBM MQ** | ibmmq | Enterprise messaging, SSL/TLS support, queue manager integration |
| **File System** | file | JSON Lines format, auto rotation, network share support |

### EventPublisher Interface

All publishers implement a common interface, making it easy to add new destinations:

```java
public interface EventPublisher {
    String getType();                               // "kafka", "rabbitmq", "file"
    String getDisplayName();                        // "Apache Kafka", "RabbitMQ"
    void initialize();                              // Setup connections at startup
    void onStartupOrchestration();                  // Create topics/queues
    boolean isEnabledForRegion(String region);      // Check if enabled
    void publish(String region, CachePublishingEvent event);  // Publish event
    void shutdown();                                // Cleanup
}
```

### Event Message Format

```json
// Insert/Update Events
{
  "key": "user:1001",
  "action": "inserted",  // or "updated"
  "region": "customers",
  "payload": { "name": "John", "email": "john@example.com" },
  "timestamp": "2025-12-06T12:00:00Z",
  "nodeId": "kuber-01"
}

// Delete Events
{
  "key": "user:1001",
  "action": "deleted",
  "region": "customers",
  "timestamp": "2025-12-06T12:05:00Z",
  "nodeId": "kuber-01"
}
```

### Publishing Architecture

```
CacheService.set() / delete()
         │
         │ Submit to async queue (non-blocking)
         ▼
┌─────────────────────────────┐
│  Publishing Thread Pool     │ (configurable, default: 4 threads)
└────────────┬────────────────┘
             │
             ▼
      PublisherRegistry
             │
    ┌────┬───┴───┬────┬────┐
    ▼    ▼       ▼    ▼    ▼
 Kafka  AMQ  RabbitMQ IBM  File
                      MQ
```

### Key Design Principles

1. **Interface-driven**: Easy to add new publisher implementations
2. **Non-blocking**: Main cache operations never wait for publishing
3. **Multi-destination**: One region can publish to multiple destinations simultaneously
4. **Isolated failures**: One publisher failing doesn't affect others
5. **Per-region config**: Each region can have different brokers/topics/files
6. **Fire-and-forget**: Publishing failures don't affect cache operations

### Thread Model Update (v1.2.7)

| Thread Pool | Purpose |
|-------------|---------|
| kuber-event-publisher-[N] | Async event publishing to all configured destinations |

---

*End of Architecture Document*
