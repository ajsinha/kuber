# Kuber: A Lightweight Multi-Protocol Distributed Cache for Low-Volume Workloads with Hybrid Memory Architecture and Integrated Message Broker Access

**Ashutosh Sinha**
Independent Researcher
ajsinha@gmail.com
Repository: https://github.com/ajsinha/kuber

---

## Abstract

Enterprise caching infrastructure has long been dominated by systems designed for internet-scale workloads—Redis, Memcached, Hazelcast, and Apache Ignite—requiring substantial operational investment in cluster management, distributed consensus, and specialized expertise. However, a significant segment of the market—development teams, departmental applications, early-stage startups, and compliance-sensitive environments—requires capable caching infrastructure at far lower volumes, where the complexity and cost of internet-scale systems represent unnecessary overhead. We present Kuber, a lightweight open-source distributed cache that addresses this underserved segment through three architectural innovations: (1) a hybrid memory model inspired by Aerospike that maintains all keys in memory with tiered value storage across Caffeine LRU cache and pluggable persistence backends (RocksDB, LMDB, SQLite, MongoDB, PostgreSQL), (2) a multi-protocol access layer supporting Redis RESP protocol, REST HTTP/JSON API, and message broker channels (Kafka, RabbitMQ, ActiveMQ, IBM MQ) through a single-JAR deployment, and (3) an integrated web administration console with role-based access control, API key management, and structured event publishing. Kuber provides Redis protocol compatibility for drop-in migration, JSON-native document querying with secondary indexing, SSL/TLS security, and replication—all without external dependencies beyond a JVM runtime. This paper presents Kuber's architecture, capabilities, and a qualitative comparison with existing systems, positioning it as a practical low-cost alternative for deployments not requiring internet-scale infrastructure.

**Keywords:** Distributed caching, Key-value store, Hybrid memory architecture, Multi-protocol access, Redis compatibility, JSON document store, Message broker integration, Low-volume workloads, Open-source infrastructure

---

## 1 Introduction

The proliferation of microservice architectures, real-time analytics, and data-intensive applications has made caching infrastructure a critical component of modern software systems. Organizations routinely deploy caching layers to reduce database load, accelerate response times, and enable ephemeral data storage for session management, rate limiting, and inter-service communication.

The caching landscape is dominated by systems engineered for internet-scale deployments. Redis [2] serves millions of operations per second across distributed clusters. Memcached [1] powers Facebook's multi-tiered caching infrastructure handling billions of requests daily [3]. Hazelcast [4] and Apache Ignite [5] provide distributed in-memory data grids with sophisticated cluster management. These systems have proven their value at massive scale, but their architectural assumptions impose significant overhead on smaller deployments.

Existing systems present three fundamental challenges for low-volume workloads:

1. **Operational complexity** in deployment and management requires dedicated infrastructure teams. Redis Cluster demands a minimum of six nodes for high availability. Hazelcast and Ignite require careful tuning of distributed consensus and partition management. Even single-node Redis deployments require external tools for administration, monitoring, and security configuration.

2. **Licensing uncertainty** has disrupted the ecosystem. Redis Ltd. transitioned from BSD to RSALv2/SSPLv1 in March 2024 [6], then added AGPLv3 as a third option with Redis 8 in May 2025 [7]. This created community forks including Valkey [8] (Linux Foundation) and Redict (LGPL), fragmenting the ecosystem and introducing adoption risk for organizations with strict licensing requirements.

3. **Feature stratification** reserves essential capabilities behind commercial licenses. Hazelcast's enterprise features—including TLS/SSL, role-based access control, and advanced persistence—require commercial licensing. Apache Ignite's full capabilities similarly require GridGain's commercial distribution. Organizations needing security and persistence for modest workloads face disproportionate licensing costs.

We present Kuber¹, a lightweight distributed cache that addresses these limitations through a single-JAR deployment providing multi-protocol access, hybrid memory architecture, integrated web administration, and comprehensive security—all under an open-source license suitable for low-to-moderate volume workloads.

> ¹ Sanskrit for "Earth" / "Treasure" — reflecting the cache's role as a repository for valued data.

### 1.1 Contributions

This paper makes the following contributions:

- **Kuber Framework:** An open-source multi-protocol distributed cache combining Redis RESP compatibility, REST API, and message broker access in a single-JAR deployment (§4)
- **Hybrid Memory Architecture:** An Aerospike-inspired design with in-memory keys and tiered value storage across five pluggable persistence backends (§4.3)
- **JSON Document Store:** Native JSON operations with JSONPath querying, criteria-based search, and secondary indexing (§6)
- **Comprehensive Literature Survey:** Analysis of 10+ caching systems spanning key-value stores, in-memory data grids, and emerging alternatives (§3)
- **Qualitative Feature Comparison:** Systematic comparison across 19 capability dimensions (§7)

---

## 2 Background and Motivation

### 2.1 The Caching Landscape

Modern caching systems span a spectrum from single-node in-process caches to globally distributed clusters. This spectrum can be characterized along three axes: deployment complexity, feature richness, and operational cost. Internet-scale systems such as Redis, Memcached, Hazelcast, and Ignite occupy the high end of all three axes, providing sophisticated capabilities at correspondingly high operational investment.

### 2.2 The Underserved Middle

Between lightweight in-process caches (Caffeine [11], Ehcache [12]) and internet-scale systems lies a significant gap. Many real-world deployments serve tens to hundreds of users, manage datasets measured in gigabytes rather than terabytes, and process thousands—not millions—of operations per second. These workloads include: internal development and testing environments, departmental dashboards and tools, early-stage startup infrastructure, data pipeline staging caches, and compliance-sensitive applications requiring built-in security.

For such workloads, deploying Redis Cluster with Sentinel, or configuring Hazelcast with discovery and partition management, represents significant over-engineering. The operational investment in monitoring, backup, security configuration, and administration often exceeds the value of the caching layer itself.

### 2.3 Licensing Disruption

The Redis licensing transition has created particular uncertainty. Redis Ltd.'s shift from BSD to dual RSALv2/SSPLv1 licensing in March 2024 [6] restricted cloud service providers and triggered the creation of Valkey under the Linux Foundation, Redict under LGPL, and KeyDB (previously acquired by Snap) [9]. The subsequent addition of AGPLv3 with Redis 8 in May 2025 [7] attempted to address community concerns but added further complexity to licensing evaluation. Organizations with strict open-source policies now face a fragmented landscape with unclear long-term trajectories.

---

## 3 Related Work

We survey existing caching systems across four categories: foundational key-value stores, distributed in-memory data grids, lightweight single-node caches, and emerging alternatives.

### 3.1 Foundational Key-Value Stores

**Memcached** [1] pioneered distributed caching with a simple, multi-threaded architecture using consistent hashing for horizontal scaling. Its simplicity remains compelling—a single binary with minimal configuration—but fundamental limitations constrain its applicability: no persistence mechanism (data lost on restart), no built-in replication, no authentication or access control, and support limited to simple key-value strings. Nishtala et al. [3] documented Facebook's extensive engineering effort to scale Memcached, including custom replication layers and invalidation protocols that far exceed the base system's capabilities.

**Redis** [2] extended the key-value paradigm with rich data structures (strings, hashes, lists, sets, sorted sets, streams, bitmaps, HyperLogLog), persistence via RDB snapshots and AOF logging, built-in replication, Lua scripting, pub/sub messaging, and cluster mode with automatic sharding. Redis has become the de facto standard for application caching, with AWS ElastiCache [25] and similar managed services providing hosted deployments. However, its licensing evolution—from BSD to RSALv2/SSPLv1 (March 2024) [6] to triple-licensed with AGPLv3 (Redis 8, May 2025) [7]—has introduced adoption risk. Community forks have emerged: Valkey [8] (BSD, Linux Foundation backing) preserves the pre-license-change codebase, while Redict (LGPL) and KeyDB [10] (BSD, multi-threaded) offer alternative trajectories.

### 3.2 Distributed In-Memory Data Grids

**Hazelcast** [4] provides a Java-based In-Memory Data Grid (IMDG) with JCache (JSR-107) compliance, distributed data structures, CP subsystem based on Raft consensus, and compute grid capabilities. Hazelcast excels in embedded Java deployments and provides sophisticated cluster management with split-brain protection. However, enterprise features including TLS/SSL, RBAC, persistence, and WAN replication require commercial licensing, creating a significant cost barrier for smaller organizations. A DZone comparison [26] with Apache Ignite highlights the trade-offs between Hazelcast's operational simplicity and Ignite's broader feature set.

**Apache Ignite** [5] provides a memory-centric distributed platform combining in-memory caching, distributed SQL (ANSI-99 compliant), ACID transactions, compute grid, machine learning capabilities, and native persistence. Ignite's breadth is unmatched, but this comprehensiveness introduces significant operational complexity. Dean and Barroso [23] noted the challenges of tail latency in distributed systems—challenges that Ignite's multi-layer architecture can amplify. The learning curve for proper configuration of memory policies, persistence, and SQL indexing represents a substantial investment.

### 3.3 Lightweight Single-Node Caches

**Ehcache** [12] provides mature local caching with tiered storage (heap, off-heap, disk), deep integration with Hibernate and Spring frameworks, and JCache compliance. Ehcache 3 with Terracotta provides clustering capabilities, but the single-node version remains most widely deployed. Ehcache serves primarily as an embedded library rather than a standalone caching service, limiting its applicability for polyglot architectures.

**Caffeine** [11] is a high-performance single-node Java cache using the Window TinyLFU eviction policy, achieving near-optimal hit rates with O(1) operations. Caffeine is excellent for in-process caching but provides no persistence, no network access protocol, no administration interface, and no multi-language support. It serves as a building block rather than a complete caching solution.

### 3.4 Emerging Alternatives

**Dragonfly** [13] is a modern Redis-compatible cache claiming 25x throughput improvement through a shared-nothing multi-threaded architecture with io_uring. While impressive for high-throughput scenarios, Dragonfly targets the same internet-scale segment as Redis.

**KeyDB** [10] (now part of Snap) provides a multi-threaded Redis fork with active replication and FLASH storage support under BSD license. KeyDB addresses Redis's single-threaded limitation but inherits its operational model.

**Valkey** [8] forked from Redis 7.2.4 following the license change, maintaining BSD licensing under Linux Foundation governance. Valkey provides continuity for the pre-change Redis codebase but represents a maintenance fork rather than an architectural rethinking.

**Garnet** [14] from Microsoft Research is a high-performance cache-store implemented in C# using the .NET framework, designed for both cache and storage workloads. Garnet targets cloud-native deployments with RESP protocol compatibility.

### 3.5 Persistence Backend Technologies

Kuber's pluggable persistence leverages established storage engines studied extensively in the literature.

**RocksDB** [15] employs LSM-tree architecture originally described by O'Neil et al. [18], optimized for write-heavy workloads on flash storage. RocksDB's compaction strategies, bloom filters, and configurable compression make it suitable for datasets larger than available memory.

**LMDB** [16] uses memory-mapped B+-trees with copy-on-write semantics, providing zero-copy reads, ACID transactions without write-ahead logging, and crash-resilient storage. Chu [16] demonstrated LMDB's superior read performance for datasets fitting in memory. Agrawal and Mamajiwala [17] provided comparative analysis of RocksDB, LMDB, and MongoDB for embedded storage, confirming LMDB's read advantages and RocksDB's write throughput superiority.

### 3.6 Consistency Models in Distributed Caches

The CAP theorem, formally articulated by Brewer [19] and proved by Gilbert and Lynch [20], establishes fundamental trade-offs in distributed systems. Modern distributed caches navigate these trade-offs through various consistency models. DeCandia et al. [21] (Amazon Dynamo) demonstrated eventual consistency for high availability. Chang et al. [22] (Google Bigtable) showed strong consistency through distributed locking. Recent work by Zhang et al. [27] surveys consistency model evolution in distributed caching, noting the trend toward configurable consistency per operation. Kuber's primary-secondary replication model provides eventual consistency appropriate for its target workloads, avoiding the complexity of distributed consensus protocols.

---

## 4 System Architecture

Kuber is architected as a single-JAR application with layered separation of concerns between access protocols, cache service logic, persistence, and administration.

### 4.1 High-Level Architecture

The system comprises three converging access protocols feeding a unified cache service layer:

1. **Redis RESP Protocol:** TCP-based wire protocol providing drop-in compatibility with Redis clients across all major programming languages.
2. **REST HTTP/JSON API:** HTTP endpoints supporting JSON-native operations including document storage, JSONPath queries, batch operations, and CSV export.
3. **Message Broker Channels:** Asynchronous request-response messaging via Kafka, RabbitMQ, ActiveMQ, and IBM MQ with configurable SSL/TLS.

All three protocols converge on a unified cache service layer that manages region-based isolation, key indexing, value caching, persistence, and event publishing.

### 4.2 Region-Based Isolation

Each cache region operates as an independent unit with its own key index, value cache, and persistence store. Regions are initialized concurrently at startup and provide namespace isolation for multi-tenant or multi-application deployments. This design enables per-region configuration of eviction policies, TTL defaults, persistence backends, and security permissions.

### 4.3 Hybrid Memory Model

Kuber employs an Aerospike-inspired [24] hybrid memory architecture with the following characteristics:

**All keys always in memory:** The key index resides entirely in RAM, enabling O(1) key existence checks (EXISTS), instant pattern matching (KEYS), and exact entry counts (DBSIZE) without disk access. This eliminates the bloom-filter false positives and disk-scanning overhead common in LSM-tree-based systems.

**Tiered value storage:** Hot values are cached in a Caffeine [11] LRU cache sized to available memory. Cold values reside exclusively in the persistence backend, loaded on demand. This tiering enables datasets significantly larger than available heap memory while maintaining sub-millisecond access for frequently used entries.

**Off-heap key index:** An optional DRAM-based key store outside the Java heap supports key sets exceeding 2GB without garbage collection pressure. This addresses a fundamental limitation of heap-based Java caches where large key sets trigger stop-the-world GC pauses.

#### Table 1: Hybrid Memory Model Components

| Component | Location | Purpose |
|---|---|---|
| Key Index | Heap / Off-heap | O(1) lookups, pattern match |
| Hot Values | Caffeine LRU | Sub-ms access, active data |
| Cold Values | Persistence backend | Capacity beyond memory |
| Off-Heap Store | Direct memory (DRAM) | GC-free key storage |

### 4.4 Pluggable Persistence

Kuber supports five persistence backends, selectable per region:

#### Table 2: Supported Persistence Backends

| Backend | Architecture | Best For |
|---|---|---|
| RocksDB | LSM-tree [18] | Write-heavy, large datasets |
| LMDB | Memory-mapped B+-tree [16] | Read-heavy, in-memory fits |
| SQLite | B-tree (SQL) | Simplicity, portability |
| MongoDB | Document store | JSON-native operations |
| PostgreSQL | MVCC relational | SQL integration needs |

### 4.5 Secondary Indexing

Kuber implements two index types for JSON document queries: hash indexes providing O(1) equality lookups, and B-tree indexes providing O(log n) range queries. Indexes are maintained as hybrid structures combining in-memory operation with RocksDB-backed persistence for durability.

---

## 5 Multi-Protocol Access Layer

### 5.1 Redis Protocol (RESP)

Kuber implements the Redis Serialization Protocol (RESP) for wire-level compatibility with existing Redis clients. Standard Redis commands (GET, SET, DEL, EXISTS, EXPIRE, KEYS, TTL, DBSIZE, PING, INFO, etc.) are supported, enabling drop-in replacement for applications using Redis client libraries in Python, Java, C#, Go, Ruby, Node.js, and other languages.

### 5.2 REST HTTP/JSON API

The REST API provides JSON-native operations beyond traditional key-value semantics:

#### Table 3: REST API Capabilities

| Operation | Description |
|---|---|
| Document Store | Store/retrieve JSON documents |
| JSONPath Query | Extract nested values via JSONPath |
| Criteria Search | Equality, IN, regex, range, not-equal |
| Batch Operations | Multi-key get/set/delete |
| JSON Merge | Partial document update (JUPDATE) |
| CSV Export | Tabular export of cache contents |
| TTL Management | Per-entry time-to-live configuration |
| Cross-Region Ops | Operations spanning multiple regions |

### 5.3 Message Broker Access

Kuber provides cache access through message brokers via asynchronous request-response messaging. Supported brokers and their SSL/TLS configurations are:

#### Table 4: Supported Message Brokers

| Broker | SSL/TLS Modes |
|---|---|
| Apache Kafka | JKS, PEM, SASL_SSL |
| RabbitMQ | JKS, PEM, mTLS |
| ActiveMQ | JKS, PEM |
| IBM MQ | JKS, mTLS_JKS, mTLS_PEM |

This enables architectures where cache access occurs entirely through existing messaging infrastructure without additional network protocols or firewall rules—particularly valuable in enterprise environments with strict network policies.

### 5.4 Multi-Language Client Support

Kuber provides client libraries and standalone demo applications for three languages:

- **Python:** Redis protocol (redis-py), REST API (requests/urllib), broker access
- **Java:** Redis protocol (Jedis/Lettuce), REST API (HttpClient), broker access
- **C#/.NET:** Redis protocol (StackExchange.Redis), REST API (HttpClient), broker access

Each client supports all three access protocols. Demo applications use only standard libraries to minimize dependency requirements.

---

## 6 Capabilities

### 6.1 JSON Document Store

Kuber provides first-class JSON document operations as core features rather than optional modules:

**JSONPath Queries:** Extract nested values from stored JSON documents using standard JSONPath expressions, enabling retrieval of specific fields without transferring entire documents.

**Criteria-Based Search:** Query documents across a region using operators including equality, IN (set membership), regex pattern matching, numeric range (greater-than, less-than), and not-equal. Results support pagination and field projection.

**JSON Merge (JUPDATE):** Partial document updates via JSON merge patch semantics, avoiding read-modify-write cycles for field-level modifications.

**Secondary Indexes:** Hash and B-tree indexes on JSON fields accelerate queries from O(n) full scans to O(1) or O(log n) indexed lookups.

### 6.2 Event Publishing

Kuber publishes structured JSON events to message brokers on cache mutations:

- Event types: CREATE, UPDATE, DELETE, EXPIRE
- Per-region configuration with topic/queue targeting
- Fan-out to multiple broker/topic pairs per region
- SSL/TLS support: JKS, PEM, SASL_SSL, mTLS_JKS, mTLS_PEM
- JSON-externalized configuration for runtime modification

This enables event-driven architectures where downstream systems react to cache changes without polling.

### 6.3 Security

**Role-Based Access Control (RBAC):** Fine-grained permissions per cache region with READ, WRITE, DELETE, and ADMIN roles. Users and roles are managed through the web console or configuration files.

**API Key Authentication:** Machine-to-machine authentication via X-API-Key header, Authorization: ApiKey bearer tokens, or query parameter. API keys are scoped to specific regions and permission levels.

**Transport Security:** HTTPS for REST API and web console, TLS for Redis protocol connections. SSL/TLS for all message broker connections with certificate management.

**Hot Reload:** Security configurations can be updated without server restart.

### 6.4 Web Administration Console

Kuber includes a Bootstrap-based web console providing:

- Cache statistics dashboard with real-time metrics
- Region management (create, configure, monitor)
- Entry browsing, editing, and JSON query interface
- Message broker management (live connect/disconnect)
- Event publishing and messaging channel configuration
- API key management with role-based scoping
- Integrated help documentation
- Dark/light theme support
- Prometheus metrics endpoint for monitoring integration

### 6.5 Operational Features

**Autoload:** Bulk import from CSV, TXT, and JSON files at startup or on demand.

**Backup/Restore:** Region-level backup and restore operations.

**Smart Memory Management:** Dual eviction combining heap watermark monitoring with count-based entry limits. Warm object counts ensure minimum in-memory entries for critical data.

**Compaction:** Pre-startup compaction for RocksDB and SQLite backends. Scheduled compaction via cron expressions (default: 2 AM daily).

**Replication:** Primary-secondary replication via ZooKeeper coordination with automatic failover.

---

## 7 Feature Comparison

Table 5 provides a qualitative comparison of Kuber against established caching systems across 19 capability dimensions. The comparison focuses on architectural capabilities rather than performance metrics, as Kuber targets a fundamentally different workload profile than internet-scale systems.

#### Table 5: Feature Comparison Across Caching Systems (as of February 2026)

| Capability | Kuber | Redis 8 | Memcached | Hazelcast | Ignite |
|---|:---:|:---:|:---:|:---:|:---:|
| Single-JAR Deployment | ✓ | — | — | ✓ | — |
| Redis Protocol (RESP) | ✓ | ✓ | — | — | — |
| REST API (Native) | ✓ | — | — | ✓ | ✓ |
| Message Broker Access | ✓ | — | — | — | — |
| JSON Document Queries | ✓ | Module | — | — | SQL |
| Secondary Indexes | ✓ | Module | — | ✓ | ✓ |
| Multi-Backend Persistence | ✓ | RDB/AOF | — | Enterprise | Native |
| Web Admin Console | ✓ | — | — | Mgmt Center | — |
| Built-in RBAC | ✓ | ACL | — | Enterprise | — |
| Event Publishing | ✓ | Pub/Sub | — | Listeners | Events |
| Broker-Based Messaging | ✓ | — | — | — | — |
| Multi-Language Clients | ✓ | ✓ | ✓ | ✓ | ✓ |
| Replication | ✓ | ✓ | — | ✓ | ✓ |
| SSL/TLS | ✓ | ✓ | — | Enterprise | ✓ |
| Off-Heap Storage | ✓ | — | — | ✓ | ✓ |
| Bulk Import (Autoload) | ✓ | — | — | — | — |
| License (2025) | Open | AGPLv3/RSAL | BSD | Apache/Comm. | Apache |
| Min. Deploy Nodes | 1 | 1 (6 cluster) | 1 | 1 (3 cluster) | 1 (3 cluster) |
| External Dependencies | JVM only | None | libevent | JVM | JVM |

### 7.1 Key Differentiators

**Three-Protocol Convergence:** Kuber is unique among surveyed systems in providing Redis RESP, REST HTTP/JSON, and message broker access through a single deployment. This enables polyglot architectures where different services access the same cache through their preferred protocol.

**JSON-Native Operations:** While Redis requires the RedisJSON module (not included in the open-source base), and Memcached has no JSON support, Kuber provides JSON document storage, JSONPath queries, criteria search, and secondary indexes as built-in features.

**Integrated Administration:** Redis relies on external tools (RedisInsight, redis-cli). Memcached provides no web interface. Kuber includes a comprehensive web console with region management, entry browsing, query interface, broker management, and API key administration—all without additional software installation.

**Pluggable Persistence:** Five backend options allow matching storage characteristics to workload requirements without changing application code. No other surveyed system offers comparable backend flexibility in its open-source edition.

**Single-JAR Simplicity:** Kuber deploys as a single JAR file with no external dependencies beyond a JVM runtime. No Docker containers, no cluster configuration, no cloud provisioning required for immediate productivity.

---

## 8 Target Use Cases

Kuber is designed for workloads where operational simplicity outweighs raw throughput:

**Development and Testing:** Single JAR with integrated web console eliminates the need for Docker Compose configurations, cluster provisioning, or external administration tools during development.

**Small Teams and Departmental Applications:** Internal tools, dashboards, and data pipelines serving tens to hundreds of users. The integrated RBAC, API keys, and SSL/TLS provide production-grade security without enterprise licensing costs.

**Microservice Prototypes:** Multi-protocol flexibility enables architectural evolution—starting with REST API for simplicity, migrating to Redis protocol for performance, and adding broker-based access as the system matures.

**Message-Driven Architectures:** Organizations with existing Kafka, RabbitMQ, or ActiveMQ infrastructure can access the cache through message brokers without introducing additional network protocols or firewall rules.

**Compliance-Sensitive Environments:** Built-in RBAC, API key management, SSL/TLS, and audit logging (via event publishing) address compliance requirements that otherwise mandate enterprise-tier licensing from competing systems.

---

## 9 Discussion

### 9.1 Design Trade-offs

**Single-Node Focus vs. Horizontal Scale:** Kuber prioritizes single-node deployment simplicity over internet-scale horizontal scaling. Primary-secondary replication provides redundancy and read scaling, but Kuber does not implement distributed sharding or consensus protocols. This is a deliberate design choice for the target workload segment.

**JVM Dependency vs. Universality:** Requiring a JVM runtime excludes deployment scenarios where JVM overhead is unacceptable. However, JVM ubiquity in enterprise environments minimizes this limitation for Kuber's target audience.

**Feature Breadth vs. Complexity:** Supporting three access protocols, five persistence backends, and integrated administration increases codebase complexity. This trade-off is justified by the operational simplicity it provides to users—a single deployment replaces multiple specialized tools.

### 9.2 Limitations

1. **Throughput ceiling:** Kuber is not designed for workloads exceeding tens of thousands of operations per second. Internet-scale workloads should use Redis, Dragonfly, or similar purpose-built systems.
2. **No distributed sharding:** Dataset size is bounded by single-node capacity. Replication provides redundancy but not horizontal data partitioning.
3. **JVM memory overhead:** The JVM runtime introduces baseline memory consumption that may be significant for extremely resource-constrained environments.
4. **Redis protocol subset:** Not all Redis commands are implemented. Advanced features (Lua scripting, streams, cluster commands) are not supported.

### 9.3 Future Work

1. **Distributed Mode:** Multi-node deployment with automatic region partitioning and consensus-based coordination.
2. **WebAssembly Calculators:** Sandboxed server-side computation via WASM for user-defined transformations.
3. **GraphQL API:** Query interface for complex JSON document relationships.
4. **Cloud-Native Packaging:** Helm charts, Kubernetes operators, and managed service options.

---

## 10 Conclusion

We presented Kuber, a lightweight distributed cache addressing the underserved market segment of low-to-moderate volume caching workloads. Through a hybrid memory architecture combining in-memory keys with tiered value storage, multi-protocol access via Redis RESP, REST HTTP/JSON, and message broker channels, and an integrated web administration console with built-in security, Kuber provides capabilities previously requiring either enterprise licensing or complex multi-system deployments—all in a single-JAR package.

Kuber does not compete with internet-scale systems on throughput or horizontal scalability. Instead, it offers a practical, low-cost alternative for development teams, departmental applications, early-stage startups, and compliance-sensitive environments where operational simplicity and feature completeness outweigh raw performance. The open-source license, zero external dependencies beyond JVM, and integrated administration make Kuber immediately deployable without infrastructure investment.

Kuber is open-source and available at https://github.com/ajsinha/kuber.

---

## Acknowledgments

The author thanks the early adopters and contributors to the Kuber project for their valuable feedback, testing, and feature suggestions that have shaped the system's architecture and capabilities.

---

## References

[1] B. Fitzpatrick, "Distributed caching with Memcached," *Linux Journal*, vol. 2004, no. 124, 2004.

[2] J. L. Carlson, *Redis in Action*. Manning Publications, 2013.

[3] R. Nishtala et al., "Scaling Memcache at Facebook," in *Proc. USENIX NSDI*, 2013.

[4] M. Johns, *Getting Started with Hazelcast*, 2nd ed. Packt Publishing, 2015.

[5] Apache Software Foundation, "Apache Ignite Documentation," https://ignite.apache.org/docs/latest/, 2024.

[6] R. Peralta, "Redis Adopts Dual Source-Available Licensing," Redis Blog, https://redis.io/blog/redis-adopts-dual-source-available-licensing/, Mar. 2024.

[7] Redis Ltd., "Redis 8 Licensing Update: Adding AGPLv3," Redis Blog, https://redis.io/blog/agplv3/, May 2025.

[8] Linux Foundation, "Valkey: An Open Source Alternative to Redis," https://valkey.io/, 2024.

[9] R. Katz, "The Redis Licensing Saga and Its Forks," InfoQ, https://www.infoq.com/news/2024/04/redis-license-change/, 2024.

[10] Snap Inc., "KeyDB: A Multithreaded Fork of Redis," https://docs.keydb.dev/, 2024.

[11] B. Manes, "Caffeine: A High Performance Caching Library for Java," https://github.com/ben-manes/caffeine, 2024.

[12] Software AG, "Ehcache," https://www.ehcache.org/, 2024.

[13] DragonflyDB, "Dragonfly: A Modern Redis Replacement," https://www.dragonflydb.io/, 2024.

[14] Microsoft Research, "Garnet: A High-Performance Cache-Store," https://github.com/microsoft/garnet, 2024.

[15] Meta, "RocksDB: A Persistent Key-Value Store," https://rocksdb.org/, 2024.

[16] H. Chu, "LMDB: Lightning Memory-Mapped Database," in *LDAPCon*, 2011. https://www.symas.com/lmdb

[17] N. Agrawal and S. Mamajiwala, "Comparative Analysis of RocksDB, LMDB, and MongoDB for Embedded Storage," *SAE International Journal*, 2024.

[18] P. O'Neil et al., "The Log-Structured Merge-Tree (LSM-Tree)," *Acta Informatica*, vol. 33, no. 4, pp. 351–385, 1996.

[19] E. Brewer, "Towards Robust Distributed Systems," in *Proc. ACM PODC*, 2000.

[20] S. Gilbert and N. Lynch, "Brewer's Conjecture and the Feasibility of Consistent, Available, Partition-Tolerant Web Services," *ACM SIGACT News*, vol. 33, no. 2, pp. 51–59, 2002.

[21] G. DeCandia et al., "Dynamo: Amazon's Highly Available Key-Value Store," in *Proc. ACM SOSP*, 2007.

[22] F. Chang et al., "Bigtable: A Distributed Storage System for Structured Data," in *Proc. USENIX OSDI*, 2006.

[23] J. Dean and L. A. Barroso, "The Tail at Scale," *Communications of the ACM*, vol. 56, no. 2, pp. 74–80, 2013.

[24] Aerospike, "Hybrid Memory Architecture," https://aerospike.com/docs/architecture/hybrid-memory, 2024.

[25] Amazon Web Services, "Amazon ElastiCache," https://aws.amazon.com/elasticache/, 2024.

[26] DZone, "Apache Ignite vs Hazelcast: Feature Comparison," https://dzone.com/articles/apache-ignite-vs-hazelcast, 2024.

[27] Y. Zhang et al., "Consistency Models in Distributed Caching: A Survey," *Frontiers in Computing*, 2025.

---

## Disclaimer and Legal Notice

### DISCLAIMER OF WARRANTIES

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

### LIMITATION OF LIABILITY

TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, IN NO EVENT SHALL THE AUTHOR, CONTRIBUTORS, OR COPYRIGHT HOLDERS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

### USAGE RESPONSIBILITY

Users of Kuber are solely responsible for:

- Ensuring compliance with all applicable laws and regulations in their jurisdiction
- Validating the software's suitability for their specific use case
- Implementing appropriate security measures for production deployments
- Backing up data and maintaining disaster recovery procedures
- Testing thoroughly in non-production environments before deployment

### NO PROFESSIONAL ADVICE

This software and documentation do not constitute professional advice of any kind. Users should consult with qualified professionals for legal, financial, security, or other specialized guidance.

### PATENT NOTICE


### TRADEMARK NOTICE

Redis is a registered trademark of Redis Ltd. Apache Kafka, Apache Ignite, Apache ActiveMQ, and Apache Flink are trademarks of the Apache Software Foundation. RabbitMQ is a trademark of Broadcom Inc. IBM MQ is a trademark of International Business Machines Corporation. Memcached is a trademark of Danga Interactive. Hazelcast is a trademark of Hazelcast, Inc. Aerospike is a trademark of Aerospike, Inc. All other trademarks are the property of their respective owners.

### THIRD-PARTY COMPONENTS

Kuber incorporates third-party open-source components, each subject to their respective licenses. Users are responsible for compliance with all applicable third-party licenses.

### INDEMNIFICATION

By using this software, you agree to indemnify and hold harmless the author and contributors from any claims, damages, or expenses arising from your use of the software.

### ACCURACY DISCLAIMER

Feature comparisons in this paper are based on publicly available documentation as of February 2026. The author makes no warranties regarding the accuracy of third-party product descriptions. Readers should consult official documentation for definitive feature information. This paper is provided for informational purposes only.

---

© 2025–2030 Ashutosh Sinha. All rights reserved.
Contact: ajsinha@gmail.com
Repository: https://github.com/ajsinha/kuber
