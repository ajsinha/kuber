# Kuber Distributed Cache - Application Properties Reference

**Version 2.6.0**

This document provides a comprehensive reference for all configuration properties available in Kuber Distributed Cache.

---

## Table of Contents

- [Core Settings](#core-settings)
- [Network Configuration](#network-configuration)
- [Cache Configuration](#cache-configuration)
- [Persistence Configuration](#persistence-configuration)
- [Security Configuration](#security-configuration)
- [Autoload Configuration](#autoload-configuration)
- [Backup & Restore Configuration](#backup--restore-configuration)
- [Graceful Shutdown Configuration](#graceful-shutdown-configuration)
- [Memory Management Configuration](#memory-management-configuration)
- [Replication Configuration](#replication-configuration)
- [Event Publishing Configuration](#event-publishing-configuration)
- [Request/Response Messaging Configuration](#requestresponse-messaging-configuration)
- [Logging Configuration](#logging-configuration)

---

## Core Settings

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.version` | `2.6.0` | Current application version (read-only) |
| `kuber.base.datadir` | `./kuberdata` | Base directory for all data files. All other paths are relative to this. Override with `-Dkuber.base.datadir=/path` or `KUBER_BASE_DATADIR` env var |
| `kuber.secure.folder` | `config/secure` | Directory for sensitive configuration files (users.json, apikeys.json). Auto-created if missing |
| `server.app.name` | `Kuber` | Application display name shown in Web UI |
| `server.port` | `8080` | HTTP port for REST API and Web UI |
| `server.servlet.session.timeout` | `30m` | Web session timeout duration |
| `server.shutdown` | `graceful` | Shutdown mode. `graceful` waits for active requests to complete |
| `spring.lifecycle.timeout-per-shutdown-phase` | `30s` | Maximum time to wait during each shutdown phase |

---

## Network Configuration

Redis protocol network settings for client connections.

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.network.port` | `6380` | Redis protocol port for client connections |
| `kuber.network.bind-address` | `0.0.0.0` | IP address to bind to. `0.0.0.0` binds to all interfaces |
| `kuber.network.max-connections` | `10000` | Maximum concurrent client connections |
| `kuber.network.connection-timeout-ms` | `30000` | Connection establishment timeout in milliseconds |
| `kuber.network.read-timeout-ms` | `30000` | Socket read timeout in milliseconds |
| `kuber.network.decoder-max-line-length` | `1048576` | Maximum command line length (1MB default) |
| `kuber.network.read-buffer-size` | `2048` | Socket read buffer size in bytes |
| `kuber.network.write-buffer-size` | `2048` | Socket write buffer size in bytes |
| `kuber.network.io-processor-count` | `0` | I/O processor threads. `0` = auto-detect CPU cores |

---

## Cache Configuration

In-memory cache settings.

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.cache.max-memory-entries` | `100000` | Maximum entries to keep in memory per region |
| `kuber.cache.max-key-length-bytes` | `256` | Maximum allowed key length in bytes. Keys exceeding this limit are rejected with error and the key/value logged for debugging. Applies to all persistence stores. |
| `kuber.cache.persistent-mode` | `false` | `true` = sync writes to disk, `false` = async writes |
| `kuber.cache.persistence-batch-size` | `100` | Number of entries per async persistence batch |
| `kuber.cache.persistence-interval-ms` | `1000` | Interval between async persistence flushes in milliseconds |
| `kuber.cache.default-ttl-seconds` | `-1` | Default TTL for entries. `-1` = no expiration |
| `kuber.cache.eviction-policy` | `LRU` | Eviction policy: `LRU`, `LFU`, or `FIFO` |
| `kuber.cache.ttl-cleanup-interval-seconds` | `60` | How often to scan for expired entries |
| `kuber.cache.enable-statistics` | `true` | Enable cache hit/miss statistics |

### Off-Heap Key Index (v1.2.2+)

For large datasets with millions of keys.

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.cache.off-heap-key-index` | `false` | Store key index in off-heap memory (reduces GC pressure) |
| `kuber.cache.off-heap-key-index-initial-size-mb` | `16` | Initial off-heap buffer size per region in MB |
| `kuber.cache.off-heap-key-index-max-size-mb` | `8192` | Maximum off-heap buffer size per region in MB |

---

## Persistence Configuration

Data storage backend settings.

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.persistence.type` | `rocksdb` | Backend type: `rocksdb` (default v1.9.0+), `lmdb`, `mongodb`, `sqlite`, `postgresql`, `memory` |
| `kuber.persistence.sync-individual-writes` | `false` | `true` = wait for disk sync on each write (slower but durable) |

### RocksDB Settings (Default)

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.persistence.rocksdb.path` | `${kuber.base.datadir}/data/rocksdb` | RocksDB data directory |
| `kuber.persistence.rocksdb.compaction-enabled` | `true` | Enable scheduled compaction to reclaim disk space |
| `kuber.persistence.rocksdb.compaction-cron` | `0 0 2 * * ?` | Compaction cron schedule (default: 2 AM daily) |

### LMDB Settings

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.persistence.lmdb.path` | `${kuber.base.datadir}/data/lmdb` | LMDB data directory |
| `kuber.persistence.lmdb.map-size` | `1073741824` | Maximum database size in bytes (1GB default) |

### MongoDB Settings

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.mongo.uri` | `mongodb://localhost:27017` | MongoDB connection URI |
| `kuber.mongo.database` | `kuber` | MongoDB database name |
| `kuber.mongo.connection-pool-size` | `50` | Connection pool size |
| `kuber.mongo.connect-timeout-ms` | `10000` | Connection timeout |
| `kuber.mongo.socket-timeout-ms` | `30000` | Socket timeout |
| `kuber.mongo.server-selection-timeout-ms` | `30000` | Server selection timeout |
| `kuber.mongo.write-concern` | `ACKNOWLEDGED` | Write concern level |

### SQLite Settings

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.persistence.sqlite.path` | `${kuber.base.datadir}/data/kuber.db` | SQLite database file path |

### PostgreSQL Settings

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.persistence.postgresql.url` | `jdbc:postgresql://localhost:5432/kuber` | JDBC connection URL |
| `kuber.persistence.postgresql.username` | `kuber` | Database username |
| `kuber.persistence.postgresql.password` | `kuber` | Database password |
| `kuber.persistence.postgresql.pool-size` | `10` | Connection pool size |
| `kuber.persistence.postgresql.min-idle` | `2` | Minimum idle connections |
| `kuber.persistence.postgresql.connection-timeout-ms` | `30000` | Connection timeout |
| `kuber.persistence.postgresql.idle-timeout-ms` | `600000` | Idle connection timeout |
| `kuber.persistence.postgresql.max-lifetime-ms` | `1800000` | Maximum connection lifetime |

---

## Security Configuration

> ⚠️ **v2.2.0+**: API Keys required for all programmatic access. Username/password only for Web UI.

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.secure.folder` | `config/secure` | Directory for security files |
| `kuber.security.users-file` | `${kuber.secure.folder}/users.json` | User credentials file. **REQUIRED** - app fails without it |
| `kuber.security.api-keys-file` | `${kuber.secure.folder}/apikeys.json` | API keys file (created automatically if missing) |
| `kuber.security.session-timeout-minutes` | `30` | Web UI session timeout |
| `kuber.security.redis-password` | (empty) | *Deprecated* - Use API keys instead |

### Secure Folder Contents

The config/secure folder contains sensitive configuration files:

| File | Required | Description |
|------|----------|-------------|
| `users.json` | **Yes** | Web UI user credentials |
| `apikeys.json` | No | API keys (auto-created) |

### users.json Format

```json
[
  {
    "userId": "admin",
    "password": "admin123",
    "roles": ["ADMIN", "USER"],
    "enabled": true
  }
]
```

### apikeys.json Format

```json
[
  {
    "keyId": "kub_abc123...",
    "name": "Production App",
    "userId": "admin",
    "roles": ["ADMIN"],
    "enabled": true,
    "createdAt": "2025-01-01T00:00:00Z"
  }
]
```

---

## Autoload Configuration

Automatic data loading from CSV/JSON files.

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.autoload.enabled` | `true` | Enable automatic loading from inbox folder |
| `kuber.autoload.directory` | `${kuber.base.datadir}/autoload` | Directory to watch for files |
| `kuber.autoload.scan-interval-seconds` | `60` | How often to scan for new files |
| `kuber.autoload.max-records-per-file` | `0` | Maximum records per file. `0` = unlimited |
| `kuber.autoload.batch-size` | `32768` | Records per persistence batch |
| `kuber.autoload.warm-percentage` | `10` | % of data to pre-warm into memory after load |
| `kuber.autoload.create-directories` | `true` | Create inbox/processed/error directories |
| `kuber.autoload.file-encoding` | `UTF-8` | File character encoding |

---

## Backup & Restore Configuration

Scheduled backup and automatic restore.

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.backup.enabled` | `true` | Enable scheduled backups |
| `kuber.backup.backup-directory` | `${kuber.base.datadir}/backup` | Directory for backup files |
| `kuber.backup.restore-directory` | `${kuber.base.datadir}/restore` | Directory to monitor for restore files |
| `kuber.backup.cron` | `0 0 23 * * *` | Backup cron schedule (default: 11 PM daily) |
| `kuber.backup.max-backups-per-region` | `10` | Backups to retain per region. `0` = unlimited |
| `kuber.backup.batch-size` | `10000` | Entries per batch during backup/restore |
| `kuber.backup.compress` | `true` | Compress backup files (gzip) |
| `kuber.backup.create-directories` | `true` | Create directories if missing |
| `kuber.backup.file-encoding` | `UTF-8` | File encoding |

---

## Graceful Shutdown Configuration

Multiple shutdown methods supported.

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.shutdown.file-enabled` | `true` | Enable file-based shutdown (`touch kuber.shutdown`) |
| `kuber.shutdown.file-path` | `${kuber.base.datadir}/kuber.shutdown` | Shutdown signal file path |
| `kuber.shutdown.check-interval-ms` | `5000` | How often to check for shutdown file |
| `kuber.shutdown.api-enabled` | `true` | Enable REST API shutdown endpoint |
| `kuber.shutdown.phase-delay-seconds` | `5` | Delay between shutdown phases |

### Shutdown Methods

1. **File**: `touch ./kuberdata/kuber.shutdown`
2. **REST API**: `POST /api/admin/shutdown` with API key
3. **SIGTERM**: Standard Unix signal

---

## Memory Management Configuration

Automatic memory pressure handling.

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.cache.memory-watcher-enabled` | `true` | Enable automatic memory pressure handling |
| `kuber.cache.memory-high-watermark-percent` | `85` | Start evicting when heap exceeds this % |
| `kuber.cache.memory-low-watermark-percent` | `50` | Stop evicting when heap drops below this % |
| `kuber.cache.memory-eviction-batch-size` | `1000` | Entries to evict per batch |
| `kuber.cache.memory-watcher-interval-ms` | `5000` | Memory check interval in milliseconds |

### Count-Based Value Cache Limiting (v1.7.4)

Proactive count-based eviction to limit values in memory per region. The effective limit is the **LOWER** of percentage-based or absolute limit.

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.cache.value-cache-limit-enabled` | `true` | Enable count-based value cache limiting |
| `kuber.cache.value-cache-max-percent` | `20` | Max % of keys to keep values in memory |
| `kuber.cache.value-cache-max-entries` | `10000` | Max values per region in memory |
| `kuber.cache.value-cache-limit-check-interval-ms` | `30000` | Interval between limit checks |

**Example:** With 100,000 keys, 20% limit = 20,000 max, but if max-entries = 10,000, effective limit is **10,000** (lower wins).

---

## Replication Configuration

ZooKeeper-based cluster replication.

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.zookeeper.enabled` | `false` | Enable ZooKeeper-based replication |
| `kuber.zookeeper.connect-string` | `localhost:2181` | ZooKeeper connection string |
| `kuber.zookeeper.session-timeout-ms` | `30000` | ZK session timeout |
| `kuber.zookeeper.connection-timeout-ms` | `10000` | ZK connection timeout |
| `kuber.zookeeper.base-path` | `/kuber` | ZK base path for Kuber nodes |
| `kuber.zookeeper.retry-base-sleep-ms` | `1000` | Retry base sleep time |
| `kuber.zookeeper.retry-max-attempts` | `3` | Maximum retry attempts |
| `kuber.replication.sync-batch-size` | `1000` | Entries per sync batch |
| `kuber.replication.sync-timeout-ms` | `30000` | Sync operation timeout |
| `kuber.replication.heartbeat-interval-ms` | `5000` | Heartbeat interval between nodes |
| `kuber.replication.primary-check-interval-ms` | `10000` | Primary node check interval |

---

## Event Publishing Configuration

Publish cache events to message brokers.

### Global Settings

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.publishing.thread-pool-size` | `4` | Publisher thread pool size |
| `kuber.publishing.queue-capacity` | `10000` | Event queue capacity |
| `kuber.publishing.broker-config-file` | `config/message_brokers.json` | External JSON file for broker definitions (v2.2.0) |
| `kuber.publishing.region-config-file` | `config/event_publishing.json` | External JSON file for region publishing config |

### Message Broker Definitions (External JSON Config)

Broker definitions are managed in `config/message_brokers.json`. Each broker specifies type, connection details, and optional SSL/TLS configuration.

**Admin UI:** `/admin/brokers` — view brokers and edit JSON inline.

```json
{
  "brokers": {
    "kafka-prod": {
      "enabled": true,
      "type": "kafka",
      "bootstrap-servers": "kafka1:9093,kafka2:9093",
      "partitions": 6,
      "ssl": {
        "enabled": true,
        "mode": "jks",
        "trust-store-path": "/certs/truststore.jks",
        "trust-store-password": "changeit"
      }
    }
  }
}
```

### SSL/TLS Modes

| Mode | Description | Required Fields |
|------|-------------|-----------------|
| `jks` | Server-side SSL with JKS/PKCS12 trust store | `trust-store-path`, `trust-store-password` |
| `pem` | Server-side SSL with PEM CA certificate | `trust-cert-path` |
| `sasl_ssl` | SASL authentication over SSL (Kafka) | `sasl-mechanism`, `sasl-jaas-config`, trust store or PEM |
| `mtls_jks` | Mutual TLS with JKS/PKCS12 key store | `key-store-path`, `key-store-password`, trust store |
| `mtls_pem` | Mutual TLS with PEM client cert + key | `trust-cert-path`, `key-cert-path`, `key-path` |

### Region Event Publishing (External JSON Config)

Region-to-broker mappings are managed in `config/event_publishing.json`.

**Admin UI:** `/admin/event-publishing` — view regions and edit JSON inline.

```json
{
  "regions": {
    "trades": {
      "enabled": true,
      "destinations": [
        { "broker": "kafka-prod", "topic": "kuber.trades.events" },
        { "broker": "activemq-legacy", "topic": "TRADES.EVENTS.QUEUE" },
        { "broker": "audit-files", "topic": "trades" }
      ]
    }
  }
}
```

Regions from the JSON file override same-named regions in `application.properties`.
Legacy per-region config in `application.properties` is still supported for backward compatibility.

### Supported Broker Types

| Type | Description |
|------|-------------|
| `kafka` | Apache Kafka |
| `rabbitmq` | RabbitMQ |
| `activemq` | Apache ActiveMQ |
| `ibmmq` | IBM MQ |
| `file` | File-based (for audit logs) |

---

## Request/Response Messaging Configuration

**New in v1.7.1, updated in v2.2.0** - Process cache operations via message brokers (Kafka, ActiveMQ, RabbitMQ, IBM MQ).

Configuration is stored in an external JSON file. The configuration is **hot-reloadable** — changes are detected automatically without server restart.

### Configuration File Location

| Property | Default | Description |
|----------|---------|-------------|
| `kuber.messaging.request-response-config-file` | `config/request_response.json` | Path to request/response messaging JSON config (relative to working directory or absolute) |

A default config file is auto-created on first start if not found.

### Global Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable/disable request/response messaging globally |
| `max_queue_depth` | integer | `100` | Maximum internal work queue size. When reached, message consumption pauses (backpressure) |
| `thread_pool_size` | integer | `10` | Number of threads for processing requests |
| `max_batch_get_size` | integer | `128` | Maximum keys allowed in MGET operations. Requests exceeding this are rejected |
| `max_batch_set_size` | integer | `128` | Maximum entries allowed in MSET operations. Requests exceeding this are rejected |
| `max_search_results` | integer | `10000` | Maximum results for KEYS/JSEARCH operations. Excess results are truncated with a warning in `server_message` |

### Broker Configuration

Brokers are defined in the `brokers` map, keyed by a unique broker name.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `enabled` | boolean | `true` | Enable/disable this specific broker |
| `type` | string | (required) | Broker type: `kafka`, `activemq`, `rabbitmq`, `ibmmq` |
| `display_name` | string | (optional) | Human-readable name for admin UI |
| `connection` | object | `{}` | Connection properties (varies by broker type) |
| `request_topics` | array | `[]` | List of request topics/queues to subscribe to |

### Response Topic Convention

Response topics are automatically inferred from request topics:
- `ccs_request` → `ccs_response`
- `orders_request` → `orders_response`
- `my_topic` → `my_topic_response` (if no `_request` suffix)

---

## Request/Response JSON Configuration Examples

### Apache Kafka

```json
{
  "enabled": true,
  "max_queue_depth": 100,
  "thread_pool_size": 10,
  "max_batch_get_size": 128,
  "max_batch_set_size": 128,
  "max_search_results": 10000,
  "brokers": {
    "kafka_primary": {
      "enabled": true,
      "type": "kafka",
      "display_name": "Production Kafka Cluster",
      "connection": {
        "bootstrap_servers": "kafka1.prod.local:9092,kafka2.prod.local:9092,kafka3.prod.local:9092",
        "group_id": "kuber-cache-processor",
        "auto_offset_reset": "earliest"
      },
      "request_topics": [
        "ccs_request",
        "agg_request",
        "calc_request"
      ]
    }
  }
}
```

**Kafka Connection Properties:**

| Property | Default | Description |
|----------|---------|-------------|
| `bootstrap_servers` | (required) | Comma-separated list of Kafka broker addresses |
| `group_id` | `kuber-request-processor` | Consumer group ID for coordinated consumption |
| `auto_offset_reset` | `earliest` | Where to start reading: `earliest`, `latest` |

---

### Apache ActiveMQ

```json
{
  "enabled": true,
  "max_queue_depth": 100,
  "thread_pool_size": 10,
  "max_batch_get_size": 128,
  "max_batch_set_size": 128,
  "max_search_results": 10000,
  "brokers": {
    "activemq_primary": {
      "enabled": true,
      "type": "activemq",
      "display_name": "Production ActiveMQ",
      "connection": {
        "broker_url": "tcp://activemq.prod.local:61616",
        "username": "kuber_service",
        "password": "secure_password_here"
      },
      "request_topics": [
        "KUBER.REQUEST.ORDERS",
        "KUBER.REQUEST.INVENTORY",
        "KUBER.REQUEST.CUSTOMERS"
      ]
    }
  }
}
```

**ActiveMQ Connection Properties:**

| Property | Default | Description |
|----------|---------|-------------|
| `broker_url` | (required) | ActiveMQ broker URL (e.g., `tcp://host:61616`, `ssl://host:61617`) |
| `username` | (optional) | Authentication username |
| `password` | (optional) | Authentication password |

**ActiveMQ Failover Configuration:**

```json
{
  "connection": {
    "broker_url": "failover:(tcp://amq1.prod:61616,tcp://amq2.prod:61616)?randomize=false&maxReconnectAttempts=10",
    "username": "kuber",
    "password": "secret"
  }
}
```

---

### RabbitMQ

```json
{
  "enabled": true,
  "max_queue_depth": 100,
  "thread_pool_size": 10,
  "max_batch_get_size": 128,
  "max_batch_set_size": 128,
  "max_search_results": 10000,
  "brokers": {
    "rabbitmq_primary": {
      "enabled": true,
      "type": "rabbitmq",
      "display_name": "Production RabbitMQ Cluster",
      "connection": {
        "host": "rabbitmq.prod.local",
        "port": "5672",
        "virtual_host": "/kuber",
        "username": "kuber_service",
        "password": "secure_password_here"
      },
      "request_topics": [
        "kuber.request.trading",
        "kuber.request.positions",
        "kuber.request.risk"
      ]
    }
  }
}
```

**RabbitMQ Connection Properties:**

| Property | Default | Description |
|----------|---------|-------------|
| `host` | `localhost` | RabbitMQ server hostname |
| `port` | `5672` | RabbitMQ AMQP port (use `5671` for TLS) |
| `virtual_host` | `/` | RabbitMQ virtual host |
| `username` | (optional) | Authentication username |
| `password` | (optional) | Authentication password |

**RabbitMQ with TLS:**

```json
{
  "connection": {
    "host": "rabbitmq.prod.local",
    "port": "5671",
    "virtual_host": "/kuber",
    "username": "kuber",
    "password": "secret",
    "ssl_enabled": "true"
  }
}
```

---

### IBM MQ

```json
{
  "enabled": true,
  "max_queue_depth": 100,
  "thread_pool_size": 10,
  "max_batch_get_size": 128,
  "max_batch_set_size": 128,
  "max_search_results": 10000,
  "brokers": {
    "ibmmq_primary": {
      "enabled": true,
      "type": "ibmmq",
      "display_name": "Production IBM MQ",
      "connection": {
        "queue_manager": "QM_PROD",
        "channel": "KUBER.SVRCONN",
        "conn_name": "mq.prod.local(1414)",
        "username": "kuber_svc",
        "password": "secure_password_here"
      },
      "request_topics": [
        "KUBER.REQ.CACHE",
        "KUBER.REQ.SESSION",
        "KUBER.REQ.CONFIG"
      ]
    }
  }
}
```

**IBM MQ Connection Properties:**

| Property | Default | Description |
|----------|---------|-------------|
| `queue_manager` | (required) | IBM MQ Queue Manager name |
| `channel` | (required) | Server connection channel name |
| `conn_name` | (required) | Connection name in format `hostname(port)` |
| `username` | (optional) | Authentication username |
| `password` | (optional) | Authentication password |

**IBM MQ with Multiple Hosts (HA):**

```json
{
  "connection": {
    "queue_manager": "QM_PROD",
    "channel": "KUBER.SVRCONN",
    "conn_name": "mq1.prod.local(1414),mq2.prod.local(1414)",
    "username": "kuber",
    "password": "secret"
  }
}
```

---

### Multi-Broker Configuration

You can configure multiple brokers of different types simultaneously:

```json
{
  "enabled": true,
  "max_queue_depth": 200,
  "thread_pool_size": 20,
  "max_batch_get_size": 256,
  "max_batch_set_size": 256,
  "max_search_results": 50000,
  "brokers": {
    "kafka_realtime": {
      "enabled": true,
      "type": "kafka",
      "display_name": "Real-time Kafka",
      "connection": {
        "bootstrap_servers": "kafka1:9092,kafka2:9092",
        "group_id": "kuber-realtime"
      },
      "request_topics": ["rt_cache_request", "rt_session_request"]
    },
    "activemq_batch": {
      "enabled": true,
      "type": "activemq",
      "display_name": "Batch Processing ActiveMQ",
      "connection": {
        "broker_url": "tcp://activemq:61616",
        "username": "kuber",
        "password": "secret"
      },
      "request_topics": ["BATCH.CACHE.REQUEST", "BATCH.BULK.REQUEST"]
    },
    "ibmmq_legacy": {
      "enabled": true,
      "type": "ibmmq",
      "display_name": "Legacy IBM MQ",
      "connection": {
        "queue_manager": "QM_LEGACY",
        "channel": "KUBER.CHANNEL",
        "conn_name": "legacymq:1414"
      },
      "request_topics": ["LEGACY.CACHE.REQ"]
    },
    "rabbitmq_events": {
      "enabled": false,
      "type": "rabbitmq",
      "display_name": "Event RabbitMQ (disabled)",
      "connection": {
        "host": "rabbitmq",
        "port": "5672"
      },
      "request_topics": ["events.cache.request"]
    }
  }
}
```

---

### Sample Configuration File

A sample configuration file is provided at:

```
kuber-server/src/main/resources/secure-sample/request_response.json.sample
```

Copy this to your config folder and customize:

```bash
mkdir -p config
cp secure-sample/request_response.json.sample ./config/request_response.json
```

---

## Logging Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `logging.level.root` | `INFO` | Root logging level |
| `logging.level.com.kuber` | `DEBUG` | Kuber application logging level |
| `logging.level.org.apache.mina` | `WARN` | MINA networking logging level |
| `logging.level.org.springframework.security` | `WARN` | Spring Security logging level |
| `logging.pattern.console` | `%d{yyyy-MM-dd HH:mm:ss} [%thread] %-5level %logger{36} - %msg%n` | Console log pattern |

---

## Environment Variable Override

All properties can be overridden via environment variables:

1. Replace `.` with `_`
2. Convert to uppercase
3. Replace `-` with `_`

**Examples:**

| Property | Environment Variable |
|----------|---------------------|
| `kuber.cache.max-memory-entries` | `KUBER_CACHE_MAX_MEMORY_ENTRIES` |
| `kuber.network.port` | `KUBER_NETWORK_PORT` |
| `kuber.persistence.type` | `KUBER_PERSISTENCE_TYPE` |

---

## Quick Start Configuration

Minimal production configuration:

```properties
# Base directory
kuber.base.datadir=/var/kuber/data

# Persistence (rocksdb is default as of v1.9.0)
kuber.persistence.type=rocksdb

# Security (create users.json in secure folder)
kuber.secure.folder=/var/kuber/config/secure

# Cache tuning
kuber.cache.max-memory-entries=1000000

# Backup
kuber.backup.enabled=true
kuber.backup.cron=0 0 2 * * *
```

### Enabling Request/Response Messaging

1. Create `request_response.json` in your config folder (or customize path via `kuber.messaging.request-response-config-file`):

```json
{
  "enabled": true,
  "max_queue_depth": 100,
  "thread_pool_size": 10,
  "max_batch_get_size": 128,
  "max_batch_set_size": 128,
  "max_search_results": 10000,
  "brokers": {
    "my_kafka": {
      "enabled": true,
      "type": "kafka",
      "display_name": "My Kafka",
      "connection": {
        "bootstrap_servers": "localhost:9092",
        "group_id": "kuber-processor"
      },
      "request_topics": ["cache_request"]
    }
  }
}
```

2. Generate an API key via the Web UI (Admin → API Keys)

3. Send requests to your configured topics with the API key

---

*Copyright © 2025-2030, All Rights Reserved - Ashutosh Sinha*
