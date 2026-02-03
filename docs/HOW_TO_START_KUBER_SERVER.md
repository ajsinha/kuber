# How to Start Kuber Server

**Version 1.9.0**

Copyright © 2025-2030, All Rights Reserved  
Ashutosh Sinha | Email: ajsinha@gmail.com

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Building the Project](#2-building-the-project)
3. [Running the Server](#3-running-the-server)
4. [Configuration Methods](#4-configuration-methods)
5. [Command Line Property Overrides](#5-command-line-property-overrides)
6. [Environment Variables](#6-environment-variables)
7. [Request/Response Messaging Setup](#7-requestresponse-messaging-setup)
8. [Common Configuration Examples](#8-common-configuration-examples)
9. [Verifying the Server](#9-verifying-the-server)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Prerequisites

### Required Software

| Software | Minimum Version | Recommended | Purpose |
|----------|-----------------|-------------|---------|
| **Java JDK** | 17 | 21 | Runtime environment |
| **Maven** | 3.8 | 3.9+ | Build tool |
| **Git** | 2.x | Latest | Source control (optional) |

### Verify Installation

```bash
# Check Java version
java -version
# Expected: openjdk version "17.x.x" or higher

# Check Maven version
mvn -version
# Expected: Apache Maven 3.8.x or higher

# Check JAVA_HOME is set
echo $JAVA_HOME
# Should point to your JDK installation
```

### System Requirements

| Resource | Minimum | Recommended | Notes |
|----------|---------|-------------|-------|
| **RAM** | 512 MB | 2-4 GB | Depends on cache size |
| **Disk** | 1 GB | 10+ GB | For persistence data |
| **CPU** | 1 core | 4+ cores | For concurrent operations |

### JVM Requirements (Java 9+)

When running on Java 9 or later, the following JVM option is **required** for LMDB persistence support:

```bash
--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

This allows the LMDB Java library to access internal ByteBuffer fields for direct memory operations. The startup scripts (`kuber-start.sh`, `kuber-start.bat`) include this option automatically.

### Optional Components

| Component | Purpose | When Needed |
|-----------|---------|-------------|
| ZooKeeper 3.8+ | Primary/Secondary replication | High availability setup |
| Apache Kafka | Event publishing & messaging | Streaming integrations |
| RabbitMQ | Event publishing & messaging | AMQP messaging |
| MongoDB | Persistence backend | Document-oriented storage |
| PostgreSQL | Persistence backend | Relational storage |

---

## 2. Building the Project

### Clone and Build

```bash
# Clone the repository (if using Git)
git clone <repository-url> kuber
cd kuber

# Build all modules
mvn clean install -DskipTests

# Build with tests
mvn clean install
```

### Build Output

After successful build, the server JAR is located at:
```
kuber-server/target/kuber-server-1.9.0.jar
```

### Build Specific Module

```bash
# Build only the server module
mvn clean install -pl kuber-server -am -DskipTests

# Build Java client
mvn clean install -pl kuber-client-java -am -DskipTests
```

### Build C# Client

```bash
cd kuber-client-csharp
dotnet build
```

---

## 3. Running the Server

### Method 1: Using the JAR File (Recommended for Production)

```bash
# Basic start (includes required JVM options)
java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -jar kuber-server/target/kuber-server-1.9.0.jar

# With specific memory settings
java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -Xms512m -Xmx2g \
     -jar kuber-server/target/kuber-server-1.9.0.jar

# With garbage collector tuning (for large heaps)
java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -Xms2g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 \
     -jar kuber-server/target/kuber-server-1.9.0.jar
```

### Method 2: Using Maven (Recommended for Development)

```bash
# From the project root directory
cd kuber-server

# Basic run with required JVM options
mvn spring-boot:run -Dspring-boot.run.jvmArguments="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

# With specific profile
mvn spring-boot:run -Dspring-boot.run.profiles=dev \
    -Dspring-boot.run.jvmArguments="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

# With additional JVM arguments
mvn spring-boot:run -Dspring-boot.run.jvmArguments="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Xms512m -Xmx2g"
```

### Method 3: Using the Startup Scripts

```bash
# Unix/Linux/Mac
chmod +x scripts/kuber-start.sh
./scripts/kuber-start.sh

# Windows
scripts\kuber-start.bat
```

### Method 4: Running from IDE

**IntelliJ IDEA:**
1. Import as Maven project
2. Navigate to `kuber-server/src/main/java/com/kuber/server/KuberApplication.java`
3. Right-click → Run 'KuberApplication'

**Eclipse:**
1. Import as Maven project
2. Right-click on `KuberApplication.java` → Run As → Java Application

**VS Code:**
1. Install Java Extension Pack
2. Open the project folder
3. Navigate to `KuberApplication.java`
4. Click "Run" above the main method

---

## 4. Configuration Methods

Kuber uses Spring Boot's configuration system. Configuration can be provided in multiple ways (in order of precedence, highest first):

### 4.1 Configuration Precedence (Highest to Lowest)

1. Command line arguments (`--property=value`)
2. Java system properties (`-Dproperty=value`)
3. Environment variables (`PROPERTY_NAME=value`)
4. `application.yml` in current directory
5. `application.yml` in classpath (inside JAR)
6. Default values in code

### 4.2 Configuration File Locations

Kuber looks for `application.yml` in these locations:

```
1. ./config/application.yml          (current directory /config)
2. ./application.yml                  (current directory)
3. classpath:/config/application.yml  (inside JAR /config)
4. classpath:/application.yml         (inside JAR root)
```

### 4.3 Creating External Configuration File

Create `application.yml` in the same directory as the JAR:

```yaml
kuber:
  network:
    port: 6380
    bind-address: 0.0.0.0
  
  cache:
    max-memory-entries: 100000
    cache-implementation: CAFFEINE
    collections-implementation: DEFAULT
  
  persistence:
    type: rocksdb
    rocksdb:
      path: ./data/rocksdb
  
  secure:
    folder: ./secure

spring:
  security:
    user:
      name: admin
      password: your-secure-password
```

### 4.4 Secure Configuration Files

Place security-sensitive files in the `secure` folder:

```
./secure/
├── users.json              # User accounts with role assignments
├── roles.json              # RBAC role definitions (v1.7.3)
├── apikeys.json            # API keys
└── request_response.json   # Messaging configuration
```

**Sample users.json:**

```json
{
  "users": [
    {
      "userId": "admin",
      "password": "admin123",
      "fullName": "System Administrator",
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

**Sample roles.json:**

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
      "region": "default",
      "permissions": ["READ"]
    },
    {
      "name": "default_full",
      "region": "default",
      "permissions": ["READ", "WRITE", "DELETE"]
    }
  ]
}
```

**RBAC Permission Types:**

| Permission | Operations | Description |
|------------|------------|-------------|
| `READ` | GET, EXISTS, KEYS, SCAN, SEARCH | View and search entries |
| `WRITE` | SET, SETEX, INCR, APPEND, HSET | Create and update entries |
| `DELETE` | DEL, HDEL, JDEL, FLUSHDB | Remove entries |
| `ADMIN` | Region create/delete, user/role mgmt | Full system control |

**Role Naming Convention:**

| Pattern | Example | Description |
|---------|---------|-------------|
| `admin` | `admin` | Reserved system admin role |
| `{region}_readonly` | `orders_readonly` | Read-only access to region |
| `{region}_readwrite` | `orders_readwrite` | Read and write access |
| `{region}_full` | `orders_full` | Full access (read, write, delete) |

**Hot Reload:**

Security files are automatically reloaded without server restart:
- Changes detected every 30 seconds
- When `users.json` OR `roles.json` changes, BOTH are reloaded
- Use Admin UI "Reload" button for immediate effect

**Default File Creation:**

If files are missing at startup:
- `users.json` missing → Created with `admin` user (password: `admin123`)
- `roles.json` missing → Created with `admin` role

> ⚠️ **Important**: Change default admin password immediately!

**Sample Files:**

Copy from `secure-sample/` for comprehensive examples:
```bash
cp secure-sample/users.json.sample secure/users.json
cp secure-sample/roles.json.sample secure/roles.json
# Edit with your configuration
```

---

## 5. Command Line Property Overrides

### 5.1 Using Double-Dash Syntax (Spring Boot Standard)

```bash
java -jar kuber-server.jar \
  --kuber.network.port=6380 \
  --kuber.network.bind-address=0.0.0.0 \
  --kuber.cache.max-memory-entries=500000 \
  --kuber.persistence.type=lmdb \
  --kuber.persistence.rocksdb.path=/data/kuber/rocksdb \
  --spring.security.user.name=admin \
  --spring.security.user.password=secret123
```

### 5.2 Common Property Overrides

| Property | Default | Description |
|----------|---------|-------------|
| `--server.port=8080` | 8080 | HTTP/REST API port |
| `--kuber.network.port=6380` | 6380 | Redis protocol port |
| `--kuber.network.bind-address=0.0.0.0` | 0.0.0.0 | Network interface |
| `--kuber.cache.max-memory-entries=100000` | 100000 | Max entries per region |
| `--kuber.cache.global-max-memory-entries=500000` | 0 | Global max (0=unlimited) |
| `--kuber.cache.memory-watcher-enabled=true` | true | Enable heap-based eviction |
| `--kuber.cache.memory-high-watermark-percent=85` | 85 | Start evicting when heap exceeds % |
| `--kuber.cache.value-cache-limit-enabled=true` | true | Enable count-based limiting (v1.7.4) |
| `--kuber.cache.value-cache-max-percent=20` | 20 | Max % of keys to cache values |
| `--kuber.cache.value-cache-max-entries=10000` | 10000 | Max values per region |
| `--kuber.persistence.type=lmdb` | lmdb | Persistence backend |
| `--kuber.persistence.rocksdb.path=./data` | ./data/rocksdb | Data directory |
| `--kuber.secure.folder=./secure` | ./secure | Secure config folder |

### 5.3 Persistence Configuration

```bash
# Use RocksDB (default, recommended)
java -jar kuber-server.jar \
  --kuber.persistence.type=lmdb \
  --kuber.persistence.rocksdb.path=/var/kuber/data

# Use LMDB
java -jar kuber-server.jar \
  --kuber.persistence.type=lmdb \
  --kuber.persistence.lmdb.path=/var/kuber/lmdb

# Use MongoDB
java -jar kuber-server.jar \
  --kuber.persistence.type=mongodb \
  --kuber.mongo.uri=mongodb://localhost:27017 \
  --kuber.mongo.database=kuber

# Use PostgreSQL
java -jar kuber-server.jar \
  --kuber.persistence.type=postgresql \
  --spring.datasource.url=jdbc:postgresql://localhost:5432/kuber

# Use SQLite
java -jar kuber-server.jar \
  --kuber.persistence.type=sqlite \
  --kuber.persistence.sqlite.path=/var/kuber/kuber.db

# In-memory only (no persistence)
java -jar kuber-server.jar \
  --kuber.persistence.type=memory
```

---

## 6. Environment Variables

Spring Boot automatically maps environment variables to properties:

### 6.1 Mapping Rules

1. Replace dots (`.`) with underscores (`_`)
2. Remove dashes (`-`)
3. Convert to uppercase

### 6.2 Examples

| Property | Environment Variable |
|----------|---------------------|
| `kuber.network.port` | `KUBER_NETWORK_PORT` |
| `kuber.cache.max-memory-entries` | `KUBER_CACHE_MAXMEMORYENTRIES` |
| `kuber.persistence.type` | `KUBER_PERSISTENCE_TYPE` |
| `spring.security.user.password` | `SPRING_SECURITY_USER_PASSWORD` |

### 6.3 Docker Environment Variables

```bash
docker run -d \
  -e KUBER_NETWORK_PORT=6380 \
  -e KUBER_PERSISTENCE_TYPE=rocksdb \
  -e SPRING_SECURITY_USER_PASSWORD=secret \
  -p 8080:8080 \
  -p 6380:6380 \
  kuber-server:1.9.0
```

---

## 7. Request/Response Messaging Setup

### 7.1 Create Configuration File

Create `secure/request_response.json`:

```json
{
  "enabled": true,
  "max_queue_depth": 100,
  "thread_pool_size": 10,
  "brokers": {
    "kafka_primary": {
      "enabled": true,
      "type": "kafka",
      "display_name": "Primary Kafka",
      "connection": {
        "bootstrap_servers": "localhost:9092",
        "group_id": "kuber-request-processor"
      },
      "request_topics": ["ccs_cache_request"]
    }
  }
}
```

### 7.2 Supported Broker Types

| Type | Configuration |
|------|--------------|
| `kafka` | `bootstrap_servers`, `group_id` |
| `activemq` | `broker_url`, `username`, `password` |
| `rabbitmq` | `host`, `port`, `virtual_host`, `username`, `password` |
| `ibmmq` | `queue_manager`, `channel`, `conn_name`, `username`, `password` |

### 7.3 ActiveMQ Example

```json
{
  "brokers": {
    "activemq_main": {
      "enabled": true,
      "type": "activemq",
      "display_name": "ActiveMQ Broker",
      "connection": {
        "broker_url": "tcp://localhost:61616",
        "username": "admin",
        "password": "admin"
      },
      "request_topics": ["ccs_cache_request"]
    }
  }
}
```

### 7.4 RabbitMQ Example

```json
{
  "brokers": {
    "rabbitmq_main": {
      "enabled": true,
      "type": "rabbitmq",
      "display_name": "RabbitMQ Broker",
      "connection": {
        "host": "localhost",
        "port": "5672",
        "virtual_host": "/",
        "username": "guest",
        "password": "guest"
      },
      "request_topics": ["ccs_cache_request"]
    }
  }
}
```

### 7.5 Managing Brokers at Runtime

Access the Admin UI at `/admin/messaging` to:

- **Enable**: Connect to a disabled broker
- **Disable**: Disconnect and stop consuming
- **Pause**: Stop consuming but keep connection
- **Resume**: Resume consuming after pause
- **Reconnect**: Retry failed connections

---

## 8. Common Configuration Examples

### 8.1 Development Setup

```bash
java -jar kuber-server.jar \
  --spring.profiles.active=dev \
  --kuber.persistence.type=memory \
  --logging.level.com.kuber=DEBUG
```

### 8.2 Production Setup with RocksDB

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -Xms4g -Xmx8g \
     -XX:+UseG1GC \
     -XX:MaxGCPauseMillis=200 \
     -jar kuber-server.jar \
  --kuber.network.port=6380 \
  --kuber.cache.max-memory-entries=1000000 \
  --kuber.persistence.type=lmdb \
  --kuber.persistence.rocksdb.path=/var/kuber/data \
  --kuber.backup.enabled=true \
  --kuber.backup.cron="0 0 23 * * *" \
  --spring.security.user.password=${KUBER_PASSWORD}
```

### 8.3 High-Availability Setup with ZooKeeper

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -jar kuber-server.jar \
  --kuber.node-id=node1 \
  --kuber.zookeeper.enabled=true \
  --kuber.zookeeper.connect-string=zk1:2181,zk2:2181,zk3:2181 \
  --kuber.persistence.type=lmdb
```

### 8.4 Off-Heap Key Index (Large Datasets)

```bash
java -XX:MaxDirectMemorySize=8g \
     -jar kuber-server.jar \
  --kuber.cache.off-heap-key-index=true \
  --kuber.cache.off-heap-key-index-initial-size-mb=256 \
  --kuber.cache.off-heap-key-index-max-size-mb=4096
```

### 8.5 Complete Production Script

Create `start-kuber.sh`:

```bash
#!/bin/bash

KUBER_HOME=/opt/kuber
# Required: --add-opens for LMDB persistence on Java 9+
JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Xms4g -Xmx8g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
KUBER_JAR="${KUBER_HOME}/kuber-server-1.9.0.jar"
CONFIG_DIR="${KUBER_HOME}/config"
LOG_DIR="${KUBER_HOME}/logs"
PID_FILE="${KUBER_HOME}/kuber.pid"

mkdir -p ${LOG_DIR}

nohup java ${JAVA_OPTS} \
  -jar ${KUBER_JAR} \
  --spring.config.location=${CONFIG_DIR}/application.yml \
  --kuber.secure.folder=${CONFIG_DIR}/secure \
  --logging.file.path=${LOG_DIR} \
  > ${LOG_DIR}/kuber-console.log 2>&1 &

echo $! > ${PID_FILE}
echo "Kuber started with PID $(cat ${PID_FILE})"
```

---

## 9. Verifying the Server

### 9.1 Check Server Status

```bash
# Check if process is running
ps aux | grep kuber-server

# Check listening ports
ss -tlnp | grep -E "8080|6380"
```

### 9.2 Health Check Endpoints

```bash
# REST API health check
curl http://localhost:8080/actuator/health

# Server info
curl -u admin:admin123 http://localhost:8080/api/info
```

### 9.3 Redis Protocol Test

```bash
# Using redis-cli
redis-cli -p 6380 PING
# Expected: PONG

redis-cli -p 6380 SET hello world
redis-cli -p 6380 GET hello
# Expected: "world"
```

### 9.4 Web UI Access

Open browser: `http://localhost:8080`

- Default credentials: `admin` / `admin123`
- Help documentation: `http://localhost:8080/help`
- Admin panel: `http://localhost:8080/admin`

### 9.5 Test Messaging (if configured)

```bash
# Check messaging status
curl -u admin:admin123 http://localhost:8080/api/v1/messaging/status
```

---

## 10. Troubleshooting

### 10.1 Server Won't Start

**LMDB Error: InaccessibleObjectException (Java 9+):**

If you see this error when using LMDB persistence:
```
java.lang.reflect.InaccessibleObjectException: Unable to make field long java.nio.Buffer.address accessible
```

Add the following JVM option:
```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -jar kuber-server.jar
```

Or use the startup scripts which include this option automatically:
```bash
./scripts/kuber-start.sh
```

**Port already in use:**
```bash
# Find process using port
lsof -i :8080
lsof -i :6380

# Kill process or use different ports
java -jar kuber-server.jar --server.port=9080 --kuber.network.port=7380
```

**Insufficient memory:**
```bash
java -Xms1g -Xmx2g -jar kuber-server.jar
```

**Permission denied on data directory:**
```bash
sudo chown -R $USER:$USER /var/kuber/data
chmod -R 755 /var/kuber/data
```

### 10.2 Connection Refused

```bash
# Check if server is listening
ss -tlnp | grep kuber

# Verify bind address allows external connections
java -jar kuber-server.jar --kuber.network.bind-address=0.0.0.0
```

### 10.3 Messaging Issues

**Broker not connecting:**
- Check broker is running and accessible
- Verify connection parameters in `request_response.json`
- Check Admin UI at `/admin/messaging` for error details
- Try "Reconnect" button for failed brokers

**Messages not processing:**
- Check if broker is paused (yellow badge in Admin UI)
- Verify API key in messages is valid
- Check queue depth and backpressure status

### 10.4 Enable Debug Logging

```bash
# Enable debug for all Kuber classes
java -jar kuber-server.jar --logging.level.com.kuber=DEBUG

# Enable trace logging (very verbose)
java -jar kuber-server.jar --logging.level.com.kuber=TRACE

# Log to file
java -jar kuber-server.jar \
  --logging.file.name=/var/log/kuber/kuber.log \
  --logging.level.com.kuber=DEBUG
```

---

## Quick Reference

### Minimum Start Command

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -jar kuber-server-1.9.0.jar
```

### Typical Production Start

```bash
java --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -Xms2g -Xmx4g \
     -jar kuber-server.jar \
     --kuber.persistence.type=lmdb \
     --kuber.persistence.rocksdb.path=/var/kuber/data \
     --spring.security.user.password=${KUBER_PASSWORD}
```

### Key URLs

| URL | Purpose |
|-----|---------|
| `http://localhost:8080` | Web UI |
| `http://localhost:8080/help` | Documentation |
| `http://localhost:8080/admin` | Admin Panel |
| `http://localhost:8080/admin/messaging` | Messaging Management |
| `http://localhost:8080/actuator/health` | Health Check |
| `localhost:6380` | Redis Protocol Port |

### Key Files

| File | Purpose |
|------|---------|
| `application.yml` | Main configuration |
| `secure/users.json` | User accounts |
| `secure/apikeys.json` | API keys |
| `secure/request_response.json` | Messaging configuration |

---

*For more information, see the full [Architecture Documentation](ARCHITECTURE.md) and [README](../README.md).*
