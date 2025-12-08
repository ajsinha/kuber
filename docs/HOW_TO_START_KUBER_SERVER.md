# How to Start Kuber Server

**Version 1.5.0**

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
7. [Common Configuration Examples](#7-common-configuration-examples)
8. [Verifying the Server](#8-verifying-the-server)
9. [Troubleshooting](#9-troubleshooting)

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
kuber-server/target/kuber-server-1.5.0-SNAPSHOT.jar
```

### Build Specific Module

```bash
# Build only the server module
mvn clean install -pl kuber-server -am -DskipTests
```

---

## 3. Running the Server

### Method 1: Using the JAR File (Recommended for Production)

```bash
# Basic start
java -jar kuber-server/target/kuber-server-1.5.0-SNAPSHOT.jar

# With specific memory settings
java -Xms512m -Xmx2g -jar kuber-server/target/kuber-server-1.5.0-SNAPSHOT.jar

# With garbage collector tuning (for large heaps)
java -Xms2g -Xmx4g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 \
     -jar kuber-server/target/kuber-server-1.5.0-SNAPSHOT.jar
```

### Method 2: Using Maven (Recommended for Development)

```bash
# From the project root directory
cd kuber-server
mvn spring-boot:run

# With specific profile
mvn spring-boot:run -Dspring-boot.run.profiles=dev

# With JVM arguments
mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Xms512m -Xmx2g"
```

### Method 3: Using the Build Script

```bash
# Make script executable
chmod +x build.sh

# Build and run
./build.sh run

# Build only
./build.sh build
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

spring:
  security:
    user:
      name: admin
      password: your-secure-password
```

### 4.4 Using Spring Profiles

Create profile-specific configuration files:

```
application.yml           # Default configuration
application-dev.yml       # Development overrides
application-prod.yml      # Production overrides
application-test.yml      # Test overrides
```

Activate a profile:

```bash
# Using command line argument
java -jar kuber-server.jar --spring.profiles.active=prod

# Using environment variable
export SPRING_PROFILES_ACTIVE=prod
java -jar kuber-server.jar

# Using system property
java -Dspring.profiles.active=prod -jar kuber-server.jar
```

---

## 5. Command Line Property Overrides

### 5.1 Using Double-Dash Syntax (Spring Boot Standard)

```bash
java -jar kuber-server.jar \
  --kuber.network.port=6380 \
  --kuber.network.bind-address=0.0.0.0 \
  --kuber.cache.max-memory-entries=500000 \
  --kuber.persistence.type=rocksdb \
  --kuber.persistence.rocksdb.path=/data/kuber/rocksdb \
  --spring.security.user.name=admin \
  --spring.security.user.password=secret123
```

### 5.2 Using Java System Properties (-D Syntax)

```bash
java -Dkuber.network.port=6380 \
     -Dkuber.cache.max-memory-entries=500000 \
     -Dkuber.persistence.type=rocksdb \
     -Dspring.security.user.name=admin \
     -Dspring.security.user.password=secret123 \
     -jar kuber-server.jar
```

### 5.3 Common Property Overrides

| Property | Default | Description |
|----------|---------|-------------|
| `--server.port=8080` | 8080 | HTTP/REST API port |
| `--kuber.network.port=6380` | 6380 | Redis protocol port |
| `--kuber.network.bind-address=0.0.0.0` | 0.0.0.0 | Network interface |
| `--kuber.cache.max-memory-entries=100000` | 100000 | Max entries per region |
| `--kuber.cache.global-max-memory-entries=500000` | 0 | Global max (0=unlimited) |
| `--kuber.persistence.type=rocksdb` | rocksdb | Persistence backend |
| `--kuber.persistence.rocksdb.path=./data` | ./data/rocksdb | Data directory |
| `--kuber.persistence.sync-individual-writes=false` | false | Sync/async writes |
| `--spring.security.user.name=admin` | admin | Web UI username |
| `--spring.security.user.password=admin` | admin | Web UI password |

### 5.4 Network Configuration

```bash
# Change both ports
java -jar kuber-server.jar \
  --server.port=9080 \
  --kuber.network.port=7380

# Bind to specific interface
java -jar kuber-server.jar \
  --kuber.network.bind-address=192.168.1.100

# Increase max connections
java -jar kuber-server.jar \
  --kuber.network.max-connections=50000
```

### 5.5 Persistence Configuration

```bash
# Use RocksDB (default, recommended)
java -jar kuber-server.jar \
  --kuber.persistence.type=rocksdb \
  --kuber.persistence.rocksdb.path=/var/kuber/data

# Use LMDB
java -jar kuber-server.jar \
  --kuber.persistence.type=lmdb \
  --kuber.persistence.lmdb.path=/var/kuber/lmdb \
  --kuber.persistence.lmdb.map-size=10737418240

# Use MongoDB
java -jar kuber-server.jar \
  --kuber.persistence.type=mongodb \
  --kuber.mongo.uri=mongodb://localhost:27017 \
  --kuber.mongo.database=kuber

# Use PostgreSQL
java -jar kuber-server.jar \
  --kuber.persistence.type=postgresql \
  --spring.datasource.url=jdbc:postgresql://localhost:5432/kuber \
  --spring.datasource.username=kuber \
  --spring.datasource.password=secret

# Use SQLite
java -jar kuber-server.jar \
  --kuber.persistence.type=sqlite \
  --kuber.persistence.sqlite.path=/var/kuber/kuber.db

# In-memory only (no persistence)
java -jar kuber-server.jar \
  --kuber.persistence.type=memory
```

### 5.6 Security Configuration

```bash
# Set admin credentials
java -jar kuber-server.jar \
  --spring.security.user.name=myadmin \
  --spring.security.user.password=MySecurePass123!

# Enable API key authentication file
java -jar kuber-server.jar \
  --kuber.security.api-keys-file=/etc/kuber/api-keys.json
```

### 5.7 Replication Configuration

```bash
# Enable ZooKeeper-based replication
java -jar kuber-server.jar \
  --kuber.zookeeper.enabled=true \
  --kuber.zookeeper.connect-string=zk1:2181,zk2:2181,zk3:2181 \
  --kuber.zookeeper.session-timeout=30000
```

### 5.8 Event Publishing Configuration

```bash
# Enable Kafka publishing
java -jar kuber-server.jar \
  --kuber.publishing.kafka.enabled=true \
  --kuber.publishing.kafka.bootstrap-servers=kafka1:9092,kafka2:9092 \
  --kuber.publishing.kafka.topic=kuber-events

# Enable RabbitMQ publishing
java -jar kuber-server.jar \
  --kuber.publishing.rabbitmq.enabled=true \
  --kuber.publishing.rabbitmq.host=rabbitmq.local \
  --kuber.publishing.rabbitmq.queue=kuber-events
```

---

## 6. Environment Variables

Spring Boot automatically maps environment variables to properties. The mapping rules:

1. Replace dots (`.`) with underscores (`_`)
2. Remove dashes (`-`)
3. Convert to uppercase

### 6.1 Examples

| Property | Environment Variable |
|----------|---------------------|
| `kuber.network.port` | `KUBER_NETWORK_PORT` |
| `kuber.cache.max-memory-entries` | `KUBER_CACHE_MAXMEMORYENTRIES` |
| `kuber.persistence.type` | `KUBER_PERSISTENCE_TYPE` |
| `spring.security.user.password` | `SPRING_SECURITY_USER_PASSWORD` |

### 6.2 Using Environment Variables

```bash
# Set environment variables
export KUBER_NETWORK_PORT=6380
export KUBER_CACHE_MAXMEMORYENTRIES=500000
export KUBER_PERSISTENCE_TYPE=rocksdb
export SPRING_SECURITY_USER_NAME=admin
export SPRING_SECURITY_USER_PASSWORD=secret123

# Run the server
java -jar kuber-server.jar
```

### 6.3 Docker Environment Variables

```bash
docker run -d \
  -e KUBER_NETWORK_PORT=6380 \
  -e KUBER_PERSISTENCE_TYPE=rocksdb \
  -e SPRING_SECURITY_USER_PASSWORD=secret \
  -p 8080:8080 \
  -p 6380:6380 \
  kuber-server:1.5.0
```

### 6.4 Using .env File (with Docker Compose)

Create `.env` file:
```
KUBER_NETWORK_PORT=6380
KUBER_CACHE_MAXMEMORYENTRIES=500000
KUBER_PERSISTENCE_TYPE=rocksdb
SPRING_SECURITY_USER_PASSWORD=secret123
```

Docker Compose will automatically load these variables.

---

## 7. Common Configuration Examples

### 7.1 Development Setup

```bash
java -jar kuber-server.jar \
  --spring.profiles.active=dev \
  --kuber.persistence.type=memory \
  --logging.level.com.kuber=DEBUG
```

### 7.2 Production Setup with RocksDB

```bash
java -Xms4g -Xmx8g \
     -XX:+UseG1GC \
     -XX:MaxGCPauseMillis=200 \
     -jar kuber-server.jar \
  --kuber.network.port=6380 \
  --kuber.cache.max-memory-entries=1000000 \
  --kuber.cache.global-max-memory-entries=5000000 \
  --kuber.persistence.type=rocksdb \
  --kuber.persistence.rocksdb.path=/var/kuber/data \
  --kuber.persistence.sync-individual-writes=false \
  --kuber.backup.enabled=true \
  --kuber.backup.cron="0 0 2 * * ?" \
  --spring.security.user.password=${KUBER_ADMIN_PASSWORD}
```

### 7.3 High-Availability Setup with ZooKeeper

```bash
java -jar kuber-server.jar \
  --kuber.node-id=node1 \
  --kuber.zookeeper.enabled=true \
  --kuber.zookeeper.connect-string=zk1:2181,zk2:2181,zk3:2181 \
  --kuber.persistence.type=rocksdb \
  --kuber.persistence.rocksdb.path=/var/kuber/node1
```

### 7.4 Off-Heap Key Index (Large Datasets)

```bash
java -XX:MaxDirectMemorySize=8g \
     -jar kuber-server.jar \
  --kuber.cache.off-heap-key-index=true \
  --kuber.cache.off-heap-key-index-initial-size-mb=256 \
  --kuber.cache.off-heap-key-index-max-size-mb=4096
```

### 7.5 Complete Production Example

Create `start-kuber.sh`:

```bash
#!/bin/bash

# Configuration
KUBER_HOME=/opt/kuber
JAVA_OPTS="-Xms4g -Xmx8g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"
KUBER_JAR="${KUBER_HOME}/kuber-server.jar"
CONFIG_FILE="${KUBER_HOME}/config/application.yml"
LOG_DIR="${KUBER_HOME}/logs"
PID_FILE="${KUBER_HOME}/kuber.pid"

# Create log directory
mkdir -p ${LOG_DIR}

# Start the server
nohup java ${JAVA_OPTS} \
  -jar ${KUBER_JAR} \
  --spring.config.location=${CONFIG_FILE} \
  --logging.file.path=${LOG_DIR} \
  > ${LOG_DIR}/kuber-console.log 2>&1 &

# Save PID
echo $! > ${PID_FILE}
echo "Kuber started with PID $(cat ${PID_FILE})"
```

---

## 8. Verifying the Server

### 8.1 Check Server Status

```bash
# Check if process is running
ps aux | grep kuber-server

# Check listening ports
netstat -tlnp | grep -E "8080|6380"
# or
ss -tlnp | grep -E "8080|6380"
```

### 8.2 Health Check Endpoints

```bash
# REST API health check
curl http://localhost:8080/actuator/health

# Server info
curl -u admin:admin http://localhost:8080/api/info
```

### 8.3 Redis Protocol Test

```bash
# Using redis-cli
redis-cli -p 6380 -a admin:admin PING
# Expected: PONG

# Using telnet
telnet localhost 6380
AUTH admin admin
PING
```

### 8.4 Web UI Access

Open browser: `http://localhost:8080`

Login with configured credentials (default: admin/admin)

---

## 9. Troubleshooting

### 9.1 Server Won't Start

**Port already in use:**
```bash
# Find process using port
lsof -i :8080
lsof -i :6380

# Kill process
kill -9 <PID>

# Or use different ports
java -jar kuber-server.jar --server.port=9080 --kuber.network.port=7380
```

**Insufficient memory:**
```bash
# Increase heap size
java -Xms1g -Xmx2g -jar kuber-server.jar
```

**Permission denied on data directory:**
```bash
# Check permissions
ls -la /var/kuber/data

# Fix permissions
sudo chown -R $USER:$USER /var/kuber/data
chmod -R 755 /var/kuber/data
```

### 9.2 Connection Refused

```bash
# Check if server is listening
ss -tlnp | grep kuber

# Check firewall
sudo ufw status
sudo iptables -L -n

# Verify bind address allows external connections
# Use 0.0.0.0 instead of 127.0.0.1 for external access
java -jar kuber-server.jar --kuber.network.bind-address=0.0.0.0
```

### 9.3 Authentication Failures

```bash
# Verify credentials in configuration
grep -A2 "security:" application.yml

# Reset to default credentials
java -jar kuber-server.jar \
  --spring.security.user.name=admin \
  --spring.security.user.password=admin
```

### 9.4 Persistence Issues

```bash
# Check data directory exists and is writable
ls -la /var/kuber/data

# Check disk space
df -h /var/kuber

# View persistence logs
grep -i "persistence\|rocksdb\|lmdb" logs/kuber.log
```

### 9.5 Enable Debug Logging

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

### 9.6 JVM Tuning for Large Heaps

```bash
java \
  -Xms8g \
  -Xmx16g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=200 \
  -XX:+ParallelRefProcEnabled \
  -XX:+DisableExplicitGC \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=/var/kuber/heapdump.hprof \
  -jar kuber-server.jar
```

---

## Quick Reference

### Minimum Start Command

```bash
java -jar kuber-server-1.5.0-SNAPSHOT.jar
```

### Typical Production Start

```bash
java -Xms2g -Xmx4g \
  -jar kuber-server.jar \
  --kuber.persistence.type=rocksdb \
  --kuber.persistence.rocksdb.path=/var/kuber/data \
  --spring.security.user.password=${KUBER_PASSWORD}
```

### Key URLs

| URL | Purpose |
|-----|---------|
| `http://localhost:8080` | Web UI |
| `http://localhost:8080/api/info` | Server Info API |
| `http://localhost:8080/actuator/health` | Health Check |
| `localhost:6380` | Redis Protocol Port |

---

*For more information, see the full [Architecture Documentation](ARCHITECTURE.md) and [README](../README.md).*
