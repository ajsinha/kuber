# Kuber Scripts

**Version 1.7.8**

This folder contains utility scripts for managing the Kuber cache server.

## JVM Options

The startup scripts automatically include required JVM options for Java 9+:

```
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
```

These options are **required** for LMDB persistence support, as the LMDB Java library needs to access internal ByteBuffer and DirectBuffer fields for direct memory operations.

## Available Scripts

| Script | Platform | Description |
|--------|----------|-------------|
| `kuber-start.sh` | Linux/Mac | Start Kuber server |
| `kuber-start.bat` | Windows | Start Kuber server |
| `kuber-shutdown.sh` | Linux/Mac | Gracefully shutdown Kuber |
| `kuber-shutdown.bat` | Windows | Gracefully shutdown Kuber |
| `kuber-status.sh` | Linux/Mac | Check server status |

## Quick Start

### Linux/Mac

```bash
# Make scripts executable
chmod +x *.sh

# Start the server
./kuber-start.sh

# Check status
./kuber-status.sh

# Shutdown
./kuber-shutdown.sh
```

### Windows

```batch
REM Start the server
kuber-start.bat

REM Shutdown
kuber-shutdown.bat
```

## Startup Script Options

### kuber-start.sh

```
Usage: ./kuber-start.sh [options]

Options:
  -j, --jar PATH       Path to kuber-server JAR file
  -c, --config PATH    Path to application.yml config file
  -p, --profile NAME   Spring profile (dev, prod, test)
  -m, --memory SIZE    JVM heap size (default: 2g)
  -d, --daemon         Run as background daemon
  -l, --log-dir DIR    Directory for log files (default: ./logs)
  -P, --pid-file PATH  Path to PID file (default: ./kuber.pid)
  --redis-port PORT    Redis protocol port (default: 6380)
  --http-port PORT     HTTP/REST port (default: 8080)
  --debug              Enable remote debugging on port 5005
  --gc-log             Enable GC logging

Examples:
  ./kuber-start.sh                                    # Start with defaults
  ./kuber-start.sh -m 4g -p prod                      # 4GB heap, prod profile
  ./kuber-start.sh -d -l /var/log/kuber               # Daemon mode
  ./kuber-start.sh --redis-port 6381 --http-port 8081 # Custom ports
```

### kuber-start.bat

```
Usage: kuber-start.bat [options]

Options:
  /jar:PATH           Path to kuber-server JAR file
  /config:PATH        Path to application.yml config file
  /profile:NAME       Spring profile (dev, prod, test)
  /memory:SIZE        JVM heap size (default: 2g)
  /redis-port:PORT    Redis protocol port (default: 6380)
  /http-port:PORT     HTTP/REST port (default: 8080)
  /debug              Enable remote debugging on port 5005

Examples:
  kuber-start.bat                                    - Start with defaults
  kuber-start.bat /memory:4g /profile:prod           - 4GB heap, prod profile
  kuber-start.bat /redis-port:6381 /http-port:8081   - Custom ports
```

## Shutdown Script Options

### kuber-shutdown.sh

```
Usage: ./kuber-shutdown.sh [options]

Options:
  -f, --file         Use file-based shutdown (default)
  -a, --api          Use REST API shutdown
  -k, --api-key KEY  API key for REST API shutdown
  -h, --host HOST    API host (default: localhost)
  -p, --port PORT    API port (default: 8080)
  -d, --dir DIR      Directory containing kuber.shutdown file (default: ./kuberdata)
  -r, --reason TEXT  Shutdown reason (for logging)

Examples:
  ./kuber-shutdown.sh                           # File-based shutdown
  ./kuber-shutdown.sh -a -k myapikey            # API-based shutdown
  ./kuber-shutdown.sh -r "Maintenance window"   # With reason
```

## Startup Sequence

When Kuber starts, it follows this orchestrated sequence:

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
│  Phase 2: Persistence Setup                                  │
│  - Initialize RocksDB/SQLite stores                          │
│  - Run compaction (concurrent per region)                    │
│  - Clean expired entries                                     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 3: Cache Recovery                                     │
│  - Load persisted entries into memory                        │
│  - Build off-heap key indexes                                │
│  - Prime negative cache                                      │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 4: Service Activation                                 │
│  - Start Event Publishing (Kafka, etc.)                      │
│  - Start Redis Protocol Server (port 6380)                   │
│  - Start HTTP/REST Server (port 8080)                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 5: Background Services                                │
│  - Start Autoload Service (file monitoring)                  │
│  - Start Scheduled Tasks (compaction, cleanup)               │
│  - System Ready!                                             │
└─────────────────────────────────────────────────────────────┘
```

## Shutdown Sequence

When shutdown is triggered (via file, API, or signal):

```
Shutdown Triggered
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 1: Block New Operations                               │
│  - Signal shutdown to all components                         │
│  - Acquire exclusive persistence lock                        │
│  - Wait 5 seconds (configurable)                             │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 2: Stop Autoload Service                              │
│  - Stop file monitoring                                      │
│  - Cancel pending file processing                            │
│  - Wait 5 seconds                                            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 3: Stop Redis Protocol Server                         │
│  - Reject new connections                                    │
│  - Wait for in-flight commands (up to 5s)                    │
│  - Close all client connections                              │
│  - Wait 5 seconds                                            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 4: Stop Event Publishing                              │
│  - Flush pending events to brokers                           │
│  - Close broker connections                                  │
│  - Wait 5 seconds                                            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 5: Persist Cache Data                                 │
│  - Save all cache entries to persistence                     │
│  - Sync to disk                                              │
│  - Wait 5 seconds                                            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Phase 6: Close Persistence Store                            │
│  - Flush RocksDB WAL                                         │
│  - Close database handles                                    │
│  - Release file locks                                        │
│  - Wait 5 seconds                                            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
                 Application Exit
              (~30 seconds total)
```

## Configuration

Shutdown timing can be configured in `application.yml`:

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

## Troubleshooting

### JAR Not Found

If the startup script cannot find the JAR file:

```bash
# Build the project first
cd /path/to/kuber
mvn clean package -DskipTests

# Or specify the JAR path explicitly
./kuber-start.sh -j /path/to/kuber-server-1.3.6.jar
```

### Permission Denied

```bash
chmod +x scripts/*.sh
```

### Port Already in Use

```bash
# Check what's using the port
lsof -i :6380
lsof -i :8080

# Use different ports
./kuber-start.sh --redis-port 6381 --http-port 8081
```

### Shutdown Not Working

If file-based shutdown doesn't work:

```bash
# Check if file watcher is enabled
curl http://localhost:8080/api/admin/shutdown/status

# Use API shutdown instead
./kuber-shutdown.sh -a -k your-api-key

# Or send SIGTERM directly
kill $(cat kuber.pid)
```

---

Copyright © 2025-2030, All Rights Reserved  
Ashutosh Sinha | Email: ajsinha@gmail.com
