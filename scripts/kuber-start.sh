#!/bin/bash
# ============================================================================
# Kuber Startup Utility
# Copyright © 2025-2030, All Rights Reserved
# Ashutosh Sinha | Email: ajsinha@gmail.com
# ============================================================================
#
# This script starts the Kuber cache server with configurable options.
#
# Usage:
#   ./kuber-start.sh [options]
#
# Options:
#   -j, --jar PATH       Path to kuber-server JAR file
#   -c, --config PATH    Path to application.yml config file
#   -p, --profile NAME   Spring profile (dev, prod, test)
#   -m, --memory SIZE    JVM heap size (e.g., 2g, 4g, 8g)
#   -d, --daemon         Run as background daemon
#   -l, --log-dir DIR    Directory for log files
#   -P, --pid-file PATH  Path to PID file (for daemon mode)
#   --redis-port PORT    Redis protocol port (default: 6380)
#   --http-port PORT     HTTP/REST port (default: 8080)
#   --debug              Enable remote debugging on port 5005
#   --gc-log             Enable GC logging
#   --help               Show this help message
#
# Examples:
#   ./kuber-start.sh                                    # Start with defaults
#   ./kuber-start.sh -m 4g -p prod                      # 4GB heap, prod profile
#   ./kuber-start.sh -d -l /var/log/kuber               # Daemon with log dir
#   ./kuber-start.sh --redis-port 6381 --http-port 8081 # Custom ports
#
# ============================================================================

set -e

# Default values
JAR_PATH=""
CONFIG_PATH=""
PROFILE=""
MEMORY="2g"
DAEMON=false
LOG_DIR="./logs"
PID_FILE="./kuber.pid"
REDIS_PORT=""
HTTP_PORT=""
DEBUG=false
GC_LOG=false
JAVA_OPTS=""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"

# Print banner
print_banner() {
    echo -e "${BLUE}"
    echo "╔════════════════════════════════════════════════════════════════════╗"
    echo "║                     KUBER STARTUP UTILITY                          ║"
    echo "║                        Version 1.3.10                               ║"
    echo "╚════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# Print usage
print_usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -j, --jar PATH       Path to kuber-server JAR file"
    echo "  -c, --config PATH    Path to application.yml config file"
    echo "  -p, --profile NAME   Spring profile (dev, prod, test)"
    echo "  -m, --memory SIZE    JVM heap size (default: 2g)"
    echo "  -d, --daemon         Run as background daemon"
    echo "  -l, --log-dir DIR    Directory for log files (default: ./logs)"
    echo "  -P, --pid-file PATH  Path to PID file (default: ./kuber.pid)"
    echo "  --redis-port PORT    Redis protocol port (default: 6380)"
    echo "  --http-port PORT     HTTP/REST port (default: 8080)"
    echo "  --debug              Enable remote debugging on port 5005"
    echo "  --gc-log             Enable GC logging"
    echo "  --help               Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Start with defaults"
    echo "  $0 -m 4g -p prod                      # 4GB heap, prod profile"
    echo "  $0 -d -l /var/log/kuber               # Daemon with log dir"
    echo "  $0 --redis-port 6381 --http-port 8081 # Custom ports"
}

# Find JAR file
find_jar() {
    # Check if JAR path was provided
    if [ -n "$JAR_PATH" ] && [ -f "$JAR_PATH" ]; then
        echo "$JAR_PATH"
        return 0
    fi
    
    # Search in common locations
    local search_paths=(
        "$BASE_DIR/kuber-server/target/kuber-server-*.jar"
        "$BASE_DIR/target/kuber-server-*.jar"
        "$BASE_DIR/kuber-server-*.jar"
        "$SCRIPT_DIR/../kuber-server-*.jar"
        "./kuber-server-*.jar"
    )
    
    for pattern in "${search_paths[@]}"; do
        # shellcheck disable=SC2086
        for jar in $pattern; do
            if [ -f "$jar" ] && [[ ! "$jar" == *"-sources.jar" ]] && [[ ! "$jar" == *"-javadoc.jar" ]]; then
                echo "$jar"
                return 0
            fi
        done
    done
    
    return 1
}

# Check if already running
check_running() {
    if [ -f "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${YELLOW}Kuber is already running with PID $pid${NC}"
            echo "Use ./kuber-shutdown.sh to stop it first"
            exit 1
        else
            # Stale PID file
            rm -f "$PID_FILE"
        fi
    fi
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -j|--jar)
            JAR_PATH="$2"
            shift 2
            ;;
        -c|--config)
            CONFIG_PATH="$2"
            shift 2
            ;;
        -p|--profile)
            PROFILE="$2"
            shift 2
            ;;
        -m|--memory)
            MEMORY="$2"
            shift 2
            ;;
        -d|--daemon)
            DAEMON=true
            shift
            ;;
        -l|--log-dir)
            LOG_DIR="$2"
            shift 2
            ;;
        -P|--pid-file)
            PID_FILE="$2"
            shift 2
            ;;
        --redis-port)
            REDIS_PORT="$2"
            shift 2
            ;;
        --http-port)
            HTTP_PORT="$2"
            shift 2
            ;;
        --debug)
            DEBUG=true
            shift
            ;;
        --gc-log)
            GC_LOG=true
            shift
            ;;
        --help)
            print_banner
            print_usage
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            print_usage
            exit 1
            ;;
    esac
done

print_banner

# Check Java
if ! command -v java &> /dev/null; then
    echo -e "${RED}Error: Java is not installed or not in PATH${NC}"
    echo "Please install Java 17 or later"
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | head -n 1 | cut -d'"' -f2 | cut -d'.' -f1)
echo -e "${CYAN}Java version: $JAVA_VERSION${NC}"

if [ "$JAVA_VERSION" -lt 17 ]; then
    echo -e "${YELLOW}Warning: Java 17 or later is recommended${NC}"
fi

# Find JAR
JAR_PATH=$(find_jar)
if [ -z "$JAR_PATH" ]; then
    echo -e "${RED}Error: Could not find kuber-server JAR file${NC}"
    echo "Please build the project first: mvn clean package -DskipTests"
    echo "Or specify the JAR path: $0 -j /path/to/kuber-server.jar"
    exit 1
fi

echo -e "${CYAN}JAR file: $JAR_PATH${NC}"

# Check if already running
check_running

# Create log directory
if [ "$DAEMON" = true ]; then
    mkdir -p "$LOG_DIR"
    echo -e "${CYAN}Log directory: $LOG_DIR${NC}"
fi

# Build JVM options
JVM_OPTS="-Xms${MEMORY} -Xmx${MEMORY}"
JVM_OPTS="$JVM_OPTS -XX:+UseG1GC"
JVM_OPTS="$JVM_OPTS -XX:+HeapDumpOnOutOfMemoryError"
JVM_OPTS="$JVM_OPTS -XX:HeapDumpPath=${LOG_DIR}/heapdump.hprof"
JVM_OPTS="$JVM_OPTS -Djava.awt.headless=true"
JVM_OPTS="$JVM_OPTS -Dfile.encoding=UTF-8"

# Debug options
if [ "$DEBUG" = true ]; then
    JVM_OPTS="$JVM_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"
    echo -e "${YELLOW}Debug mode enabled on port 5005${NC}"
fi

# GC logging
if [ "$GC_LOG" = true ]; then
    JVM_OPTS="$JVM_OPTS -Xlog:gc*:file=${LOG_DIR}/gc.log:time,uptime:filecount=5,filesize=10M"
    echo -e "${CYAN}GC logging enabled${NC}"
fi

# Build Spring options
SPRING_OPTS=""

if [ -n "$CONFIG_PATH" ]; then
    SPRING_OPTS="$SPRING_OPTS --spring.config.location=$CONFIG_PATH"
    echo -e "${CYAN}Config file: $CONFIG_PATH${NC}"
fi

if [ -n "$PROFILE" ]; then
    SPRING_OPTS="$SPRING_OPTS --spring.profiles.active=$PROFILE"
    echo -e "${CYAN}Profile: $PROFILE${NC}"
fi

if [ -n "$REDIS_PORT" ]; then
    SPRING_OPTS="$SPRING_OPTS --kuber.network.redis-port=$REDIS_PORT"
    echo -e "${CYAN}Redis port: $REDIS_PORT${NC}"
fi

if [ -n "$HTTP_PORT" ]; then
    SPRING_OPTS="$SPRING_OPTS --server.port=$HTTP_PORT"
    echo -e "${CYAN}HTTP port: $HTTP_PORT${NC}"
fi

# Add any additional Java options
if [ -n "$JAVA_OPTS" ]; then
    JVM_OPTS="$JVM_OPTS $JAVA_OPTS"
fi

echo ""
echo -e "${GREEN}Starting Kuber...${NC}"
echo ""

# Start the server
if [ "$DAEMON" = true ]; then
    # Daemon mode
    nohup java $JVM_OPTS -jar "$JAR_PATH" $SPRING_OPTS \
        > "$LOG_DIR/kuber.out" 2>&1 &
    
    PID=$!
    echo "$PID" > "$PID_FILE"
    
    echo -e "${GREEN}✓ Kuber started as daemon${NC}"
    echo -e "  PID: $PID"
    echo -e "  PID file: $PID_FILE"
    echo -e "  Log file: $LOG_DIR/kuber.out"
    echo ""
    echo -e "${YELLOW}Use 'tail -f $LOG_DIR/kuber.out' to view logs${NC}"
    echo -e "${YELLOW}Use './kuber-shutdown.sh' to stop the server${NC}"
else
    # Foreground mode
    echo -e "${YELLOW}Running in foreground. Press Ctrl+C to stop.${NC}"
    echo ""
    
    exec java $JVM_OPTS -jar "$JAR_PATH" $SPRING_OPTS
fi
