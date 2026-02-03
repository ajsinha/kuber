#!/bin/bash
# ============================================================================
# Kuber Status Utility
# Copyright © 2025-2030, All Rights Reserved
# Ashutosh Sinha | Email: ajsinha@gmail.com
# ============================================================================
#
# This script checks the status of the Kuber cache server.
#
# Usage:
#   ./kuber-status.sh [options]
#
# Options:
#   -h, --host HOST      API host (default: localhost)
#   -p, --port PORT      HTTP port (default: 8080)
#   -k, --api-key KEY    API key for authentication
#   -P, --pid-file PATH  Path to PID file (default: ./kuber.pid)
#   --help               Show this help message
#
# ============================================================================

set -e

# Default values
API_HOST="localhost"
API_PORT="8080"
API_KEY=""
PID_FILE="./kuber.pid"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Print banner
print_banner() {
    echo -e "${BLUE}"
    echo "╔════════════════════════════════════════════════════════════════════╗"
    echo "║                     KUBER STATUS UTILITY                           ║"
    echo "║                        Version 1.8.3                               ║"
    echo "╚════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# Print usage
print_usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -h, --host HOST      API host (default: localhost)"
    echo "  -p, --port PORT      HTTP port (default: 8080)"
    echo "  -k, --api-key KEY    API key for authentication"
    echo "  -P, --pid-file PATH  Path to PID file (default: ./kuber.pid)"
    echo "  --help               Show this help message"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--host)
            API_HOST="$2"
            shift 2
            ;;
        -p|--port)
            API_PORT="$2"
            shift 2
            ;;
        -k|--api-key)
            API_KEY="$2"
            shift 2
            ;;
        -P|--pid-file)
            PID_FILE="$2"
            shift 2
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

# Check PID file
echo -e "${CYAN}Checking PID file...${NC}"
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo -e "${GREEN}✓ Process running with PID: $PID${NC}"
    else
        echo -e "${YELLOW}⚠ Stale PID file found (process $PID not running)${NC}"
    fi
else
    echo -e "${YELLOW}⚠ No PID file found at $PID_FILE${NC}"
fi

echo ""

# Check HTTP endpoint
echo -e "${CYAN}Checking HTTP endpoint...${NC}"
API_URL="http://${API_HOST}:${API_PORT}/api/health"

HEADERS=""
if [ -n "$API_KEY" ]; then
    HEADERS="-H \"X-API-Key: $API_KEY\""
fi

if command -v curl &> /dev/null; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API_URL" $HEADERS 2>/dev/null || echo "000")
    
    if [ "$HTTP_CODE" = "200" ]; then
        echo -e "${GREEN}✓ HTTP endpoint responding (status: $HTTP_CODE)${NC}"
        
        # Get detailed status
        echo ""
        echo -e "${CYAN}Server Status:${NC}"
        curl -s "$API_URL" $HEADERS 2>/dev/null | python3 -m json.tool 2>/dev/null || \
        curl -s "$API_URL" $HEADERS 2>/dev/null
    elif [ "$HTTP_CODE" = "000" ]; then
        echo -e "${RED}✗ Cannot connect to http://${API_HOST}:${API_PORT}${NC}"
    else
        echo -e "${YELLOW}⚠ HTTP endpoint returned status: $HTTP_CODE${NC}"
    fi
else
    echo -e "${YELLOW}⚠ curl not available - cannot check HTTP endpoint${NC}"
fi

echo ""

# Check Redis port
echo -e "${CYAN}Checking Redis protocol port...${NC}"
REDIS_PORT="6380"

if command -v nc &> /dev/null; then
    if nc -z "$API_HOST" "$REDIS_PORT" 2>/dev/null; then
        echo -e "${GREEN}✓ Redis port $REDIS_PORT is open${NC}"
        
        # Try PING command
        PONG=$(echo "PING" | nc -w 2 "$API_HOST" "$REDIS_PORT" 2>/dev/null | tr -d '\r\n' || echo "")
        if [ "$PONG" = "+PONG" ]; then
            echo -e "${GREEN}✓ Redis PING successful${NC}"
        fi
    else
        echo -e "${RED}✗ Redis port $REDIS_PORT is not responding${NC}"
    fi
elif command -v timeout &> /dev/null; then
    if timeout 2 bash -c "echo PING > /dev/tcp/$API_HOST/$REDIS_PORT" 2>/dev/null; then
        echo -e "${GREEN}✓ Redis port $REDIS_PORT is open${NC}"
    else
        echo -e "${RED}✗ Redis port $REDIS_PORT is not responding${NC}"
    fi
else
    echo -e "${YELLOW}⚠ nc not available - cannot check Redis port${NC}"
fi

echo ""
echo -e "${BLUE}Status check complete.${NC}"
