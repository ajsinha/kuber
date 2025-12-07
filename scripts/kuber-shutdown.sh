#!/bin/bash
# ============================================================================
# Kuber Shutdown Utility
# Copyright © 2025-2030, All Rights Reserved
# Ashutosh Sinha | Email: ajsinha@gmail.com
# ============================================================================
#
# This script triggers a graceful shutdown of the Kuber cache server.
#
# Usage:
#   ./kuber-shutdown.sh [options]
#
# Options:
#   -f, --file         Use file-based shutdown (default)
#   -a, --api          Use REST API shutdown
#   -k, --api-key KEY  API key for REST API shutdown
#   -h, --host HOST    API host (default: localhost)
#   -p, --port PORT    API port (default: 8080)
#   -d, --dir DIR      Directory containing kuber.shutdown file
#   -r, --reason TEXT  Shutdown reason (for logging)
#   --help             Show this help message
#
# Examples:
#   ./kuber-shutdown.sh                           # File-based shutdown
#   ./kuber-shutdown.sh -a -k myapikey            # API-based shutdown
#   ./kuber-shutdown.sh -d /opt/kuber             # Custom directory
#   ./kuber-shutdown.sh -r "Maintenance window"  # With reason
#
# ============================================================================

set -e

# Default values
SHUTDOWN_METHOD="file"
API_HOST="localhost"
API_PORT="8080"
API_KEY=""
SHUTDOWN_DIR="."
SHUTDOWN_FILE="kuber.shutdown"
REASON="Manual shutdown"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print banner
print_banner() {
    echo -e "${BLUE}"
    echo "╔════════════════════════════════════════════════════════════════════╗"
    echo "║                    KUBER SHUTDOWN UTILITY                          ║"
    echo "║                        Version 1.3.10                               ║"
    echo "╚════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# Print usage
print_usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -f, --file         Use file-based shutdown (default)"
    echo "  -a, --api          Use REST API shutdown"
    echo "  -k, --api-key KEY  API key for REST API shutdown"
    echo "  -h, --host HOST    API host (default: localhost)"
    echo "  -p, --port PORT    API port (default: 8080)"
    echo "  -d, --dir DIR      Directory containing kuber.shutdown file"
    echo "  -r, --reason TEXT  Shutdown reason (for logging)"
    echo "  --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                           # File-based shutdown"
    echo "  $0 -a -k myapikey            # API-based shutdown"
    echo "  $0 -d /opt/kuber             # Custom directory"
    echo "  $0 -r \"Maintenance\"         # With reason"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--file)
            SHUTDOWN_METHOD="file"
            shift
            ;;
        -a|--api)
            SHUTDOWN_METHOD="api"
            shift
            ;;
        -k|--api-key)
            API_KEY="$2"
            shift 2
            ;;
        -h|--host)
            API_HOST="$2"
            shift 2
            ;;
        -p|--port)
            API_PORT="$2"
            shift 2
            ;;
        -d|--dir)
            SHUTDOWN_DIR="$2"
            shift 2
            ;;
        -r|--reason)
            REASON="$2"
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

# File-based shutdown
file_shutdown() {
    SHUTDOWN_PATH="${SHUTDOWN_DIR}/${SHUTDOWN_FILE}"
    
    echo -e "${YELLOW}Method: File-based shutdown${NC}"
    echo -e "Creating shutdown file: ${SHUTDOWN_PATH}"
    echo ""
    
    # Create the shutdown file with reason
    echo "Shutdown requested at $(date)" > "$SHUTDOWN_PATH"
    echo "Reason: $REASON" >> "$SHUTDOWN_PATH"
    
    if [ -f "$SHUTDOWN_PATH" ]; then
        echo -e "${GREEN}✓ Shutdown file created successfully${NC}"
        echo ""
        echo "Kuber will detect the file within 5 seconds and begin graceful shutdown."
        echo "The shutdown process takes approximately 30 seconds to complete."
        echo ""
        echo -e "${YELLOW}Monitor the application logs for shutdown progress.${NC}"
    else
        echo -e "${RED}✗ Failed to create shutdown file${NC}"
        exit 1
    fi
}

# API-based shutdown
api_shutdown() {
    if [ -z "$API_KEY" ]; then
        echo -e "${RED}Error: API key required for API-based shutdown${NC}"
        echo "Use: $0 -a -k YOUR_API_KEY"
        exit 1
    fi
    
    API_URL="http://${API_HOST}:${API_PORT}/api/admin/shutdown"
    
    echo -e "${YELLOW}Method: REST API shutdown${NC}"
    echo -e "API URL: ${API_URL}"
    echo ""
    
    # Check if curl is available
    if ! command -v curl &> /dev/null; then
        echo -e "${RED}Error: curl is required for API-based shutdown${NC}"
        exit 1
    fi
    
    # Make the API call
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$API_URL" \
        -H "X-API-Key: $API_KEY" \
        -H "Content-Type: application/json" \
        -d "{\"reason\": \"$REASON\"}" 2>&1)
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
    BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [ "$HTTP_CODE" = "202" ] || [ "$HTTP_CODE" = "200" ]; then
        echo -e "${GREEN}✓ Shutdown request accepted${NC}"
        echo ""
        echo "Response: $BODY"
        echo ""
        echo "Kuber is now shutting down gracefully."
        echo "The shutdown process takes approximately 30 seconds to complete."
    else
        echo -e "${RED}✗ Shutdown request failed (HTTP $HTTP_CODE)${NC}"
        echo "Response: $BODY"
        exit 1
    fi
}

# Execute shutdown based on method
case $SHUTDOWN_METHOD in
    file)
        file_shutdown
        ;;
    api)
        api_shutdown
        ;;
    *)
        echo -e "${RED}Unknown shutdown method: $SHUTDOWN_METHOD${NC}"
        exit 1
        ;;
esac

echo ""
echo -e "${BLUE}Shutdown initiated. Goodbye!${NC}"
