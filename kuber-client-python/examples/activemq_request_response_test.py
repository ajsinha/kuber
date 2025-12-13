#!/usr/bin/env python3
"""
Kuber ActiveMQ Request/Response Test Client (v1.7.5)

Tests the Kuber Request/Response messaging feature via ActiveMQ STOMP protocol.
Publishes cache operation requests and subscribes to responses.

Prerequisites:
- ActiveMQ running with STOMP connector enabled (default port 61613)
- Kuber server running with messaging enabled
- stomp.py package: pip install stomp.py

IMPORTANT: ActiveMQ has multiple protocol ports:
- 61616: OpenWire (Java native protocol) - DO NOT USE with stomp.py
- 61613: STOMP protocol - USE THIS with stomp.py
- 5672:  AMQP protocol
- 1883:  MQTT protocol
- 8161:  Web Console (HTTP)

Copyright © 2025-2030, All Rights Reserved
Ashutosh Sinha | Email: ajsinha@gmail.com
"""

import threading
import time
import json
import uuid
import logging
import sys
from datetime import datetime

# Try to import stomp
try:
    import stomp
except ImportError:
    print("ERROR: stomp.py not installed. Run: pip install stomp.py")
    sys.exit(1)

# Set up logging with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Reduce stomp.py logging noise
logging.getLogger('stomp.py').setLevel(logging.WARNING)

# ==============================================================================
# Configuration
# ==============================================================================

# ActiveMQ STOMP port (NOT 61616 which is OpenWire!)
ACTIVEMQ_HOST = 'localhost'
ACTIVEMQ_STOMP_PORT = 61613  # STOMP protocol port

# Queue names (use /queue/ prefix for STOMP)
REQUEST_QUEUE = '/queue/ccs_cache_request'
RESPONSE_QUEUE = '/queue/ccs_cache_response'

# Your Kuber API key (replace with actual key)
API_KEY = 'kub_d427a1192ad2e45e8376e518f958b882cbdd98fdbded654ecb4e0efe70968d08'

# Region to test
TEST_REGION = 'default'

# Track pending requests for correlation
pending_requests = {}
received_responses = []
stop_consumer = threading.Event()
consumer_ready = threading.Event()


# ==============================================================================
# STOMP Connection Listener
# ==============================================================================

class KuberResponseListener(stomp.ConnectionListener):
    """Listener for ActiveMQ STOMP messages."""
    
    def __init__(self):
        self.connected = False
        self.subscribed = False
        self.connection_error = None
    
    def on_connected(self, frame):
        """Called when STOMP connection is established."""
        logger.info("✓ STOMP connection established")
        logger.info(f"   Server: {frame.headers.get('server', 'unknown')}")
        logger.info(f"   Session: {frame.headers.get('session', 'unknown')}")
        self.connected = True
    
    def on_disconnected(self):
        """Called when disconnected from broker."""
        logger.info("Disconnected from ActiveMQ")
        self.connected = False
    
    def on_error(self, frame):
        """Called when an error frame is received."""
        logger.error(f"STOMP Error: {frame.body}")
        self.connection_error = frame.body
    
    def on_message(self, frame):
        """Called when a message is received."""
        try:
            logger.info("=" * 60)
            logger.info("📨 RAW MESSAGE RECEIVED")
            logger.info(f"   Destination: {frame.headers.get('destination', 'unknown')}")
            logger.info(f"   Message-ID: {frame.headers.get('message-id', 'unknown')}")
            logger.info(f"   Content-Length: {len(frame.body)} bytes")
            
            # Log raw body for debugging
            if len(frame.body) > 300:
                logger.info(f"   Body (first 300 chars): {frame.body[:300]}...")
            else:
                logger.info(f"   Body: {frame.body}")
            
            # Parse response JSON
            response = json.loads(frame.body) if frame.body else {}
            
            # Kuber response structure is nested:
            # {
            #   "request_receive_timestamp": "...",
            #   "response_time": "...",
            #   "processing_time_ms": 123,
            #   "request": { original request with message_id, operation, etc. },
            #   "response": { "success": true/false, "result": ..., "error": "..." }
            # }
            request_obj = response.get('request', {})
            response_obj = response.get('response', {})
            
            msg_id = request_obj.get('message_id', 'unknown')
            success = response_obj.get('success', False)
            operation = request_obj.get('operation', 'unknown')
            processing_time = response.get('processing_time_ms', 0)
            
            logger.info("📥 PARSED RESPONSE")
            logger.info(f"   Message ID: {msg_id}")
            logger.info(f"   Success: {success}")
            logger.info(f"   Operation: {operation}")
            logger.info(f"   Processing Time: {processing_time}ms")
            
            if success:
                result = response_obj.get('result')
                if result is not None:
                    result_str = str(result)
                    if len(result_str) > 100:
                        logger.info(f"   Result: {result_str[:100]}...")
                    else:
                        logger.info(f"   Result: {result_str}")
                key = request_obj.get('key')
                if key:
                    logger.info(f"   Key: {key}")
            else:
                error_code = response_obj.get('error_code', 'UNKNOWN')
                error_msg = response_obj.get('error', 'Unknown error')
                logger.info(f"   Error Code: {error_code}")
                logger.info(f"   Error: {error_msg}")
            
            logger.info("=" * 60)
            
            # Track response
            flat_response = {
                'message_id': msg_id,
                'success': success,
                'operation': operation,
                'processing_time_ms': processing_time,
                'result': response_obj.get('result'),
                'error': response_obj.get('error'),
                'error_code': response_obj.get('error_code'),
                'raw': response
            }
            received_responses.append(flat_response)
            if msg_id in pending_requests:
                del pending_requests[msg_id]
                
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse response JSON: {e}")
            logger.warning(f"Raw body: {frame.body}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            import traceback
            traceback.print_exc()
    
    def on_heartbeat_timeout(self):
        """Called when heartbeat times out."""
        logger.warning("Heartbeat timeout")
    
    def on_receipt(self, frame):
        """Called when a receipt is received."""
        logger.debug(f"Receipt: {frame.headers.get('receipt-id', 'unknown')}")


# ==============================================================================
# Request Functions
# ==============================================================================

def create_request(operation, region, key, value=None, ttl=None):
    """
    Create a properly formatted Kuber request message.
    
    Supported operations: GET, SET, DELETE, EXISTS, KEYS, MGET
    """
    msg_id = f"req-{uuid.uuid4().hex[:12]}"
    
    request = {
        'operation': operation.upper(),
        'region': region,
        'key': key,
        'api_key': API_KEY,
        'message_id': msg_id
    }
    
    if value is not None:
        request['value'] = value
        
    if ttl is not None:
        request['ttl'] = ttl
    
    return msg_id, request


def publish_request(conn, request):
    """Publish a request to the request queue."""
    msg_id = request['message_id']
    
    # Track pending request
    pending_requests[msg_id] = {
        'request': request,
        'sent_at': datetime.now()
    }
    
    # Serialize and send
    request_json = json.dumps(request)
    
    logger.info(f"📤 SENDING REQUEST")
    logger.info(f"   Message ID: {msg_id}")
    logger.info(f"   Operation: {request['operation']}")
    logger.info(f"   Key: {request.get('key', 'N/A')}")
    logger.info(f"   Destination: {REQUEST_QUEUE}")
    
    conn.send(
        destination=REQUEST_QUEUE,
        body=request_json,
        content_type='application/json',
        headers={'message-id': msg_id}
    )
    
    logger.info(f"   ✓ Sent")


# ==============================================================================
# Test Suite
# ==============================================================================

def run_test_suite(conn):
    """Run a series of cache operation tests."""
    
    logger.info("\n" + "=" * 60)
    logger.info("STARTING TEST SUITE")
    logger.info("=" * 60)
    
    # Test 1: SET operation
    logger.info("\n--- Test 1: SET operation ---")
    msg_id, request = create_request(
        operation='SET',
        region=TEST_REGION,
        key='activemq-test:user:1001',
        value={'name': 'John Doe', 'email': 'john@example.com', 'broker': 'activemq'},
        ttl=300
    )
    publish_request(conn, request)
    time.sleep(1)
    
    # Test 2: GET operation
    logger.info("\n--- Test 2: GET operation ---")
    msg_id, request = create_request(
        operation='GET',
        region=TEST_REGION,
        key='activemq-test:user:1001'
    )
    publish_request(conn, request)
    time.sleep(1)
    
    # Test 3: EXISTS operation
    logger.info("\n--- Test 3: EXISTS operation ---")
    msg_id, request = create_request(
        operation='EXISTS',
        region=TEST_REGION,
        key='activemq-test:user:1001'
    )
    publish_request(conn, request)
    time.sleep(1)
    
    # Test 4: Another SET
    logger.info("\n--- Test 4: SET another key ---")
    msg_id, request = create_request(
        operation='SET',
        region=TEST_REGION,
        key='activemq-test:user:1002',
        value={'name': 'Jane Smith', 'role': 'admin', 'broker': 'activemq'}
    )
    publish_request(conn, request)
    time.sleep(1)
    
    # Test 5: KEYS operation
    logger.info("\n--- Test 5: KEYS operation ---")
    msg_id, request = create_request(
        operation='KEYS',
        region=TEST_REGION,
        key='activemq-test:*'
    )
    publish_request(conn, request)
    time.sleep(1)
    
    # Test 6: DELETE operation
    logger.info("\n--- Test 6: DELETE operation ---")
    msg_id, request = create_request(
        operation='DELETE',
        region=TEST_REGION,
        key='activemq-test:user:1002'
    )
    publish_request(conn, request)
    time.sleep(1)
    
    # Test 7: GET non-existent key
    logger.info("\n--- Test 7: GET non-existent key ---")
    msg_id, request = create_request(
        operation='GET',
        region=TEST_REGION,
        key='activemq-test:nonexistent:key'
    )
    publish_request(conn, request)
    time.sleep(1)
    
    logger.info("\n✓ All requests sent")


def print_summary():
    """Print test summary."""
    logger.info("\n" + "=" * 60)
    logger.info("TEST SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total responses received: {len(received_responses)}")
    logger.info(f"Pending requests (no response): {len(pending_requests)}")
    
    if pending_requests:
        logger.warning("Requests without responses:")
        for msg_id, info in pending_requests.items():
            logger.warning(f"  - {msg_id}: {info['request']['operation']} {info['request'].get('key', 'N/A')}")
    
    success_count = sum(1 for r in received_responses if r.get('success', False))
    error_count = len(received_responses) - success_count
    
    logger.info(f"Successful operations: {success_count}")
    logger.info(f"Failed operations: {error_count}")
    logger.info("=" * 60)


# ==============================================================================
# Main Execution
# ==============================================================================

def main():
    print("""
╔════════════════════════════════════════════════════════════════════╗
║  Kuber ActiveMQ Request/Response Test Client (v1.7.5)              ║
║  Tests cache operations via ActiveMQ STOMP protocol                ║
╚════════════════════════════════════════════════════════════════════╝
    """)
    
    logger.info(f"ActiveMQ Host: {ACTIVEMQ_HOST}")
    logger.info(f"STOMP Port: {ACTIVEMQ_STOMP_PORT}")
    logger.info(f"Request Queue: {REQUEST_QUEUE}")
    logger.info(f"Response Queue: {RESPONSE_QUEUE}")
    
    # Create connection listener
    listener = KuberResponseListener()
    
    # Create STOMP connection
    conn = stomp.Connection(
        [(ACTIVEMQ_HOST, ACTIVEMQ_STOMP_PORT)],
        heartbeats=(10000, 10000)  # 10 second heartbeats
    )
    conn.set_listener('kuber', listener)
    
    try:
        # Connect to ActiveMQ
        logger.info(f"\nConnecting to ActiveMQ STOMP at {ACTIVEMQ_HOST}:{ACTIVEMQ_STOMP_PORT}...")
        conn.connect(wait=True)
        
        # Wait for connection
        time.sleep(1)
        
        if not listener.connected:
            if listener.connection_error:
                logger.error(f"Connection failed: {listener.connection_error}")
            else:
                logger.error("Connection failed: No response from server")
            logger.error("")
            logger.error("Troubleshooting:")
            logger.error("  1. Verify ActiveMQ is running")
            logger.error("  2. Check STOMP connector is enabled on port 61613")
            logger.error("  3. Check firewall settings")
            logger.error("")
            logger.error("To enable STOMP in ActiveMQ, ensure this is in activemq.xml:")
            logger.error('  <transportConnector name="stomp" uri="stomp://0.0.0.0:61613"/>')
            return 1
        
        logger.info("✓ Connected to ActiveMQ")
        
        # Subscribe to response queue
        logger.info(f"Subscribing to response queue: {RESPONSE_QUEUE}")
        conn.subscribe(
            destination=RESPONSE_QUEUE,
            id='kuber-response-sub',
            ack='auto'
        )
        logger.info("✓ Subscribed to response queue")
        
        # Give subscription time to be established
        time.sleep(1)
        
        # Run test suite
        run_test_suite(conn)
        
        # Wait for responses
        logger.info("\nWaiting for responses (15 seconds)...")
        time.sleep(15)
        
    except stomp.exception.ConnectFailedException as e:
        logger.error(f"Connection failed: {e}")
        logger.error("")
        logger.error("Common causes:")
        logger.error(f"  1. ActiveMQ not running on {ACTIVEMQ_HOST}:{ACTIVEMQ_STOMP_PORT}")
        logger.error("  2. STOMP connector not enabled (port 61613)")
        logger.error("  3. Wrong port - port 61616 is OpenWire, not STOMP!")
        return 1
        
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        # Disconnect
        if conn.is_connected():
            logger.info("Disconnecting...")
            conn.disconnect()
        
        # Print summary
        print_summary()
    
    logger.info("Test complete.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
