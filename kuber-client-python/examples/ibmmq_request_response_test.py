#!/usr/bin/env python3
"""
Kuber IBM MQ Request/Response Test Client (v1.7.5)

Tests the Kuber Request/Response messaging feature via IBM MQ.
Publishes cache operation requests and subscribes to responses.

Prerequisites:
- IBM MQ running with queue manager accessible
- Kuber server running with messaging enabled
- pymqi package: pip install pymqi
- IBM MQ client libraries installed

Note: IBM MQ requires client libraries which can be downloaded from IBM.
See: https://www.ibm.com/docs/en/ibm-mq/latest?topic=overview-downloading-mq-redistributable-client

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

# Try to import pymqi
try:
    import pymqi
    PYMQI_AVAILABLE = True
except ImportError:
    PYMQI_AVAILABLE = False
    print("WARNING: pymqi not installed. Run: pip install pymqi")
    print("         Also requires IBM MQ client libraries.")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==============================================================================
# Configuration
# ==============================================================================

# IBM MQ Connection settings
QUEUE_MANAGER = 'QM1'
CHANNEL = 'DEV.APP.SVRCONN'
HOST = 'localhost'
PORT = '1414'
CONN_INFO = f'{HOST}({PORT})'

# Queue names
REQUEST_QUEUE = 'ccs_cache_request'
RESPONSE_QUEUE = 'ccs_cache_response'

# Your Kuber API key
API_KEY = 'kub_27d6eb2319ad721472f8c1b740e5b97188efa45e8830cfe193606714887e798d'

# Region to test
TEST_REGION = 'default'

# State
pending_requests = {}
received_responses = []
stop_consumer = threading.Event()
consumer_ready = threading.Event()


# ==============================================================================
# Response Consumer (Background Thread)
# ==============================================================================

def response_consumer():
    """Background thread that consumes responses from IBM MQ."""
    global consumer_ready
    
    if not PYMQI_AVAILABLE:
        logger.error("pymqi not available - cannot start consumer")
        return
    
    logger.info(f"Starting response consumer on queue: {RESPONSE_QUEUE}")
    
    try:
        # Connect to queue manager
        qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, CONN_INFO)
        logger.info(f"✓ Connected to Queue Manager: {QUEUE_MANAGER}")
        
        # Open response queue for input
        response_queue = pymqi.Queue(qmgr, RESPONSE_QUEUE)
        logger.info(f"✓ Opened queue: {RESPONSE_QUEUE}")
        
        # Signal ready
        consumer_ready.set()
        
        # Message descriptor and get options
        md = pymqi.MD()
        gmo = pymqi.GMO()
        gmo.Options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
        gmo.WaitInterval = 1000  # 1 second wait
        
        while not stop_consumer.is_set():
            try:
                # Reset message descriptor
                md.MsgId = pymqi.CMQC.MQMI_NONE
                md.CorrelId = pymqi.CMQC.MQCI_NONE
                
                # Get message
                message = response_queue.get(None, md, gmo)
                
                if message:
                    process_message(message.decode('utf-8'))
                    
            except pymqi.MQMIError as e:
                if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                    # No message available - normal timeout
                    continue
                else:
                    logger.error(f"MQ Error: {e}")
                    
        # Cleanup
        response_queue.close()
        qmgr.disconnect()
        logger.info("Consumer closed")
        
    except Exception as e:
        logger.error(f"Consumer error: {e}")
        import traceback
        traceback.print_exc()


def process_message(body):
    """Process a received message."""
    try:
        logger.info("=" * 60)
        logger.info("📨 RAW MESSAGE RECEIVED")
        logger.info(f"   Content-Length: {len(body)} bytes")
        
        if len(body) > 300:
            logger.info(f"   Body (first 300 chars): {body[:300]}...")
        else:
            logger.info(f"   Body: {body}")
        
        response = json.loads(body)
        
        # Parse nested response structure
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
    except Exception as e:
        logger.error(f"Error processing message: {e}")


# ==============================================================================
# Request Functions
# ==============================================================================

def create_request(operation, region, key, value=None, ttl=None):
    """Create a properly formatted Kuber request message."""
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


def publish_request(qmgr, request):
    """Publish a request to the request queue."""
    msg_id = request['message_id']
    
    # Track pending request
    pending_requests[msg_id] = {
        'request': request,
        'sent_at': datetime.now()
    }
    
    request_json = json.dumps(request)
    
    logger.info(f"📤 SENDING REQUEST")
    logger.info(f"   Message ID: {msg_id}")
    logger.info(f"   Operation: {request['operation']}")
    logger.info(f"   Key: {request.get('key', 'N/A')}")
    logger.info(f"   Queue: {REQUEST_QUEUE}")
    
    # Open queue and send
    request_queue = pymqi.Queue(qmgr, REQUEST_QUEUE)
    
    md = pymqi.MD()
    md.Format = pymqi.CMQC.MQFMT_STRING
    
    request_queue.put(request_json.encode('utf-8'), md)
    request_queue.close()
    
    logger.info(f"   ✓ Sent")


# ==============================================================================
# Test Suite
# ==============================================================================

def run_test_suite(qmgr):
    """Run a series of cache operation tests."""
    
    logger.info("\n" + "=" * 60)
    logger.info("STARTING TEST SUITE")
    logger.info("=" * 60)
    
    tests = [
        ('SET', TEST_REGION, 'ibmmq-test:user:1001', 
         {'name': 'John Doe', 'email': 'john@example.com', 'broker': 'ibmmq'}, 300),
        ('GET', TEST_REGION, 'ibmmq-test:user:1001', None, None),
        ('EXISTS', TEST_REGION, 'ibmmq-test:user:1001', None, None),
        ('SET', TEST_REGION, 'ibmmq-test:user:1002',
         {'name': 'Jane Smith', 'role': 'admin', 'broker': 'ibmmq'}, None),
        ('KEYS', TEST_REGION, 'ibmmq-test:*', None, None),
        ('DELETE', TEST_REGION, 'ibmmq-test:user:1002', None, None),
        ('GET', TEST_REGION, 'ibmmq-test:nonexistent:key', None, None),
    ]
    
    for i, (op, region, key, value, ttl) in enumerate(tests, 1):
        logger.info(f"\n--- Test {i}: {op} operation ---")
        msg_id, request = create_request(op, region, key, value, ttl)
        publish_request(qmgr, request)
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
║  Kuber IBM MQ Request/Response Test Client (v1.7.5)                ║
║  Tests cache operations via IBM MQ messaging                       ║
╚════════════════════════════════════════════════════════════════════╝
    """)
    
    if not PYMQI_AVAILABLE:
        logger.error("pymqi is required but not installed.")
        logger.error("Install with: pip install pymqi")
        logger.error("Also requires IBM MQ client libraries.")
        return 1
    
    logger.info(f"Queue Manager: {QUEUE_MANAGER}")
    logger.info(f"Channel: {CHANNEL}")
    logger.info(f"Connection: {CONN_INFO}")
    logger.info(f"Request Queue: {REQUEST_QUEUE}")
    logger.info(f"Response Queue: {RESPONSE_QUEUE}")
    
    # Start consumer thread
    consumer_thread = threading.Thread(
        target=response_consumer,
        name='ConsumerThread',
        daemon=True
    )
    consumer_thread.start()
    
    # Wait for consumer to be ready
    logger.info("\nWaiting for consumer to connect...")
    if not consumer_ready.wait(timeout=10):
        logger.error("Consumer failed to start within 10 seconds")
        return 1
    
    try:
        # Connect for publishing
        qmgr = pymqi.connect(QUEUE_MANAGER, CHANNEL, CONN_INFO)
        logger.info("✓ Producer connected")
        
        # Run test suite
        run_test_suite(qmgr)
        
        # Wait for responses
        logger.info("\nWaiting for responses (15 seconds)...")
        time.sleep(15)
        
        # Disconnect
        qmgr.disconnect()
        
    except pymqi.MQMIError as e:
        logger.error(f"IBM MQ Error: {e}")
        logger.error("")
        logger.error("Troubleshooting:")
        logger.error(f"  1. Verify Queue Manager '{QUEUE_MANAGER}' is running")
        logger.error(f"  2. Verify channel '{CHANNEL}' exists")
        logger.error(f"  3. Check connection at {CONN_INFO}")
        return 1
        
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        stop_consumer.set()
        time.sleep(1)
        print_summary()
    
    logger.info("Test complete.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
