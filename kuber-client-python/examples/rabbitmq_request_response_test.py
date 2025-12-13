#!/usr/bin/env python3
"""
Kuber RabbitMQ Request/Response Test Client (v1.7.5)

Tests the Kuber Request/Response messaging feature via RabbitMQ AMQP protocol.
Publishes cache operation requests and subscribes to responses.

Prerequisites:
- RabbitMQ running on localhost:5672
- Kuber server running with messaging enabled
- pika package: pip install pika

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

# Try to import pika
try:
    import pika
except ImportError:
    print("ERROR: pika not installed. Run: pip install pika")
    sys.exit(1)

# Set up logging with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Reduce pika logging noise
logging.getLogger('pika').setLevel(logging.WARNING)

# ==============================================================================
# Configuration
# ==============================================================================

RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'guest'
RABBITMQ_PASS = 'guest'
RABBITMQ_VHOST = '/'

# Queue names
REQUEST_QUEUE = 'ccs_cache_request'
RESPONSE_QUEUE = 'ccs_cache_response'

# Your Kuber API key (replace with actual key)
API_KEY = 'kub_27d6eb2319ad721472f8c1b740e5b97188efa45e8830cfe193606714887e798d'

# Region to test
TEST_REGION = 'default'

# Track pending requests for correlation
pending_requests = {}
received_responses = []
stop_consumer = threading.Event()
consumer_ready = threading.Event()


# ==============================================================================
# Response Consumer (Background Thread)
# ==============================================================================

def response_consumer():
    """Background thread that consumes responses from RabbitMQ."""
    global consumer_ready
    
    logger.info(f"Starting response consumer on queue: {RESPONSE_QUEUE}")
    
    try:
        # Create connection
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials,
            heartbeat=60
        )
        
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Declare queue (in case it doesn't exist)
        channel.queue_declare(queue=RESPONSE_QUEUE, durable=True)
        
        logger.info(f"✓ Connected to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
        logger.info(f"✓ Listening on queue: {RESPONSE_QUEUE}")
        
        # Signal ready
        consumer_ready.set()
        
        def callback(ch, method, properties, body):
            """Process received message."""
            try:
                logger.info("=" * 60)
                logger.info("📨 RAW MESSAGE RECEIVED")
                logger.info(f"   Delivery Tag: {method.delivery_tag}")
                logger.info(f"   Content-Length: {len(body)} bytes")
                
                # Decode and parse
                body_str = body.decode('utf-8')
                if len(body_str) > 300:
                    logger.info(f"   Body (first 300 chars): {body_str[:300]}...")
                else:
                    logger.info(f"   Body: {body_str}")
                
                response = json.loads(body_str)
                
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
                
                # Acknowledge message
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse response JSON: {e}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                import traceback
                traceback.print_exc()
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        # Set up consumer
        channel.basic_qos(prefetch_count=10)
        channel.basic_consume(queue=RESPONSE_QUEUE, on_message_callback=callback)
        
        # Consume until stopped
        while not stop_consumer.is_set():
            connection.process_data_events(time_limit=1)
        
        # Cleanup
        channel.close()
        connection.close()
        logger.info("Consumer closed")
        
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        logger.error("")
        logger.error("Troubleshooting:")
        logger.error(f"  1. Verify RabbitMQ is running on {RABBITMQ_HOST}:{RABBITMQ_PORT}")
        logger.error("  2. Check credentials (default: guest/guest)")
        logger.error("  3. Check virtual host permissions")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
        import traceback
        traceback.print_exc()


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


def publish_request(channel, request):
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
    logger.info(f"   Queue: {REQUEST_QUEUE}")
    
    channel.basic_publish(
        exchange='',
        routing_key=REQUEST_QUEUE,
        body=request_json,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Persistent
            content_type='application/json',
            message_id=msg_id
        )
    )
    
    logger.info(f"   ✓ Sent")


# ==============================================================================
# Test Suite
# ==============================================================================

def run_test_suite(channel):
    """Run a series of cache operation tests."""
    
    logger.info("\n" + "=" * 60)
    logger.info("STARTING TEST SUITE")
    logger.info("=" * 60)
    
    tests = [
        ('SET', TEST_REGION, 'rabbitmq-test:user:1001', 
         {'name': 'John Doe', 'email': 'john@example.com', 'broker': 'rabbitmq'}, 300),
        ('GET', TEST_REGION, 'rabbitmq-test:user:1001', None, None),
        ('EXISTS', TEST_REGION, 'rabbitmq-test:user:1001', None, None),
        ('SET', TEST_REGION, 'rabbitmq-test:user:1002',
         {'name': 'Jane Smith', 'role': 'admin', 'broker': 'rabbitmq'}, None),
        ('KEYS', TEST_REGION, 'rabbitmq-test:*', None, None),
        ('DELETE', TEST_REGION, 'rabbitmq-test:user:1002', None, None),
        ('GET', TEST_REGION, 'rabbitmq-test:nonexistent:key', None, None),
    ]
    
    for i, (op, region, key, value, ttl) in enumerate(tests, 1):
        logger.info(f"\n--- Test {i}: {op} operation ---")
        msg_id, request = create_request(op, region, key, value, ttl)
        publish_request(channel, request)
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
║  Kuber RabbitMQ Request/Response Test Client (v1.7.5)              ║
║  Tests cache operations via RabbitMQ AMQP protocol                 ║
╚════════════════════════════════════════════════════════════════════╝
    """)
    
    logger.info(f"RabbitMQ Host: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    logger.info(f"Virtual Host: {RABBITMQ_VHOST}")
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
        # Create producer connection
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials
        )
        
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # Declare request queue
        channel.queue_declare(queue=REQUEST_QUEUE, durable=True)
        
        logger.info("✓ Producer connected")
        
        # Run test suite
        run_test_suite(channel)
        
        # Wait for responses
        logger.info("\nWaiting for responses (15 seconds)...")
        time.sleep(15)
        
        # Cleanup
        channel.close()
        connection.close()
        
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Connection failed: {e}")
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
