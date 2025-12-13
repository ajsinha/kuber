#!/usr/bin/env python3
"""
Kuber Kafka Request/Response Test Client (v1.7.5)

Tests the Kuber Request/Response messaging feature via Kafka.
Publishes cache operation requests and subscribes to responses.

Prerequisites:
- Kafka running on localhost:9092
- Kuber server running with messaging enabled
- kafka-python package: pip install kafka-python

Copyright © 2025-2030, All Rights Reserved
Ashutosh Sinha | Email: ajsinha@gmail.com
"""

import threading
import time
import json
import uuid
import logging
from datetime import datetime

# Try to import kafka-python
try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError
except ImportError:
    print("ERROR: kafka-python not installed. Run: pip install kafka-python")
    exit(1)

# Set up logging with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ==============================================================================
# Configuration
# ==============================================================================

BOOTSTRAP_SERVERS = ['localhost:9092']
REQUEST_TOPIC = 'ccs_cache_request'
RESPONSE_TOPIC = 'ccs_cache_response'

# Use a unique group ID to avoid conflicts with Kuber's internal consumers
CONSUMER_GROUP_ID = f'kuber-test-client-{uuid.uuid4().hex[:8]}'

# Your Kuber API key (replace with actual key)
API_KEY = 'kub_d427a1192ad2e45e8376e518f958b882cbdd98fdbded654ecb4e0efe70968d08'

# Region to test
TEST_REGION = 'default'

# Track pending requests for correlation
pending_requests = {}
received_responses = []
stop_consumer = threading.Event()

# Track when consumer is ready (has partition assignments)
consumer_ready = threading.Event()


# ==============================================================================
# Response Subscriber (Background Thread)
# ==============================================================================

def response_subscriber():
    """
    Background thread that listens for responses from Kuber.
    Runs until stop_consumer event is set.
    """
    global consumer_ready
    
    logger.info(f"Starting response subscriber on topic: {RESPONSE_TOPIC}")
    logger.info(f"Consumer group: {CONSUMER_GROUP_ID}")
    
    consumer = None
    
    try:
        # First, create an admin client to check topics
        from kafka.admin import KafkaAdminClient
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
            topics = admin.list_topics()
            logger.info(f"📋 Available Kafka topics: {topics}")
            admin.close()
        except Exception as e:
            logger.warning(f"Could not list topics: {e}")
        
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP_ID,
            auto_offset_reset='earliest',  # Changed to 'earliest' to not miss messages
            enable_auto_commit=True,
            value_deserializer=lambda x: x.decode('utf-8'),
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
        )
        
        # Subscribe to response topic
        consumer.subscribe([RESPONSE_TOPIC])
        logger.info(f"✓ Consumer subscribed to: {RESPONSE_TOPIC}")
        
        # Force partition assignment by polling
        logger.info("Waiting for partition assignment...")
        max_wait = 30  # seconds
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            # Poll to trigger partition assignment
            consumer.poll(timeout_ms=1000)
            
            # Check if we have partitions assigned
            partitions = consumer.assignment()
            if partitions:
                logger.info(f"✓ Partitions assigned: {partitions}")
                
                # Log current position and end offset for each partition
                for tp in partitions:
                    position = consumer.position(tp)
                    end_offsets = consumer.end_offsets([tp])
                    end_offset = end_offsets.get(tp, 'unknown')
                    beginning_offsets = consumer.beginning_offsets([tp])
                    begin_offset = beginning_offsets.get(tp, 'unknown')
                    logger.info(f"   Partition {tp.partition}: position={position}, begin={begin_offset}, end={end_offset}")
                    
                    # Check if there are messages we might have missed
                    if end_offset > position:
                        logger.warning(f"   ⚠️ There are {end_offset - position} messages ahead of current position!")
                
                break
            else:
                logger.info("   Still waiting for partition assignment...")
        else:
            logger.error("✗ Timeout waiting for partition assignment!")
            return
        
        # Signal that consumer is ready
        consumer_ready.set()
        logger.info("✓ Consumer is READY to receive messages")
        
    except KafkaError as e:
        logger.error(f"✗ Failed to connect consumer: {e}")
        return
    except Exception as e:
        logger.error(f"✗ Consumer initialization error: {e}")
        import traceback
        traceback.print_exc()
        return

    poll_count = 0
    try:
        while not stop_consumer.is_set():
            # Poll for messages (non-blocking with timeout)
            try:
                poll_count += 1
                messages = consumer.poll(timeout_ms=1000, max_records=100)
                
                # Log every poll for debugging
                total_msgs = sum(len(v) for v in messages.values()) if messages else 0
                if poll_count % 5 == 0 or total_msgs > 0:  # Log every 5th poll or when messages received
                    logger.info(f"[Poll #{poll_count}] Messages received: {total_msgs}")
                
                if not messages:
                    continue
                    
                for topic_partition, records in messages.items():
                    logger.info(f"Processing {len(records)} records from {topic_partition}")
                    
                    for record in records:
                        try:
                            logger.info(f"📨 Raw message: partition={record.partition}, offset={record.offset}, key={record.key}")
                            logger.info(f"📨 Value (first 200 chars): {record.value[:200] if len(record.value) > 200 else record.value}")
                            
                            # Parse response JSON
                            response_str = record.value
                            response = json.loads(response_str) if response_str else {}
                            
                            # Kuber response structure is nested
                            request_obj = response.get('request', {})
                            response_obj = response.get('response', {})
                            
                            msg_id = request_obj.get('message_id', 'unknown')
                            success = response_obj.get('success', False)
                            operation = request_obj.get('operation', 'unknown')
                            processing_time = response.get('processing_time_ms', 0)
                            
                            logger.info("=" * 60)
                            logger.info(f"📥 RESPONSE RECEIVED")
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
                            logger.warning(f"Failed to parse response: {e}")
                            logger.warning(f"Raw value: {record.value}")
                            
            except Exception as e:
                if not stop_consumer.is_set():
                    logger.error(f"Error polling messages: {e}")
                    import traceback
                    traceback.print_exc()
                    
    except Exception as e:
        logger.error(f"Consumer loop error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if consumer:
            consumer.close()
        logger.info("Consumer closed")


# ==============================================================================
# Request Publisher
# ==============================================================================

def create_request(operation, region, key, value=None, ttl=None):
    """
    Create a properly formatted Kuber request message.
    
    Supported operations: GET, SET, DELETE, EXISTS, KEYS, MGET
    """
    msg_id = f"req-{uuid.uuid4().hex[:12]}"
    
    request = {
        "api_key": API_KEY,
        "message_id": msg_id,
        "operation": operation.upper(),
        "region": region,
        "key": key
    }
    
    if value is not None:
        request["value"] = value
    
    if ttl is not None:
        request["ttl"] = ttl
    
    return msg_id, request


def publish_request(producer, request):
    """
    Publish a request to Kafka and track it.
    """
    msg_id = request['message_id']
    
    try:
        # Send request
        future = producer.send(
            REQUEST_TOPIC,
            key=msg_id.encode('utf-8'),
            value=request  # Will be serialized by value_serializer
        )
        
        # Wait for send confirmation
        record_metadata = future.get(timeout=10)
        
        logger.info(f"📤 REQUEST SENT")
        logger.info(f"   Message ID: {msg_id}")
        logger.info(f"   Operation: {request['operation']}")
        logger.info(f"   Region: {request['region']}")
        logger.info(f"   Key: {request['key']}")
        logger.info(f"   Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
        
        # Track pending request
        pending_requests[msg_id] = {
            'request': request,
            'sent_at': datetime.now()
        }
        
        return True
        
    except KafkaError as e:
        logger.error(f"✗ Failed to send request: {e}")
        return False


def run_test_suite():
    """
    Run a suite of test operations against Kuber via Kafka.
    """
    logger.info("=" * 60)
    logger.info("Kuber Kafka Request/Response Test")
    logger.info("=" * 60)
    
    # Initialize producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k if isinstance(k, bytes) else k.encode('utf-8'),
            retries=3,
            acks='all'
        )
        logger.info(f"✓ Producer connected to {BOOTSTRAP_SERVERS}")
    except Exception as e:
        logger.error(f"✗ Failed to create producer: {e}")
        return
    
    try:
        # Test 1: SET operation
        logger.info("\n--- Test 1: SET Operation ---")
        msg_id, request = create_request(
            operation='SET',
            region=TEST_REGION,
            key='kafka-test:user:1001',
            value=json.dumps({"name": "John Doe", "email": "john@example.com", "timestamp": datetime.now().isoformat()}),
            ttl=3600
        )
        publish_request(producer, request)
        time.sleep(1)
        
        # Test 2: GET operation
        logger.info("\n--- Test 2: GET Operation ---")
        msg_id, request = create_request(
            operation='GET',
            region=TEST_REGION,
            key='kafka-test:user:1001'
        )
        publish_request(producer, request)
        time.sleep(1)
        
        # Test 3: EXISTS operation
        logger.info("\n--- Test 3: EXISTS Operation ---")
        msg_id, request = create_request(
            operation='EXISTS',
            region=TEST_REGION,
            key='kafka-test:user:1001'
        )
        publish_request(producer, request)
        time.sleep(1)
        
        # Test 4: SET another key
        logger.info("\n--- Test 4: SET Another Key ---")
        msg_id, request = create_request(
            operation='SET',
            region=TEST_REGION,
            key='kafka-test:user:1002',
            value=json.dumps({"name": "Jane Smith", "role": "admin"})
        )
        publish_request(producer, request)
        time.sleep(1)
        
        # Test 5: KEYS operation (pattern match)
        logger.info("\n--- Test 5: KEYS Operation ---")
        msg_id, request = create_request(
            operation='KEYS',
            region=TEST_REGION,
            key='kafka-test:*'
        )
        publish_request(producer, request)
        time.sleep(1)
        
        # Test 6: DELETE operation
        logger.info("\n--- Test 6: DELETE Operation ---")
        msg_id, request = create_request(
            operation='DELETE',
            region=TEST_REGION,
            key='kafka-test:user:1002'
        )
        publish_request(producer, request)
        time.sleep(1)
        
        # Test 7: GET non-existent key (should return null/error)
        logger.info("\n--- Test 7: GET Non-existent Key ---")
        msg_id, request = create_request(
            operation='GET',
            region=TEST_REGION,
            key='kafka-test:nonexistent:key'
        )
        publish_request(producer, request)
        time.sleep(1)
        
        # Test with your actual keys
        logger.info("\n--- Test 8: GET Your Existing Keys ---")
        test_keys = [
            'c_0916687/person_4581',
            'c_0109710/person_553',
        ]
        
        for key in test_keys:
            msg_id, request = create_request(
                operation='GET',
                region='commits',  # Your region
                key=key
            )
            publish_request(producer, request)
            time.sleep(0.5)
        
        # Flush all messages
        producer.flush()
        logger.info("\n✓ All requests sent and flushed")
        
    finally:
        producer.close()
        logger.info("Producer closed")


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
            logger.warning(f"  - {msg_id}: {info['request']['operation']} {info['request']['key']}")
    
    success_count = sum(1 for r in received_responses if r.get('success', False))
    error_count = len(received_responses) - success_count
    
    logger.info(f"Successful operations: {success_count}")
    logger.info(f"Failed operations: {error_count}")
    logger.info("=" * 60)


# ==============================================================================
# Main Execution
# ==============================================================================

if __name__ == '__main__':
    threading.current_thread().name = 'MainThread'
    
    print("""
╔════════════════════════════════════════════════════════════════════╗
║  Kuber Kafka Request/Response Test Client                          ║
║  Tests cache operations via Kafka messaging                        ║
╚════════════════════════════════════════════════════════════════════╝
    """)
    
    # Start the response subscriber thread
    subscriber_thread = threading.Thread(
        target=response_subscriber,
        name='SubscriberThread',
        daemon=True
    )
    subscriber_thread.start()
    
    # Give subscriber time to connect
    logger.info("Waiting for subscriber to be ready (partition assignment)...")
    
    # Wait for consumer to be ready (with timeout)
    if not consumer_ready.wait(timeout=30):
        logger.error("✗ Consumer failed to become ready within 30 seconds!")
        logger.error("Check that Kafka broker is running and topic exists")
        exit(1)
    
    logger.info("✓ Consumer is ready, starting test suite...")
    
    try:
        # Run the test suite
        run_test_suite()
        
        # Wait for responses
        logger.info("\nWaiting for responses (10 seconds)...")
        time.sleep(10)
        
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
    finally:
        # Signal subscriber to stop
        stop_consumer.set()
        time.sleep(1)
        
        # Print summary
        print_summary()
        
    logger.info("Test complete.")
