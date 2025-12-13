#!/usr/bin/env python3
"""
Kuber Kafka Diagnostics Tool

Run this script to verify:
1. Kafka connectivity
2. Available topics
3. Messages in response topic
4. Consumer group offsets

Usage:
    python kafka_diagnostics.py [--from-beginning] [--count N]

Copyright © 2025-2030, All Rights Reserved
"""

import sys
import time
import json
import argparse

try:
    from kafka import KafkaConsumer, KafkaAdminClient, TopicPartition
    from kafka.errors import KafkaError
except ImportError:
    print("ERROR: kafka-python not installed. Run: pip install kafka-python")
    sys.exit(1)

# Configuration
BOOTSTRAP_SERVERS = ['localhost:9092']
REQUEST_TOPIC = 'ccs_cache_request'
RESPONSE_TOPIC = 'ccs_cache_response'


def list_topics():
    """List all Kafka topics."""
    print("\n" + "=" * 60)
    print("LISTING KAFKA TOPICS")
    print("=" * 60)
    
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        topics = admin.list_topics()
        print(f"\n✓ Connected to Kafka at {BOOTSTRAP_SERVERS}")
        print(f"\nAvailable topics ({len(topics)}):")
        for topic in sorted(topics):
            print(f"  - {topic}")
        
        if REQUEST_TOPIC in topics:
            print(f"\n✓ Request topic '{REQUEST_TOPIC}' EXISTS")
        else:
            print(f"\n✗ Request topic '{REQUEST_TOPIC}' NOT FOUND")
            
        if RESPONSE_TOPIC in topics:
            print(f"✓ Response topic '{RESPONSE_TOPIC}' EXISTS")
        else:
            print(f"✗ Response topic '{RESPONSE_TOPIC}' NOT FOUND")
            
        admin.close()
        return topics
        
    except Exception as e:
        print(f"\n✗ Failed to connect to Kafka: {e}")
        return []


def check_topic_offsets(topic):
    """Check the offsets for a topic."""
    print("\n" + "=" * 60)
    print(f"CHECKING OFFSETS FOR TOPIC: {topic}")
    print("=" * 60)
    
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=None,  # No group - just checking offsets
        )
        
        # Get partitions for topic
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            print(f"\n✗ Topic '{topic}' has no partitions (might not exist)")
            consumer.close()
            return
            
        print(f"\n✓ Topic '{topic}' has {len(partitions)} partition(s)")
        
        for partition in sorted(partitions):
            tp = TopicPartition(topic, partition)
            
            # Get beginning and end offsets
            beginning = consumer.beginning_offsets([tp])[tp]
            end = consumer.end_offsets([tp])[tp]
            
            message_count = end - beginning
            print(f"\n  Partition {partition}:")
            print(f"    Beginning offset: {beginning}")
            print(f"    End offset: {end}")
            print(f"    Message count: {message_count}")
            
        consumer.close()
        
    except Exception as e:
        print(f"\n✗ Error checking offsets: {e}")


def read_messages(topic, from_beginning=False, max_messages=10, timeout=10, live_mode=False):
    """Read messages from a topic."""
    print("\n" + "=" * 60)
    print(f"READING MESSAGES FROM TOPIC: {topic}")
    print(f"  from_beginning={from_beginning}, max_messages={max_messages}, timeout={timeout}s")
    if live_mode:
        print("  LIVE MODE: Will continue watching for new messages (Ctrl+C to stop)")
    print("=" * 60)
    
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=f'diagnostics-{int(time.time())}',  # Unique group
            auto_offset_reset='earliest' if from_beginning else 'latest',
            enable_auto_commit=False,
            value_deserializer=lambda x: x.decode('utf-8'),
            consumer_timeout_ms=None if live_mode else (timeout * 1000),
        )
        
        print(f"\n✓ Consumer connected, waiting for messages...")
        
        # If reading from beginning, seek to start
        if from_beginning:
            consumer.poll(timeout_ms=1000)  # Trigger assignment
            partitions = consumer.assignment()
            if partitions:
                print(f"  Assigned partitions: {partitions}")
                for tp in partitions:
                    consumer.seek_to_beginning(tp)
                    print(f"  Seeked partition {tp.partition} to beginning")
        
        messages_read = 0
        start_time = time.time()
        
        if live_mode:
            print(f"\n--- LIVE MODE: Watching for messages (Ctrl+C to stop) ---\n")
        else:
            print(f"\n--- Messages (waiting up to {timeout}s) ---\n")
        
        try:
            while True:
                # Poll with timeout
                records = consumer.poll(timeout_ms=1000, max_records=10)
                
                if not records:
                    if not live_mode:
                        elapsed = time.time() - start_time
                        if elapsed >= timeout:
                            break
                    continue
                
                for tp, messages in records.items():
                    for message in messages:
                        messages_read += 1
                        
                        print(f"{'='*60}")
                        print(f"📨 Message #{messages_read} @ {time.strftime('%H:%M:%S')}")
                        print(f"   Partition: {message.partition}")
                        print(f"   Offset: {message.offset}")
                        print(f"   Key: {message.key}")
                        print(f"   Timestamp: {message.timestamp}")
                        
                        # Try to pretty-print JSON
                        try:
                            parsed = json.loads(message.value)
                            print(f"   Value (JSON):")
                            formatted = json.dumps(parsed, indent=4)
                            if len(formatted) > 500:
                                print(formatted[:500])
                                print("    ... (truncated)")
                            else:
                                print(formatted)
                        except:
                            value_preview = message.value[:200] if len(message.value) > 200 else message.value
                            print(f"   Value: {value_preview}")
                        
                        print()
                        
                        if not live_mode and messages_read >= max_messages:
                            print(f"(Reached max messages limit: {max_messages})")
                            raise StopIteration()
                            
        except (StopIteration, KeyboardInterrupt):
            pass
        
        elapsed = time.time() - start_time
        print(f"\n--- Read {messages_read} messages in {elapsed:.1f}s ---")
        
        consumer.close()
        
    except Exception as e:
        print(f"\n✗ Error reading messages: {e}")
        import traceback
        traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(description='Kuber Kafka Diagnostics Tool')
    parser.add_argument('--from-beginning', action='store_true', 
                        help='Read messages from beginning of topic')
    parser.add_argument('--count', type=int, default=10,
                        help='Maximum number of messages to read (default: 10)')
    parser.add_argument('--timeout', type=int, default=10,
                        help='Timeout in seconds (default: 10)')
    parser.add_argument('--topic', type=str, default=RESPONSE_TOPIC,
                        help=f'Topic to read from (default: {RESPONSE_TOPIC})')
    parser.add_argument('--live', action='store_true',
                        help='Live mode: continuously watch for new messages')
    parser.add_argument('--watch-only', action='store_true',
                        help='Skip diagnostics and just watch for messages')
    args = parser.parse_args()
    
    print("""
╔════════════════════════════════════════════════════════════════════╗
║  Kuber Kafka Diagnostics Tool                                       ║
║  Verifies Kafka connectivity and message flow                       ║
╚════════════════════════════════════════════════════════════════════╝
    """)
    
    print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")
    print(f"Request topic: {REQUEST_TOPIC}")
    print(f"Response topic: {RESPONSE_TOPIC}")
    
    if args.watch_only:
        # Just watch for messages
        read_messages(
            args.topic,
            from_beginning=args.from_beginning,
            max_messages=args.count,
            timeout=args.timeout,
            live_mode=args.live
        )
        return 0
    
    # Step 1: List topics
    topics = list_topics()
    
    if not topics:
        print("\n✗ Cannot continue - Kafka not accessible")
        return 1
    
    # Step 2: Check offsets for response topic
    if args.topic in topics or RESPONSE_TOPIC in topics:
        check_topic_offsets(args.topic)
    else:
        print(f"\n⚠️ Topic '{args.topic}' not found - skipping offset check")
    
    # Step 3: Read messages
    if args.topic in topics:
        read_messages(
            args.topic,
            from_beginning=args.from_beginning,
            max_messages=args.count,
            timeout=args.timeout,
            live_mode=args.live
        )
    else:
        print(f"\n⚠️ Topic '{args.topic}' not found - skipping message read")
        print("  (The topic might be created when Kuber first publishes to it)")
    
    print("\n" + "=" * 60)
    print("DIAGNOSTICS COMPLETE")
    print("=" * 60)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
