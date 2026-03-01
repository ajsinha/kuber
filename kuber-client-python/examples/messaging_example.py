#!/usr/bin/env python3
"""
Copyright © 2025-2030, All Rights Reserved
Ashutosh Sinha | Email: ajsinha@gmail.com

Kuber Request/Response Messaging Example

This example demonstrates how to interact with Kuber's request/response
messaging system using a message broker. It shows the JSON message format
for various cache operations.

Prerequisites:
    - Kuber server running with request/response messaging enabled
    - Message broker (Kafka, ActiveMQ, RabbitMQ, or IBM MQ) configured
    - Valid API key from Kuber admin panel

Message Format:
    Request:
        {
            "api_key": "your-api-key",
            "message_id": "unique-correlation-id",
            "operation": "GET|SET|DELETE|MGET|MSET|etc.",
            "region": "default",
            "key": "key-name",
            "value": "value-for-set",
            ...
        }
    
    Response:
        {
            "request_receive_timestamp": "ISO timestamp",
            "response_time": "ISO timestamp",
            "processing_time_ms": 123,
            "request": { original request },
            "response": {
                "success": true/false,
                "result": operation result,
                "error": "error message if failed"
            }
        }
"""

import json
import uuid
import time
from datetime import datetime
from typing import Optional, Dict, Any, List

# For Kafka
try:
    from kafka import KafkaProducer, KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# For ActiveMQ/STOMP
try:
    import stomp
    STOMP_AVAILABLE = True
except ImportError:
    STOMP_AVAILABLE = False

# For RabbitMQ
try:
    import pika
    PIKA_AVAILABLE = True
except ImportError:
    PIKA_AVAILABLE = False


class KuberMessagingClient:
    """
    Client for Kuber request/response messaging.
    
    This client demonstrates the message format and provides helper methods
    for building cache operation requests.
    """
    
    def __init__(self, api_key: str, region: str = "default"):
        """
        Initialize the messaging client.
        
        Args:
            api_key: API key for authentication with Kuber
            region: Default region for cache operations
        """
        self.api_key = api_key
        self.region = region
        self.pending_requests: Dict[str, dict] = {}
    
    def _generate_message_id(self) -> str:
        """Generate a unique message ID for request correlation."""
        return str(uuid.uuid4())
    
    def build_request(self, operation: str, **kwargs) -> dict:
        """
        Build a cache request message.
        
        Args:
            operation: Cache operation (GET, SET, DELETE, etc.)
            **kwargs: Operation-specific parameters
            
        Returns:
            dict: Request message ready to be serialized to JSON
        """
        request = {
            "api_key": self.api_key,
            "message_id": self._generate_message_id(),
            "operation": operation.upper(),
            "region": kwargs.pop("region", self.region)
        }
        request.update(kwargs)
        return request
    
    # ==========================================================================
    # String Operations
    # ==========================================================================
    
    def build_get(self, key: str, region: str = None) -> dict:
        """Build a GET request."""
        return self.build_request("GET", key=key, region=region or self.region)
    
    def build_set(self, key: str, value: Any, ttl: int = None, region: str = None) -> dict:
        """Build a SET request with optional TTL."""
        request = self.build_request("SET", key=key, value=value, region=region or self.region)
        if ttl:
            request["ttl"] = ttl
        return request
    
    def build_delete(self, key: str, region: str = None) -> dict:
        """Build a DELETE request for a single key."""
        return self.build_request("DELETE", key=key, region=region or self.region)
    
    def build_delete_multi(self, keys: List[str], region: str = None) -> dict:
        """Build a DELETE request for multiple keys."""
        return self.build_request("DELETE", keys=keys, region=region or self.region)
    
    def build_exists(self, key: str, region: str = None) -> dict:
        """Build an EXISTS request."""
        return self.build_request("EXISTS", key=key, region=region or self.region)
    
    # ==========================================================================
    # Batch Operations
    # ==========================================================================
    
    def build_mget(self, keys: List[str], region: str = None) -> dict:
        """Build a multi-GET request."""
        return self.build_request("MGET", keys=keys, region=region or self.region)
    
    def build_mset(self, entries: Dict[str, Any], ttl: int = None, region: str = None) -> dict:
        """Build a multi-SET request."""
        request = self.build_request("MSET", entries=entries, region=region or self.region)
        if ttl:
            request["ttl"] = ttl
        return request
    
    # ==========================================================================
    # Key Operations
    # ==========================================================================
    
    def build_keys(self, pattern: str = "*", region: str = None) -> dict:
        """Build a KEYS request to list keys matching pattern."""
        return self.build_request("KEYS", pattern=pattern, region=region or self.region)
    
    def build_ttl(self, key: str, region: str = None) -> dict:
        """Build a TTL request to get remaining time-to-live."""
        return self.build_request("TTL", key=key, region=region or self.region)
    
    def build_expire(self, key: str, ttl: int, region: str = None) -> dict:
        """Build an EXPIRE request to set expiration."""
        return self.build_request("EXPIRE", key=key, ttl=ttl, region=region or self.region)
    
    # ==========================================================================
    # Hash Operations
    # ==========================================================================
    
    def build_hget(self, key: str, field: str, region: str = None) -> dict:
        """Build a HGET request."""
        return self.build_request("HGET", key=key, field=field, region=region or self.region)
    
    def build_hset(self, key: str, field: str, value: Any, region: str = None) -> dict:
        """Build a HSET request."""
        return self.build_request("HSET", key=key, field=field, value=value, region=region or self.region)
    
    def build_hgetall(self, key: str, region: str = None) -> dict:
        """Build a HGETALL request."""
        return self.build_request("HGETALL", key=key, region=region or self.region)
    
    def build_hmset(self, key: str, fields: Dict[str, str], region: str = None) -> dict:
        """Build a HMSET request."""
        return self.build_request("HMSET", key=key, fields=fields, region=region or self.region)
    
    # ==========================================================================
    # JSON Operations
    # ==========================================================================
    
    def build_jset(self, key: str, value: Any, region: str = None) -> dict:
        """Build a JSON SET request."""
        return self.build_request("JSET", key=key, value=value, region=region or self.region)
    
    def build_jget(self, key: str, path: str = None, region: str = None) -> dict:
        """Build a JSON GET request with optional JSONPath."""
        request = self.build_request("JGET", key=key, region=region or self.region)
        if path:
            request["path"] = path
        return request
    
    def build_jsearch(self, query: str, region: str = None) -> dict:
        """Build a JSON SEARCH request."""
        return self.build_request("JSEARCH", query=query, region=region or self.region)
    
    # ==========================================================================
    # Admin Operations
    # ==========================================================================
    
    def build_ping(self) -> dict:
        """Build a PING request."""
        return self.build_request("PING")
    
    def build_info(self) -> dict:
        """Build an INFO request."""
        return self.build_request("INFO")
    
    def build_regions(self) -> dict:
        """Build a REGIONS request to list all regions."""
        return self.build_request("REGIONS")
    
    def parse_response(self, response_json: str) -> dict:
        """
        Parse a response message from Kuber.
        
        Args:
            response_json: JSON string from response topic
            
        Returns:
            dict: Parsed response with success/failure info
        """
        response = json.loads(response_json)
        return {
            "message_id": response.get("request", {}).get("message_id"),
            "success": response.get("response", {}).get("success", False),
            "result": response.get("response", {}).get("result"),
            "error": response.get("response", {}).get("error"),
            "error_code": response.get("response", {}).get("error_code"),
            "server_message": response.get("response", {}).get("server_message"),
            "processing_time_ms": response.get("processing_time_ms"),
            "raw": response
        }


# ==============================================================================
# Kafka Implementation Example
# ==============================================================================

class KafkaKuberClient(KuberMessagingClient):
    """Kuber messaging client using Apache Kafka."""
    
    def __init__(self, api_key: str, bootstrap_servers: str = "localhost:9092",
                 request_topic: str = "kuber_request", 
                 response_topic: str = "kuber_response",
                 region: str = "default"):
        super().__init__(api_key, region)
        
        if not KAFKA_AVAILABLE:
            raise ImportError("kafka-python is required. Install with: pip install kafka-python")
        
        self.bootstrap_servers = bootstrap_servers
        self.request_topic = request_topic
        self.response_topic = response_topic
        
        # Initialize producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Initialize consumer for responses
        self.consumer = KafkaConsumer(
            response_topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            consumer_timeout_ms=30000
        )
    
    def send_request(self, request: dict) -> str:
        """Send a request and return the message ID."""
        message_id = request["message_id"]
        self.pending_requests[message_id] = request
        self.producer.send(self.request_topic, value=request)
        self.producer.flush()
        return message_id
    
    def receive_response(self, message_id: str, timeout_seconds: int = 30) -> Optional[dict]:
        """Wait for a response matching the message ID."""
        start_time = time.time()
        
        for message in self.consumer:
            response = message.value
            resp_message_id = response.get("request", {}).get("message_id")
            
            if resp_message_id == message_id:
                return self.parse_response(json.dumps(response))
            
            if time.time() - start_time > timeout_seconds:
                break
        
        return None
    
    def execute(self, request: dict, timeout_seconds: int = 30) -> dict:
        """Send request and wait for response."""
        message_id = self.send_request(request)
        response = self.receive_response(message_id, timeout_seconds)
        
        if response is None:
            return {
                "message_id": message_id,
                "success": False,
                "error": "Timeout waiting for response",
                "error_code": "TIMEOUT"
            }
        
        return response
    
    def close(self):
        """Close producer and consumer connections."""
        self.producer.close()
        self.consumer.close()


# ==============================================================================
# Message Format Examples (for any broker)
# ==============================================================================

def print_example_messages():
    """Print example request/response messages for documentation."""
    
    client = KuberMessagingClient(api_key="your-api-key-here")
    
    print("=" * 70)
    print("KUBER REQUEST/RESPONSE MESSAGING - MESSAGE FORMAT EXAMPLES")
    print("=" * 70)
    
    # GET operation
    print("\n--- GET Operation ---")
    get_request = client.build_get("user:1001")
    print("Request:")
    print(json.dumps(get_request, indent=2))
    print("\nExpected Response:")
    print(json.dumps({
        "request_receive_timestamp": "2025-01-15T10:30:00.123Z",
        "response_time": "2025-01-15T10:30:00.125Z",
        "processing_time_ms": 2,
        "request": get_request,
        "response": {
            "success": True,
            "result": '{"name": "John", "email": "john@example.com"}',
            "error": ""
        }
    }, indent=2))
    
    # SET operation
    print("\n--- SET Operation ---")
    set_request = client.build_set("user:1002", {"name": "Jane", "email": "jane@example.com"}, ttl=3600)
    print("Request:")
    print(json.dumps(set_request, indent=2))
    print("\nExpected Response:")
    print(json.dumps({
        "request_receive_timestamp": "2025-01-15T10:30:01.000Z",
        "response_time": "2025-01-15T10:30:01.005Z",
        "processing_time_ms": 5,
        "request": set_request,
        "response": {
            "success": True,
            "result": "OK",
            "error": ""
        }
    }, indent=2))
    
    # MGET operation
    print("\n--- MGET (Batch GET) Operation ---")
    mget_request = client.build_mget(["user:1001", "user:1002", "user:1003"])
    print("Request:")
    print(json.dumps(mget_request, indent=2))
    print("\nExpected Response (key-value pairs format):")
    print(json.dumps({
        "request_receive_timestamp": "2025-01-15T10:30:02.000Z",
        "response_time": "2025-01-15T10:30:02.010Z",
        "processing_time_ms": 10,
        "request": mget_request,
        "response": {
            "success": True,
            "result": [
                {"key": "user:1001", "value": '{"name": "John"}'},
                {"key": "user:1002", "value": '{"name": "Jane"}'},
                {"key": "user:1003", "value": None}
            ],
            "error": ""
        }
    }, indent=2))
    
    # MSET operation
    print("\n--- MSET (Batch SET) Operation ---")
    mset_request = client.build_mset({
        "config:theme": "dark",
        "config:language": "en",
        "config:timezone": "UTC"
    }, ttl=86400)
    print("Request:")
    print(json.dumps(mset_request, indent=2))
    print("\nExpected Response:")
    print(json.dumps({
        "request_receive_timestamp": "2025-01-15T10:30:03.000Z",
        "response_time": "2025-01-15T10:30:03.015Z",
        "processing_time_ms": 15,
        "request": mset_request,
        "response": {
            "success": True,
            "result": "OK",
            "error": ""
        }
    }, indent=2))
    
    # KEYS operation
    print("\n--- KEYS Operation ---")
    keys_request = client.build_keys("user:*")
    print("Request:")
    print(json.dumps(keys_request, indent=2))
    print("\nExpected Response (with truncation):")
    print(json.dumps({
        "request_receive_timestamp": "2025-01-15T10:30:04.000Z",
        "response_time": "2025-01-15T10:30:04.050Z",
        "processing_time_ms": 50,
        "request": keys_request,
        "response": {
            "success": True,
            "result": ["user:1001", "user:1002", "user:1003"],
            "error": "",
            "server_message": "Results truncated: showing 3 of 1500 matching keys",
            "total_count": 1500,
            "returned_count": 3
        }
    }, indent=2))
    
    # HSET/HGET operations
    print("\n--- Hash Operations ---")
    hset_request = client.build_hset("session:abc123", "last_active", "2025-01-15T10:30:00Z")
    print("HSET Request:")
    print(json.dumps(hset_request, indent=2))
    
    hmset_request = client.build_hmset("session:abc123", {
        "user_id": "1001",
        "ip_address": "192.168.1.100",
        "user_agent": "Mozilla/5.0"
    })
    print("\nHMSET Request:")
    print(json.dumps(hmset_request, indent=2))
    
    hgetall_request = client.build_hgetall("session:abc123")
    print("\nHGETALL Request:")
    print(json.dumps(hgetall_request, indent=2))
    
    # JSON operations
    print("\n--- JSON Operations ---")
    jset_request = client.build_jset("product:5001", {
        "name": "Laptop",
        "price": 999.99,
        "specs": {
            "cpu": "Intel i7",
            "ram": "16GB",
            "storage": "512GB SSD"
        },
        "tags": ["electronics", "computers"]
    })
    print("JSET Request:")
    print(json.dumps(jset_request, indent=2))
    
    jget_request = client.build_jget("product:5001", "$.specs.cpu")
    print("\nJGET with JSONPath Request:")
    print(json.dumps(jget_request, indent=2))
    
    jsearch_request = client.build_jsearch("price > 500 AND tags CONTAINS 'electronics'")
    print("\nJSEARCH Request:")
    print(json.dumps(jsearch_request, indent=2))
    
    # Error response example
    print("\n--- Error Response Example ---")
    print(json.dumps({
        "request_receive_timestamp": "2025-01-15T10:30:05.000Z",
        "response_time": "2025-01-15T10:30:05.001Z",
        "processing_time_ms": 1,
        "request": {
            "api_key": "invalid-key",
            "message_id": "msg-12345",
            "operation": "GET",
            "key": "test"
        },
        "response": {
            "success": False,
            "result": None,
            "error": "Invalid or expired API key",
            "error_code": "AUTHENTICATION_FAILED"
        }
    }, indent=2))
    
    print("\n" + "=" * 70)
    print("END OF EXAMPLES")
    print("=" * 70)


# ==============================================================================
# Interactive Demo
# ==============================================================================

def interactive_demo():
    """Run an interactive demonstration (simulated without actual broker)."""
    
    print("\n" + "=" * 70)
    print("KUBER MESSAGING CLIENT - INTERACTIVE DEMO")
    print("=" * 70)
    print("\nThis demo shows how to build requests for various cache operations.")
    print("In production, you would send these to your message broker.")
    
    # Simulated API key (in production, get from Kuber admin panel)
    api_key = "demo-api-key-12345"
    
    client = KuberMessagingClient(api_key=api_key, region="default")
    
    print("\n--- Building Sample Requests ---\n")
    
    # 1. Store user data
    print("1. Storing user data with TTL:")
    request = client.build_set(
        key="user:john_doe",
        value={
            "name": "John Doe",
            "email": "john@example.com",
            "role": "admin"
        },
        ttl=3600  # 1 hour
    )
    print(f"   Topic: kuber_request")
    print(f"   Message: {json.dumps(request)}")
    
    # 2. Batch store configuration
    print("\n2. Batch storing configuration:")
    request = client.build_mset(
        entries={
            "app:config:theme": "dark",
            "app:config:lang": "en-US",
            "app:config:version": "2.6.2"
        }
    )
    print(f"   Topic: kuber_request")
    print(f"   Message: {json.dumps(request)}")
    
    # 3. Retrieve multiple users
    print("\n3. Batch retrieving users:")
    request = client.build_mget(
        keys=["user:john_doe", "user:jane_doe", "user:bob_smith"]
    )
    print(f"   Topic: kuber_request")
    print(f"   Message: {json.dumps(request)}")
    
    # 4. Search for products
    print("\n4. Searching JSON documents:")
    request = client.build_jsearch(
        query="category = 'electronics' AND price < 1000"
    )
    print(f"   Topic: kuber_request")
    print(f"   Message: {json.dumps(request)}")
    
    # 5. Check server health
    print("\n5. Health check (PING):")
    request = client.build_ping()
    print(f"   Topic: kuber_request")
    print(f"   Message: {json.dumps(request)}")
    
    print("\n" + "-" * 70)
    print("Response Topic: kuber_response")
    print("Responses include the original request for correlation via message_id")
    print("-" * 70)


# ==============================================================================
# Main
# ==============================================================================

if __name__ == "__main__":
    import sys
    
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║          KUBER REQUEST/RESPONSE MESSAGING - PYTHON CLIENT            ║
║                         Version 2.6.2                                ║
╚══════════════════════════════════════════════════════════════════════╝
""")
    
    if len(sys.argv) > 1 and sys.argv[1] == "--examples":
        print_example_messages()
    else:
        interactive_demo()
        print("\nRun with --examples flag to see detailed message format examples.")
        print("\nTo use with a real broker:")
        print("  1. Install broker client: pip install kafka-python")
        print("  2. Configure your broker in Kuber's request_response.json")
        print("  3. Get an API key from Kuber admin panel")
        print("  4. Modify this script with your broker details")
