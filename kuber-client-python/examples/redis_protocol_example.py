#!/usr/bin/env python3
# Copyright © 2025-2030, All Rights Reserved
# Ashutosh Sinha | Email: ajsinha@gmail.com
#
# Legal Notice: This module and the associated software architecture are proprietary
# and confidential. Unauthorized copying, distribution, modification, or use is
# strictly prohibited without explicit written permission from the copyright holder.
#

"""
Kuber Redis Protocol Client - Comprehensive Examples

This example demonstrates all capabilities of the Kuber client using Redis protocol
with Kuber extensions for regions and JSON operations.

Usage:
    python redis_protocol_example.py [--host HOST] [--port PORT]
    
Default connection: localhost:6380
"""

import sys
import json
import logging
from datetime import timedelta
from kuber import KuberClient, KuberException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_section(title: str):
    """Print a section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def print_result(description: str, result):
    """Print a result with description."""
    print(f"  {description}: {result}")


def example_basic_operations(client: KuberClient):
    """Demonstrate basic GET/SET operations."""
    print_section("1. BASIC STRING OPERATIONS")
    
    # Simple SET and GET
    client.set('greeting', 'Hello, Kuber!')
    value = client.get('greeting')
    print_result("SET/GET", value)
    
    # SET with TTL
    client.set('temp_key', 'expires soon', ttl=60)  # 60 seconds
    print_result("SET with TTL (60s)", client.get('temp_key'))
    print_result("TTL remaining", client.ttl('temp_key'))
    
    # SET with timedelta
    client.set('session', 'session_data', ttl=timedelta(hours=1))
    print_result("SET with timedelta (1 hour)", f"TTL: {client.ttl('session')} seconds")
    
    # SETNX - Set only if not exists
    result1 = client.setnx('unique_key', 'first value')
    result2 = client.setnx('unique_key', 'second value')
    print_result("SETNX first time", result1)
    print_result("SETNX second time", result2)
    print_result("Value after SETNX attempts", client.get('unique_key'))
    
    # SETEX - Set with expiration
    client.setex('cache_item', 'cached data', 300)
    print_result("SETEX (5 minutes)", client.get('cache_item'))
    
    # INCR/DECR operations
    client.set('counter', '0')
    client.incr('counter')
    client.incr('counter')
    client.incrby('counter', 10)
    print_result("After INCR, INCR, INCRBY(10)", client.get('counter'))
    
    client.decr('counter')
    client.decrby('counter', 5)
    print_result("After DECR, DECRBY(5)", client.get('counter'))
    
    # APPEND
    client.set('message', 'Hello')
    client.append('message', ' World!')
    print_result("After APPEND", client.get('message'))
    
    # STRLEN
    print_result("String length", client.strlen('message'))


def example_mget_mset(client: KuberClient):
    """Demonstrate MGET and MSET operations."""
    print_section("2. MULTI-KEY OPERATIONS (MGET/MSET)")
    
    # MSET - Set multiple keys at once
    client.mset({
        'user:1:name': 'Alice',
        'user:1:email': 'alice@example.com',
        'user:2:name': 'Bob',
        'user:2:email': 'bob@example.com',
        'user:3:name': 'Charlie',
        'user:3:email': 'charlie@example.com'
    })
    print("  MSET: Set 6 keys for 3 users")
    
    # MGET - Get multiple keys at once
    names = client.mget('user:1:name', 'user:2:name', 'user:3:name')
    print_result("MGET names", names)
    
    emails = client.mget('user:1:email', 'user:2:email', 'user:3:email')
    print_result("MGET emails", emails)
    
    # MGET with some missing keys
    mixed = client.mget('user:1:name', 'user:99:name', 'user:2:name')
    print_result("MGET with missing key", mixed)


def example_key_pattern_search(client: KuberClient):
    """Demonstrate key pattern searching."""
    print_section("3. KEY PATTERN SEARCH")
    
    # First, create some test data
    client.mset({
        'product:1001': 'Widget',
        'product:1002': 'Gadget',
        'product:2001': 'Gizmo',
        'order:5001': 'Order A',
        'order:5002': 'Order B',
        'session:abc123': 'Session 1',
        'session:def456': 'Session 2'
    })
    print("  Created test keys: product:*, order:*, session:*")
    
    # Find all keys
    all_keys = client.keys('*')
    print_result("KEYS * (all keys)", len(all_keys))
    
    # Find product keys
    product_keys = client.keys('product:*')
    print_result("KEYS product:*", product_keys)
    
    # Find order keys
    order_keys = client.keys('order:*')
    print_result("KEYS order:*", order_keys)
    
    # Find session keys
    session_keys = client.keys('session:*')
    print_result("KEYS session:*", session_keys)
    
    # Pattern with single character wildcard
    keys_1xxx = client.keys('product:1???')
    print_result("KEYS product:1???", keys_1xxx)
    
    # Pattern with character class
    keys_ab = client.keys('session:[ad]*')
    print_result("KEYS session:[ad]*", keys_ab)
    
    # SCAN for iterative search
    print("\n  SCAN iteration:")
    cursor = 0
    iteration = 0
    while True:
        cursor, keys = client.scan(cursor, match='*', count=5)
        iteration += 1
        print(f"    Iteration {iteration}: cursor={cursor}, keys={len(keys)}")
        if cursor == 0:
            break


def example_key_operations(client: KuberClient):
    """Demonstrate key management operations."""
    print_section("4. KEY MANAGEMENT OPERATIONS")
    
    # EXISTS
    client.set('exists_test', 'value')
    print_result("EXISTS exists_test", client.exists('exists_test'))
    print_result("EXISTS nonexistent", client.exists('nonexistent'))
    print_result("EXISTS multiple keys", client.exists('exists_test', 'nonexistent', 'greeting'))
    
    # TYPE
    client.set('string_key', 'value')
    print_result("TYPE string_key", client.type('string_key'))
    
    # EXPIRE and TTL
    client.set('expire_test', 'will expire')
    client.expire('expire_test', 120)
    print_result("TTL after EXPIRE(120)", client.ttl('expire_test'))
    
    # PERSIST
    client.persist('expire_test')
    print_result("TTL after PERSIST", client.ttl('expire_test'))
    
    # RENAME
    client.set('old_name', 'data')
    client.rename('old_name', 'new_name')
    print_result("RENAME old_name -> new_name", client.get('new_name'))
    
    # DELETE
    client.set('to_delete1', 'value1')
    client.set('to_delete2', 'value2')
    deleted = client.delete('to_delete1', 'to_delete2', 'nonexistent')
    print_result("DEL (3 keys, 2 existed)", deleted)


def example_hash_operations(client: KuberClient):
    """Demonstrate hash operations."""
    print_section("5. HASH OPERATIONS")
    
    # HSET and HGET
    client.hset('user:profile:1', 'name', 'Alice Johnson')
    client.hset('user:profile:1', 'email', 'alice@example.com')
    client.hset('user:profile:1', 'age', '28')
    client.hset('user:profile:1', 'city', 'New York')
    
    print_result("HGET name", client.hget('user:profile:1', 'name'))
    print_result("HGET email", client.hget('user:profile:1', 'email'))
    
    # HMSET - Set multiple fields
    client.hmset('user:profile:2', {
        'name': 'Bob Smith',
        'email': 'bob@example.com',
        'age': '35',
        'city': 'San Francisco'
    })
    print("  HMSET: Set 4 fields for user:profile:2")
    
    # HMGET - Get multiple fields
    fields = client.hmget('user:profile:2', 'name', 'email', 'city')
    print_result("HMGET name,email,city", fields)
    
    # HGETALL - Get all fields and values
    all_fields = client.hgetall('user:profile:1')
    print_result("HGETALL", all_fields)
    
    # HKEYS and HVALS
    print_result("HKEYS", client.hkeys('user:profile:1'))
    print_result("HVALS", client.hvals('user:profile:1'))
    
    # HLEN
    print_result("HLEN", client.hlen('user:profile:1'))
    
    # HEXISTS
    print_result("HEXISTS 'name'", client.hexists('user:profile:1', 'name'))
    print_result("HEXISTS 'nonexistent'", client.hexists('user:profile:1', 'nonexistent'))
    
    # HINCRBY
    client.hset('stats', 'views', '100')
    client.hincrby('stats', 'views', 10)
    print_result("HINCRBY views +10", client.hget('stats', 'views'))
    
    # HDEL
    client.hdel('user:profile:1', 'city')
    print_result("HDEL city, remaining fields", client.hkeys('user:profile:1'))


def example_region_operations(client: KuberClient):
    """Demonstrate region operations - storing JSON in different regions."""
    print_section("6. REGION OPERATIONS - Multi-Tenant Data Isolation")
    
    # List existing regions
    regions = client.list_regions()
    print_result("Current regions", regions)
    
    # Create custom regions
    client.create_region('products', 'Product catalog')
    client.create_region('orders', 'Customer orders')
    client.create_region('sessions', 'User sessions')
    
    regions = client.list_regions()
    print_result("After creating 3 regions", regions)
    
    # Store product data in products region
    client.select_region('products')
    print(f"\n  Selected region: {client.current_region}")
    
    client.json_set('prod:1001', {
        'name': 'Wireless Headphones',
        'brand': 'TechAudio',
        'price': 79.99,
        'category': 'Electronics',
        'in_stock': True,
        'tags': ['audio', 'wireless', 'bluetooth']
    })
    
    client.json_set('prod:1002', {
        'name': 'USB-C Cable',
        'brand': 'CablePro',
        'price': 12.99,
        'category': 'Electronics',
        'in_stock': True,
        'tags': ['cable', 'usb', 'charging']
    })
    
    client.json_set('prod:2001', {
        'name': 'Office Chair',
        'brand': 'ComfortSeating',
        'price': 299.99,
        'category': 'Furniture',
        'in_stock': False,
        'tags': ['office', 'ergonomic', 'chair']
    })
    
    print("  Stored 3 products in 'products' region")
    print_result("Products region size", client.dbsize())
    
    # Store order data in orders region
    client.select_region('orders')
    print(f"\n  Selected region: {client.current_region}")
    
    client.json_set('order:5001', {
        'customer_id': 'cust:101',
        'product_id': 'prod:1001',
        'quantity': 2,
        'total': 159.98,
        'status': 'shipped',
        'created_at': '2025-01-15T10:30:00Z'
    })
    
    client.json_set('order:5002', {
        'customer_id': 'cust:102',
        'product_id': 'prod:1002',
        'quantity': 5,
        'total': 64.95,
        'status': 'pending',
        'created_at': '2025-01-16T14:45:00Z'
    })
    
    client.json_set('order:5003', {
        'customer_id': 'cust:101',
        'product_id': 'prod:2001',
        'quantity': 1,
        'total': 299.99,
        'status': 'delivered',
        'created_at': '2025-01-10T09:00:00Z'
    })
    
    print("  Stored 3 orders in 'orders' region")
    print_result("Orders region size", client.dbsize())
    
    # Store session data in sessions region with TTL
    client.select_region('sessions')
    print(f"\n  Selected region: {client.current_region}")
    
    client.json_set('sess:abc123', {
        'user_id': 'cust:101',
        'ip': '192.168.1.100',
        'login_time': '2025-01-16T08:00:00Z',
        'last_activity': '2025-01-16T10:30:00Z'
    }, ttl=timedelta(hours=2))
    
    client.json_set('sess:def456', {
        'user_id': 'cust:102',
        'ip': '192.168.1.101',
        'login_time': '2025-01-16T09:00:00Z',
        'last_activity': '2025-01-16T10:45:00Z'
    }, ttl=timedelta(hours=2))
    
    print("  Stored 2 sessions with 2-hour TTL")
    print_result("Sessions region size", client.dbsize())
    
    # Demonstrate data isolation - each region has its own keys
    print("\n  Data Isolation Demonstration:")
    client.select_region('products')
    prod_keys = client.keys('*')
    print_result("Keys in 'products' region", prod_keys)
    
    client.select_region('orders')
    order_keys = client.keys('*')
    print_result("Keys in 'orders' region", order_keys)
    
    client.select_region('sessions')
    sess_keys = client.keys('*')
    print_result("Keys in 'sessions' region", sess_keys)
    
    # Switch back to default
    client.select_region('default')


def example_json_operations(client: KuberClient):
    """Demonstrate JSON operations."""
    print_section("7. JSON OPERATIONS")
    
    # Create a region for JSON demo
    client.create_region('json_demo', 'JSON operations demo')
    client.select_region('json_demo')
    
    # JSON SET with complex structure
    user_data = {
        'id': 'user:1001',
        'name': 'John Doe',
        'email': 'john@example.com',
        'age': 32,
        'active': True,
        'roles': ['admin', 'developer'],
        'preferences': {
            'theme': 'dark',
            'notifications': True,
            'language': 'en'
        },
        'address': {
            'street': '123 Main St',
            'city': 'Boston',
            'state': 'MA',
            'zip': '02101'
        },
        'created_at': '2025-01-01T00:00:00Z'
    }
    
    client.json_set('user:1001', user_data)
    print("  Stored complex user JSON document")
    
    # JSON GET - full document
    retrieved = client.json_get('user:1001')
    print_result("JSON GET full document", json.dumps(retrieved, indent=2)[:100] + '...')
    
    # JSON GET - specific path
    name = client.json_get('user:1001', '$.name')
    print_result("JSON GET $.name", name)
    
    email = client.json_get('user:1001', '$.email')
    print_result("JSON GET $.email", email)
    
    # JSON GET - nested path
    city = client.json_get('user:1001', '$.address.city')
    print_result("JSON GET $.address.city", city)
    
    theme = client.json_get('user:1001', '$.preferences.theme')
    print_result("JSON GET $.preferences.theme", theme)
    
    # JSON GET - array
    roles = client.json_get('user:1001', '$.roles')
    print_result("JSON GET $.roles", roles)
    
    # JSON TYPE
    print_result("JSON TYPE $.name", client.json_type('user:1001', '$.name'))
    print_result("JSON TYPE $.age", client.json_type('user:1001', '$.age'))
    print_result("JSON TYPE $.active", client.json_type('user:1001', '$.active'))
    print_result("JSON TYPE $.roles", client.json_type('user:1001', '$.roles'))
    
    # Store more users for search demo
    client.json_set('user:1002', {
        'id': 'user:1002',
        'name': 'Jane Smith',
        'email': 'jane@example.com',
        'age': 28,
        'active': True,
        'roles': ['developer'],
        'preferences': {'theme': 'light', 'notifications': False, 'language': 'en'},
        'address': {'street': '456 Oak Ave', 'city': 'New York', 'state': 'NY', 'zip': '10001'}
    })
    
    client.json_set('user:1003', {
        'id': 'user:1003',
        'name': 'Bob Wilson',
        'email': 'bob@example.com',
        'age': 45,
        'active': False,
        'roles': ['viewer'],
        'preferences': {'theme': 'dark', 'notifications': True, 'language': 'es'},
        'address': {'street': '789 Pine Rd', 'city': 'Boston', 'state': 'MA', 'zip': '02102'}
    })
    
    client.json_set('user:1004', {
        'id': 'user:1004',
        'name': 'Alice Brown',
        'email': 'alice@company.com',
        'age': 35,
        'active': True,
        'roles': ['admin', 'manager'],
        'preferences': {'theme': 'dark', 'notifications': True, 'language': 'en'},
        'address': {'street': '321 Elm St', 'city': 'Chicago', 'state': 'IL', 'zip': '60601'}
    })
    
    print("\n  Stored 4 user documents for search demos")
    
    # Switch back to default
    client.select_region('default')


def example_json_deep_search(client: KuberClient):
    """Demonstrate JSON deep search capabilities."""
    print_section("8. JSON DEEP SEARCH")
    
    client.select_region('json_demo')
    
    # Search by equality
    print("\n  Search by Equality:")
    results = client.json_search('$.active=true')
    print_result("$.active=true", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']}")
    
    # Search by comparison
    print("\n  Search by Comparison:")
    results = client.json_search('$.age>30')
    print_result("$.age>30", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (age: {doc['value']['age']})")
    
    results = client.json_search('$.age>=35')
    print_result("$.age>=35", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (age: {doc['value']['age']})")
    
    # Search nested fields
    print("\n  Search Nested Fields:")
    results = client.json_search('$.address.city=Boston')
    print_result("$.address.city=Boston", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']}")
    
    results = client.json_search('$.preferences.theme=dark')
    print_result("$.preferences.theme=dark", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']}")
    
    # Combined search conditions
    print("\n  Combined Conditions:")
    results = client.json_search('$.active=true,$.age>30')
    print_result("$.active=true,$.age>30", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (active, age: {doc['value']['age']})")
    
    # Search with LIKE pattern
    print("\n  Pattern Matching (LIKE):")
    results = client.json_search('$.email LIKE %@example.com')
    print_result("$.email LIKE %@example.com", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['email']}")
    
    # Search array contains
    print("\n  Array Contains:")
    results = client.json_search('$.roles CONTAINS admin')
    print_result("$.roles CONTAINS admin", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (roles: {doc['value']['roles']})")
    
    results = client.json_search('$.roles CONTAINS developer')
    print_result("$.roles CONTAINS developer", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (roles: {doc['value']['roles']})")
    
    # Search with inequality
    print("\n  Inequality Search:")
    results = client.json_search('$.preferences.language!=en')
    print_result("$.preferences.language!=en", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (lang: {doc['value']['preferences']['language']})")
    
    client.select_region('default')


def example_cross_region_search(client: KuberClient):
    """Demonstrate searching across different regions."""
    print_section("9. CROSS-REGION OPERATIONS")
    
    # Search in products region
    print("\n  Search in 'products' region:")
    results = client.json_search('$.category=Electronics', region='products')
    print_result("$.category=Electronics", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (${doc['value']['price']})")
    
    results = client.json_search('$.in_stock=true', region='products')
    print_result("$.in_stock=true", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']}")
    
    results = client.json_search('$.price>50', region='products')
    print_result("$.price>50", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (${doc['value']['price']})")
    
    # Search in orders region
    print("\n  Search in 'orders' region:")
    results = client.json_search('$.status=shipped', region='orders')
    print_result("$.status=shipped", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: customer {doc['value']['customer_id']}")
    
    results = client.json_search('$.customer_id=cust:101', region='orders')
    print_result("$.customer_id=cust:101", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['status']}, ${doc['value']['total']}")
    
    results = client.json_search('$.total>100', region='orders')
    print_result("$.total>100", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: ${doc['value']['total']}")


def example_server_info(client: KuberClient):
    """Demonstrate server information commands."""
    print_section("10. SERVER INFORMATION")
    
    # PING
    print_result("PING", client.ping())
    
    # INFO
    info = client.info()
    print("\n  INFO (first 500 chars):")
    print(f"    {info[:500]}...")
    
    # STATUS
    status = client.status()
    print_result("\n  STATUS", json.dumps(status, indent=2)[:300] + '...')
    
    # DBSIZE
    client.select_region('default')
    print_result("DBSIZE (default region)", client.dbsize())
    
    # REPL_INFO
    repl = client.repl_info()
    print_result("REPLINFO", repl[:200] if repl else "N/A")
    
    # TIME
    server_time = client.time()
    print_result("TIME", f"seconds={server_time[0]}, microseconds={server_time[1]}")


def cleanup(client: KuberClient):
    """Clean up test data."""
    print_section("CLEANUP")
    
    # Purge test regions
    try:
        client.purge_region('products')
        client.purge_region('orders')
        client.purge_region('sessions')
        client.purge_region('json_demo')
        print("  Purged test regions: products, orders, sessions, json_demo")
    except Exception as e:
        print(f"  Cleanup warning: {e}")
    
    # Delete test regions
    try:
        client.delete_region('products')
        client.delete_region('orders')
        client.delete_region('sessions')
        client.delete_region('json_demo')
        print("  Deleted test regions")
    except Exception as e:
        print(f"  Cleanup warning: {e}")
    
    # Clean default region
    client.select_region('default')
    client.flushdb()
    print("  Flushed default region")


def main():
    """Main function to run all examples."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Kuber Redis Protocol Client Examples')
    parser.add_argument('--host', default='localhost', help='Kuber server host')
    parser.add_argument('--port', type=int, default=6380, help='Kuber server port')
    parser.add_argument('--password', default=None, help='Authentication password')
    parser.add_argument('--no-cleanup', action='store_true', help='Skip cleanup')
    args = parser.parse_args()
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║     KUBER REDIS PROTOCOL CLIENT - COMPREHENSIVE EXAMPLES     ║
║                                                              ║
║  Connecting to: {args.host}:{args.port}                              ║
╚══════════════════════════════════════════════════════════════╝
""")
    
    try:
        with KuberClient(args.host, args.port, password=args.password) as client:
            # Run all examples
            example_basic_operations(client)
            example_mget_mset(client)
            example_key_pattern_search(client)
            example_key_operations(client)
            example_hash_operations(client)
            example_region_operations(client)
            example_json_operations(client)
            example_json_deep_search(client)
            example_cross_region_search(client)
            example_server_info(client)
            
            if not args.no_cleanup:
                cleanup(client)
            
            print_section("ALL EXAMPLES COMPLETED SUCCESSFULLY")
            
    except KuberException as e:
        logger.error(f"Kuber error: {e}")
        sys.exit(1)
    except ConnectionRefusedError:
        logger.error(f"Could not connect to Kuber at {args.host}:{args.port}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
