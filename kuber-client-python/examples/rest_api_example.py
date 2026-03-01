#!/usr/bin/env python3
# Copyright © 2025-2030, All Rights Reserved
# Ashutosh Sinha | Email: ajsinha@gmail.com
#
# Legal Notice: This module and the associated software architecture are proprietary
# and confidential. Unauthorized copying, distribution, modification, or use is
# strictly prohibited without explicit written permission from the copyright holder.
#

"""
Kuber REST API Client - Comprehensive Examples

This example demonstrates all capabilities of the Kuber REST client using
HTTP REST API endpoints. No Redis protocol required.

Usage:
    python rest_api_example.py [--host HOST] [--port PORT]
    
Default connection: localhost:8080
"""

import sys
import json
import logging
from datetime import timedelta
from kuber.rest_client import KuberRestClient, KuberRestException

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
    if isinstance(result, (dict, list)):
        result_str = json.dumps(result, indent=2)
        if len(result_str) > 200:
            result_str = result_str[:200] + '...'
        print(f"  {description}:\n    {result_str.replace(chr(10), chr(10) + '    ')}")
    else:
        print(f"  {description}: {result}")


def example_server_operations(client: KuberRestClient):
    """Demonstrate server operations."""
    print_section("1. SERVER OPERATIONS")
    
    # Ping
    is_alive = client.ping()
    print_result("PING", "OK" if is_alive else "FAILED")
    
    # Info
    info = client.info()
    print_result("INFO", info)
    
    # Status
    status = client.status()
    print_result("STATUS", status)
    
    # Stats
    stats = client.stats()
    print_result("STATS", stats)


def example_basic_operations(client: KuberRestClient):
    """Demonstrate basic GET/SET operations."""
    print_section("2. BASIC KEY-VALUE OPERATIONS")
    
    # Simple SET and GET
    client.set('greeting', 'Hello, Kuber REST!')
    value = client.get('greeting')
    print_result("SET/GET", value)
    
    # SET with TTL
    client.set('temp_key', 'expires soon', ttl=60)
    print_result("SET with TTL (60s)", client.get('temp_key'))
    print_result("TTL remaining", client.ttl('temp_key'))
    
    # SET with timedelta
    client.set('session', 'session_data', ttl=timedelta(hours=1))
    print_result("SET with timedelta", f"TTL: {client.ttl('session')} seconds")
    
    # Check EXISTS
    print_result("EXISTS 'greeting'", client.exists('greeting'))
    print_result("EXISTS 'nonexistent'", client.exists('nonexistent'))
    
    # DELETE
    client.set('to_delete', 'delete me')
    deleted = client.delete('to_delete')
    print_result("DELETE", deleted)
    print_result("GET after DELETE", client.get('to_delete'))
    
    # EXPIRE
    client.set('expire_test', 'will expire')
    client.expire('expire_test', 120)
    print_result("EXPIRE set to 120s", client.ttl('expire_test'))
    
    # DBSIZE
    print_result("DBSIZE (default region)", client.dbsize())


def example_mget_mset(client: KuberRestClient):
    """Demonstrate MGET and MSET operations."""
    print_section("3. MULTI-KEY OPERATIONS (MGET/MSET)")
    
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
    names = client.mget(['user:1:name', 'user:2:name', 'user:3:name'])
    print_result("MGET names", names)
    
    emails = client.mget(['user:1:email', 'user:2:email', 'user:3:email'])
    print_result("MGET emails", emails)
    
    # MGET with some missing keys
    mixed = client.mget(['user:1:name', 'user:99:name', 'user:2:name'])
    print_result("MGET with missing key", mixed)


def example_key_pattern_search(client: KuberRestClient):
    """Demonstrate key pattern searching."""
    print_section("4. KEY PATTERN SEARCH")
    
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
    print_result("KEYS * (all keys)", f"{len(all_keys)} keys")
    
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


def example_region_operations(client: KuberRestClient):
    """Demonstrate region operations."""
    print_section("5. REGION OPERATIONS")
    
    # List existing regions
    regions = client.list_regions()
    print_result("Current regions", regions)
    
    # Create custom regions
    client.create_region('products', 'Product catalog')
    client.create_region('orders', 'Customer orders')
    client.create_region('sessions', 'User sessions')
    
    regions = client.list_regions()
    print_result("After creating 3 regions", [r.get('name') for r in regions] if isinstance(regions, list) else regions)
    
    # Get region info
    prod_info = client.get_region('products')
    print_result("Products region info", prod_info)
    
    # Store data in products region
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
    
    # Store session data with TTL
    client.select_region('sessions')
    print(f"\n  Selected region: {client.current_region}")
    
    client.json_set('sess:abc123', {
        'user_id': 'cust:101',
        'ip': '192.168.1.100',
        'login_time': '2025-01-16T08:00:00Z'
    }, ttl=timedelta(hours=2))
    
    client.json_set('sess:def456', {
        'user_id': 'cust:102',
        'ip': '192.168.1.101',
        'login_time': '2025-01-16T09:00:00Z'
    }, ttl=timedelta(hours=2))
    
    print("  Stored 2 sessions with 2-hour TTL")
    
    # Demonstrate data isolation
    print("\n  Data Isolation:")
    client.select_region('products')
    print_result("Keys in 'products'", client.keys('*'))
    
    client.select_region('orders')
    print_result("Keys in 'orders'", client.keys('*'))
    
    client.select_region('sessions')
    print_result("Keys in 'sessions'", client.keys('*'))
    
    # Switch back to default
    client.select_region('default')


def example_json_operations(client: KuberRestClient):
    """Demonstrate JSON operations."""
    print_section("6. JSON OPERATIONS")
    
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
        }
    }
    
    client.json_set('user:1001', user_data)
    print("  Stored complex user JSON document")
    
    # JSON GET - full document
    retrieved = client.json_get('user:1001')
    print_result("JSON GET full document", retrieved)
    
    # JSON GET - specific path
    name = client.json_get('user:1001', path='$.name')
    print_result("JSON GET $.name", name)
    
    # JSON GET - nested path
    city = client.json_get('user:1001', path='$.address.city')
    print_result("JSON GET $.address.city", city)
    
    # Store more users for search demo
    client.json_set('user:1002', {
        'id': 'user:1002',
        'name': 'Jane Smith',
        'email': 'jane@example.com',
        'age': 28,
        'active': True,
        'roles': ['developer'],
        'preferences': {'theme': 'light', 'notifications': False, 'language': 'en'},
        'address': {'city': 'New York', 'state': 'NY'}
    })
    
    client.json_set('user:1003', {
        'id': 'user:1003',
        'name': 'Bob Wilson',
        'email': 'bob@example.com',
        'age': 45,
        'active': False,
        'roles': ['viewer'],
        'preferences': {'theme': 'dark', 'notifications': True, 'language': 'es'},
        'address': {'city': 'Boston', 'state': 'MA'}
    })
    
    client.json_set('user:1004', {
        'id': 'user:1004',
        'name': 'Alice Brown',
        'email': 'alice@company.com',
        'age': 35,
        'active': True,
        'roles': ['admin', 'manager'],
        'preferences': {'theme': 'dark', 'notifications': True, 'language': 'en'},
        'address': {'city': 'Chicago', 'state': 'IL'}
    })
    
    print("\n  Stored 4 user documents for search demos")
    
    # JSON MGET
    users = client.json_mget(['user:1001', 'user:1002'])
    print_result("JSON MGET", users)
    
    # JSON DELETE path
    client.json_set('temp_user', {'name': 'Temp', 'extra': 'to delete'})
    client.json_delete('temp_user', path='$.extra')
    print_result("JSON DELETE $.extra", client.json_get('temp_user'))
    
    client.select_region('default')


def example_json_deep_search(client: KuberRestClient):
    """Demonstrate JSON deep search capabilities."""
    print_section("7. JSON DEEP SEARCH")
    
    client.select_region('json_demo')
    
    # Search by equality
    print("\n  Search by Equality:")
    results = client.json_search('$.active=true')
    print_result("$.active=true", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc.get('key', 'N/A')}: {doc.get('value', {}).get('name', 'N/A')}")
    
    # Search by comparison
    print("\n  Search by Comparison:")
    results = client.json_search('$.age>30')
    print_result("$.age>30", f"{len(results)} results")
    for doc in results:
        value = doc.get('value', {})
        print(f"    - {doc.get('key')}: {value.get('name')} (age: {value.get('age')})")
    
    # Search nested fields
    print("\n  Search Nested Fields:")
    results = client.json_search('$.address.city=Boston')
    print_result("$.address.city=Boston", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc.get('key')}: {doc.get('value', {}).get('name')}")
    
    results = client.json_search('$.preferences.theme=dark')
    print_result("$.preferences.theme=dark", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc.get('key')}: {doc.get('value', {}).get('name')}")
    
    # Combined conditions
    print("\n  Combined Conditions:")
    results = client.json_search('$.active=true,$.age>30')
    print_result("$.active=true,$.age>30", f"{len(results)} results")
    for doc in results:
        value = doc.get('value', {})
        print(f"    - {doc.get('key')}: {value.get('name')} (age: {value.get('age')})")
    
    # Pattern matching
    print("\n  Pattern Matching (LIKE):")
    results = client.json_search('$.email LIKE %@example.com')
    print_result("$.email LIKE %@example.com", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc.get('key')}: {doc.get('value', {}).get('email')}")
    
    # Array contains
    print("\n  Array Contains:")
    results = client.json_search('$.roles CONTAINS admin')
    print_result("$.roles CONTAINS admin", f"{len(results)} results")
    for doc in results:
        value = doc.get('value', {})
        print(f"    - {doc.get('key')}: {value.get('name')} (roles: {value.get('roles')})")
    
    client.select_region('default')


def example_cross_region_search(client: KuberRestClient):
    """Demonstrate searching across different regions."""
    print_section("8. CROSS-REGION OPERATIONS")
    
    # Search in products region
    print("\n  Search in 'products' region:")
    results = client.json_search('$.category=Electronics', region='products')
    print_result("$.category=Electronics", f"{len(results)} results")
    for doc in results:
        value = doc.get('value', {})
        print(f"    - {doc.get('key')}: {value.get('name')} (${value.get('price')})")
    
    results = client.json_search('$.in_stock=true', region='products')
    print_result("$.in_stock=true", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc.get('key')}: {doc.get('value', {}).get('name')}")
    
    results = client.json_search('$.price>50', region='products')
    print_result("$.price>50", f"{len(results)} results")
    for doc in results:
        value = doc.get('value', {})
        print(f"    - {doc.get('key')}: {value.get('name')} (${value.get('price')})")
    
    # Search in orders region
    print("\n  Search in 'orders' region:")
    results = client.json_search('$.status=shipped', region='orders')
    print_result("$.status=shipped", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc.get('key')}: customer {doc.get('value', {}).get('customer_id')}")
    
    results = client.json_search('$.customer_id=cust:101', region='orders')
    print_result("$.customer_id=cust:101", f"{len(results)} results")
    for doc in results:
        value = doc.get('value', {})
        print(f"    - {doc.get('key')}: {value.get('status')}, ${value.get('total')}")
    
    results = client.json_search('$.total>100', region='orders')
    print_result("$.total>100", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc.get('key')}: ${doc.get('value', {}).get('total')}")


def example_hash_operations(client: KuberRestClient):
    """Demonstrate hash operations."""
    print_section("9. HASH OPERATIONS")
    
    # HSET and HGET
    client.hset('user:profile:1', 'name', 'Alice Johnson')
    client.hset('user:profile:1', 'email', 'alice@example.com')
    client.hset('user:profile:1', 'age', '28')
    
    print_result("HGET name", client.hget('user:profile:1', 'name'))
    print_result("HGET email", client.hget('user:profile:1', 'email'))
    
    # HMSET
    client.hmset('user:profile:2', {
        'name': 'Bob Smith',
        'email': 'bob@example.com',
        'age': '35',
        'city': 'San Francisco'
    })
    print("  HMSET: Set 4 fields for user:profile:2")
    
    # HGETALL
    all_fields = client.hgetall('user:profile:2')
    print_result("HGETALL", all_fields)
    
    # HKEYS
    keys = client.hkeys('user:profile:1')
    print_result("HKEYS", keys)
    
    # HDEL
    client.hdel('user:profile:1', 'age')
    print_result("HDEL 'age', remaining", client.hkeys('user:profile:1'))


def example_bulk_operations(client: KuberRestClient):
    """Demonstrate bulk import/export."""
    print_section("10. BULK OPERATIONS")
    
    # Create a region for bulk demo
    client.create_region('bulk_demo', 'Bulk operations demo')
    client.select_region('bulk_demo')
    
    # Bulk import
    entries = [
        {'key': 'item:1', 'value': {'name': 'Item 1', 'price': 10.99}},
        {'key': 'item:2', 'value': {'name': 'Item 2', 'price': 20.99}},
        {'key': 'item:3', 'value': {'name': 'Item 3', 'price': 30.99}, 'ttl': 3600},
        {'key': 'item:4', 'value': {'name': 'Item 4', 'price': 40.99}},
        {'key': 'item:5', 'value': {'name': 'Item 5', 'price': 50.99}}
    ]
    
    result = client.bulk_import(entries)
    print_result("Bulk import 5 items", result)
    
    # Bulk export
    exported = client.bulk_export(pattern='item:*')
    print_result("Bulk export item:*", f"{len(exported)} items")
    
    client.select_region('default')


def cleanup(client: KuberRestClient):
    """Clean up test data."""
    print_section("CLEANUP")
    
    # Purge and delete test regions
    regions_to_clean = ['products', 'orders', 'sessions', 'json_demo', 'bulk_demo']
    
    for region in regions_to_clean:
        try:
            client.purge_region(region)
            client.delete_region(region)
            print(f"  Cleaned up region: {region}")
        except Exception as e:
            print(f"  Warning cleaning {region}: {e}")
    
    # Clean default region
    client.select_region('default')
    try:
        # Delete known test keys
        for key in client.keys('*'):
            client.delete(key)
        print("  Cleaned default region")
    except Exception:
        pass


def main():
    """Main function to run all examples."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Kuber REST API Client Examples')
    parser.add_argument('--host', default='localhost', help='Kuber server host')
    parser.add_argument('--port', type=int, default=8080, help='HTTP port')
    parser.add_argument('--username', default=None, help='Authentication username')
    parser.add_argument('--password', default=None, help='Authentication password')
    parser.add_argument('--ssl', action='store_true', help='Use HTTPS')
    parser.add_argument('--no-cleanup', action='store_true', help='Skip cleanup')
    args = parser.parse_args()
    
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║       KUBER REST API CLIENT - COMPREHENSIVE EXAMPLES         ║
║                                                              ║
║  Connecting to: {'https' if args.ssl else 'http'}://{args.host}:{args.port}                       ║
╚══════════════════════════════════════════════════════════════╝
""")
    
    try:
        with KuberRestClient(
            args.host, 
            args.port,
            username=args.username,
            password=args.password,
            use_ssl=args.ssl
        ) as client:
            # Run all examples
            example_server_operations(client)
            example_basic_operations(client)
            example_mget_mset(client)
            example_key_pattern_search(client)
            example_region_operations(client)
            example_json_operations(client)
            example_json_deep_search(client)
            example_cross_region_search(client)
            example_hash_operations(client)
            example_bulk_operations(client)
            
            if not args.no_cleanup:
                cleanup(client)
            
            print_section("ALL EXAMPLES COMPLETED SUCCESSFULLY")
            
    except KuberRestException as e:
        logger.error(f"Kuber REST error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
