#!/usr/bin/env python3
"""
Kuber REST API - Generic Search Demo (v1.8.0)

This standalone script demonstrates ALL search capabilities of the Kuber
Generic Search API including:

1. Single key lookup
2. Multi-key lookup
3. Single key pattern (regex) search
4. Multi-pattern (regex) search
5. JSON attribute search with:
   - Simple equality
   - IN clause (list of values)
   - Regex matching
   - Comparison operators (gt, gte, lt, lte, eq, ne)
   - Combined AND logic across multiple attributes
6. Field projection (select specific fields)
7. Regex Key Search (v1.8.0):
   - keys_regex() - Keys only (faster)
   - search_keys() - Keys with values

Usage:
    python rest_generic_search_demo.py [--host HOST] [--port PORT] [--api-key KEY]

Example:
    python rest_generic_search_demo.py --host localhost --port 7070 --api-key kub_admin

Copyright (c) 2025-2030 Ashutosh Sinha. All Rights Reserved.
"""

import requests
import json
import argparse
from typing import Dict, List, Any, Optional
from datetime import datetime


class KuberSearchClient:
    """REST API client for Kuber Generic Search operations."""
    
    def __init__(self, host: str = "localhost", port: int = 7070, api_key: str = "kub_admin", use_ssl: bool = False):
        """
        Initialize the Kuber search client.
        
        Args:
            host: Kuber server hostname
            port: Kuber server port
            api_key: API key for authentication
            use_ssl: Use HTTPS instead of HTTP
        """
        protocol = "https" if use_ssl else "http"
        self.base_url = f"{protocol}://{host}:{port}/api/v1"
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/json",
            "X-API-Key": api_key
        }
    
    def generic_search(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a generic search request.
        
        Args:
            request: Search request dictionary
            
        Returns:
            Search results or error response
        """
        # Add API key to request body (alternative to header)
        request["apiKey"] = self.api_key
        
        url = f"{self.base_url}/genericsearch"
        try:
            response = requests.post(url, json=request, headers=self.headers, timeout=30)
            return {
                "status": response.status_code,
                "data": response.json() if response.content else None
            }
        except requests.exceptions.RequestException as e:
            return {"status": 0, "error": str(e)}
    
    def keys_regex(self, region: str, pattern: str, limit: int = 1000) -> Dict[str, Any]:
        """
        Find keys matching a regex pattern (keys only, no values).
        More efficient than search_keys when you only need key names.
        
        Args:
            region: Cache region
            pattern: Java regex pattern (e.g., "^user:\\d+$")
            limit: Maximum number of keys to return
            
        Returns:
            Dict with status and list of matching keys
        """
        url = f"{self.base_url.replace('/v1', '')}/cache/{region}/keys/regex"
        params = {"pattern": pattern, "limit": limit}
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            return {
                "status": response.status_code,
                "data": response.json() if response.content else []
            }
        except requests.exceptions.RequestException as e:
            return {"status": 0, "error": str(e)}
    
    def search_keys(self, region: str, pattern: str, limit: int = 1000) -> Dict[str, Any]:
        """
        Search keys by regex pattern and return key-value pairs.
        Returns full details including key, value, type, and TTL.
        
        Args:
            region: Cache region
            pattern: Java regex pattern (e.g., "^order:.*")
            limit: Maximum number of results
            
        Returns:
            Dict with status and list of dicts (key, value, type, ttl)
        """
        url = f"{self.base_url.replace('/v1', '')}/cache/{region}/ksearch"
        params = {"pattern": pattern, "limit": limit}
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            return {
                "status": response.status_code,
                "data": response.json() if response.content else []
            }
        except requests.exceptions.RequestException as e:
            return {"status": 0, "error": str(e)}
    
    def put_json(self, region: str, key: str, value: Dict[str, Any], ttl: int = -1) -> Dict[str, Any]:
        """
        Store a JSON document.
        
        Args:
            region: Cache region
            key: Document key
            value: JSON document
            ttl: Time to live in seconds (-1 for no expiration)
            
        Returns:
            API response
        """
        url = f"{self.base_url}/cache/{region}/{key}"
        params = {"ttl": ttl} if ttl > 0 else {}
        try:
            response = requests.post(url, json=value, headers=self.headers, params=params, timeout=30)
            return {"status": response.status_code, "success": response.status_code == 200}
        except requests.exceptions.RequestException as e:
            return {"status": 0, "error": str(e)}


def print_header(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def print_subheader(title: str):
    """Print a formatted subsection header."""
    print(f"\n--- {title} ---")


def print_request(request: Dict[str, Any]):
    """Print the request JSON."""
    # Remove apiKey for cleaner display
    display_request = {k: v for k, v in request.items() if k != "apiKey"}
    print(f"Request: {json.dumps(display_request, indent=2)}")


def print_result(result: Dict[str, Any], max_items: int = 5):
    """Print search results."""
    if result.get("error"):
        print(f"❌ Error: {result['error']}")
        return
    
    status = result.get("status", 0)
    data = result.get("data", [])
    
    if status == 200:
        count = len(data) if isinstance(data, list) else 1
        print(f"✅ Status: {status} | Results: {count}")
        
        if isinstance(data, list):
            for i, item in enumerate(data[:max_items]):
                print(f"  [{i+1}] {json.dumps(item, default=str)[:200]}")
            if len(data) > max_items:
                print(f"  ... and {len(data) - max_items} more results")
        else:
            print(f"  {json.dumps(data, default=str)[:200]}")
    else:
        print(f"❌ Status: {status} | Response: {data}")


def setup_test_data(client: KuberSearchClient, region: str):
    """Insert sample test data for search demonstrations."""
    print_header("SETUP: Inserting Test Data")
    
    # Sample customer data with various attributes
    customers = [
        {"id": "C001", "name": "John Smith", "email": "john@gmail.com", "status": "active", "city": "NYC", "tier": "premium", "age": 35, "balance": 15000.50},
        {"id": "C002", "name": "Jane Doe", "email": "jane@yahoo.com", "status": "active", "city": "LA", "tier": "standard", "age": 28, "balance": 5200.00},
        {"id": "C003", "name": "Bob Wilson", "email": "bob@gmail.com", "status": "pending", "city": "NYC", "tier": "premium", "age": 42, "balance": 32000.75},
        {"id": "C004", "name": "Alice Brown", "email": "alice@outlook.com", "status": "inactive", "city": "Chicago", "tier": "basic", "age": 31, "balance": 1500.25},
        {"id": "C005", "name": "Charlie Davis", "email": "charlie@gmail.com", "status": "active", "city": "Boston", "tier": "premium", "age": 45, "balance": 48000.00},
        {"id": "C006", "name": "Eva Martinez", "email": "eva@yahoo.com", "status": "pending", "city": "Miami", "tier": "standard", "age": 29, "balance": 7800.50},
        {"id": "C007", "name": "Frank Johnson", "email": "frank@company.com", "status": "active", "city": "NYC", "tier": "enterprise", "age": 52, "balance": 125000.00},
        {"id": "C008", "name": "Grace Lee", "email": "grace@gmail.com", "status": "trial", "city": "Seattle", "tier": "basic", "age": 24, "balance": 500.00},
        {"id": "C009", "name": "Henry Wang", "email": "henry@outlook.com", "status": "active", "city": "LA", "tier": "premium", "age": 38, "balance": 22000.00},
        {"id": "C010", "name": "Ivy Chen", "email": "ivy@company.com", "status": "active", "city": "SF", "tier": "enterprise", "age": 41, "balance": 95000.00},
    ]
    
    # Sample order data
    orders = [
        {"orderId": "ORD001", "customerId": "C001", "status": "shipped", "region": "US", "priority": "high", "amount": 450.00, "items": 3},
        {"orderId": "ORD002", "customerId": "C002", "status": "delivered", "region": "US", "priority": "normal", "amount": 125.50, "items": 1},
        {"orderId": "ORD003", "customerId": "C003", "status": "pending", "region": "CA", "priority": "urgent", "amount": 890.00, "items": 5},
        {"orderId": "ORD004", "customerId": "C001", "status": "shipped", "region": "MX", "priority": "high", "amount": 220.00, "items": 2},
        {"orderId": "ORD005", "customerId": "C005", "status": "processing", "region": "US", "priority": "normal", "amount": 1500.00, "items": 8},
    ]
    
    # Insert customers
    print(f"\nInserting {len(customers)} customers into region '{region}'...")
    for cust in customers:
        key = f"customer_{cust['id']}"
        result = client.put_json(region, key, cust)
        status = "✓" if result.get("success") else "✗"
        print(f"  {status} {key}")
    
    # Insert orders
    print(f"\nInserting {len(orders)} orders into region '{region}'...")
    for order in orders:
        key = f"order_{order['orderId']}"
        result = client.put_json(region, key, order)
        status = "✓" if result.get("success") else "✗"
        print(f"  {status} {key}")
    
    print(f"\n✅ Test data setup complete!")


def demo_key_lookups(client: KuberSearchClient, region: str):
    """Demonstrate key-based lookups."""
    print_header("1. KEY-BASED LOOKUPS")
    
    # 1a. Single key lookup
    print_subheader("1a. Single Key Lookup")
    request = {
        "region": region,
        "key": "customer_C001"
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 1b. Multi-key lookup
    print_subheader("1b. Multi-Key Lookup (3 keys)")
    request = {
        "region": region,
        "keys": ["customer_C001", "customer_C003", "customer_C005"]
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 1c. Multi-key lookup with field projection
    print_subheader("1c. Multi-Key Lookup with Field Projection")
    request = {
        "region": region,
        "keys": ["customer_C001", "customer_C002", "customer_C003"],
        "fields": ["name", "email", "status"]
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)


def demo_pattern_searches(client: KuberSearchClient, region: str):
    """Demonstrate regex pattern searches."""
    print_header("2. REGEX PATTERN SEARCHES")
    
    # 2a. Single pattern search
    print_subheader("2a. Single Pattern - All customers")
    request = {
        "region": region,
        "keypattern": "customer_.*"
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 2b. Single pattern - Specific prefix
    print_subheader("2b. Single Pattern - Orders only")
    request = {
        "region": region,
        "keypattern": "order_.*"
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 2c. Multi-pattern search (OR logic between patterns)
    print_subheader("2c. Multi-Pattern - Customers C001-C003 OR Orders")
    request = {
        "region": region,
        "keypatterns": ["customer_C00[1-3]", "order_.*"]
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)


def demo_json_equality_search(client: KuberSearchClient, region: str):
    """Demonstrate JSON attribute search with equality."""
    print_header("3. JSON SEARCH - EQUALITY")
    
    # 3a. Single attribute equality
    print_subheader("3a. Single Attribute - status = 'active'")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "status": "active"
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 3b. Multiple attributes (AND logic)
    print_subheader("3b. Multiple Attributes (AND) - status='active' AND city='NYC'")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "status": "active",
            "city": "NYC"
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 3c. Three attributes (AND logic)
    print_subheader("3c. Three Attributes (AND) - status='active' AND city='NYC' AND tier='premium'")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "status": "active",
            "city": "NYC",
            "tier": "premium"
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)


def demo_json_in_clause(client: KuberSearchClient, region: str):
    """Demonstrate JSON attribute search with IN clause (list of values)."""
    print_header("4. JSON SEARCH - IN CLAUSE (List of Values)")
    
    # 4a. Single attribute with list
    print_subheader("4a. Single IN Clause - status IN ['active', 'pending']")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "status": ["active", "pending"]
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 4b. Single attribute with longer list
    print_subheader("4b. Single IN Clause - tier IN ['premium', 'enterprise']")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "tier": ["premium", "enterprise"]
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 4c. Multiple IN clauses (AND between attributes, OR within each list)
    print_subheader("4c. Multiple IN Clauses - status IN [...] AND city IN [...]")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "status": ["active", "pending"],
            "city": ["NYC", "LA", "SF"]
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 4d. IN clause combined with equality
    print_subheader("4d. IN Clause + Equality - tier IN [...] AND status='active'")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "tier": ["premium", "enterprise"],
            "status": "active"
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)


def demo_json_regex_search(client: KuberSearchClient, region: str):
    """Demonstrate JSON attribute search with regex."""
    print_header("5. JSON SEARCH - REGEX MATCHING")
    
    # 5a. Email regex - Gmail users
    print_subheader("5a. Regex - Gmail users (email matches '.*@gmail.com')")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "email": {"regex": ".*@gmail\\.com"}
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 5b. Regex with other conditions
    print_subheader("5b. Regex + Equality - Gmail users who are active")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "email": {"regex": ".*@gmail\\.com"},
            "status": "active"
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 5c. Regex + IN clause
    print_subheader("5c. Regex + IN Clause - Gmail/Yahoo users with premium/enterprise tier")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "email": {"regex": ".*@(gmail|yahoo)\\.com"},
            "tier": ["premium", "enterprise"]
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 5d. Name pattern regex
    print_subheader("5d. Regex - Names starting with 'J'")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "name": {"regex": "^J.*"}
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)


def demo_json_comparison_operators(client: KuberSearchClient, region: str):
    """Demonstrate JSON attribute search with comparison operators."""
    print_header("6. JSON SEARCH - COMPARISON OPERATORS")
    
    # 6a. Greater than
    print_subheader("6a. Greater Than - balance > 20000")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "balance": {"gt": 20000}
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 6b. Less than or equal
    print_subheader("6b. Less Than or Equal - age <= 30")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "age": {"lte": 30}
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 6c. Range query (between)
    print_subheader("6c. Range Query - age >= 30 AND age <= 45")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "age": {"gte": 30, "lte": 45}
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 6d. Comparison + other conditions
    print_subheader("6d. Comparison + IN Clause - balance > 10000 AND status IN ['active', 'pending']")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "balance": {"gt": 10000},
            "status": ["active", "pending"]
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 6e. Multiple comparison operators
    print_subheader("6e. Multiple Comparisons - balance >= 5000 AND age < 40")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "balance": {"gte": 5000},
            "age": {"lt": 40}
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)


def demo_complex_combined_searches(client: KuberSearchClient, region: str):
    """Demonstrate complex combined search scenarios."""
    print_header("7. COMPLEX COMBINED SEARCHES")
    
    # 7a. All operators combined
    print_subheader("7a. All Operators Combined")
    print("    - status IN ['active', 'pending']")
    print("    - city IN ['NYC', 'LA', 'SF']")
    print("    - tier = 'premium'")
    print("    - balance > 10000")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "status": ["active", "pending"],
            "city": ["NYC", "LA", "SF"],
            "tier": "premium",
            "balance": {"gt": 10000}
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 7b. Regex + Range + IN
    print_subheader("7b. Regex + Range + IN Clause")
    print("    - email matches Gmail/Yahoo")
    print("    - age between 25-45")
    print("    - status IN ['active', 'trial']")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "email": {"regex": ".*@(gmail|yahoo)\\.com"},
            "age": {"gte": 25, "lte": 45},
            "status": ["active", "trial"]
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 7c. Orders search - complex
    print_subheader("7c. Orders Search - Multiple Conditions")
    print("    - status IN ['shipped', 'delivered']")
    print("    - region IN ['US', 'CA']")
    print("    - amount > 200")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "status": ["shipped", "delivered"],
            "region": ["US", "CA"],
            "amount": {"gt": 200}
        }
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)


def demo_field_projection(client: KuberSearchClient, region: str):
    """Demonstrate field projection with various searches."""
    print_header("8. FIELD PROJECTION (Select Specific Fields)")
    
    # 8a. Key lookup with projection
    print_subheader("8a. Key Lookup - Return only name, email, status")
    request = {
        "region": region,
        "key": "customer_C001",
        "fields": ["name", "email", "status"]
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 8b. JSON search with projection
    print_subheader("8b. JSON Search with Projection - Active users, return name and balance only")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "status": "active"
        },
        "fields": ["name", "balance"]
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)
    
    # 8c. Complex search with projection
    print_subheader("8c. Complex Search with Projection")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "tier": ["premium", "enterprise"],
            "balance": {"gt": 20000}
        },
        "fields": ["name", "tier", "balance", "city"]
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result)


def demo_limit_results(client: KuberSearchClient, region: str):
    """Demonstrate limiting search results."""
    print_header("9. LIMITING RESULTS")
    
    # 9a. Limit to 3 results
    print_subheader("9a. All Customers, Limit 3")
    request = {
        "region": region,
        "keypattern": "customer_.*",
        "limit": 3
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result, max_items=10)
    
    # 9b. JSON search with limit
    print_subheader("9b. Active Users, Limit 2")
    request = {
        "region": region,
        "type": "json",
        "attributes": {
            "status": "active"
        },
        "limit": 2
    }
    print_request(request)
    result = client.generic_search(request)
    print_result(result, max_items=10)


def demo_regex_key_search(client: KuberSearchClient, region: str):
    """Demonstrate regex key search APIs (v1.8.0)."""
    print_header("10. REGEX KEY SEARCH (v1.8.0 - REST API)")
    
    print("""
    New in v1.8.0: Two dedicated REST endpoints for regex-based key search:
    
    1. keys_regex() - Returns KEYS ONLY (faster, no value retrieval)
       Endpoint: GET /api/cache/{region}/keys/regex?pattern={regex}&limit={limit}
    
    2. search_keys() - Returns KEYS + VALUES (full details)
       Endpoint: GET /api/cache/{region}/ksearch?pattern={regex}&limit={limit}
    """)
    
    # 10a. Keys only - find all customer keys
    print_subheader("10a. keys_regex() - Find Customer Keys (keys only)")
    print("    Pattern: ^customer_C00[1-5]$")
    print("    Returns: List of matching key names only")
    result = client.keys_regex(region, r"^customer_C00[1-5]$")
    print(f"    URL: GET /api/cache/{region}/keys/regex?pattern=^customer_C00[1-5]$")
    print_result(result, max_items=10)
    
    # 10b. Keys only - find all order keys
    print_subheader("10b. keys_regex() - Find Order Keys")
    print("    Pattern: ^order_.*")
    result = client.keys_regex(region, r"^order_.*")
    print(f"    URL: GET /api/cache/{region}/keys/regex?pattern=^order_.*")
    print_result(result, max_items=10)
    
    # 10c. Keys with values - customer search
    print_subheader("10c. search_keys() - Customer Keys with Values")
    print("    Pattern: ^customer_C00[1-3]$")
    print("    Returns: key, value, type, ttl for each match")
    result = client.search_keys(region, r"^customer_C00[1-3]$")
    print(f"    URL: GET /api/cache/{region}/ksearch?pattern=^customer_C00[1-3]$")
    print_result(result, max_items=5)
    
    # 10d. Keys with values - order search with limit
    print_subheader("10d. search_keys() - Orders with Limit")
    print("    Pattern: ^order_ORD00.*")
    print("    Limit: 3")
    result = client.search_keys(region, r"^order_ORD00.*", limit=3)
    print(f"    URL: GET /api/cache/{region}/ksearch?pattern=^order_ORD00.*&limit=3")
    print_result(result, max_items=5)
    
    # 10e. Complex regex pattern
    print_subheader("10e. Complex Regex - Keys ending with digits 1-5")
    print("    Pattern: .*(1|2|3|4|5)$")
    result = client.keys_regex(region, r".*(1|2|3|4|5)$", limit=10)
    print_result(result, max_items=10)
    
    # 10f. Performance comparison note
    print_subheader("10f. Performance Comparison")
    print("""
    ┌─────────────────┬────────────────────────────────────────────────────┐
    │ Method          │ Use Case                                           │
    ├─────────────────┼────────────────────────────────────────────────────┤
    │ keys_regex()    │ When you only need key names (faster, less data)   │
    │                 │ Example: Count keys, list for deletion, pagination │
    ├─────────────────┼────────────────────────────────────────────────────┤
    │ search_keys()   │ When you need key + value + metadata               │
    │                 │ Example: Display results, export data, debugging   │
    └─────────────────┴────────────────────────────────────────────────────┘
    
    Both methods use in-memory KeyIndex for O(1) key lookups!
    """)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Kuber REST API Generic Search Demo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python rest_generic_search_demo.py
    python rest_generic_search_demo.py --host 192.168.1.100 --port 7070
    python rest_generic_search_demo.py --api-key kub_mykey --ssl
        """
    )
    parser.add_argument("--host", default="localhost", help="Kuber server host (default: localhost)")
    parser.add_argument("--port", type=int, default=7070, help="Kuber server port (default: 7070)")
    parser.add_argument("--api-key", default="kub_admin", help="API key (default: kub_admin)")
    parser.add_argument("--ssl", action="store_true", help="Use HTTPS")
    parser.add_argument("--region", default="search_demo", help="Region name (default: search_demo)")
    parser.add_argument("--skip-setup", action="store_true", help="Skip test data setup")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("  KUBER REST API - GENERIC SEARCH DEMO (v1.8.0)")
    print("=" * 80)
    print(f"  Server:  {args.host}:{args.port}")
    print(f"  Region:  {args.region}")
    print(f"  SSL:     {'Yes' if args.ssl else 'No'}")
    print(f"  Time:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Initialize client
    client = KuberSearchClient(
        host=args.host,
        port=args.port,
        api_key=args.api_key,
        use_ssl=args.ssl
    )
    
    region = args.region
    
    try:
        # Setup test data (unless skipped)
        if not args.skip_setup:
            setup_test_data(client, region)
        
        # Run all demonstrations
        demo_key_lookups(client, region)
        demo_pattern_searches(client, region)
        demo_json_equality_search(client, region)
        demo_json_in_clause(client, region)
        demo_json_regex_search(client, region)
        demo_json_comparison_operators(client, region)
        demo_complex_combined_searches(client, region)
        demo_field_projection(client, region)
        demo_limit_results(client, region)
        demo_regex_key_search(client, region)  # New in v1.8.0
        
        # Summary
        print_header("DEMO COMPLETE")
        print("""
This demo showcased all Generic Search API capabilities:

  1. KEY LOOKUPS
     - Single key: {"key": "abc"}
     - Multi-key: {"keys": ["a", "b", "c"]}

  2. PATTERN SEARCHES (Regex via Generic Search)
     - Single pattern: {"keypattern": "user_.*"}
     - Multi-pattern: {"keypatterns": ["user_.*", "admin_.*"]}

  3. JSON ATTRIBUTE SEARCH
     - Equality: {"attributes": {"status": "active"}}
     - IN clause: {"attributes": {"status": ["active", "pending"]}}
     - Regex: {"attributes": {"email": {"regex": ".*@gmail.com"}}}
     - Comparisons: {"attributes": {"age": {"gt": 25, "lte": 50}}}

  4. COMBINING CRITERIA (AND logic)
     - All criteria in "attributes" are ANDed together
     - Each list value within an attribute uses OR (IN clause)

  5. FIELD PROJECTION
     - Return specific fields: {"fields": ["name", "email"]}

  6. LIMITING RESULTS
     - Max results: {"limit": 100}

  7. REGEX KEY SEARCH (v1.8.0 - REST API only)
     - Keys only:   GET /api/cache/{region}/keys/regex?pattern={regex}
     - Keys+Values: GET /api/cache/{region}/ksearch?pattern={regex}
     - Python:      client.keys_regex(pattern) / client.search_keys(pattern)
     - Java:        client.keysRegex(pattern) / client.ksearch(pattern)
     - C#:          client.KeysRegexAsync(pattern) / client.SearchKeysAsync(pattern)

API Endpoints:
  - Generic Search: POST /api/v1/genericsearch
  - Keys by Regex:  GET /api/cache/{region}/keys/regex
  - Key Search:     GET /api/cache/{region}/ksearch
        """)
        
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
