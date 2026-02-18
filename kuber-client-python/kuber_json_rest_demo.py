#!/usr/bin/env python3
# Copyright © 2025-2030, All Rights Reserved
# Ashutosh Sinha | Email: ajsinha@gmail.com
#
# Kuber Distributed Cache - JSON Operations via REST API Demo
#
# This standalone script demonstrates all JSON-related operations
# using the Kuber REST API (HTTP/JSON protocol):
#
#   a) Store JSON documents (PUT /api/v1/json/{region}/{key})
#   b) Retrieve JSON documents (GET /api/v1/json/{region}/{key})
#   c) Retrieve JSON at specific JSONPath (GET with ?path=)
#   d) Update JSON documents (partial merge)
#   e) Search JSON with multiple operators:
#      - Equality:     $.field=value
#      - Comparison:   $.field>value, $.field<value, $.field>=value, $.field<=value
#      - Inequality:   $.field!=value
#      - Pattern:      $.field LIKE %pattern%
#      - Array:        $.array CONTAINS value
#      - Combined:     $.field1=value1,$.field2>value2
#   f) Store JSON with TTL (time-to-live)
#   g) Delete JSON documents (DELETE /api/v1/json/{region}/{key})
#   h) Cross-region JSON search
#
# Usage:
#     python kuber_json_rest_demo.py [host] [port] [apiKey]
#
# Example:
#     python kuber_json_rest_demo.py localhost 8080 kub_admin_sample_key_replace_me
#
# Dependencies: None (uses only Python standard library)
#
# @version 2.6.0

import json
import os
import sys
import urllib.request
import urllib.parse
import urllib.error
from typing import Any, Dict, List, Optional


# ============================================================================
# CONFIGURATION - Update these values for your environment
# ============================================================================
KUBER_HOST = os.getenv('KUBER_HOST', 'localhost')
KUBER_PORT = int(os.getenv('KUBER_PORT', '8080'))
KUBER_API_KEY = os.getenv('KUBER_API_KEY', 'kub_admin_sample_key_replace_me')


# ============================================================================
# KUBER JSON REST CLIENT
# ============================================================================

class KuberJsonRestClient:
    """
    Lightweight Kuber REST client for JSON operations demo.
    Uses only Python standard library (urllib).
    """

    def __init__(self, host: str, port: int, api_key: str):
        if not api_key or not api_key.startswith('kub_'):
            raise ValueError("API key must start with 'kub_'")
        self.base_url = f"http://{host}:{port}/api/v1"
        self.api_key = api_key

    # ==================== HTTP Helpers ====================

    def _request(self, method: str, path: str, body: Any = None,
                 params: Optional[Dict[str, str]] = None) -> Optional[Dict]:
        """
        Send an HTTP request and return parsed JSON response.

        Args:
            method: HTTP method (GET, PUT, POST, DELETE)
            path: API path (appended to base_url)
            body: Request body (will be JSON-encoded)
            params: Query parameters

        Returns:
            Parsed JSON response dict, or None
        """
        url = self.base_url + path
        if params:
            url += '?' + urllib.parse.urlencode(params)

        data = None
        if body is not None:
            data = json.dumps(body).encode('utf-8')

        headers = {
            'X-API-Key': self.api_key,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }

        req = urllib.request.Request(url, data=data, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                content = resp.read()
                if content:
                    return json.loads(content.decode('utf-8'))
                return {}
        except urllib.error.HTTPError as e:
            body_text = ''
            if e.fp:
                body_text = e.fp.read().decode('utf-8', errors='replace')
            print(f"    ⚠ HTTP {e.code}: {body_text[:200]}")
            return None
        except urllib.error.URLError as e:
            print(f"    ⚠ Connection error: {e.reason}")
            return None

    # ==================== Server Operations ====================

    def ping(self) -> bool:
        """Ping the server to verify connectivity."""
        result = self._request('GET', '/ping')
        return result is not None and result.get('status') == 'OK'

    def info(self) -> Optional[Dict]:
        """Get server info."""
        return self._request('GET', '/info')

    # ==================== Region Operations ====================

    def create_region(self, name: str, description: str = '') -> bool:
        """Create a region."""
        result = self._request('POST', '/regions', {
            'name': name,
            'description': description
        })
        return result is not None

    def delete_region(self, name: str) -> bool:
        """Delete a region."""
        result = self._request('DELETE', f'/regions/{name}')
        return result is not None

    # ==================== JSON Operations ====================

    def json_set(self, key: str, value: Any, region: str,
                 ttl: Optional[int] = None) -> bool:
        """
        Store a JSON document.

        Args:
            key: Cache key
            value: Python object to store as JSON
            region: Region name
            ttl: Optional TTL in seconds

        Returns:
            True if stored successfully
        """
        data = {'value': value}
        if ttl is not None:
            data['ttl'] = ttl
        result = self._request('PUT', f'/json/{region}/{urllib.parse.quote(key, safe="")}', data)
        return result is not None

    def json_get(self, key: str, region: str, path: str = '$') -> Any:
        """
        Get a JSON document or a specific path within it.

        Args:
            key: Cache key
            region: Region name
            path: JSONPath expression (default: $ for root document)

        Returns:
            Parsed JSON value, or None
        """
        params = {'path': path} if path != '$' else None
        result = self._request('GET', f'/json/{region}/{urllib.parse.quote(key, safe="")}', params=params)
        if result is None:
            return None
        if isinstance(result, dict):
            return result.get('value', result)
        return result

    def json_delete(self, key: str, region: str, path: str = '$') -> bool:
        """
        Delete a JSON document or a specific path within it.

        Args:
            key: Cache key
            region: Region name
            path: JSONPath expression (default: $ to delete entire document)

        Returns:
            True if deleted successfully
        """
        params = {'path': path} if path != '$' else None
        result = self._request('DELETE', f'/json/{region}/{urllib.parse.quote(key, safe="")}', params=params)
        return result is not None

    def json_search(self, query: str, region: str) -> List[Dict]:
        """
        Search JSON documents using query operators.

        Supported query operators:
        - Equality:       $.field=value
        - Greater than:   $.field>value   or $.field>=value
        - Less than:      $.field<value   or $.field<=value
        - Inequality:     $.field!=value
        - Pattern match:  $.field LIKE %pattern%
        - Array contains: $.array CONTAINS value
        - Combined (AND): $.field1=value1,$.field2>value2

        Args:
            query: Search query string
            region: Region name to search in

        Returns:
            List of matching documents [{key, value}, ...]
        """
        result = self._request('POST', f'/json/{region}/search', {'query': query})
        if result is None:
            return []
        return result.get('results', [])

    def cache_set(self, key: str, value: str, region: str,
                  ttl: Optional[int] = None) -> bool:
        """Store a simple string value via cache API."""
        data = {'value': value}
        if ttl:
            data['ttl'] = ttl
        result = self._request('POST', f'/cache/{region}/{urllib.parse.quote(key, safe="")}', data)
        return result is not None

    def cache_get(self, key: str, region: str) -> Optional[str]:
        """Get a simple string value via cache API."""
        result = self._request('GET', f'/cache/{region}/{urllib.parse.quote(key, safe="")}')
        if result is None:
            return None
        return result.get('value')

    def cache_delete(self, key: str, region: str) -> bool:
        """Delete a key via cache API."""
        result = self._request('DELETE', f'/cache/{region}/{urllib.parse.quote(key, safe="")}')
        return result is not None

    def region_size(self, region: str) -> int:
        """Get the number of keys in a region."""
        result = self._request('GET', f'/cache/{region}/size')
        if result:
            return result.get('size', 0)
        return 0


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def print_header(title: str):
    """Print section header."""
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def print_subheader(title: str):
    """Print sub-section header."""
    print(f"\n  --- {title} ---")


def print_json(label: str, value: Any, indent: int = 4):
    """Print a JSON value with label."""
    if isinstance(value, (dict, list)):
        formatted = json.dumps(value, indent=2, default=str)
        # Indent each line for alignment
        lines = formatted.split('\n')
        prefix = ' ' * indent
        print(f"{prefix}{label}:")
        for line in lines:
            print(f"{prefix}  {line}")
    else:
        print(f"{' ' * indent}{label}: {value}")


def print_results(results: List[Dict], fields: Optional[List[str]] = None):
    """Print search results in a formatted way."""
    print(f"    Found {len(results)} result(s):")
    for r in results:
        key = r.get('key', 'N/A')
        val = r.get('value', {})
        if fields and isinstance(val, dict):
            parts = [f"{f}={val.get(f, 'N/A')}" for f in fields]
            print(f"      - {key}: {', '.join(parts)}")
        elif isinstance(val, dict):
            name = val.get('name', val.get('product_name', 'N/A'))
            print(f"      - {key}: {name}")
        else:
            print(f"      - {key}: {val}")


# ============================================================================
# SAMPLE DATA
# ============================================================================

EMPLOYEES = [
    {
        "id": "EMP001",
        "name": "John Smith",
        "email": "john.smith@company.com",
        "department": "Engineering",
        "salary": 95000,
        "level": "Senior",
        "skills": ["Python", "Java", "Kubernetes"],
        "address": {
            "city": "San Francisco",
            "state": "CA",
            "zip": "94102"
        },
        "active": True
    },
    {
        "id": "EMP002",
        "name": "Jane Doe",
        "email": "jane.doe@company.com",
        "department": "Engineering",
        "salary": 105000,
        "level": "Lead",
        "skills": ["JavaScript", "React", "Node.js"],
        "address": {
            "city": "New York",
            "state": "NY",
            "zip": "10001"
        },
        "active": True
    },
    {
        "id": "EMP003",
        "name": "Bob Johnson",
        "email": "bob.j@company.com",
        "department": "Sales",
        "salary": 85000,
        "level": "Senior",
        "skills": ["CRM", "Negotiation"],
        "address": {
            "city": "Chicago",
            "state": "IL",
            "zip": "60601"
        },
        "active": True
    },
    {
        "id": "EMP004",
        "name": "Alice Brown",
        "email": "alice.brown@company.com",
        "department": "Engineering",
        "salary": 115000,
        "level": "Principal",
        "skills": ["Python", "Machine Learning", "TensorFlow"],
        "address": {
            "city": "Seattle",
            "state": "WA",
            "zip": "98101"
        },
        "active": False
    },
    {
        "id": "EMP005",
        "name": "Charlie Wilson",
        "email": "charlie.w@company.com",
        "department": "Marketing",
        "salary": 78000,
        "level": "Junior",
        "skills": ["SEO", "Content Marketing"],
        "address": {
            "city": "Los Angeles",
            "state": "CA",
            "zip": "90001"
        },
        "active": True
    }
]

PRODUCTS = [
    {
        "product_id": "PROD001",
        "product_name": "Mechanical Keyboard Pro",
        "category": "Electronics",
        "subcategory": "Peripherals",
        "brand": "TechGear",
        "price": 149.99,
        "in_stock": True,
        "stock_count": 230,
        "tags": ["gaming", "mechanical", "rgb"],
        "specs": {
            "switch_type": "Cherry MX Blue",
            "backlighting": "RGB",
            "connectivity": "USB-C"
        }
    },
    {
        "product_id": "PROD002",
        "product_name": "Wireless Noise-Cancelling Headphones",
        "category": "Electronics",
        "subcategory": "Audio",
        "brand": "SoundTech",
        "price": 299.99,
        "in_stock": True,
        "stock_count": 85,
        "tags": ["wireless", "noise-cancelling", "bluetooth"],
        "specs": {
            "driver_size": "40mm",
            "battery_life": "30h",
            "connectivity": "Bluetooth 5.2"
        }
    },
    {
        "product_id": "PROD003",
        "product_name": "Ergonomic Office Chair",
        "category": "Furniture",
        "subcategory": "Chairs",
        "brand": "ComfortPlus",
        "price": 449.00,
        "in_stock": True,
        "stock_count": 42,
        "tags": ["ergonomic", "office", "adjustable"],
        "specs": {
            "material": "Mesh",
            "lumbar_support": True,
            "weight_capacity": "300lbs"
        }
    },
    {
        "product_id": "PROD004",
        "product_name": "USB-C Hub Adapter",
        "category": "Electronics",
        "subcategory": "Accessories",
        "brand": "TechGear",
        "price": 39.99,
        "in_stock": False,
        "stock_count": 0,
        "tags": ["usb-c", "hub", "adapter"],
        "specs": {
            "ports": "7-in-1",
            "hdmi": True,
            "power_delivery": "100W"
        }
    },
    {
        "product_id": "PROD005",
        "product_name": "Standing Desk Converter",
        "category": "Furniture",
        "subcategory": "Desks",
        "brand": "ErgoRise",
        "price": 279.00,
        "in_stock": True,
        "stock_count": 118,
        "tags": ["standing-desk", "ergonomic", "adjustable"],
        "specs": {
            "surface_area": "36x24 inches",
            "height_range": "6-17 inches",
            "weight_capacity": "35lbs"
        }
    }
]


# ============================================================================
# DEMO FUNCTIONS
# ============================================================================

def demo_a_store_json(client: KuberJsonRestClient, region: str):
    """Demo a) Store JSON documents via REST PUT."""
    print_header("A) STORE JSON DOCUMENTS (PUT /api/v1/json/{region}/{key})")

    print("\n  Storing 5 employee records...")
    for emp in EMPLOYEES:
        key = f"employee:{emp['id']}"
        success = client.json_set(key, emp, region)
        status = "OK" if success else "FAIL"
        print(f"    [{status}] {key} -> {emp['name']} ({emp['department']}, ${emp['salary']:,})")

    print(f"\n  Region size: {client.region_size(region)} entries")


def demo_b_retrieve_json(client: KuberJsonRestClient, region: str):
    """Demo b) Retrieve JSON documents via REST GET."""
    print_header("B) RETRIEVE JSON DOCUMENTS (GET /api/v1/json/{region}/{key})")

    # Retrieve full document
    print_subheader("Full Document Retrieval")
    emp = client.json_get("employee:EMP001", region)
    if emp:
        print_json("employee:EMP001", emp)

    # Retrieve another document
    emp2 = client.json_get("employee:EMP003", region)
    if emp2:
        print(f"\n    employee:EMP003 -> {emp2.get('name')}, {emp2.get('department')}")

    # Non-existent key
    missing = client.json_get("employee:EMP999", region)
    print(f"\n    employee:EMP999 (missing) -> {missing}")


def demo_c_retrieve_json_path(client: KuberJsonRestClient, region: str):
    """Demo c) Retrieve specific JSON paths via REST GET with ?path= parameter."""
    print_header("C) RETRIEVE JSON AT SPECIFIC PATH (GET with ?path=)")

    key = "employee:EMP001"
    paths = [
        ("$.name", "Name"),
        ("$.salary", "Salary"),
        ("$.department", "Department"),
        ("$.skills", "Skills"),
        ("$.address", "Address (nested)"),
        ("$.address.city", "City (deep path)"),
        ("$.address.state", "State (deep path)"),
        ("$.active", "Active status"),
    ]

    print(f"\n  Querying paths on '{key}':\n")
    for path, label in paths:
        value = client.json_get(key, region, path=path)
        print(f"    {label:22s} ({path:22s}) -> {value}")


def demo_d_update_json(client: KuberJsonRestClient, region: str):
    """Demo d) Update JSON documents via REST PUT (full replace)."""
    print_header("D) UPDATE JSON DOCUMENTS (PUT - full replace)")

    key = "employee:EMP005"

    # Show original
    original = client.json_get(key, region)
    print(f"\n  Original: {original.get('name')} - salary=${original.get('salary'):,}, " +
          f"dept={original.get('department')}")

    # Update with new values (full replace)
    updated = dict(original)
    updated['salary'] = 92000
    updated['department'] = 'Engineering'
    updated['level'] = 'Senior'
    updated['skills'] = ['SEO', 'Content Marketing', 'Python', 'Data Analytics']

    success = client.json_set(key, updated, region)
    print(f"\n  Updated (PUT replace): {'OK' if success else 'FAIL'}")

    # Verify the update
    after = client.json_get(key, region)
    print(f"  After:    {after.get('name')} - salary=${after.get('salary'):,}, " +
          f"dept={after.get('department')}")
    print(f"            skills={after.get('skills')}")

    # Restore original
    client.json_set(key, original, region)
    print(f"\n  Restored original values for subsequent demos.")


def demo_e_search_equality(client: KuberJsonRestClient, region: str):
    """Demo e) JSON search with equality operator."""
    print_header("E) JSON SEARCH - EQUALITY ($.field=value)")

    # Search by department
    print_subheader("Search: $.department=Engineering")
    results = client.json_search("$.department=Engineering", region)
    print_results(results, ['name', 'department', 'salary'])

    # Search by boolean
    print_subheader("Search: $.active=true")
    results = client.json_search("$.active=true", region)
    print_results(results, ['name', 'active'])

    # Search by nested field
    print_subheader("Search: $.address.state=CA")
    results = client.json_search("$.address.state=CA", region)
    print_results(results, ['name', 'address'])

    # Search by level
    print_subheader("Search: $.level=Senior")
    results = client.json_search("$.level=Senior", region)
    print_results(results, ['name', 'department', 'level'])


def demo_f_search_comparison(client: KuberJsonRestClient, region: str):
    """Demo f) JSON search with comparison operators."""
    print_header("F) JSON SEARCH - COMPARISON (>, <, >=, <=)")

    # Greater than
    print_subheader("Search: $.salary>100000")
    results = client.json_search("$.salary>100000", region)
    print_results(results, ['name', 'salary'])

    # Less than
    print_subheader("Search: $.salary<90000")
    results = client.json_search("$.salary<90000", region)
    print_results(results, ['name', 'salary'])

    # Greater than or equal
    print_subheader("Search: $.salary>=95000")
    results = client.json_search("$.salary>=95000", region)
    print_results(results, ['name', 'salary'])

    # Less than or equal
    print_subheader("Search: $.salary<=85000")
    results = client.json_search("$.salary<=85000", region)
    print_results(results, ['name', 'salary'])


def demo_g_search_inequality(client: KuberJsonRestClient, region: str):
    """Demo g) JSON search with inequality operator."""
    print_header("G) JSON SEARCH - INEQUALITY ($.field!=value)")

    # Not equal to department
    print_subheader("Search: $.department!=Engineering")
    results = client.json_search("$.department!=Engineering", region)
    print_results(results, ['name', 'department'])

    # Not equal to active status
    print_subheader("Search: $.active!=true")
    results = client.json_search("$.active!=true", region)
    print_results(results, ['name', 'active'])


def demo_h_search_pattern(client: KuberJsonRestClient, region: str):
    """Demo h) JSON search with LIKE pattern matching."""
    print_header("H) JSON SEARCH - PATTERN MATCH ($.field LIKE %pattern%)")

    # Name containing pattern
    print_subheader("Search: $.name LIKE %son%")
    results = client.json_search("$.name LIKE %son%", region)
    print_results(results, ['name'])

    # Email domain pattern
    print_subheader("Search: $.email LIKE %@company.com")
    results = client.json_search("$.email LIKE %@company.com", region)
    print_results(results, ['name', 'email'])

    # Zip code pattern
    print_subheader("Search: $.address.zip LIKE 9%")
    results = client.json_search("$.address.zip LIKE 9%", region)
    print_results(results, ['name', 'address'])


def demo_i_search_contains(client: KuberJsonRestClient, region: str):
    """Demo i) JSON search with CONTAINS for arrays."""
    print_header("I) JSON SEARCH - ARRAY CONTAINS ($.array CONTAINS value)")

    # Skills containing Python
    print_subheader("Search: $.skills CONTAINS Python")
    results = client.json_search("$.skills CONTAINS Python", region)
    print_results(results, ['name', 'skills'])

    # Skills containing CRM
    print_subheader("Search: $.skills CONTAINS CRM")
    results = client.json_search("$.skills CONTAINS CRM", region)
    print_results(results, ['name', 'skills'])

    # Skills containing JavaScript
    print_subheader("Search: $.skills CONTAINS JavaScript")
    results = client.json_search("$.skills CONTAINS JavaScript", region)
    print_results(results, ['name', 'skills'])


def demo_j_search_combined(client: KuberJsonRestClient, region: str):
    """Demo j) JSON search with combined conditions (AND logic)."""
    print_header("J) JSON SEARCH - COMBINED CONDITIONS (AND logic)")

    # Department AND salary range
    print_subheader("Search: $.department=Engineering,$.salary>100000")
    results = client.json_search("$.department=Engineering,$.salary>100000", region)
    print_results(results, ['name', 'department', 'salary'])

    # Active AND in California
    print_subheader("Search: $.active=true,$.address.state=CA")
    results = client.json_search("$.active=true,$.address.state=CA", region)
    print_results(results, ['name', 'active', 'address'])

    # Department AND active AND salary
    print_subheader("Search: $.department=Engineering,$.active=true,$.salary>=95000")
    results = client.json_search("$.department=Engineering,$.active=true,$.salary>=95000", region)
    print_results(results, ['name', 'department', 'salary', 'active'])

    # State AND level
    print_subheader("Search: $.address.state!=CA,$.level=Senior")
    results = client.json_search("$.address.state!=CA,$.level=Senior", region)
    print_results(results, ['name', 'level', 'address'])


def demo_k_json_with_ttl(client: KuberJsonRestClient, region: str):
    """Demo k) Store JSON documents with TTL."""
    print_header("K) STORE JSON WITH TTL (time-to-live)")

    # Store a session document with 60-second TTL
    session = {
        "session_id": "sess:ABC123",
        "user": "jsmith",
        "ip": "192.168.1.100",
        "login_time": "2025-01-15T10:30:00Z",
        "permissions": ["read", "write", "admin"]
    }

    print("\n  Storing session with 60-second TTL...")
    success = client.json_set("session:ABC123", session, region, ttl=60)
    print(f"    Store: {'OK' if success else 'FAIL'}")

    # Verify it exists
    retrieved = client.json_get("session:ABC123", region)
    print(f"    Verify: user={retrieved.get('user')}, perms={retrieved.get('permissions')}")

    # Store a cache entry with 300-second TTL
    api_result = {
        "cached_at": "2025-01-15T10:30:00Z",
        "data": {"rates": {"USD": 1.0, "EUR": 0.85, "GBP": 0.73}},
        "source": "exchange-api"
    }

    print("\n  Storing API cache entry with 300-second TTL...")
    success = client.json_set("api_cache:exchange_rates", api_result, region, ttl=300)
    print(f"    Store: {'OK' if success else 'FAIL'}")

    retrieved = client.json_get("api_cache:exchange_rates", region)
    print(f"    Verify: source={retrieved.get('source')}, rates={retrieved.get('data', {}).get('rates')}")


def demo_l_cross_region(client: KuberJsonRestClient):
    """Demo l) Cross-region JSON operations and search."""
    print_header("L) CROSS-REGION JSON OPERATIONS")

    region_products = 'products_json_rest_demo'
    client.create_region(region_products, 'Product catalog for JSON REST demo')

    # Store products in a different region
    print(f"\n  Storing {len(PRODUCTS)} products in '{region_products}' region...")
    for prod in PRODUCTS:
        key = f"product:{prod['product_id']}"
        success = client.json_set(key, prod, region_products)
        status = "OK" if success else "FAIL"
        print(f"    [{status}] {key} -> {prod['product_name']} (${prod['price']})")

    # Search products - Equality
    print_subheader(f"Search in '{region_products}': $.category=Electronics")
    results = client.json_search("$.category=Electronics", region_products)
    print_results(results, ['product_name', 'price'])

    # Search products - Price comparison
    print_subheader(f"Search in '{region_products}': $.price>200")
    results = client.json_search("$.price>200", region_products)
    print_results(results, ['product_name', 'price'])

    # Search products - In stock
    print_subheader(f"Search in '{region_products}': $.in_stock=true,$.price<300")
    results = client.json_search("$.in_stock=true,$.price<300", region_products)
    print_results(results, ['product_name', 'price', 'in_stock'])

    # Search products - Nested spec
    print_subheader(f"Search in '{region_products}': $.specs.backlighting=RGB")
    results = client.json_search("$.specs.backlighting=RGB", region_products)
    print_results(results, ['product_name', 'specs'])

    # Search products - Pattern match on brand
    print_subheader(f"Search in '{region_products}': $.brand LIKE %Tech%")
    results = client.json_search("$.brand LIKE %Tech%", region_products)
    print_results(results, ['product_name', 'brand'])

    # Search products - Array contains
    print_subheader(f"Search in '{region_products}': $.tags CONTAINS gaming")
    results = client.json_search("$.tags CONTAINS gaming", region_products)
    print_results(results, ['product_name', 'tags'])

    # Search products - Stock count comparison
    print_subheader(f"Search in '{region_products}': $.stock_count>=100")
    results = client.json_search("$.stock_count>=100", region_products)
    print_results(results, ['product_name', 'stock_count'])

    return region_products


def demo_m_delete_json(client: KuberJsonRestClient, region: str, products_region: str):
    """Demo m) Delete JSON documents."""
    print_header("M) DELETE JSON DOCUMENTS (DELETE /api/v1/json/{region}/{key})")

    # Delete individual employee
    print("\n  Deleting employee records...")
    for emp in EMPLOYEES:
        key = f"employee:{emp['id']}"
        success = client.json_delete(key, region)
        status = "OK" if success else "FAIL"
        print(f"    [{status}] Deleted {key}")

    # Delete session and cache entries
    for key in ["session:ABC123", "api_cache:exchange_rates"]:
        success = client.json_delete(key, region)
        status = "OK" if success else "FAIL"
        print(f"    [{status}] Deleted {key}")

    # Delete product records
    print(f"\n  Deleting product records from '{products_region}'...")
    for prod in PRODUCTS:
        key = f"product:{prod['product_id']}"
        success = client.json_delete(key, products_region)
        status = "OK" if success else "FAIL"
        print(f"    [{status}] Deleted {key}")

    # Verify regions are empty
    emp_size = client.region_size(region)
    prod_size = client.region_size(products_region)
    print(f"\n  Region '{region}' size after cleanup: {emp_size}")
    print(f"  Region '{products_region}' size after cleanup: {prod_size}")


def demo_n_slash_keys(client: KuberJsonRestClient, region: str):
    """Demo n) Keys containing forward slashes."""
    print_header("N) KEYS WITH FORWARD SLASHES (employee/EMP001)")

    # Keys with slashes are common in hierarchical naming schemes:
    # employee/EMP001, department/engineering/team-lead, etc.
    # The client URL-encodes the '/' as '%2F' automatically.

    slash_docs = [
        ("employee/EMP001", {"name": "Alice Walker", "dept": "Engineering", "level": "L5"}),
        ("employee/EMP002", {"name": "Bob Chen", "dept": "Marketing", "level": "L4"}),
        ("department/eng/lead", {"name": "Carol Davis", "role": "Director", "reports": 42}),
        ("config/app/v2.1/settings", {"theme": "dark", "locale": "en-US", "timeout": 30}),
    ]

    # Store
    print("\n  Storing documents with slash keys...")
    for key, doc in slash_docs:
        success = client.json_set(key, doc, region)
        status = "OK" if success else "FAIL"
        print(f"    [{status}] {key}")

    # Retrieve
    print("\n  Retrieving documents by slash key...")
    for key, original in slash_docs:
        retrieved = client.json_get(key, region)
        if retrieved:
            match = "MATCH" if retrieved.get("name", retrieved.get("theme")) == original.get("name", original.get("theme")) else "MISMATCH"
            print(f"    [{match}] {key} -> {json.dumps(retrieved, default=str)[:80]}...")
        else:
            print(f"    [FAIL] {key} -> NOT FOUND")

    # JSONPath on slash key
    print("\n  JSONPath on slash key 'employee/EMP001' ($.dept)...")
    dept = client.json_get("employee/EMP001", region, path="$.dept")
    print(f"    Department: {dept}")

    # Delete
    print("\n  Deleting slash-key documents...")
    for key, _ in slash_docs:
        success = client.json_delete(key, region)
        status = "OK" if success else "FAIL"
        print(f"    [{status}] Deleted {key}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    # Parse command line arguments
    host = sys.argv[1] if len(sys.argv) > 1 else KUBER_HOST
    port = int(sys.argv[2]) if len(sys.argv) > 2 else KUBER_PORT
    api_key = sys.argv[3] if len(sys.argv) > 3 else KUBER_API_KEY

    print("""
+======================================================================+
|                                                                      |
|   KUBER DISTRIBUTED CACHE - JSON OPERATIONS VIA REST API             |
|                                                                      |
|   Demonstrates all JSON operations through the REST/HTTP API:        |
|                                                                      |
|   a) Store JSON documents           PUT  /api/v1/json/{r}/{k}       |
|   b) Retrieve JSON documents        GET  /api/v1/json/{r}/{k}       |
|   c) Retrieve at JSONPath           GET  with ?path=$.field          |
|   d) Update JSON documents          PUT  (full replace)              |
|   e-j) Search with operators        POST /api/v1/json/{r}/search    |
|       - Equality, Comparison, Inequality, Pattern, Contains, AND     |
|   k) Store with TTL                 PUT  with ttl parameter          |
|   l) Cross-region operations        Multiple regions                 |
|   m) Delete JSON documents          DELETE /api/v1/json/{r}/{k}     |
|   n) Keys with forward slashes      employee/EMP001 etc.            |
|                                                                      |
|   v2.6.0                                                             |
+======================================================================+
    """)

    print(f"Configuration:")
    print(f"  Host:    {host}")
    print(f"  Port:    {port}")
    print(f"  API Key: {api_key[:12]}...{api_key[-4:]}")

    region = 'employees_json_rest_demo'

    try:
        client = KuberJsonRestClient(host, port, api_key)

        # Verify connectivity
        if not client.ping():
            print("\n  ✗ Cannot connect to Kuber server")
            sys.exit(1)
        print(f"\n  ✓ Connected to Kuber server at {host}:{port}")

        # Server info
        info = client.info()
        if info:
            print(f"  ✓ Server version: {info.get('version', 'N/A')}")

        # Create demo region
        client.create_region(region, 'Employee data for JSON REST demo')
        print(f"  ✓ Region: {region}")

        # Run all demos
        demo_a_store_json(client, region)
        demo_b_retrieve_json(client, region)
        demo_c_retrieve_json_path(client, region)
        demo_d_update_json(client, region)
        demo_e_search_equality(client, region)
        demo_f_search_comparison(client, region)
        demo_g_search_inequality(client, region)
        demo_h_search_pattern(client, region)
        demo_i_search_contains(client, region)
        demo_j_search_combined(client, region)
        demo_k_json_with_ttl(client, region)
        products_region = demo_l_cross_region(client)
        demo_m_delete_json(client, region, products_region)
        demo_n_slash_keys(client, region)

        print_header("ALL DEMOS COMPLETED SUCCESSFULLY")
        print()

    except ValueError as e:
        print(f"\n  ✗ Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n  ✗ Error: {e}")
        print("\n  Troubleshooting:")
        print("    1. Ensure Kuber server is running")
        print("    2. Check host/port (default: localhost:8080)")
        print("    3. Verify API key starts with 'kub_'")
        print("    4. Check REST API is enabled on the server")
        sys.exit(1)


if __name__ == "__main__":
    main()
