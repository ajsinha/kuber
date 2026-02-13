#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Copyright © 2025-2030, All Rights Reserved
Ashutosh Sinha | Email: ajsinha@gmail.com

Legal Notice: This module and the associated software architecture are proprietary
and confidential. Unauthorized copying, distribution, modification, or use is
strictly prohibited without explicit written permission from the copyright holder.

Patent Pending: Certain architectural patterns and implementations described in
this module may be subject to patent applications.

================================================================================
KUBER STANDALONE PYTHON CLIENT - HTTP REST API
================================================================================

A comprehensive standalone Python client for Kuber Distributed Cache using
pure HTTP REST API. This client does not depend on any external Kuber library
and uses only the standard library (urllib).

v2.1.0: API Key Authentication ONLY
All programmatic access requires an API key (starts with "kub_").
Username/password authentication is only for the Web UI.

Features:
- All cache operations via REST endpoints
- Key pattern searching
- Region management
- JSON document storage and retrieval
- Deep JSON value search with multiple operators
- Cross-region operations
- API Key Authentication (X-API-Key header)
- Bulk import/export operations

Usage:
    python kuber_rest_standalone.py [--host HOST] [--port PORT] [--api-key KEY]

Default connection: http://localhost:8080
"""

import json
import urllib.request
import urllib.error
import urllib.parse
import base64
import argparse
import sys
import os
from typing import Optional, List, Dict, Any, Union
from datetime import timedelta


class KuberRestClient:
    """
    Standalone REST API client for Kuber Distributed Cache.
    
    v2.1.0: API Key Authentication ONLY.
    All programmatic access requires an API key (starts with "kub_").
    Username/password authentication is only for the Web UI.
    
    Uses pure HTTP REST endpoints, no Redis protocol required.
    """
    
    def __init__(self, host: str = 'localhost', port: int = 8080,
                 api_key: str = None,
                 use_ssl: bool = False, timeout: int = 30):
        """
        Initialize connection to Kuber REST API.
        
        Args:
            host: Kuber server hostname
            port: HTTP port (default 8080)
            api_key: API Key for authentication (must start with "kub_")
            use_ssl: Use HTTPS if True
            timeout: Request timeout in seconds
            
        Raises:
            ValueError: If API key is not provided or has invalid format
        """
        if not api_key:
            raise ValueError("API Key is required for authentication")
        if not api_key.startswith('kub_'):
            raise ValueError("Invalid API key format - must start with 'kub_'")
        
        self.scheme = 'https' if use_ssl else 'http'
        self.host = host
        self.port = port
        self.timeout = timeout
        self.current_region = 'default'
        self.api_key = api_key
    
    @property
    def base_url(self) -> str:
        """Get base URL for API requests."""
        return f"{self.scheme}://{self.host}:{self.port}"
    
    def _request(self, method: str, path: str, data: Any = None,
                 params: Optional[Dict] = None) -> Any:
        """
        Make an HTTP request to the API.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            path: API path (e.g., /api/v1/cache/region/key)
            data: Request body (will be JSON encoded)
            params: Query parameters
            
        Returns:
            Parsed JSON response or None
        """
        url = f"{self.base_url}{path}"
        
        if params:
            query = urllib.parse.urlencode(params)
            url = f"{url}?{query}"
        
        body = None
        if data is not None:
            body = json.dumps(data).encode('utf-8')
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-API-Key': self.api_key
        }
        
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                content = response.read()
                if content:
                    return json.loads(content.decode('utf-8'))
                return None
        except urllib.error.HTTPError as e:
            content = e.read().decode('utf-8')
            raise KuberRestError(f"HTTP {e.code}: {content}")
        except urllib.error.URLError as e:
            raise KuberRestError(f"Connection error: {e.reason}")
    
    def close(self):
        """Close client (no-op for REST client)."""
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
    
    # ==================== Server Operations ====================
    
    def ping(self) -> bool:
        """Ping the server."""
        try:
            result = self._request('GET', '/api/v1/ping')
            return result.get('status') == 'OK' if result else False
        except:
            return False
    
    def info(self) -> Dict[str, Any]:
        """Get server information."""
        return self._request('GET', '/api/v1/info') or {}
    
    def status(self) -> Dict[str, Any]:
        """Get server status."""
        return self._request('GET', '/api/v1/status') or {}
    
    def stats(self) -> Dict[str, Any]:
        """Get server statistics."""
        return self._request('GET', '/api/v1/stats') or {}
    
    # ==================== Region Operations ====================
    
    def select_region(self, region: str):
        """Select a region for subsequent operations."""
        self.current_region = region
    
    def list_regions(self) -> List[Dict[str, Any]]:
        """List all available regions."""
        return self._request('GET', '/api/v1/regions') or []
    
    def get_region(self, name: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific region."""
        return self._request('GET', f'/api/v1/regions/{name}')
    
    def create_region(self, name: str, description: str = ''):
        """Create a new region."""
        self._request('POST', '/api/v1/regions', {
            'name': name,
            'description': description
        })
    
    def delete_region(self, name: str):
        """Delete a region."""
        self._request('DELETE', f'/api/v1/regions/{name}')
    
    def purge_region(self, name: str):
        """Purge all entries in a region."""
        self._request('POST', f'/api/v1/regions/{name}/purge')
    
    # ==================== Attribute Mapping ====================
    
    def set_attribute_mapping(self, region: str, mapping: Dict[str, str]):
        """
        Set attribute mapping for a region.
        
        When JSON data is stored in a region with attribute mapping,
        the source attribute names are automatically renamed to target names.
        
        Args:
            region: Region name
            mapping: Dictionary mapping source attribute names to target names
                     Example: {"firstName": "first_name", "lastName": "last_name"}
                     
        Example:
            client.set_attribute_mapping('users', {
                'firstName': 'first_name',
                'lastName': 'last_name',
                'emailAddress': 'email'
            })
        """
        self._request('PUT', f'/api/v1/regions/{region}/attributemapping', mapping)
    
    def get_attribute_mapping(self, region: str) -> Optional[Dict[str, str]]:
        """
        Get attribute mapping for a region.
        
        Args:
            region: Region name
            
        Returns:
            Dictionary of attribute mappings, or None if no mapping set
            
        Example:
            mapping = client.get_attribute_mapping('users')
            # Returns: {"firstName": "first_name", "lastName": "last_name"}
        """
        try:
            result = self._request('GET', f'/api/v1/regions/{region}/attributemapping')
            return result if result else None
        except KuberRestError:
            return None
    
    def clear_attribute_mapping(self, region: str):
        """
        Clear attribute mapping for a region.
        
        Args:
            region: Region name
            
        Example:
            client.clear_attribute_mapping('users')
        """
        self._request('DELETE', f'/api/v1/regions/{region}/attributemapping')
    
    # ==================== Cache Operations ====================
    
    def get(self, key: str, region: Optional[str] = None) -> Optional[str]:
        """
        Get value by key.
        
        Args:
            key: Key name
            region: Optional region (uses current region if not specified)
        """
        r = region or self.current_region
        try:
            result = self._request('GET', f'/api/v1/cache/{r}/{key}')
            if result and 'value' in result:
                return result['value']
            return None
        except KuberRestError:
            return None
    
    def set(self, key: str, value: str, ttl: Optional[Union[int, timedelta]] = None,
            region: Optional[str] = None):
        """
        Set a value.
        
        Args:
            key: Key name
            value: Value to store
            ttl: Optional TTL in seconds or timedelta
            region: Optional region
        """
        r = region or self.current_region
        data = {'value': value}
        
        if ttl is not None:
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            data['ttl'] = ttl
        
        self._request('PUT', f'/api/v1/cache/{r}/{key}', data)
    
    def delete(self, key: str, region: Optional[str] = None) -> bool:
        """Delete a key."""
        r = region or self.current_region
        try:
            self._request('DELETE', f'/api/v1/cache/{r}/{key}')
            return True
        except KuberRestError:
            return False
    
    def exists(self, key: str, region: Optional[str] = None) -> bool:
        """Check if key exists."""
        r = region or self.current_region
        try:
            result = self._request('GET', f'/api/v1/cache/{r}/{key}/exists')
            return result.get('exists', False) if result else False
        except KuberRestError:
            return False
    
    def ttl(self, key: str, region: Optional[str] = None) -> int:
        """Get TTL of a key."""
        r = region or self.current_region
        result = self._request('GET', f'/api/v1/cache/{r}/{key}/ttl')
        return result.get('ttl', -2) if result else -2
    
    def expire(self, key: str, seconds: int, region: Optional[str] = None):
        """Set key expiration."""
        r = region or self.current_region
        self._request('POST', f'/api/v1/cache/{r}/{key}/expire', {'seconds': seconds})
    
    def mget(self, keys: List[str], region: Optional[str] = None) -> List[Optional[str]]:
        """Get multiple values."""
        r = region or self.current_region
        result = self._request('POST', f'/api/v1/cache/{r}/mget', {'keys': keys})
        if result and 'values' in result:
            return result['values']
        return [None] * len(keys)
    
    def mset(self, mapping: Dict[str, str], region: Optional[str] = None):
        """Set multiple values."""
        r = region or self.current_region
        entries = [{'key': k, 'value': v} for k, v in mapping.items()]
        self._request('POST', f'/api/v1/cache/{r}/mset', {'entries': entries})
    
    def keys(self, pattern: str = '*', region: Optional[str] = None) -> List[str]:
        """
        Find keys matching pattern.
        
        Pattern supports:
        - * matches any sequence
        - ? matches single character
        """
        r = region or self.current_region
        result = self._request('GET', f'/api/v1/cache/{r}/keys', params={'pattern': pattern})
        return result if isinstance(result, list) else (result.get('keys', []) if isinstance(result, dict) else [])
    
    def dbsize(self, region: Optional[str] = None) -> int:
        """Get number of keys in region."""
        r = region or self.current_region
        result = self._request('GET', f'/api/v1/cache/{r}/size')
        return result.get('size', 0) if isinstance(result, dict) else 0
    
    def ksearch(self, regex_pattern: str, region: Optional[str] = None, 
                limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Search keys by regex pattern and return full key-value objects.
        
        Unlike keys() which uses glob pattern and returns only key names,
        ksearch uses regex and returns complete key-value objects including
        value, type, and TTL.
        
        Args:
            regex_pattern: Regular expression pattern to match keys
            region: Optional region (uses current region if not specified)
            limit: Maximum number of results (default 1000)
            
        Returns:
            List of dicts with keys: key, value, type, ttl
            
        Example:
            # Find all user keys and their values
            results = client.ksearch(r'user:.*', region='users')
            for item in results:
                print(f"Key: {item['key']}, Value: {item['value']}")
        """
        r = region or self.current_region
        result = self._request('GET', f'/api/v1/cache/{r}/ksearch', 
                              params={'pattern': regex_pattern, 'limit': limit})
        return result if isinstance(result, list) else []
    
    # ==================== Hash Operations ====================
    
    def hget(self, key: str, field: str, region: Optional[str] = None) -> Optional[str]:
        """Get hash field value."""
        r = region or self.current_region
        result = self._request('GET', f'/api/v1/cache/{r}/{key}/hash/{field}')
        return result.get('value') if isinstance(result, dict) else result
    
    def hset(self, key: str, field: str, value: str, region: Optional[str] = None):
        """Set hash field."""
        r = region or self.current_region
        self._request('PUT', f'/api/v1/cache/{r}/{key}/hash/{field}', {'value': value})
    
    def hmset(self, key: str, mapping: Dict[str, str], region: Optional[str] = None):
        """Set multiple hash fields."""
        r = region or self.current_region
        self._request('PUT', f'/api/v1/cache/{r}/{key}/hash', {'fields': mapping})
    
    def hmget(self, key: str, fields: List[str], region: Optional[str] = None) -> List[Optional[str]]:
        """Get multiple hash fields."""
        r = region or self.current_region
        result = self._request('POST', f'/api/v1/cache/{r}/{key}/hash/mget', {'fields': fields})
        return result.get('values', [None] * len(fields)) if isinstance(result, dict) else [None] * len(fields)
    
    def hgetall(self, key: str, region: Optional[str] = None) -> Dict[str, str]:
        """Get all hash fields and values."""
        r = region or self.current_region
        result = self._request('GET', f'/api/v1/cache/{r}/{key}/hash')
        return result if isinstance(result, dict) else {}
    
    def hdel(self, key: str, field: str, region: Optional[str] = None) -> bool:
        """Delete hash field."""
        r = region or self.current_region
        try:
            self._request('DELETE', f'/api/v1/cache/{r}/{key}/hash/{field}')
            return True
        except:
            return False
    
    def hkeys(self, key: str, region: Optional[str] = None) -> List[str]:
        """Get all hash field names."""
        r = region or self.current_region
        result = self._request('GET', f'/api/v1/cache/{r}/{key}/hash/keys')
        return result.get('keys', []) if isinstance(result, dict) else (result if isinstance(result, list) else [])
    
    def hvals(self, key: str, region: Optional[str] = None) -> List[str]:
        """Get all hash values."""
        r = region or self.current_region
        result = self._request('GET', f'/api/v1/cache/{r}/{key}/hash/values')
        return result.get('values', []) if isinstance(result, dict) else (result if isinstance(result, list) else [])
    
    # ==================== JSON Operations ====================
    
    def json_set(self, key: str, value: Any, region: Optional[str] = None,
                 ttl: Optional[Union[int, timedelta]] = None):
        """
        Store a JSON document.
        
        Args:
            key: Key name
            value: Python object to store as JSON
            region: Optional region (uses current region if not specified)
            ttl: Optional TTL in seconds or timedelta
        """
        r = region or self.current_region
        data = {'value': value}
        
        if ttl is not None:
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            data['ttl'] = ttl
        
        self._request('PUT', f'/api/v1/json/{r}/{key}', data)
    
    def json_get(self, key: str, path: str = '$', region: Optional[str] = None) -> Any:
        """
        Get a JSON document or path.
        
        Args:
            key: Key name
            path: JSONPath expression (default: $ for root)
            region: Optional region
            
        Returns:
            Parsed JSON object
        """
        r = region or self.current_region
        params = {'path': path} if path != '$' else None
        result = self._request('GET', f'/api/v1/json/{r}/{key}', params=params)
        return result.get('value') if isinstance(result, dict) else result
    
    def json_delete(self, key: str, path: str = '$', region: Optional[str] = None) -> bool:
        """Delete a JSON document or path."""
        r = region or self.current_region
        try:
            params = {'path': path} if path != '$' else None
            self._request('DELETE', f'/api/v1/json/{r}/{key}', params=params)
            return True
        except:
            return False
    
    def json_search(self, query: str, region: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search JSON documents.
        
        Supported query operators:
        - Equality: $.field=value
        - Comparison: $.field>value, $.field<value, $.field>=value, $.field<=value
        - Inequality: $.field!=value
        - Pattern: $.field LIKE %pattern%
        - Array contains: $.array CONTAINS value
        - Combined: $.field1=value1,$.field2>value2
        
        Args:
            query: Search query
            region: Optional region to search in
            
        Returns:
            List of matching documents with key and value
        """
        r = region or self.current_region
        result = self._request('POST', f'/api/v1/json/{r}/search', {'query': query})
        return result.get('results', []) if isinstance(result, dict) else (result if isinstance(result, list) else [])
    
    # ==================== Generic Search API ====================
    
    def generic_search(self, region: str = None, 
                       key: str = None,
                       keypattern: str = None,
                       search_type: str = None,
                       values: List[Dict[str, Any]] = None,
                       fields: List[str] = None,
                       limit: int = None) -> List[Dict[str, Any]]:
        """
        Perform generic search using the unified search API.
        
        Supports multiple search modes:
        1. Simple key lookup: key="ABC"
        2. Key pattern (regex): keypattern="ABC.*"
        3. JSON attribute search: search_type="json", values=[{"k1": "abc"}, {"k2": "def"}]
        
        JSON attribute conditions support:
        - Equality: {"fieldName": "value"}
        - Regex: {"fieldName": "pattern", "type": "regex"}
        - IN operator: {"fieldName": ["value1", "value2"]}
        
        Args:
            region: Region to search in (uses current region if not specified)
            key: Exact key to lookup
            keypattern: Regex pattern to match keys
            search_type: Set to "json" for JSON attribute search
            values: List of attribute conditions for JSON search
            fields: Optional list of fields to return (supports nested paths like "address.city")
            limit: Maximum results (default: 1000)
            
        Returns:
            List of matching key-value pairs
            
        Example:
            # Simple key lookup
            results = client.generic_search(region="test", key="ABC")
            
            # Key pattern search
            results = client.generic_search(region="test", keypattern="user:.*")
            
            # JSON search with equality
            results = client.generic_search(
                region="test",
                search_type="json",
                values=[{"status": "active"}, {"age": "30"}]
            )
            
            # JSON search with regex
            results = client.generic_search(
                region="test",
                search_type="json",
                values=[{"name": "John.*", "type": "regex"}]
            )
            
            # JSON search with IN operator
            results = client.generic_search(
                region="test",
                search_type="json",
                values=[{"status": ["active", "pending"]}]
            )
            
            # Search with field projection
            results = client.generic_search(
                region="users",
                keypattern="user:.*",
                fields=["name", "email", "address.city"]
            )
        """
        r = region or self.current_region
        
        request_body = {"region": r}
        
        if key:
            request_body["key"] = key
        if keypattern:
            request_body["keypattern"] = keypattern
        if search_type:
            request_body["type"] = search_type
        if values:
            request_body["values"] = values
        if fields:
            request_body["fields"] = fields
        if limit:
            request_body["limit"] = limit
        
        result = self._request('POST', '/api/v1/genericsearch', request_body)
        return result if isinstance(result, list) else []
    
    # ==================== Bulk Operations ====================
    
    def bulk_import(self, entries: List[Dict[str, Any]], region: Optional[str] = None) -> Dict[str, Any]:
        """
        Bulk import entries.
        
        Args:
            entries: List of entries with 'key', 'value', and optional 'ttl'
            region: Optional region
            
        Returns:
            Import statistics
        """
        r = region or self.current_region
        return self._request('POST', f'/api/v1/cache/{r}/import', {'entries': entries}) or {}
    
    def bulk_export(self, pattern: str = '*', region: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Bulk export entries.
        
        Args:
            pattern: Key pattern to export
            region: Optional region
            
        Returns:
            List of exported entries
        """
        r = region or self.current_region
        result = self._request('GET', f'/api/v1/cache/{r}/export', {'pattern': pattern})
        return result.get('entries', []) if result else []


class KuberRestError(Exception):
    """Exception for Kuber REST API errors."""
    pass


# ==============================================================================
# COMPREHENSIVE EXAMPLES
# ==============================================================================

def print_section(title: str):
    """Print a section header."""
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}\n")


def print_result(description: str, result):
    """Print a result with description."""
    if isinstance(result, (dict, list)):
        result_str = json.dumps(result, indent=2, default=str)
        if len(result_str) > 300:
            result_str = result_str[:300] + '...'
        print(f"  {description}:\n    {result_str.replace(chr(10), chr(10) + '    ')}")
    else:
        print(f"  {description}: {result}")


def example_1_server_operations(client: KuberRestClient):
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


def example_2_basic_operations(client: KuberRestClient):
    """Demonstrate basic GET/SET operations."""
    print_section("2. BASIC KEY-VALUE OPERATIONS (GET, SET)")
    
    # Simple SET and GET
    client.set('greeting', 'Hello, Kuber REST!')
    value = client.get('greeting')
    print_result("SET/GET 'greeting'", value)
    
    # SET with TTL
    client.set('temp_key', 'expires in 60 seconds', ttl=60)
    print_result("SET with TTL (60s)", client.get('temp_key'))
    print_result("TTL remaining", client.ttl('temp_key'))
    
    # SET with timedelta
    client.set('session_data', 'user session', ttl=timedelta(hours=1))
    print_result("SET with timedelta (1 hour)", f"TTL: {client.ttl('session_data')} seconds")
    
    # Check EXISTS
    print_result("EXISTS 'greeting'", client.exists('greeting'))
    print_result("EXISTS 'nonexistent'", client.exists('nonexistent'))
    
    # DELETE
    client.set('to_delete', 'delete me')
    print_result("GET before DELETE", client.get('to_delete'))
    deleted = client.delete('to_delete')
    print_result("DELETE result", deleted)
    print_result("GET after DELETE", client.get('to_delete'))
    
    # EXPIRE
    client.set('expire_test', 'will expire')
    client.expire('expire_test', 120)
    print_result("TTL after EXPIRE(120)", client.ttl('expire_test'))
    
    # DBSIZE
    print_result("DBSIZE (default region)", client.dbsize())


def example_3_mget_mset_operations(client: KuberRestClient):
    """Demonstrate MGET and MSET operations."""
    print_section("3. MULTI-KEY OPERATIONS (MGET, MSET)")
    
    # MSET - Set multiple keys at once
    client.mset({
        'user:1:name': 'Alice',
        'user:1:email': 'alice@example.com',
        'user:1:role': 'admin',
        'user:2:name': 'Bob',
        'user:2:email': 'bob@example.com',
        'user:2:role': 'developer',
        'user:3:name': 'Charlie',
        'user:3:email': 'charlie@example.com',
        'user:3:role': 'analyst'
    })
    print("  MSET: Set 9 keys for 3 users")
    
    # MGET - Get multiple keys at once
    names = client.mget(['user:1:name', 'user:2:name', 'user:3:name'])
    print_result("MGET all names", names)
    
    emails = client.mget(['user:1:email', 'user:2:email', 'user:3:email'])
    print_result("MGET all emails", emails)
    
    # MGET with some missing keys
    mixed = client.mget(['user:1:name', 'user:99:name', 'user:2:name'])
    print_result("MGET with missing key (user:99)", mixed)
    
    # Get all properties for one user
    user1_props = client.mget(['user:1:name', 'user:1:email', 'user:1:role'])
    print_result("MGET user:1 all properties", user1_props)


def example_4_key_pattern_search(client: KuberRestClient):
    """Demonstrate key pattern searching."""
    print_section("4. KEY PATTERN SEARCH (KEYS)")
    
    # Create test data with various patterns
    client.mset({
        'product:electronics:1001': 'Laptop',
        'product:electronics:1002': 'Smartphone',
        'product:electronics:1003': 'Tablet',
        'product:furniture:2001': 'Chair',
        'product:furniture:2002': 'Desk',
        'order:2024:001': 'Order Jan',
        'order:2024:002': 'Order Feb',
        'order:2025:001': 'Order New Year',
        'session:abc123': 'Session 1',
        'session:def456': 'Session 2',
        'session:xyz789': 'Session 3'
    })
    print("  Created test keys: product:*, order:*, session:*")
    
    # Find all keys
    all_keys = client.keys('*')
    print_result("KEYS * (all keys count)", len(all_keys))
    
    # Find product keys
    product_keys = client.keys('product:*')
    print_result("KEYS product:*", product_keys)
    
    # Find electronics products only
    electronics_keys = client.keys('product:electronics:*')
    print_result("KEYS product:electronics:*", electronics_keys)
    
    # Find order keys
    order_keys = client.keys('order:*')
    print_result("KEYS order:*", order_keys)
    
    # Find 2024 orders
    order_2024 = client.keys('order:2024:*')
    print_result("KEYS order:2024:*", order_2024)
    
    # Find session keys
    session_keys = client.keys('session:*')
    print_result("KEYS session:*", session_keys)
    
    # Pattern with single character wildcard
    keys_100x = client.keys('product:electronics:100?')
    print_result("KEYS product:electronics:100?", keys_100x)


def example_5_region_operations(client: KuberRestClient):
    """Demonstrate region management."""
    print_section("5. REGION OPERATIONS (CREATE, SELECT, LIST)")
    
    # List existing regions
    regions = client.list_regions()
    print_result("Initial regions", [r.get('name') for r in regions] if regions else [])
    
    # Create custom regions
    client.create_region('inventory_rest', 'Product inventory data')
    client.create_region('customers_rest', 'Customer information')
    client.create_region('analytics_rest', 'Analytics and metrics')
    print("  Created 3 new regions: inventory_rest, customers_rest, analytics_rest")
    
    # List regions again
    regions = client.list_regions()
    print_result("Regions after creation", [r.get('name') for r in regions] if regions else [])
    
    # Select and use inventory region
    client.select_region('inventory_rest')
    print(f"\n  Selected region: {client.current_region}")
    
    # Store data in inventory region
    client.set('sku:1001', 'Laptop - Stock: 50')
    client.set('sku:1002', 'Mouse - Stock: 200')
    client.set('sku:1003', 'Keyboard - Stock: 150')
    
    print_result("DBSIZE in inventory_rest region", client.dbsize())
    print_result("KEYS in inventory_rest", client.keys('*'))
    
    # Select customers region
    client.select_region('customers_rest')
    print(f"\n  Selected region: {client.current_region}")
    
    # Store data in customers region
    client.set('cust:C001', 'Acme Corp')
    client.set('cust:C002', 'TechStart Inc')
    
    print_result("DBSIZE in customers_rest region", client.dbsize())
    print_result("KEYS in customers_rest", client.keys('*'))
    
    # Verify isolation - inventory keys not visible in customers
    laptop = client.get('sku:1001')
    print_result("GET sku:1001 from customers_rest region", laptop)  # Should be None
    
    # Get region info
    region_info = client.get_region('inventory_rest')
    print_result("\nRegion info for inventory_rest", region_info)
    
    # Switch back to default
    client.select_region('default')
    print(f"\n  Back to region: {client.current_region}")


def example_6_hash_operations(client: KuberRestClient):
    """Demonstrate hash operations."""
    print_section("6. HASH OPERATIONS (HGET, HSET, HMSET, HGETALL)")
    
    # HSET - Set individual fields
    client.hset('user:profile:1', 'name', 'Alice Johnson')
    client.hset('user:profile:1', 'email', 'alice@example.com')
    client.hset('user:profile:1', 'age', '28')
    client.hset('user:profile:1', 'city', 'New York')
    client.hset('user:profile:1', 'department', 'Engineering')
    print("  HSET: Set 5 fields for user:profile:1")
    
    # HGET - Get individual fields
    print_result("HGET name", client.hget('user:profile:1', 'name'))
    print_result("HGET email", client.hget('user:profile:1', 'email'))
    print_result("HGET age", client.hget('user:profile:1', 'age'))
    
    # HMSET - Set multiple fields at once
    client.hmset('user:profile:2', {
        'name': 'Bob Smith',
        'email': 'bob@example.com',
        'age': '35',
        'city': 'San Francisco',
        'department': 'Product'
    })
    print("\n  HMSET: Set 5 fields for user:profile:2")
    
    # HMGET - Get multiple fields at once
    fields = client.hmget('user:profile:2', ['name', 'email', 'city'])
    print_result("HMGET name, email, city", fields)
    
    # HGETALL - Get all fields
    all_fields = client.hgetall('user:profile:1')
    print_result("HGETALL user:profile:1", all_fields)
    
    # HKEYS - Get all field names
    keys = client.hkeys('user:profile:1')
    print_result("HKEYS", keys)
    
    # HVALS - Get all values
    vals = client.hvals('user:profile:1')
    print_result("HVALS", vals)
    
    # HDEL - Delete field
    client.hdel('user:profile:1', 'age')
    print_result("After HDEL 'age', remaining keys", client.hkeys('user:profile:1'))


def example_7_json_operations_with_regions(client: KuberRestClient):
    """Demonstrate JSON operations with specific region storage."""
    print_section("7. JSON OPERATIONS WITH REGION STORAGE (json_set, json_get)")
    
    # Create a products region for JSON documents
    client.create_region('products_json_rest', 'Product catalog as JSON')
    
    # Store JSON documents in specific region
    print("\n  Storing products in 'products_json_rest' region:")
    
    products = [
        {
            'id': 'prod:1001',
            'name': 'Wireless Headphones',
            'brand': 'AudioTech',
            'price': 79.99,
            'category': 'Electronics',
            'subcategory': 'Audio',
            'in_stock': True,
            'stock_count': 150,
            'specs': {
                'battery_life': '20 hours',
                'connectivity': 'Bluetooth 5.0',
                'driver_size': '40mm'
            },
            'tags': ['wireless', 'bluetooth', 'audio', 'headphones']
        },
        {
            'id': 'prod:1002',
            'name': 'Mechanical Keyboard',
            'brand': 'KeyMaster',
            'price': 149.99,
            'category': 'Electronics',
            'subcategory': 'Input Devices',
            'in_stock': True,
            'stock_count': 75,
            'specs': {
                'switch_type': 'Cherry MX Blue',
                'backlighting': 'RGB',
                'layout': 'Full-size'
            },
            'tags': ['mechanical', 'keyboard', 'rgb', 'gaming']
        },
        {
            'id': 'prod:1003',
            'name': 'Ergonomic Office Chair',
            'brand': 'ComfortPlus',
            'price': 349.99,
            'category': 'Furniture',
            'subcategory': 'Seating',
            'in_stock': False,
            'stock_count': 0,
            'specs': {
                'material': 'Mesh',
                'adjustable_height': True,
                'lumbar_support': True
            },
            'tags': ['ergonomic', 'office', 'chair', 'mesh']
        },
        {
            'id': 'prod:2001',
            'name': 'USB-C Hub',
            'brand': 'ConnectPro',
            'price': 45.99,
            'category': 'Electronics',
            'subcategory': 'Accessories',
            'in_stock': True,
            'stock_count': 300,
            'specs': {
                'ports': 7,
                'usb_version': '3.1',
                'power_delivery': '100W'
            },
            'tags': ['usb-c', 'hub', 'accessories', 'laptop']
        }
    ]
    
    for product in products:
        client.json_set(product['id'], product, region='products_json_rest')
        print(f"    Stored: {product['id']} - {product['name']}")
    
    # Retrieve JSON from specific region
    print("\n  Retrieving products from 'products_json_rest' region:")
    headphones = client.json_get('prod:1001', region='products_json_rest')
    print_result("GET prod:1001", headphones)
    
    # Get specific JSON path
    price = client.json_get('prod:1002', '$.price', region='products_json_rest')
    print_result("GET prod:1002 $.price", price)
    
    specs = client.json_get('prod:1001', '$.specs', region='products_json_rest')
    print_result("GET prod:1001 $.specs", specs)
    
    # Store orders in a separate region with TTL
    client.create_region('orders_json_rest', 'Customer orders')
    
    orders = [
        {
            'order_id': 'ord:10001',
            'customer_id': 'cust:C001',
            'status': 'shipped',
            'total': 225.98,
            'items': [
                {'product_id': 'prod:1001', 'quantity': 2, 'price': 79.99},
                {'product_id': 'prod:2001', 'quantity': 1, 'price': 45.99}
            ],
            'shipping': {
                'method': 'express',
                'address': '123 Main St, NYC'
            }
        },
        {
            'order_id': 'ord:10002',
            'customer_id': 'cust:C002',
            'status': 'pending',
            'total': 149.99,
            'items': [
                {'product_id': 'prod:1002', 'quantity': 1, 'price': 149.99}
            ],
            'shipping': {
                'method': 'standard',
                'address': '456 Oak Ave, SF'
            }
        },
        {
            'order_id': 'ord:10003',
            'customer_id': 'cust:C001',
            'status': 'delivered',
            'total': 395.98,
            'items': [
                {'product_id': 'prod:1003', 'quantity': 1, 'price': 349.99},
                {'product_id': 'prod:2001', 'quantity': 1, 'price': 45.99}
            ],
            'shipping': {
                'method': 'express',
                'address': '123 Main St, NYC'
            }
        }
    ]
    
    print("\n  Storing orders in 'orders_json_rest' region with 30-day TTL:")
    for order in orders:
        client.json_set(order['order_id'], order, region='orders_json_rest', ttl=timedelta(days=30))
        print(f"    Stored: {order['order_id']} - Status: {order['status']} - Total: ${order['total']}")


def example_8_json_deep_search(client: KuberRestClient):
    """Demonstrate JSON deep search capabilities."""
    print_section("8. JSON DEEP SEARCH (json_search with multiple operators)")
    
    # Search in products_json_rest region
    print("\n  === SEARCH BY EQUALITY ===")
    
    results = client.json_search('$.category=Electronics', region='products_json_rest')
    print_result("$.category=Electronics", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: {val.get('name')} (${val.get('price')})")
    
    results = client.json_search('$.in_stock=true', region='products_json_rest')
    print_result("$.in_stock=true", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: {val.get('name')} (Stock: {val.get('stock_count')})")
    
    print("\n  === SEARCH BY COMPARISON ===")
    
    results = client.json_search('$.price>100', region='products_json_rest')
    print_result("$.price>100", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: {val.get('name')} (${val.get('price')})")
    
    results = client.json_search('$.price<=50', region='products_json_rest')
    print_result("$.price<=50", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: {val.get('name')} (${val.get('price')})")
    
    results = client.json_search('$.stock_count>=100', region='products_json_rest')
    print_result("$.stock_count>=100", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: {val.get('name')} (Stock: {val.get('stock_count')})")
    
    print("\n  === SEARCH NESTED FIELDS ===")
    
    results = client.json_search('$.specs.backlighting=RGB', region='products_json_rest')
    print_result("$.specs.backlighting=RGB", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc.get('key')}: {doc.get('value', {}).get('name')}")
    
    results = client.json_search('$.specs.lumbar_support=true', region='products_json_rest')
    print_result("$.specs.lumbar_support=true", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc.get('key')}: {doc.get('value', {}).get('name')}")
    
    print("\n  === SEARCH WITH INEQUALITY ===")
    
    results = client.json_search('$.category!=Electronics', region='products_json_rest')
    print_result("$.category!=Electronics", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: {val.get('name')} (Category: {val.get('category')})")
    
    print("\n  === SEARCH WITH PATTERN MATCHING (LIKE) ===")
    
    results = client.json_search('$.name LIKE %Keyboard%', region='products_json_rest')
    print_result("$.name LIKE %Keyboard%", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc.get('key')}: {doc.get('value', {}).get('name')}")
    
    results = client.json_search('$.brand LIKE %Tech%', region='products_json_rest')
    print_result("$.brand LIKE %Tech%", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: {val.get('name')} (Brand: {val.get('brand')})")
    
    print("\n  === SEARCH ARRAY CONTAINS ===")
    
    results = client.json_search('$.tags CONTAINS wireless', region='products_json_rest')
    print_result("$.tags CONTAINS wireless", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: {val.get('name')} (Tags: {val.get('tags')})")
    
    results = client.json_search('$.tags CONTAINS gaming', region='products_json_rest')
    print_result("$.tags CONTAINS gaming", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: {val.get('name')} (Tags: {val.get('tags')})")
    
    print("\n  === COMBINED CONDITIONS ===")
    
    results = client.json_search('$.in_stock=true,$.price<100', region='products_json_rest')
    print_result("$.in_stock=true,$.price<100", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: {val.get('name')} (${val.get('price')}, In Stock)")
    
    results = client.json_search('$.category=Electronics,$.stock_count>50', region='products_json_rest')
    print_result("$.category=Electronics,$.stock_count>50", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: {val.get('name')} (Stock: {val.get('stock_count')})")


def example_9_cross_region_json_search(client: KuberRestClient):
    """Demonstrate JSON search across different regions."""
    print_section("9. CROSS-REGION JSON SEARCH")
    
    print("\n  === SEARCH IN 'products_json_rest' REGION ===")
    
    # Search by subcategory
    results = client.json_search('$.subcategory=Audio', region='products_json_rest')
    print_result("$.subcategory=Audio", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc.get('key')}: {doc.get('value', {}).get('name')}")
    
    print("\n  === SEARCH IN 'orders_json_rest' REGION ===")
    
    # Search by order status
    results = client.json_search('$.status=shipped', region='orders_json_rest')
    print_result("$.status=shipped", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: Customer {val.get('customer_id')}, Total: ${val.get('total')}")
    
    # Search by customer
    results = client.json_search('$.customer_id=cust:C001', region='orders_json_rest')
    print_result("$.customer_id=cust:C001", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: Status: {val.get('status')}, Total: ${val.get('total')}")
    
    # Search by total amount
    results = client.json_search('$.total>200', region='orders_json_rest')
    print_result("$.total>200", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc.get('key')}: ${doc.get('value', {}).get('total')}")
    
    # Search nested shipping info
    results = client.json_search('$.shipping.method=express', region='orders_json_rest')
    print_result("$.shipping.method=express", f"{len(results)} results")
    for doc in results:
        val = doc.get('value', {})
        print(f"    - {doc.get('key')}: {val.get('shipping', {}).get('address')}")


def example_10_bulk_operations(client: KuberRestClient):
    """Demonstrate bulk import/export operations."""
    print_section("10. BULK OPERATIONS (IMPORT, EXPORT)")
    
    # Create a region for bulk demo
    client.create_region('bulk_demo_rest', 'Bulk operations demo')
    
    # Bulk import
    entries = [
        {'key': 'item:1', 'value': {'name': 'Item 1', 'price': 10.99}},
        {'key': 'item:2', 'value': {'name': 'Item 2', 'price': 20.99}},
        {'key': 'item:3', 'value': {'name': 'Item 3', 'price': 30.99}, 'ttl': 3600},
        {'key': 'item:4', 'value': {'name': 'Item 4', 'price': 40.99}},
        {'key': 'item:5', 'value': {'name': 'Item 5', 'price': 50.99}}
    ]
    
    result = client.bulk_import(entries, region='bulk_demo_rest')
    print_result("Bulk import 5 items", result)
    
    # Verify import
    print_result("DBSIZE after import", client.dbsize('bulk_demo_rest'))
    print_result("KEYS after import", client.keys('*', 'bulk_demo_rest'))
    
    # Bulk export
    exported = client.bulk_export('item:*', region='bulk_demo_rest')
    print_result("Bulk export item:*", f"{len(exported)} items")
    for entry in exported[:3]:
        print(f"    - {entry.get('key')}: {entry.get('value')}")
    if len(exported) > 3:
        print(f"    ... and {len(exported) - 3} more")


def cleanup(client: KuberRestClient):
    """Clean up test data."""
    print_section("CLEANUP")
    
    # Purge and delete test regions
    regions_to_clean = ['inventory_rest', 'customers_rest', 'analytics_rest',
                        'products_json_rest', 'orders_json_rest', 'bulk_demo_rest']
    
    for region in regions_to_clean:
        try:
            client.purge_region(region)
            client.delete_region(region)
            print(f"  Cleaned up region: {region}")
        except KuberRestError as e:
            print(f"  Warning for {region}: {e}")
    
    # Clean default region
    client.select_region('default')
    for key in client.keys('*'):
        client.delete(key)
    print("  Cleaned default region")


def main():
    """Main function to run all examples."""
    parser = argparse.ArgumentParser(
        description='Kuber Standalone Python Client - HTTP REST API Examples (v2.4.0)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
v2.1.0: API Key Authentication ONLY
All programmatic access requires an API key (starts with "kub_").
Username/password authentication is only for the Web UI.

Examples:
  python kuber_rest_standalone.py --api-key kub_your_api_key_here
  python kuber_rest_standalone.py -H 192.168.1.100 -P 8080 -k kub_xxx
  python kuber_rest_standalone.py --api-key kub_xxx --ssl --port 8443
  python kuber_rest_standalone.py -k kub_xxx --no-cleanup
  
Environment variable:
  export KUBER_API_KEY=kub_your_api_key_here
  python kuber_rest_standalone.py
        """
    )
    parser.add_argument('--host', '-H', default='localhost', help='Kuber server host (default: localhost)')
    parser.add_argument('--port', '-P', type=int, default=8080, help='HTTP port (default: 8080)')
    parser.add_argument('--api-key', '-k', help='API Key for authentication (must start with "kub_")')
    parser.add_argument('--ssl', action='store_true', help='Use HTTPS')
    parser.add_argument('--no-cleanup', action='store_true', help='Skip cleanup at end')
    args = parser.parse_args()
    
    # Get API key from args or environment
    api_key = args.api_key or os.environ.get('KUBER_API_KEY')
    if not api_key:
        print("ERROR: API key is required. Use --api-key or set KUBER_API_KEY environment variable.")
        sys.exit(1)
    if not api_key.startswith('kub_'):
        print("ERROR: Invalid API key format - must start with 'kub_'")
        sys.exit(1)
    
    protocol = 'https' if args.ssl else 'http'
    
    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║     KUBER STANDALONE PYTHON CLIENT - HTTP REST API (v2.4.0)          ║
║                                                                      ║
║     Connecting to: {protocol}://{args.host}:{args.port:<36}║
║     Authentication: API Key ({api_key[:12]}...{api_key[-4:]})                     ║
║     Protocol: HTTP REST (no Redis protocol required)                 ║
╚══════════════════════════════════════════════════════════════════════╝
""")
    
    try:
        with KuberRestClient(
            args.host, 
            args.port,
            api_key=api_key,
            use_ssl=args.ssl
        ) as client:
            # Verify connection
            if not client.ping():
                print("  ERROR: Failed to connect to server or authentication failed!")
                sys.exit(1)
            print("  Authentication successful!\n")
            
            # Run all examples
            example_1_server_operations(client)
            example_2_basic_operations(client)
            example_3_mget_mset_operations(client)
            example_4_key_pattern_search(client)
            example_5_region_operations(client)
            example_6_hash_operations(client)
            example_7_json_operations_with_regions(client)
            example_8_json_deep_search(client)
            example_9_cross_region_json_search(client)
            example_10_bulk_operations(client)
            
            if not args.no_cleanup:
                cleanup(client)
            
            print_section("ALL EXAMPLES COMPLETED SUCCESSFULLY")
            print("""
    This standalone REST client demonstrated:
    ✓ Server operations (ping, info, status, stats)
    ✓ Basic key-value operations (GET, SET, DELETE, EXISTS)
    ✓ Multi-key operations (MGET, MSET)
    ✓ Key pattern search (KEYS with wildcards)
    ✓ Region management (CREATE, SELECT, LIST, PURGE, DELETE)
    ✓ Hash operations (HGET, HSET, HMSET, HGETALL, HKEYS, HVALS)
    ✓ JSON storage in specific regions with TTL
    ✓ Deep JSON search with multiple operators:
      - Equality (=)
      - Comparison (>, <, >=, <=)
      - Inequality (!=)
      - Pattern matching (LIKE)
      - Array contains (CONTAINS)
      - Combined conditions
    ✓ Cross-region JSON search
    ✓ Bulk import/export operations
    
    NOTE: This client uses pure HTTP REST API.
          No Redis protocol or dependencies required.
""")
            
    except KuberRestError as e:
        print(f"\n  ERROR: REST API error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n  ERROR: Unexpected error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
