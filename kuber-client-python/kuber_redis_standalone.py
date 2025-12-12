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
KUBER STANDALONE PYTHON CLIENT - REDIS PROTOCOL
================================================================================

A comprehensive standalone Python client for Kuber Distributed Cache using
Redis protocol with Kuber extensions. This client does not depend on any
external Kuber library - it directly implements the Redis wire protocol.

v1.6.5: API Key Authentication ONLY
All programmatic access requires an API key (starts with "kub_").
Username/password authentication is only for the Web UI.

Features:
- String operations (GET, SET, MGET, MSET, INCR, DECR, etc.)
- Key pattern searching (KEYS, SCAN)
- Hash operations (HGET, HSET, HMSET, HGETALL, etc.)
- Region management (RSELECT, RCREATE, RDROP, RPURGE)
- JSON operations (JSET, JGET, JDEL, JSEARCH)
- Deep JSON value search with multiple operators
- Cross-region operations
- API Key authentication

Usage:
    python kuber_redis_standalone.py --api-key kub_your_api_key_here [--host HOST] [--port PORT]

Default connection: localhost:6380
"""

import socket
import json
import argparse
import sys
from typing import Optional, List, Dict, Any, Tuple, Union
from datetime import timedelta


class KuberRedisClient:
    """
    Standalone Redis protocol client for Kuber Distributed Cache.
    
    v1.6.5: API Key Authentication ONLY.
    All programmatic access requires an API key (starts with "kub_").
    Username/password authentication is only for the Web UI.
    
    Implements Redis wire protocol with Kuber extensions for regions
    and JSON operations.
    """
    
    def __init__(self, host: str = 'localhost', port: int = 6380, 
                 api_key: str = None, timeout: int = 30):
        """
        Initialize connection to Kuber server.
        
        Args:
            host: Kuber server hostname
            port: Kuber Redis protocol port (default 6380)
            api_key: API Key for authentication (must start with "kub_")
            timeout: Socket timeout in seconds
            
        Raises:
            ValueError: If API key is not provided or has invalid format
        """
        if not api_key:
            raise ValueError("API Key is required for authentication")
        if not api_key.startswith('kub_'):
            raise ValueError("Invalid API key format - must start with 'kub_'")
        
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket: Optional[socket.socket] = None
        self.current_region = 'default'
        self.api_key = api_key
        
        self._connect()
        self.auth(api_key)
    
    def _connect(self):
        """Establish connection to Kuber server."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(self.timeout)
        self.socket.connect((self.host, self.port))
        self._buffer = b''
    
    def _send_command(self, *args) -> Any:
        """
        Send a command using inline protocol and read response.
        
        Args:
            *args: Command and arguments
            
        Returns:
            Parsed response
        """
        # Build inline command (server uses TextLineCodecFactory)
        parts = []
        for arg in args:
            arg_str = str(arg) if arg is not None else ''
            # Quote arguments containing spaces, quotes, or newlines
            if ' ' in arg_str or '"' in arg_str or '\n' in arg_str or '\r' in arg_str:
                escaped = arg_str.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
                parts.append(f'"{escaped}"')
            else:
                parts.append(arg_str)
        
        command = ' '.join(parts) + '\r\n'
        self.socket.sendall(command.encode('utf-8'))
        
        return self._read_response()
    
    def _read_response(self) -> Any:
        """Read and parse Redis response."""
        line = self._read_line()
        
        if not line:
            raise ConnectionError("Connection closed by server")
        
        prefix = line[0:1]
        data = line[1:]
        
        if prefix == b'+':
            # Simple string
            return data.decode('utf-8')
        elif prefix == b'-':
            # Error
            raise KuberError(data.decode('utf-8'))
        elif prefix == b':':
            # Integer
            return int(data.decode('utf-8'))
        elif prefix == b'$':
            # Bulk string
            length = int(data.decode('utf-8'))
            if length == -1:
                return None
            bulk = self._read_bytes(length)
            self._read_line()  # Consume CRLF
            return bulk.decode('utf-8')
        elif prefix == b'*':
            # Array
            count = int(data.decode('utf-8'))
            if count == -1:
                return None
            return [self._read_response() for _ in range(count)]
        else:
            # Inline response
            return line.decode('utf-8').strip()
    
    def _read_line(self) -> bytes:
        """Read a line from socket."""
        while b'\r\n' not in self._buffer:
            chunk = self.socket.recv(4096)
            if not chunk:
                raise ConnectionError("Connection closed")
            self._buffer += chunk
        
        line, self._buffer = self._buffer.split(b'\r\n', 1)
        return line
    
    def _read_bytes(self, length: int) -> bytes:
        """Read exact number of bytes."""
        while len(self._buffer) < length:
            chunk = self.socket.recv(4096)
            if not chunk:
                raise ConnectionError("Connection closed")
            self._buffer += chunk
        
        data = self._buffer[:length]
        self._buffer = self._buffer[length:]
        return data
    
    def close(self):
        """Close connection."""
        if self.socket:
            try:
                self._send_command('QUIT')
            except:
                pass
            self.socket.close()
            self.socket = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()
    
    # ==================== Authentication ====================
    
    def auth(self, api_key: str) -> bool:
        """
        Authenticate with API key.
        
        Args:
            api_key: API Key for authentication (must start with "kub_")
            
        Returns:
            True if authentication successful
        """
        result = self._send_command('AUTH', api_key)
        return result == 'OK'
    
    def ping(self) -> str:
        """Ping the server."""
        return self._send_command('PING')
    
    # ==================== Region Operations ====================
    
    def select_region(self, region: str):
        """
        Select a region (creates if not exists).
        
        Args:
            region: Region name
        """
        self._send_command('RSELECT', region)
        self.current_region = region
    
    def list_regions(self) -> List[str]:
        """List all available regions."""
        return self._send_command('REGIONS') or []
    
    def create_region(self, name: str, description: str = ''):
        """
        Create a new region.
        
        Args:
            name: Region name
            description: Optional description
        """
        self._send_command('RCREATE', name, description)
    
    def delete_region(self, name: str):
        """Delete a region."""
        self._send_command('RDROP', name)
    
    def purge_region(self, name: str):
        """Purge all entries in a region."""
        self._send_command('RPURGE', name)
    
    def region_info(self, name: str) -> str:
        """Get region information."""
        return self._send_command('RINFO', name)
    
    # ==================== Attribute Mapping ====================
    
    def set_attribute_mapping(self, region: str, mapping: Dict[str, str]) -> str:
        """
        Set attribute mapping for a region.
        
        When JSON data is stored in a region with attribute mapping,
        the source attribute names are automatically renamed to target names.
        
        Args:
            region: Region name
            mapping: Dictionary mapping source attribute names to target names
                     Example: {"firstName": "first_name", "lastName": "last_name"}
        
        Returns:
            OK on success
            
        Example:
            client.set_attribute_mapping('users', {
                'firstName': 'first_name',
                'lastName': 'last_name',
                'emailAddress': 'email'
            })
        """
        return self._send_command('RSETMAP', region, json.dumps(mapping))
    
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
        result = self._send_command('RGETMAP', region)
        if result:
            try:
                return json.loads(result)
            except (json.JSONDecodeError, TypeError):
                return None
        return None
    
    def clear_attribute_mapping(self, region: str) -> str:
        """
        Clear attribute mapping for a region.
        
        Args:
            region: Region name
            
        Returns:
            OK on success
            
        Example:
            client.clear_attribute_mapping('users')
        """
        return self._send_command('RCLEARMAP', region)
    
    # ==================== String Operations ====================
    
    def get(self, key: str) -> Optional[str]:
        """Get value by key."""
        return self._send_command('GET', key)
    
    def set(self, key: str, value: str, ttl: Optional[Union[int, timedelta]] = None) -> str:
        """
        Set a value.
        
        Args:
            key: Key name
            value: Value to store
            ttl: Optional TTL in seconds or timedelta
        """
        if ttl is not None:
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            return self._send_command('SET', key, value, 'EX', ttl)
        return self._send_command('SET', key, value)
    
    def setnx(self, key: str, value: str) -> bool:
        """Set if not exists."""
        return self._send_command('SETNX', key, value) == 1
    
    def setex(self, key: str, value: str, seconds: int) -> str:
        """Set with expiration."""
        return self._send_command('SETEX', key, seconds, value)
    
    def mget(self, *keys: str) -> List[Optional[str]]:
        """Get multiple values."""
        return self._send_command('MGET', *keys)
    
    def mset(self, mapping: Dict[str, str]):
        """Set multiple values."""
        args = []
        for k, v in mapping.items():
            args.extend([k, v])
        return self._send_command('MSET', *args)
    
    def incr(self, key: str) -> int:
        """Increment value."""
        return self._send_command('INCR', key)
    
    def incrby(self, key: str, amount: int) -> int:
        """Increment by amount."""
        return self._send_command('INCRBY', key, amount)
    
    def decr(self, key: str) -> int:
        """Decrement value."""
        return self._send_command('DECR', key)
    
    def decrby(self, key: str, amount: int) -> int:
        """Decrement by amount."""
        return self._send_command('DECRBY', key, amount)
    
    def append(self, key: str, value: str) -> int:
        """Append to string."""
        return self._send_command('APPEND', key, value)
    
    def strlen(self, key: str) -> int:
        """Get string length."""
        return self._send_command('STRLEN', key)
    
    # ==================== Key Operations ====================
    
    def keys(self, pattern: str = '*') -> List[str]:
        """
        Find keys matching pattern.
        
        Pattern supports:
        - * matches any sequence
        - ? matches single character
        - [abc] matches character class
        """
        result = self._send_command('KEYS', pattern)
        return result if result else []
    
    def ksearch(self, regex_pattern: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Search keys by regex pattern and return key-value pairs.
        
        Unlike keys() which uses glob patterns, this uses Java regex patterns.
        Returns list of dicts with 'key', 'value', 'type', 'ttl'.
        
        Args:
            regex_pattern: Java regex pattern (e.g., "user:\\d+", "order:.*")
            limit: Maximum number of results (default: 1000)
            
        Returns:
            List of dicts: [{"key": "...", "value": "...", "type": "...", "ttl": ...}, ...]
        """
        results = self._send_command('KSEARCH', regex_pattern, 'LIMIT', str(limit))
        
        if not results:
            return []
        
        parsed = []
        for result in results:
            if isinstance(result, str):
                try:
                    parsed.append(json.loads(result))
                except json.JSONDecodeError:
                    pass
        return parsed
    
    def scan(self, cursor: int = 0, match: str = '*', count: int = 10) -> Tuple[int, List[str]]:
        """
        Incrementally iterate keys.
        
        Returns:
            Tuple of (next_cursor, keys_list)
        """
        result = self._send_command('SCAN', cursor, 'MATCH', match, 'COUNT', count)
        
        # Handle response - should be [cursor_string, [keys...]]
        if not isinstance(result, list) or len(result) < 2:
            # Unexpected response format, return empty
            return 0, []
        
        try:
            next_cursor = int(result[0])
        except (ValueError, TypeError):
            # If cursor can't be parsed, return 0 to stop iteration
            return 0, []
        
        keys = result[1] if isinstance(result[1], list) else []
        return next_cursor, keys
    
    def exists(self, *keys: str) -> int:
        """Check if keys exist. Returns count of existing keys."""
        return self._send_command('EXISTS', *keys)
    
    def delete(self, *keys: str) -> int:
        """Delete keys. Returns count of deleted keys."""
        return self._send_command('DEL', *keys)
    
    def type(self, key: str) -> str:
        """Get key type."""
        return self._send_command('TYPE', key)
    
    def ttl(self, key: str) -> int:
        """Get TTL in seconds. Returns -1 if no TTL, -2 if key doesn't exist."""
        return self._send_command('TTL', key)
    
    def expire(self, key: str, seconds: int) -> bool:
        """Set key expiration."""
        return self._send_command('EXPIRE', key, seconds) == 1
    
    def persist(self, key: str) -> bool:
        """Remove TTL from key."""
        return self._send_command('PERSIST', key) == 1
    
    def rename(self, old_key: str, new_key: str):
        """Rename a key."""
        return self._send_command('RENAME', old_key, new_key)
    
    # ==================== Hash Operations ====================
    
    def hget(self, key: str, field: str) -> Optional[str]:
        """Get hash field value."""
        return self._send_command('HGET', key, field)
    
    def hset(self, key: str, field: str, value: str) -> int:
        """Set hash field."""
        return self._send_command('HSET', key, field, value)
    
    def hmset(self, key: str, mapping: Dict[str, str]):
        """Set multiple hash fields."""
        args = [key]
        for f, v in mapping.items():
            args.extend([f, v])
        return self._send_command('HMSET', *args)
    
    def hmget(self, key: str, *fields: str) -> List[Optional[str]]:
        """Get multiple hash fields."""
        return self._send_command('HMGET', key, *fields)
    
    def hgetall(self, key: str) -> Dict[str, str]:
        """Get all hash fields and values."""
        result = self._send_command('HGETALL', key)
        if not result:
            return {}
        return dict(zip(result[::2], result[1::2]))
    
    def hdel(self, key: str, *fields: str) -> int:
        """Delete hash fields."""
        return self._send_command('HDEL', key, *fields)
    
    def hexists(self, key: str, field: str) -> bool:
        """Check if hash field exists."""
        return self._send_command('HEXISTS', key, field) == 1
    
    def hkeys(self, key: str) -> List[str]:
        """Get all hash field names."""
        return self._send_command('HKEYS', key) or []
    
    def hvals(self, key: str) -> List[str]:
        """Get all hash values."""
        return self._send_command('HVALS', key) or []
    
    def hlen(self, key: str) -> int:
        """Get hash length."""
        return self._send_command('HLEN', key)
    
    # ==================== JSON Operations ====================
    
    def json_set(self, key: str, value: Any, region: Optional[str] = None,
                 ttl: Optional[Union[int, timedelta]] = None) -> str:
        """
        Store a JSON document.
        
        Args:
            key: Key name
            value: Python object to store as JSON
            region: Optional region (uses current region if not specified)
            ttl: Optional TTL in seconds or timedelta
        """
        # Switch region if specified
        original_region = self.current_region
        if region and region != original_region:
            self.select_region(region)
        
        try:
            json_str = json.dumps(value) if not isinstance(value, str) else value
            
            if ttl is not None:
                if isinstance(ttl, timedelta):
                    ttl = int(ttl.total_seconds())
                return self._send_command('JSET', key, json_str, '$', ttl)
            return self._send_command('JSET', key, json_str)
        finally:
            # Restore original region if we switched
            if region and region != original_region:
                self.select_region(original_region)
    
    def json_get(self, key: str, path: str = '$', region: Optional[str] = None) -> Any:
        """
        Get a JSON document or path.
        
        Args:
            key: Key name
            path: JSONPath expression (default: $ for root)
            region: Optional region
            
        Returns:
            Parsed JSON object, or None if key doesn't exist
        """
        original_region = self.current_region
        if region and region != original_region:
            self.select_region(region)
        
        try:
            result = self._send_command('JGET', key, path)
            # Handle None (key doesn't exist) or empty string
            if result is None or result == '':
                return None
            # Try to parse as JSON
            if isinstance(result, str):
                try:
                    return json.loads(result)
                except json.JSONDecodeError:
                    # If it's not valid JSON, return as-is (might be a simple string value)
                    return result
            return result
        finally:
            if region and region != original_region:
                self.select_region(original_region)
    
    def json_delete(self, key: str, path: str = '$') -> bool:
        """Delete a JSON path."""
        return self._send_command('JDEL', key, path) == 1
    
    def json_update(self, key: str, value: Any, region: Optional[str] = None,
                    ttl: Optional[Union[int, timedelta]] = None) -> Any:
        """
        Update/merge a JSON document (upsert with deep merge).
        
        Behavior:
        - If key doesn't exist: creates new entry with the JSON value
        - If key exists with JSON object: deep merges new JSON onto existing (new values override)
        - If key exists but not valid JSON: replaces with new JSON value
        
        Args:
            key: Key name
            value: Python object to merge as JSON
            region: Optional region (uses current region if not specified)
            ttl: Optional TTL in seconds or timedelta
            
        Returns:
            The resulting merged JSON object
            
        Example:
            # Initial user
            client.json_set('user:1', {'name': 'Alice', 'age': 30})
            
            # Update/merge - adds city, updates age
            result = client.json_update('user:1', {'age': 31, 'city': 'NYC'})
            # result: {'name': 'Alice', 'age': 31, 'city': 'NYC'}
        """
        original_region = self.current_region
        if region and region != original_region:
            self.select_region(region)
        
        try:
            json_str = json.dumps(value) if not isinstance(value, str) else value
            
            if ttl is not None:
                if isinstance(ttl, timedelta):
                    ttl = int(ttl.total_seconds())
                result = self._send_command('JUPDATE', key, json_str, ttl)
            else:
                result = self._send_command('JUPDATE', key, json_str)
            
            # Parse the returned merged JSON
            if result is None or result == '':
                return None
            if isinstance(result, str):
                try:
                    return json.loads(result)
                except json.JSONDecodeError:
                    return result
            return result
        finally:
            if region and region != original_region:
                self.select_region(original_region)
    
    def json_remove(self, key: str, attributes: List[str], region: Optional[str] = None) -> Any:
        """
        Remove specified attributes from a JSON document.
        
        Behavior:
        - If key exists and has valid JSON object: removes specified attributes and saves
        - If key doesn't exist or value is not JSON object: returns None (nothing done)
        
        Args:
            key: Key name
            attributes: List of attribute names to remove
            region: Optional region (uses current region if not specified)
            
        Returns:
            The updated JSON object, or None if nothing was done
            
        Example:
            # Initial user
            client.json_set('user:1', {'name': 'Alice', 'age': 30, 'city': 'NYC'})
            
            # Remove attributes
            result = client.json_remove('user:1', ['age', 'city'])
            # result: {'name': 'Alice'}
        """
        original_region = self.current_region
        if region and region != original_region:
            self.select_region(region)
        
        try:
            # Send attributes as JSON array
            attributes_json = json.dumps(attributes)
            result = self._send_command('JREMOVE', key, attributes_json)
            
            # Parse the returned JSON
            if result is None or result == '' or result == 0:
                return None
            if isinstance(result, str):
                try:
                    return json.loads(result)
                except json.JSONDecodeError:
                    return result
            return result
        finally:
            if region and region != original_region:
                self.select_region(original_region)
    
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
        original_region = self.current_region
        if region and region != original_region:
            self.select_region(region)
        
        try:
            results = self._send_command('JSEARCH', query)
            
            if not results:
                return []
            
            documents = []
            for result in results:
                # Result format: "key:json_value"
                # Key might contain colons (e.g., "prod:1001"), so find where JSON starts
                # JSON always starts with { or [
                if isinstance(result, str):
                    # Find the colon that precedes the JSON (look for :{ or :[)
                    json_start = -1
                    for i, c in enumerate(result):
                        if c == ':' and i + 1 < len(result) and result[i + 1] in '{[':
                            json_start = i
                            break
                    
                    if json_start > 0:
                        key = result[:json_start]
                        json_str = result[json_start + 1:]
                        try:
                            value = json.loads(json_str)
                            documents.append({'key': key, 'value': value})
                        except json.JSONDecodeError:
                            documents.append({'key': key, 'value': json_str})
                    else:
                        # Fallback: try to find last colon before first {
                        brace_pos = result.find('{')
                        if brace_pos > 0:
                            # Find last colon before brace
                            colon_pos = result.rfind(':', 0, brace_pos)
                            if colon_pos > 0:
                                key = result[:colon_pos]
                                json_str = result[colon_pos + 1:]
                                try:
                                    value = json.loads(json_str)
                                    documents.append({'key': key, 'value': value})
                                except json.JSONDecodeError:
                                    documents.append({'key': key, 'value': json_str})
            
            return documents
        finally:
            if region and region != original_region:
                self.select_region(original_region)
    
    # ==================== Generic Search (Convenience Method) ====================
    
    def generic_search(self, region: str = None, 
                       key: str = None,
                       keypattern: str = None,
                       search_type: str = None,
                       values: List[Dict[str, Any]] = None,
                       fields: List[str] = None,
                       limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Perform generic search with flexible options.
        
        This is a convenience method that combines various search operations.
        
        Supports multiple search modes:
        1. Simple key lookup: key="ABC"
        2. Key pattern (regex): keypattern="ABC.*"
        3. JSON attribute search: search_type="json", values=[{"k1": "abc"}]
        
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
            results = client.generic_search(key="user:1001")
            
            # Key pattern search
            results = client.generic_search(keypattern="user:.*")
            
            # JSON search with equality
            results = client.generic_search(
                search_type="json",
                values=[{"status": "active"}]
            )
            
            # Search with field projection
            results = client.generic_search(
                keypattern="user:.*",
                fields=["name", "email", "address.city"]
            )
        """
        r = region or self.current_region
        results = []
        
        original_region = self.current_region
        if r != original_region:
            self.select_region(r)
        
        try:
            if key:
                # Mode 1: Simple key lookup
                value = self.get(key)
                if value:
                    result_value = value
                    try:
                        result_value = json.loads(value)
                        if fields:
                            result_value = self._project_fields(result_value, fields)
                    except (json.JSONDecodeError, TypeError):
                        pass
                    results.append({'key': key, 'value': result_value})
                    
            elif keypattern:
                # Mode 2: Key pattern (regex) search
                # ksearch returns list of dicts with key, value, type, ttl
                search_results = self.ksearch(keypattern, limit)
                for item in search_results:
                    key = item.get('key')
                    value = item.get('value')
                    if key and value is not None:
                        result_value = value
                        # value might already be parsed or a string
                        if isinstance(value, str):
                            try:
                                result_value = json.loads(value)
                            except (json.JSONDecodeError, TypeError):
                                result_value = value
                        if fields and isinstance(result_value, dict):
                            result_value = self._project_fields(result_value, fields)
                        results.append({'key': key, 'value': result_value})
                        
            elif search_type == 'json' and values:
                # Mode 3: JSON attribute search
                # Build query from values
                query_parts = []
                for condition in values:
                    cond_type = condition.get('type', 'equals')
                    for field_name, field_value in condition.items():
                        if field_name == 'type':
                            continue
                        if isinstance(field_value, list):
                            # IN operator - search for each value
                            for v in field_value:
                                query_parts.append(f'$.{field_name}={v}')
                        elif cond_type == 'regex':
                            query_parts.append(f'$.{field_name} LIKE {field_value}')
                        else:
                            query_parts.append(f'$.{field_name}={field_value}')
                
                if query_parts:
                    query = ','.join(query_parts)
                    search_results = self.json_search(query)
                    for item in search_results[:limit]:
                        if fields:
                            item['value'] = self._project_fields(item['value'], fields)
                        results.append(item)
            
            return results
            
        finally:
            if r != original_region:
                self.select_region(original_region)
    
    def _project_fields(self, data: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
        """
        Project (filter) fields from a dictionary.
        Supports nested paths with dot notation.
        """
        if not isinstance(data, dict):
            return data
        
        result = {}
        for field_path in fields:
            value = self._get_nested_field(data, field_path)
            if value is not None:
                self._set_nested_field(result, field_path, value)
        return result
    
    def _get_nested_field(self, data: Dict[str, Any], field_path: str) -> Any:
        """Get a nested field value using dot notation."""
        parts = field_path.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current
    
    def _set_nested_field(self, result: Dict[str, Any], field_path: str, value: Any):
        """Set a nested field value using dot notation."""
        parts = field_path.split('.')
        current = result
        for i, part in enumerate(parts[:-1]):
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value
    
    # ==================== Server Operations ====================
    
    def info(self) -> str:
        """Get server information."""
        return self._send_command('INFO')
    
    def status(self) -> str:
        """Get server status."""
        return self._send_command('STATUS')
    
    def dbsize(self) -> int:
        """Get number of keys in current region."""
        return self._send_command('DBSIZE')
    
    def flushdb(self):
        """Flush current region."""
        return self._send_command('FLUSHDB')
    
    def time(self) -> Tuple[int, int]:
        """Get server time as (seconds, microseconds)."""
        result = self._send_command('TIME')
        return int(result[0]), int(result[1])
    
    def repl_info(self) -> str:
        """Get replication information."""
        return self._send_command('REPLINFO')


class KuberError(Exception):
    """Exception for Kuber errors."""
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


def example_1_basic_string_operations(client: KuberRedisClient):
    """Demonstrate basic string operations."""
    print_section("1. BASIC STRING OPERATIONS (GET, SET, MGET, MSET)")
    
    # Simple SET and GET
    client.set('greeting', 'Hello, Kuber!')
    value = client.get('greeting')
    print_result("SET/GET 'greeting'", value)
    
    # SET with TTL
    client.set('temp_key', 'expires in 60 seconds', ttl=60)
    print_result("SET with TTL (60s)", client.get('temp_key'))
    print_result("TTL remaining", client.ttl('temp_key'))
    
    # SET with timedelta
    client.set('session_data', 'user session', ttl=timedelta(hours=1))
    print_result("SET with timedelta (1 hour)", f"TTL: {client.ttl('session_data')} seconds")
    
    # SETNX - Set only if not exists
    result1 = client.setnx('unique_key', 'first value')
    result2 = client.setnx('unique_key', 'second value (should fail)')
    print_result("SETNX first attempt", result1)
    print_result("SETNX second attempt", result2)
    print_result("Final value", client.get('unique_key'))
    
    # SETEX - Set with expiration
    client.setex('cache_item', 'cached data', 300)
    print_result("SETEX (5 minutes TTL)", client.get('cache_item'))
    
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
    print_result("String length of 'message'", client.strlen('message'))


def example_2_mget_mset_operations(client: KuberRedisClient):
    """Demonstrate MGET and MSET operations."""
    print_section("2. MULTI-KEY OPERATIONS (MGET, MSET)")
    
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
    names = client.mget('user:1:name', 'user:2:name', 'user:3:name')
    print_result("MGET all names", names)
    
    emails = client.mget('user:1:email', 'user:2:email', 'user:3:email')
    print_result("MGET all emails", emails)
    
    # MGET with some missing keys
    mixed = client.mget('user:1:name', 'user:99:name', 'user:2:name')
    print_result("MGET with missing key (user:99)", mixed)
    
    # Get all properties for one user
    user1_props = client.mget('user:1:name', 'user:1:email', 'user:1:role')
    print_result("MGET user:1 all properties", user1_props)


def example_3_key_pattern_search(client: KuberRedisClient):
    """Demonstrate key pattern searching with KEYS and SCAN."""
    print_section("3. KEY PATTERN SEARCH (KEYS, SCAN)")
    
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
    keys_1xxx = client.keys('product:electronics:100?')
    print_result("KEYS product:electronics:100?", keys_1xxx)
    
    # Pattern with character class
    session_ad = client.keys('session:[ad]*')
    print_result("KEYS session:[ad]*", session_ad)
    
    # SCAN for iterative search (better for large datasets)
    print("\n  SCAN iteration through all keys:")
    cursor = 0
    iteration = 0
    total_keys = []
    while True:
        cursor, keys = client.scan(cursor, match='*', count=5)
        iteration += 1
        total_keys.extend(keys)
        print(f"    Iteration {iteration}: cursor={cursor}, found {len(keys)} keys")
        if cursor == 0:
            break
    print(f"    Total keys found via SCAN: {len(total_keys)}")


def example_4_hash_operations(client: KuberRedisClient):
    """Demonstrate hash operations."""
    print_section("4. HASH OPERATIONS (HGET, HSET, HMSET, HGETALL)")
    
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
    fields = client.hmget('user:profile:2', 'name', 'email', 'city')
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
    
    # HLEN - Get hash length
    print_result("HLEN", client.hlen('user:profile:1'))
    
    # HEXISTS - Check field existence
    print_result("HEXISTS 'name'", client.hexists('user:profile:1', 'name'))
    print_result("HEXISTS 'salary'", client.hexists('user:profile:1', 'salary'))
    
    # HDEL - Delete fields
    client.hdel('user:profile:1', 'age')
    print_result("After HDEL 'age', remaining keys", client.hkeys('user:profile:1'))


def example_5_region_operations(client: KuberRedisClient):
    """Demonstrate region management."""
    print_section("5. REGION OPERATIONS (CREATE, SELECT, LIST)")
    
    # List existing regions
    regions = client.list_regions()
    print_result("Initial regions", regions)
    
    # Create custom regions
    client.create_region('inventory', 'Product inventory data')
    client.create_region('customers', 'Customer information')
    client.create_region('analytics', 'Analytics and metrics')
    print("  Created 3 new regions: inventory, customers, analytics")
    
    # List regions again
    regions = client.list_regions()
    print_result("Regions after creation", regions)
    
    # Select and use inventory region
    client.select_region('inventory')
    print(f"\n  Selected region: {client.current_region}")
    
    # Store data in inventory region
    client.set('sku:1001', 'Laptop - Stock: 50')
    client.set('sku:1002', 'Mouse - Stock: 200')
    client.set('sku:1003', 'Keyboard - Stock: 150')
    
    print_result("DBSIZE in inventory region", client.dbsize())
    print_result("KEYS in inventory", client.keys('*'))
    
    # Select customers region
    client.select_region('customers')
    print(f"\n  Selected region: {client.current_region}")
    
    # Store data in customers region
    client.set('cust:C001', 'Acme Corp')
    client.set('cust:C002', 'TechStart Inc')
    
    print_result("DBSIZE in customers region", client.dbsize())
    print_result("KEYS in customers", client.keys('*'))
    
    # Verify isolation - inventory keys not visible in customers
    laptop = client.get('sku:1001')
    print_result("GET sku:1001 from customers region", laptop)  # Should be None
    
    # Switch back to default
    client.select_region('default')
    print(f"\n  Back to region: {client.current_region}")


def example_6_json_operations_with_regions(client: KuberRedisClient):
    """Demonstrate JSON operations with specific region storage."""
    print_section("6. JSON OPERATIONS WITH REGION STORAGE (JSET, JGET)")
    
    # Create a products region for JSON documents
    client.create_region('products_json', 'Product catalog as JSON')
    
    # Store JSON documents in specific region
    print("\n  Storing products in 'products_json' region:")
    
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
        client.json_set(product['id'], product, region='products_json')
        print(f"    Stored: {product['id']} - {product['name']}")
    
    # Retrieve JSON from specific region
    print("\n  Retrieving products from 'products_json' region:")
    headphones = client.json_get('prod:1001', region='products_json')
    print_result("GET prod:1001", headphones)
    
    # Get specific JSON path
    price = client.json_get('prod:1002', '$.price', region='products_json')
    print_result("GET prod:1002 $.price", price)
    
    specs = client.json_get('prod:1001', '$.specs', region='products_json')
    print_result("GET prod:1001 $.specs", specs)
    
    # Store orders in a separate region with TTL
    client.create_region('orders_json', 'Customer orders')
    
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
    
    print("\n  Storing orders in 'orders_json' region:")
    for order in orders:
        client.json_set(order['order_id'], order, region='orders_json', ttl=timedelta(days=30))
        print(f"    Stored: {order['order_id']} - Status: {order['status']} - Total: ${order['total']}")


def example_7_json_deep_search(client: KuberRedisClient):
    """Demonstrate JSON deep search capabilities."""
    print_section("7. JSON DEEP SEARCH (JSEARCH with multiple operators)")
    
    # Search in products_json region
    print("\n  === SEARCH BY EQUALITY ===")
    
    results = client.json_search('$.category=Electronics', region='products_json')
    print_result("$.category=Electronics", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (${doc['value']['price']})")
    
    results = client.json_search('$.in_stock=true', region='products_json')
    print_result("$.in_stock=true", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (Stock: {doc['value']['stock_count']})")
    
    print("\n  === SEARCH BY COMPARISON ===")
    
    results = client.json_search('$.price>100', region='products_json')
    print_result("$.price>100", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (${doc['value']['price']})")
    
    results = client.json_search('$.price<=50', region='products_json')
    print_result("$.price<=50", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (${doc['value']['price']})")
    
    results = client.json_search('$.stock_count>=100', region='products_json')
    print_result("$.stock_count>=100", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (Stock: {doc['value']['stock_count']})")
    
    print("\n  === SEARCH NESTED FIELDS ===")
    
    results = client.json_search('$.specs.backlighting=RGB', region='products_json')
    print_result("$.specs.backlighting=RGB", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']}")
    
    results = client.json_search('$.specs.lumbar_support=true', region='products_json')
    print_result("$.specs.lumbar_support=true", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']}")
    
    print("\n  === SEARCH WITH INEQUALITY ===")
    
    results = client.json_search('$.category!=Electronics', region='products_json')
    print_result("$.category!=Electronics", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (Category: {doc['value']['category']})")
    
    print("\n  === SEARCH WITH PATTERN MATCHING (LIKE) ===")
    
    results = client.json_search('$.name LIKE %Keyboard%', region='products_json')
    print_result("$.name LIKE %Keyboard%", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']}")
    
    results = client.json_search('$.brand LIKE %Tech%', region='products_json')
    print_result("$.brand LIKE %Tech%", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (Brand: {doc['value']['brand']})")
    
    print("\n  === SEARCH ARRAY CONTAINS ===")
    
    results = client.json_search('$.tags CONTAINS wireless', region='products_json')
    print_result("$.tags CONTAINS wireless", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (Tags: {doc['value']['tags']})")
    
    results = client.json_search('$.tags CONTAINS gaming', region='products_json')
    print_result("$.tags CONTAINS gaming", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (Tags: {doc['value']['tags']})")
    
    print("\n  === COMBINED CONDITIONS ===")
    
    results = client.json_search('$.in_stock=true,$.price<100', region='products_json')
    print_result("$.in_stock=true,$.price<100", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (${doc['value']['price']}, In Stock)")
    
    results = client.json_search('$.category=Electronics,$.stock_count>50', region='products_json')
    print_result("$.category=Electronics,$.stock_count>50", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']} (Stock: {doc['value']['stock_count']})")


def example_8_cross_region_json_search(client: KuberRedisClient):
    """Demonstrate JSON search across different regions."""
    print_section("8. CROSS-REGION JSON SEARCH")
    
    print("\n  === SEARCH IN 'products_json' REGION ===")
    
    # Search by subcategory
    results = client.json_search('$.subcategory=Audio', region='products_json')
    print_result("$.subcategory=Audio", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['name']}")
    
    print("\n  === SEARCH IN 'orders_json' REGION ===")
    
    # Search by order status
    results = client.json_search('$.status=shipped', region='orders_json')
    print_result("$.status=shipped", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: Customer {doc['value']['customer_id']}, Total: ${doc['value']['total']}")
    
    # Search by customer
    results = client.json_search('$.customer_id=cust:C001', region='orders_json')
    print_result("$.customer_id=cust:C001", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: Status: {doc['value']['status']}, Total: ${doc['value']['total']}")
    
    # Search by total amount
    results = client.json_search('$.total>200', region='orders_json')
    print_result("$.total>200", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: ${doc['value']['total']}")
    
    # Search nested shipping info
    results = client.json_search('$.shipping.method=express', region='orders_json')
    print_result("$.shipping.method=express", f"{len(results)} results")
    for doc in results:
        print(f"    - {doc['key']}: {doc['value']['shipping']['address']}")


def example_9_server_information(client: KuberRedisClient):
    """Demonstrate server information commands."""
    print_section("9. SERVER INFORMATION")
    
    # PING
    print_result("PING", client.ping())
    
    # INFO
    info = client.info()
    print("\n  INFO (first 600 chars):")
    print(f"    {info[:600]}...")
    
    # STATUS
    status = client.status()
    print_result("\n  STATUS", status[:400] + '...')
    
    # TIME
    seconds, microseconds = client.time()
    print_result("TIME", f"seconds={seconds}, microseconds={microseconds}")
    
    # DBSIZE for each region
    client.select_region('default')
    print_result("DBSIZE (default)", client.dbsize())
    
    client.select_region('products_json')
    print_result("DBSIZE (products_json)", client.dbsize())
    
    client.select_region('orders_json')
    print_result("DBSIZE (orders_json)", client.dbsize())
    
    # REPL_INFO
    repl = client.repl_info()
    print_result("REPLINFO", repl[:200] if repl else "N/A")


def example_10_key_management(client: KuberRedisClient):
    """Demonstrate key management operations."""
    print_section("10. KEY MANAGEMENT (EXISTS, DELETE, EXPIRE, RENAME)")
    
    client.select_region('default')
    
    # EXISTS
    client.set('exists_test_1', 'value1')
    client.set('exists_test_2', 'value2')
    print_result("EXISTS exists_test_1", client.exists('exists_test_1'))
    print_result("EXISTS nonexistent_key", client.exists('nonexistent_key'))
    print_result("EXISTS multiple keys (2 exist, 1 not)", 
                 client.exists('exists_test_1', 'exists_test_2', 'nonexistent'))
    
    # TYPE
    print_result("TYPE of string key", client.type('exists_test_1'))
    client.hset('hash_key', 'field', 'value')
    print_result("TYPE of hash key", client.type('hash_key'))
    
    # EXPIRE and TTL
    client.set('expire_test', 'will expire')
    print_result("TTL before EXPIRE", client.ttl('expire_test'))
    client.expire('expire_test', 120)
    print_result("TTL after EXPIRE(120)", client.ttl('expire_test'))
    
    # PERSIST
    client.persist('expire_test')
    print_result("TTL after PERSIST", client.ttl('expire_test'))
    
    # RENAME
    client.set('old_name', 'some data')
    client.rename('old_name', 'new_name')
    print_result("GET 'new_name' after RENAME", client.get('new_name'))
    print_result("GET 'old_name' after RENAME", client.get('old_name'))
    
    # DELETE
    client.set('to_delete_1', 'value1')
    client.set('to_delete_2', 'value2')
    deleted = client.delete('to_delete_1', 'to_delete_2', 'nonexistent')
    print_result("DEL (3 keys, 2 existed)", deleted)


def cleanup(client: KuberRedisClient):
    """Clean up test data."""
    print_section("CLEANUP")
    
    # Purge and delete test regions
    regions_to_clean = ['inventory', 'customers', 'analytics', 
                        'products_json', 'orders_json']
    
    for region in regions_to_clean:
        try:
            client.purge_region(region)
            client.delete_region(region)
            print(f"  Cleaned up region: {region}")
        except KuberError as e:
            print(f"  Warning for {region}: {e}")
    
    # Clean default region
    client.select_region('default')
    client.flushdb()
    print("  Flushed default region")


def main():
    """Main function to run all examples."""
    parser = argparse.ArgumentParser(
        description='Kuber Standalone Python Client - Redis Protocol Examples',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python kuber_redis_standalone.py --api-key kub_your_api_key_here
  python kuber_redis_standalone.py -H 192.168.1.100 -P 6380 -k kub_your_api_key
  python kuber_redis_standalone.py --api-key kub_xxx --no-cleanup
        """
    )
    parser.add_argument('--host', '-H', default='localhost', help='Kuber server host (default: localhost)')
    parser.add_argument('--port', '-P', type=int, default=6380, help='Redis protocol port (default: 6380)')
    parser.add_argument('--api-key', '-k', required=True, help='API Key for authentication (must start with kub_)')
    parser.add_argument('--no-cleanup', action='store_true', help='Skip cleanup at end')
    args = parser.parse_args()
    
    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║   KUBER STANDALONE PYTHON CLIENT - REDIS PROTOCOL WITH EXTENSIONS   ║
║                                                                      ║
║   Connecting to: {args.host}:{args.port}                                       ║
║   Auth: API Key                                                       ║
║   Protocol: Redis RESP with Kuber Extensions                         ║
╚══════════════════════════════════════════════════════════════════════╝
""")
    
    try:
        with KuberRedisClient(args.host, args.port, api_key=args.api_key) as client:
            print("  Authentication successful!\n")
            
            # Run all examples
            example_1_basic_string_operations(client)
            example_2_mget_mset_operations(client)
            example_3_key_pattern_search(client)
            example_4_hash_operations(client)
            example_5_region_operations(client)
            example_6_json_operations_with_regions(client)
            example_7_json_deep_search(client)
            example_8_cross_region_json_search(client)
            example_9_server_information(client)
            example_10_key_management(client)
            
            if not args.no_cleanup:
                cleanup(client)
            
            print_section("ALL EXAMPLES COMPLETED SUCCESSFULLY")
            print("""
    This standalone client demonstrated:
    ✓ API Key authentication (v1.6.5)
    ✓ Basic string operations (GET, SET, INCR, DECR, APPEND)
    ✓ Multi-key operations (MGET, MSET)
    ✓ Key pattern search (KEYS with wildcards, SCAN)
    ✓ Hash operations (HGET, HSET, HMSET, HGETALL, HKEYS, HVALS)
    ✓ Region management (CREATE, SELECT, LIST, PURGE, DELETE)
    ✓ JSON storage in specific regions with TTL
    ✓ Deep JSON search with multiple operators:
      - Equality (=)
      - Comparison (>, <, >=, <=)
      - Inequality (!=)
      - Pattern matching (LIKE)
      - Array contains (CONTAINS)
      - Combined conditions
    ✓ Cross-region JSON search
    ✓ Server information commands
    ✓ Key management (EXISTS, DELETE, EXPIRE, RENAME)
""")
            
    except KuberError as e:
        print(f"\n  ERROR: Kuber error: {e}", file=sys.stderr)
        sys.exit(1)
    except ConnectionRefusedError:
        print(f"\n  ERROR: Could not connect to Kuber at {args.host}:{args.port}", file=sys.stderr)
        print("         Make sure Kuber server is running.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\n  ERROR: Unexpected error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
