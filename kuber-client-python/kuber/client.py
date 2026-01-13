# Copyright © 2025-2030, All Rights Reserved
# Ashutosh Sinha | Email: ajsinha@gmail.com
#
# Legal Notice: This module and the associated software architecture are proprietary
# and confidential. Unauthorized copying, distribution, modification, or use is
# strictly prohibited without explicit written permission from the copyright holder.
#
# Patent Pending: Certain architectural patterns and implementations described in
# this module may be subject to patent applications.

"""
Kuber Client - Redis Protocol Client Implementation

This client uses the Redis protocol with Kuber extensions for regions and JSON queries.
For REST API access, use KuberRestClient from kuber.rest_client module.

v1.6.5: API Key Authentication ONLY
All programmatic access requires an API key (starts with "kub_").
Username/password authentication is only for the Web UI.

Usage:
    # Create an API key in the Kuber Web UI (Admin → API Keys)
    with KuberClient('localhost', 6380, api_key='kub_your_api_key_here') as client:
        client.set('key', 'value')
        value = client.get('key')
"""

import socket
import json
import logging
from typing import Any, Dict, List, Optional, Set, Union
from datetime import timedelta


logger = logging.getLogger(__name__)


class KuberException(Exception):
    """Exception raised for Kuber server errors."""
    pass


class KuberClient:
    """
    Python client for Kuber Distributed Cache using Redis Protocol.
    
    v1.6.5: API Key Authentication ONLY.
    All programmatic access requires an API key (starts with "kub_").
    Username/password authentication is only for the Web UI.
    
    Usage:
        with KuberClient('localhost', 6380, api_key='kub_xxx') as client:
            client.set('key', 'value')
            value = client.get('key')
    """
    
    DEFAULT_PORT = 6380
    DEFAULT_TIMEOUT = 30.0
    BUFFER_SIZE = 8192
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = DEFAULT_PORT,
        api_key: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT,
        region: str = 'default'
    ):
        """
        Initialize Kuber client.
        
        Args:
            host: Server hostname
            port: Server port (default: 6380)
            api_key: API Key for authentication (must start with "kub_")
            timeout: Socket timeout in seconds (default: 30)
            region: Initial region to select (default: 'default')
            
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
        self.api_key = api_key
        self._socket: Optional[socket.socket] = None
        self._current_region = region
        self._initial_region = region
        
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def connect(self) -> None:
        """Establish connection to Kuber server."""
        logger.debug(f"Connecting to Kuber at {self.host}:{self.port}")
        
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self.timeout)
        self._socket.connect((self.host, self.port))
        
        # Authenticate with API key
        if not self.auth(self.api_key):
            raise KuberException("Authentication failed - invalid API key")
        
        if self._initial_region != 'default':
            self.select_region(self._initial_region)
        
        logger.info(f"Connected to Kuber at {self.host}:{self.port}")
    
    def close(self) -> None:
        """Close the connection."""
        if self._socket:
            try:
                self._send_command('QUIT')
            except Exception:
                pass
            finally:
                self._socket.close()
                self._socket = None
        logger.info("Connection closed")
    
    def ping(self) -> str:
        """Ping the server."""
        return self._send_command('PING')
    
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
    
    # ==================== Region Operations ====================
    
    @property
    def current_region(self) -> str:
        """Get the current region name."""
        return self._current_region
    
    def select_region(self, region: str) -> None:
        """Select a region (creates if not exists)."""
        self._send_command('RSELECT', region)
        self._current_region = region
    
    def list_regions(self) -> List[str]:
        """List all available regions."""
        return self._send_command_list('REGIONS')
    
    def create_region(self, name: str, description: str = '') -> None:
        """Create a new region."""
        self._send_command('RCREATE', name, description)
    
    def delete_region(self, name: str) -> None:
        """Delete a region and all its data."""
        self._send_command('RDROP', name)
    
    def purge_region(self, name: str) -> None:
        """Purge all entries in a region."""
        self._send_command('RPURGE', name)
    
    def region_info(self, name: str = None) -> Dict[str, Any]:
        """Get information about a region."""
        region = name or self._current_region
        result = self._send_command('RINFO', region)
        if result:
            return json.loads(result)
        return {}
    
    # ==================== String Operations ====================
    
    def get(self, key: str) -> Optional[str]:
        """Get a value by key."""
        return self._send_command('GET', key)
    
    def set(
        self,
        key: str,
        value: str,
        ttl: Optional[Union[int, timedelta]] = None,
        nx: bool = False,
        xx: bool = False
    ) -> Optional[str]:
        """Set a value."""
        args = ['SET', key, value]
        
        if ttl is not None:
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            args.extend(['EX', str(ttl)])
        
        if nx:
            args.append('NX')
        elif xx:
            args.append('XX')
        
        return self._send_command(*args)
    
    def setnx(self, key: str, value: str) -> bool:
        """Set if not exists."""
        return self._send_command('SETNX', key, value) == '1'
    
    def setex(self, key: str, value: str, seconds: int) -> str:
        """Set with expiration."""
        return self._send_command('SETEX', key, str(seconds), value)
    
    def mget(self, *keys: str) -> List[Optional[str]]:
        """Get multiple values."""
        return self._send_command_list('MGET', *keys)
    
    def mset(self, mapping: Dict[str, str]) -> str:
        """Set multiple values."""
        args = ['MSET']
        for k, v in mapping.items():
            args.extend([k, v])
        return self._send_command(*args)
    
    def incr(self, key: str) -> int:
        """Increment a value."""
        return int(self._send_command('INCR', key))
    
    def incrby(self, key: str, amount: int) -> int:
        """Increment by amount."""
        return int(self._send_command('INCRBY', key, str(amount)))
    
    def decr(self, key: str) -> int:
        """Decrement a value."""
        return int(self._send_command('DECR', key))
    
    def decrby(self, key: str, amount: int) -> int:
        """Decrement by amount."""
        return int(self._send_command('DECRBY', key, str(amount)))
    
    def append(self, key: str, value: str) -> int:
        """Append to a value."""
        return int(self._send_command('APPEND', key, value))
    
    def strlen(self, key: str) -> int:
        """Get string length."""
        return int(self._send_command('STRLEN', key))
    
    # ==================== Key Operations ====================
    
    def delete(self, *keys: str) -> int:
        """Delete one or more keys."""
        return int(self._send_command('DEL', *keys))
    
    def exists(self, *keys: str) -> int:
        """Check how many keys exist."""
        return int(self._send_command('EXISTS', *keys))
    
    def expire(self, key: str, seconds: int) -> bool:
        """Set expiration on a key."""
        return self._send_command('EXPIRE', key, str(seconds)) == '1'
    
    def ttl(self, key: str) -> int:
        """Get TTL of a key."""
        return int(self._send_command('TTL', key))
    
    def persist(self, key: str) -> bool:
        """Remove expiration from a key."""
        return self._send_command('PERSIST', key) == '1'
    
    def type(self, key: str) -> str:
        """Get the type of a key."""
        return self._send_command('TYPE', key)
    
    def keys(self, pattern: str = '*') -> Set[str]:
        """Find keys matching pattern."""
        return set(self._send_command_list('KEYS', pattern))
    
    def scan(self, cursor: int = 0, match: str = '*', count: int = 100) -> tuple:
        """Incrementally iterate over keys."""
        result = self._send_command('SCAN', str(cursor), 'MATCH', match, 'COUNT', str(count))
        if result:
            parts = result.split('\n')
            next_cursor = int(parts[0]) if parts else 0
            keys = parts[1:] if len(parts) > 1 else []
            return (next_cursor, keys)
        return (0, [])
    
    def rename(self, old_key: str, new_key: str) -> str:
        """Rename a key."""
        return self._send_command('RENAME', old_key, new_key)
    
    # ==================== Hash Operations ====================
    
    def hget(self, key: str, field: str) -> Optional[str]:
        """Get a hash field."""
        return self._send_command('HGET', key, field)
    
    def hset(self, key: str, field: str, value: str) -> int:
        """Set a hash field."""
        return int(self._send_command('HSET', key, field, value))
    
    def hmset(self, key: str, mapping: Dict[str, str]) -> str:
        """Set multiple hash fields."""
        args = ['HMSET', key]
        for f, v in mapping.items():
            args.extend([f, v])
        return self._send_command(*args)
    
    def hmget(self, key: str, *fields: str) -> List[Optional[str]]:
        """Get multiple hash fields."""
        return self._send_command_list('HMGET', key, *fields)
    
    def hgetall(self, key: str) -> Dict[str, str]:
        """Get all hash fields and values."""
        items = self._send_command_list('HGETALL', key)
        return dict(zip(items[::2], items[1::2])) if items else {}
    
    def hdel(self, key: str, *fields: str) -> int:
        """Delete hash fields."""
        return int(self._send_command('HDEL', key, *fields))
    
    def hexists(self, key: str, field: str) -> bool:
        """Check if hash field exists."""
        return self._send_command('HEXISTS', key, field) == '1'
    
    def hkeys(self, key: str) -> Set[str]:
        """Get all hash fields."""
        return set(self._send_command_list('HKEYS', key))
    
    def hvals(self, key: str) -> List[str]:
        """Get all hash values."""
        return self._send_command_list('HVALS', key)
    
    def hlen(self, key: str) -> int:
        """Get hash length."""
        return int(self._send_command('HLEN', key))
    
    def hincrby(self, key: str, field: str, amount: int) -> int:
        """Increment hash field by integer."""
        return int(self._send_command('HINCRBY', key, field, str(amount)))
    
    # ==================== JSON Operations ====================
    
    def json_set(
        self,
        key: str,
        value: Any,
        path: str = '$',
        ttl: Optional[Union[int, timedelta]] = None
    ) -> str:
        """Set a JSON value."""
        json_str = json.dumps(value) if not isinstance(value, str) else value
        args = ['JSET', key, json_str, path]
        
        if ttl is not None:
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            args.append(str(ttl))
        
        return self._send_command(*args)
    
    def json_get(self, key: str, path: str = '$') -> Any:
        """Get a JSON value."""
        result = self._send_command('JGET', key, path)
        if result:
            return json.loads(result)
        return None
    
    def json_mget(self, *keys: str, path: str = '$') -> List[Any]:
        """Get multiple JSON values."""
        results = self._send_command_list('JMGET', *keys, path)
        return [json.loads(r) if r else None for r in results]
    
    def json_delete(self, key: str, path: str = '$') -> bool:
        """Delete a JSON path."""
        return self._send_command('JDEL', key, path) == '1'
    
    def json_type(self, key: str, path: str = '$') -> str:
        """Get JSON type at path."""
        return self._send_command('JTYPE', key, path)
    
    def json_search(self, query: str, region: str = None) -> List[Dict[str, Any]]:
        """
        Search JSON documents using deep search.
        
        Query format examples:
            - 'field=value' - single value match
            - 'field=[value1|value2|value3]' - IN clause (match any of the values)
            - 'field1=value1,field2>value2' - multiple conditions (AND)
            - 'status=[active|pending],country=[USA|UK]' - multiple IN clauses
            - Operators: =, !=, >, <, >=, <=, ~= (regex)
        
        Args:
            query: Query string with conditions
            region: Optional region to search (uses current if not specified)
        
        Returns:
            List of dicts with 'key' and 'value' for matching documents
        
        Example:
            # Single value
            results = client.json_search("status=active")
            
            # IN clause - multiple values for one field
            results = client.json_search("status=[active|pending]")
            
            # Multiple attributes with IN clauses
            results = client.json_search("status=[active|pending],country=[USA|UK|CA]")
        
        Since: 1.7.8 - Added IN clause support with [value1|value2|...] syntax
        """
        args = ['JSEARCH', query]
        if region:
            args.append(region)
        
        results = self._send_command_list(*args)
        documents = []
        
        for result in results:
            if result and ':' in result:
                idx = result.index(':')
                key = result[:idx]
                json_str = result[idx+1:]
                try:
                    documents.append({
                        'key': key,
                        'value': json.loads(json_str)
                    })
                except json.JSONDecodeError:
                    logger.warning(f"Failed to parse JSON for key {key}")
        
        return documents
    
    def json_search_in(self, conditions: Dict[str, List[str]], region: str = None) -> List[Dict[str, Any]]:
        """
        Search JSON documents with IN clause support.
        
        Convenience method for building queries with multiple attribute conditions,
        each potentially matching multiple values (IN clause).
        
        Args:
            conditions: Dict mapping field names to lists of acceptable values
            region: Optional region to search (uses current if not specified)
        
        Returns:
            List of dicts with 'key' and 'value' for matching documents
        
        Example:
            # Search for trades where status is active OR pending 
            # AND country is USA OR UK OR CA
            conditions = {
                "status": ["active", "pending"],
                "country": ["USA", "UK", "CA"]
            }
            results = client.json_search_in(conditions)
        
        Since: 1.7.8
        """
        query = self.build_in_clause_query(conditions)
        return self.json_search(query, region)
    
    @staticmethod
    def build_in_clause_query(conditions: Dict[str, List[str]]) -> str:
        """
        Build a query string with IN clause syntax from conditions dict.
        
        Args:
            conditions: Dict mapping field names to lists of acceptable values
        
        Returns:
            Query string in format: field1=[v1|v2],field2=[v3|v4]
        
        Example:
            conditions = {"status": ["active", "pending"], "country": ["USA", "UK"]}
            query = KuberClient.build_in_clause_query(conditions)
            # Returns: "status=[active|pending],country=[USA|UK]"
        
        Since: 1.7.8
        """
        if not conditions:
            return ""
        
        parts = []
        for field, values in conditions.items():
            if not field or not values:
                continue
            
            if len(values) == 1:
                parts.append(f"{field}={values[0]}")
            else:
                values_str = "|".join(str(v) for v in values)
                parts.append(f"{field}=[{values_str}]")
        
        return ",".join(parts)
    
    def json_query(self, jsonpath: str, region: str = None) -> List[Dict[str, Any]]:
        """Query JSON documents using JSONPath expression."""
        args = ['JQUERY', jsonpath]
        if region:
            args.append(region)
        
        results = self._send_command_list(*args)
        return [json.loads(r) for r in results if r]
    
    # ==================== Server Operations ====================
    
    def info(self, section: str = None) -> str:
        """Get server info."""
        if section:
            return self._send_command('INFO', section)
        return self._send_command('INFO')
    
    def dbsize(self) -> int:
        """Get number of entries in current region."""
        return int(self._send_command('DBSIZE'))
    
    def flushdb(self) -> str:
        """Flush current region."""
        return self._send_command('FLUSHDB')
    
    def status(self) -> Dict[str, Any]:
        """Get server status as JSON."""
        result = self._send_command('STATUS')
        return json.loads(result) if result else {}
    
    def repl_info(self) -> str:
        """Get replication info."""
        return self._send_command('REPLINFO')
    
    def time(self) -> tuple:
        """Get server time."""
        result = self._send_command('TIME')
        if result:
            parts = result.split('\n')
            return (int(parts[0]), int(parts[1])) if len(parts) >= 2 else (0, 0)
        return (0, 0)
    
    # ==================== Command Execution ====================
    
    def execute(self, *args: str) -> Any:
        """Execute a raw command."""
        return self._send_command(*args)
    
    def _send_command(self, *args: str) -> Optional[str]:
        """Send a command and return the response."""
        if not self._socket:
            raise KuberException("Not connected")
        
        cmd_parts = []
        for arg in args:
            arg_str = str(arg)
            if ' ' in arg_str or '"' in arg_str or '\n' in arg_str or '\r' in arg_str:
                escaped = arg_str.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                cmd_parts.append(f'"{escaped}"')
            else:
                cmd_parts.append(arg_str)
        
        cmd = ' '.join(cmd_parts) + '\r\n'
        
        logger.debug(f"Sending: {cmd.strip()}")
        self._socket.sendall(cmd.encode('utf-8'))
        
        return self._read_response()
    
    def _send_command_list(self, *args: str) -> List[str]:
        """Send a command and return list response."""
        response = self._send_command(*args)
        if response is None:
            return []
        return [r for r in response.split('\n') if r] if response else []
    
    def _read_response(self) -> Optional[str]:
        """Read and parse response from server."""
        response = b''
        
        while True:
            chunk = self._socket.recv(self.BUFFER_SIZE)
            if not chunk:
                break
            response += chunk
            if response.endswith(b'\r\n'):
                break
        
        line = response.decode('utf-8').strip()
        
        logger.debug(f"Received: {line[:200]}...")
        
        if not line:
            return None
        
        if line.startswith('+'):
            return line[1:]
        elif line.startswith('-'):
            raise KuberException(line[1:])
        elif line.startswith(':'):
            return line[1:]
        elif line.startswith('$'):
            length = int(line[1:])
            if length == -1:
                return None
            data = b''
            while len(data) < length + 2:
                data += self._socket.recv(length + 2 - len(data))
            return data[:-2].decode('utf-8')
        elif line.startswith('*'):
            count = int(line[1:])
            if count == -1:
                return None
            items = []
            for _ in range(count):
                items.append(self._read_response())
            return '\n'.join(str(i) if i is not None else '' for i in items)
        
        return line


# Alias for convenience
Client = KuberClient
