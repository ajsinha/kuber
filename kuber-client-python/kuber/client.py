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
Kuber Client - Main client implementation
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
    Python client for Kuber Distributed Cache.
    
    Supports Redis protocol with Kuber extensions for regions and JSON queries.
    
    Example:
        >>> with KuberClient('localhost', 6380) as client:
        ...     client.set('user:1001', 'John Doe')
        ...     name = client.get('user:1001')
        ...     
        ...     # JSON operations
        ...     client.json_set('user:1002', {'name': 'Jane', 'age': 30})
        ...     user = client.json_get('user:1002')
        ...     
        ...     # Region operations
        ...     client.select_region('sessions')
        ...     client.set('session:abc', 'data', ttl=timedelta(minutes=30))
    """
    
    DEFAULT_PORT = 6380
    DEFAULT_TIMEOUT = 30.0
    BUFFER_SIZE = 4096
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = DEFAULT_PORT,
        timeout: float = DEFAULT_TIMEOUT,
        password: Optional[str] = None
    ):
        """
        Initialize Kuber client.
        
        Args:
            host: Server hostname
            port: Server port (default: 6380)
            timeout: Socket timeout in seconds (default: 30)
            password: Optional password for authentication
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.password = password
        self._socket: Optional[socket.socket] = None
        self._current_region = 'default'
        
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
        
        if self.password:
            self.auth(self.password)
        
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
    
    def auth(self, password: str) -> bool:
        """Authenticate with the server."""
        result = self._send_command('AUTH', password)
        return result == 'OK'
    
    # ==================== Region Operations ====================
    
    @property
    def current_region(self) -> str:
        """Get the current region name."""
        return self._current_region
    
    def select_region(self, region: str) -> None:
        """
        Select a region (creates if not exists).
        
        Args:
            region: Region name to select
        """
        self._send_command('RSELECT', region)
        self._current_region = region
    
    def list_regions(self) -> List[str]:
        """List all available regions."""
        return self._send_command_list('REGIONS')
    
    def create_region(self, name: str, description: str = '') -> None:
        """Create a new region."""
        self._send_command('RCREATE', name, description)
    
    def delete_region(self, name: str) -> None:
        """Delete a region."""
        self._send_command('RDROP', name)
    
    def purge_region(self, name: str) -> None:
        """Purge all entries in a region."""
        self._send_command('RPURGE', name)
    
    # ==================== String Operations ====================
    
    def get(self, key: str) -> Optional[str]:
        """
        Get a value by key.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value or None if not found
        """
        return self._send_command('GET', key)
    
    def set(
        self,
        key: str,
        value: str,
        ttl: Optional[Union[int, timedelta]] = None,
        nx: bool = False,
        xx: bool = False
    ) -> Optional[str]:
        """
        Set a value.
        
        Args:
            key: The key
            value: The value
            ttl: Time-to-live (seconds or timedelta)
            nx: Only set if key doesn't exist
            xx: Only set if key exists
            
        Returns:
            'OK' on success, None if condition not met
        """
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
        """Delete keys."""
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
    
    def hgetall(self, key: str) -> Dict[str, str]:
        """Get all hash fields and values."""
        items = self._send_command_list('HGETALL', key)
        return dict(zip(items[::2], items[1::2]))
    
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
    
    # ==================== JSON Operations ====================
    
    def json_set(
        self,
        key: str,
        value: Any,
        path: str = '$',
        ttl: Optional[Union[int, timedelta]] = None
    ) -> str:
        """
        Set a JSON value.
        
        Args:
            key: The key
            value: Python object to store as JSON
            path: JSONPath (default: $ for root)
            ttl: Time-to-live
        """
        json_str = json.dumps(value) if not isinstance(value, str) else value
        args = ['JSET', key, json_str, path]
        
        if ttl is not None:
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            args.append(str(ttl))
        
        return self._send_command(*args)
    
    def json_get(self, key: str, path: str = '$') -> Any:
        """
        Get a JSON value.
        
        Args:
            key: The key
            path: JSONPath (default: $ for root)
            
        Returns:
            Parsed JSON object or None
        """
        result = self._send_command('JGET', key, path)
        if result:
            return json.loads(result)
        return None
    
    def json_delete(self, key: str, path: str = '$') -> bool:
        """Delete a JSON path."""
        return self._send_command('JDEL', key, path) == '1'
    
    def json_search(self, query: str) -> List[Dict[str, Any]]:
        """
        Search JSON documents.
        
        Args:
            query: Query string (e.g., '$.status=active,$.age>25')
            
        Returns:
            List of matching documents
        """
        results = self._send_command_list('JSEARCH', query)
        documents = []
        
        for result in results:
            if ':' in result:
                key, json_str = result.split(':', 1)
                documents.append({
                    'key': key,
                    'value': json.loads(json_str)
                })
        
        return documents
    
    # ==================== Server Operations ====================
    
    def info(self) -> str:
        """Get server info."""
        return self._send_command('INFO')
    
    def dbsize(self) -> int:
        """Get database size."""
        return int(self._send_command('DBSIZE'))
    
    def flushdb(self) -> str:
        """Flush current region."""
        return self._send_command('FLUSHDB')
    
    def status(self) -> Dict[str, Any]:
        """Get server status."""
        result = self._send_command('STATUS')
        return json.loads(result) if result else {}
    
    def repl_info(self) -> str:
        """Get replication info."""
        return self._send_command('REPLINFO')
    
    # ==================== Command Execution ====================
    
    def _send_command(self, *args: str) -> Optional[str]:
        """Send a command and return the response."""
        if not self._socket:
            raise KuberException("Not connected")
        
        # Build command
        cmd_parts = []
        for arg in args:
            if ' ' in arg or '"' in arg or '\n' in arg:
                escaped = arg.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\t', '\\t')
                cmd_parts.append(f'"{escaped}"')
            else:
                cmd_parts.append(arg)
        
        cmd = ' '.join(cmd_parts) + '\r\n'
        
        logger.debug(f"Sending: {cmd.strip()}")
        self._socket.sendall(cmd.encode('utf-8'))
        
        return self._read_response()
    
    def _send_command_list(self, *args: str) -> List[str]:
        """Send a command and return list response."""
        response = self._send_command(*args)
        if response is None:
            return []
        return response.split('\n') if response else []
    
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
        
        logger.debug(f"Received: {line}")
        
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
            # Read bulk string
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
            return '\n'.join(str(i) if i else '' for i in items)
        
        return line


# Alias for convenience
Client = KuberClient
