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
Kuber REST Client - HTTP REST API Client Implementation

This client uses the HTTP REST API for all operations.
For Redis protocol access, use KuberClient from kuber.client module.

v1.6.5: API Key Authentication ONLY
All programmatic access requires an API key (starts with "kub_").
Username/password authentication is only for the Web UI.

Usage:
    # Create an API key in the Kuber Web UI (Admin → API Keys)
    client = KuberRestClient('localhost', 8080, api_key='kub_your_api_key_here')
    client.set('key', 'value')
    value = client.get('key')
    client.close()
"""

import json
import logging
import urllib.request
import urllib.parse
import urllib.error
import base64
from typing import Any, Dict, List, Optional, Union
from datetime import timedelta


logger = logging.getLogger(__name__)


class KuberRestException(Exception):
    """Exception raised for Kuber REST API errors."""
    def __init__(self, message: str, status_code: int = None):
        super().__init__(message)
        self.status_code = status_code


class KuberRestClient:
    """
    Python client for Kuber Distributed Cache using HTTP REST API.
    
    v1.6.5: API Key Authentication ONLY.
    All programmatic access requires an API key (starts with "kub_").
    Username/password authentication is only for the Web UI.
    
    Usage:
        client = KuberRestClient('localhost', 8080, api_key='kub_xxx')
        client.set('key', 'value')
        value = client.get('key')
        client.close()
    """
    
    DEFAULT_PORT = 8080
    DEFAULT_TIMEOUT = 30.0
    
    def __init__(
        self,
        host: str = 'localhost',
        port: int = DEFAULT_PORT,
        api_key: Optional[str] = None,
        timeout: float = DEFAULT_TIMEOUT,
        use_ssl: bool = False
    ):
        """
        Initialize Kuber REST client.
        
        Args:
            host: Server hostname
            port: HTTP port (default: 8080)
            api_key: API Key for authentication (must start with "kub_")
            timeout: Request timeout in seconds (default: 30)
            use_ssl: Use HTTPS instead of HTTP
            
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
        self.scheme = 'https' if use_ssl else 'http'
        self._base_url = f"{self.scheme}://{host}:{port}"
        self._current_region = 'default'
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False
    
    @property
    def current_region(self) -> str:
        """Get the current region name."""
        return self._current_region
    
    def select_region(self, region: str) -> None:
        """Select a region for subsequent operations."""
        self._current_region = region
    
    # ==================== HTTP Helpers ====================
    
    def _build_url(self, path: str, params: Dict[str, str] = None) -> str:
        """Build full URL with optional query parameters."""
        url = f"{self._base_url}{path}"
        if params:
            query = urllib.parse.urlencode(params)
            url = f"{url}?{query}"
        return url
    
    def _get_auth_header(self) -> Dict[str, str]:
        """Get authorization header (API Key)."""
        return {'X-API-Key': self.api_key}
    
    def _request(
        self,
        method: str,
        path: str,
        data: Any = None,
        params: Dict[str, str] = None
    ) -> Any:
        """Make HTTP request and return parsed response."""
        url = self._build_url(path, params)
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            **self._get_auth_header()
        }
        
        body = None
        if data is not None:
            body = json.dumps(data).encode('utf-8')
        
        logger.debug(f"{method} {url}")
        
        try:
            request = urllib.request.Request(
                url,
                data=body,
                headers=headers,
                method=method
            )
            
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                content = response.read().decode('utf-8')
                if content:
                    try:
                        return json.loads(content)
                    except json.JSONDecodeError:
                        return content
                return None
                
        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else str(e)
            raise KuberRestException(error_body, e.code)
        except urllib.error.URLError as e:
            raise KuberRestException(f"Connection error: {e.reason}")
    
    def _get(self, path: str, params: Dict[str, str] = None) -> Any:
        """HTTP GET request."""
        return self._request('GET', path, params=params)
    
    def _post(self, path: str, data: Any = None, params: Dict[str, str] = None) -> Any:
        """HTTP POST request."""
        return self._request('POST', path, data=data, params=params)
    
    def _put(self, path: str, data: Any = None, params: Dict[str, str] = None) -> Any:
        """HTTP PUT request."""
        return self._request('PUT', path, data=data, params=params)
    
    def _delete(self, path: str, params: Dict[str, str] = None) -> Any:
        """HTTP DELETE request."""
        return self._request('DELETE', path, params=params)
    
    # ==================== Server Operations ====================
    
    def ping(self) -> bool:
        """Check if server is reachable."""
        try:
            self._get('/api/ping')
            return True
        except Exception:
            return False
    
    def info(self) -> Dict[str, Any]:
        """Get server information."""
        return self._get('/api/info')
    
    def status(self) -> Dict[str, Any]:
        """Get server status."""
        return self._get('/api/status')
    
    def stats(self) -> Dict[str, Any]:
        """Get server statistics."""
        return self._get('/api/stats')
    
    # ==================== Region Operations ====================
    
    def list_regions(self) -> List[Dict[str, Any]]:
        """List all regions."""
        return self._get('/api/regions')
    
    def get_region(self, name: str = None) -> Dict[str, Any]:
        """Get region information."""
        region = name or self._current_region
        return self._get(f'/api/regions/{region}')
    
    def create_region(self, name: str, description: str = '') -> Dict[str, Any]:
        """Create a new region."""
        return self._post('/api/regions', {
            'name': name,
            'description': description
        })
    
    def delete_region(self, name: str) -> bool:
        """Delete a region."""
        self._delete(f'/api/regions/{name}')
        return True
    
    def purge_region(self, name: str = None) -> bool:
        """Purge all entries in a region."""
        region = name or self._current_region
        self._post(f'/api/regions/{region}/purge')
        return True
    
    # ==================== Key-Value Operations ====================
    
    def get(self, key: str, region: str = None) -> Optional[str]:
        """
        Get a value by key.
        
        Args:
            key: The key to retrieve
            region: Region name (default: current region)
            
        Returns:
            The value or None if not found
        """
        region = region or self._current_region
        try:
            result = self._get(f'/api/cache/{region}/{key}')
            return result.get('value') if isinstance(result, dict) else result
        except KuberRestException as e:
            if e.status_code == 404:
                return None
            raise
    
    def set(
        self,
        key: str,
        value: str,
        region: str = None,
        ttl: Optional[Union[int, timedelta]] = None
    ) -> bool:
        """
        Set a value.
        
        Args:
            key: The key
            value: The value
            region: Region name (default: current region)
            ttl: Time-to-live in seconds or timedelta
            
        Returns:
            True on success
        """
        region = region or self._current_region
        
        data = {'value': value}
        if ttl is not None:
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            data['ttl'] = ttl
        
        self._put(f'/api/cache/{region}/{key}', data)
        return True
    
    def delete(self, key: str, region: str = None) -> bool:
        """
        Delete a key.
        
        Args:
            key: The key to delete
            region: Region name (default: current region)
            
        Returns:
            True if deleted
        """
        region = region or self._current_region
        try:
            self._delete(f'/api/cache/{region}/{key}')
            return True
        except KuberRestException as e:
            if e.status_code == 404:
                return False
            raise
    
    def exists(self, key: str, region: str = None) -> bool:
        """Check if a key exists."""
        region = region or self._current_region
        try:
            self._get(f'/api/cache/{region}/{key}/exists')
            return True
        except KuberRestException as e:
            if e.status_code == 404:
                return False
            raise
    
    def mget(self, keys: List[str], region: str = None) -> Dict[str, Optional[str]]:
        """
        Get multiple values.
        
        Args:
            keys: List of keys to retrieve
            region: Region name (default: current region)
            
        Returns:
            Dictionary of key -> value mappings
        """
        region = region or self._current_region
        result = self._post(f'/api/cache/{region}/mget', {'keys': keys})
        return result if isinstance(result, dict) else {}
    
    def mset(self, mapping: Dict[str, str], region: str = None) -> bool:
        """
        Set multiple values.
        
        Args:
            mapping: Dictionary of key -> value pairs
            region: Region name (default: current region)
            
        Returns:
            True on success
        """
        region = region or self._current_region
        self._post(f'/api/cache/{region}/mset', {'entries': mapping})
        return True
    
    def keys(self, pattern: str = '*', region: str = None) -> List[str]:
        """
        Find keys matching pattern.
        
        Args:
            pattern: Glob-style pattern (*, ?, [abc])
            region: Region name (default: current region)
            
        Returns:
            List of matching keys
        """
        region = region or self._current_region
        result = self._get(f'/api/cache/{region}/keys', {'pattern': pattern})
        return result if isinstance(result, list) else []
    
    def ttl(self, key: str, region: str = None) -> int:
        """
        Get TTL of a key.
        
        Returns:
            TTL in seconds, -1 if no expiry, -2 if not found
        """
        region = region or self._current_region
        try:
            result = self._get(f'/api/cache/{region}/{key}/ttl')
            return result.get('ttl', -2) if isinstance(result, dict) else -2
        except KuberRestException:
            return -2
    
    def expire(self, key: str, seconds: int, region: str = None) -> bool:
        """Set expiration on a key."""
        region = region or self._current_region
        try:
            self._post(f'/api/cache/{region}/{key}/expire', {'seconds': seconds})
            return True
        except KuberRestException:
            return False
    
    def dbsize(self, region: str = None) -> int:
        """Get number of entries in a region."""
        region = region or self._current_region
        result = self._get(f'/api/cache/{region}/size')
        return result.get('size', 0) if isinstance(result, dict) else 0
    
    # ==================== JSON Operations ====================
    
    def json_set(
        self,
        key: str,
        value: Any,
        region: str = None,
        path: str = '$',
        ttl: Optional[Union[int, timedelta]] = None
    ) -> bool:
        """
        Set a JSON value.
        
        Args:
            key: The key
            value: Python object to store as JSON
            region: Region name (default: current region)
            path: JSONPath (default: $ for root)
            ttl: Time-to-live
            
        Returns:
            True on success
        """
        region = region or self._current_region
        
        data = {
            'value': value,
            'path': path
        }
        if ttl is not None:
            if isinstance(ttl, timedelta):
                ttl = int(ttl.total_seconds())
            data['ttl'] = ttl
        
        self._put(f'/api/json/{region}/{key}', data)
        return True
    
    def json_get(self, key: str, region: str = None, path: str = '$') -> Any:
        """
        Get a JSON value.
        
        Args:
            key: The key
            region: Region name (default: current region)
            path: JSONPath (default: $ for root)
            
        Returns:
            Parsed JSON object or None
        """
        region = region or self._current_region
        try:
            params = {'path': path} if path != '$' else None
            result = self._get(f'/api/json/{region}/{key}', params)
            return result.get('value') if isinstance(result, dict) else result
        except KuberRestException as e:
            if e.status_code == 404:
                return None
            raise
    
    def json_delete(self, key: str, region: str = None, path: str = '$') -> bool:
        """Delete a JSON document or path."""
        region = region or self._current_region
        try:
            params = {'path': path} if path != '$' else None
            self._delete(f'/api/json/{region}/{key}', params)
            return True
        except KuberRestException:
            return False
    
    def json_mget(self, keys: List[str], region: str = None, path: str = '$') -> Dict[str, Any]:
        """
        Get multiple JSON values.
        
        Args:
            keys: List of keys
            region: Region name (default: current region)
            path: JSONPath
            
        Returns:
            Dictionary of key -> value mappings
        """
        region = region or self._current_region
        result = self._post(f'/api/json/{region}/mget', {
            'keys': keys,
            'path': path
        })
        return result if isinstance(result, dict) else {}
    
    def json_search(
        self,
        query: str,
        region: str = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Search JSON documents using deep search.
        
        Args:
            query: Query string (e.g., '$.status=active,$.age>25')
            region: Region name (default: current region)
            limit: Maximum results to return
            
        Query format:
            - Simple: '$.field=value'
            - Multiple: '$.field1=value1,$.field2>value2'
            - Operators: =, !=, >, <, >=, <=, LIKE, IN, CONTAINS
            
        Returns:
            List of matching documents with keys
        """
        region = region or self._current_region
        result = self._post(f'/api/json/{region}/search', {
            'query': query,
            'limit': limit
        })
        return result if isinstance(result, list) else []
    
    def json_query(
        self,
        jsonpath: str,
        region: str = None,
        limit: int = 100
    ) -> List[Any]:
        """
        Query JSON documents using JSONPath.
        
        Args:
            jsonpath: JSONPath expression
            region: Region name (default: current region)
            limit: Maximum results
            
        Returns:
            List of matching values
        """
        region = region or self._current_region
        result = self._post(f'/api/json/{region}/query', {
            'jsonpath': jsonpath,
            'limit': limit
        })
        return result if isinstance(result, list) else []
    
    # ==================== Hash Operations ====================
    
    def hget(self, key: str, field: str, region: str = None) -> Optional[str]:
        """Get a hash field."""
        region = region or self._current_region
        try:
            result = self._get(f'/api/hash/{region}/{key}/{field}')
            return result.get('value') if isinstance(result, dict) else result
        except KuberRestException as e:
            if e.status_code == 404:
                return None
            raise
    
    def hset(self, key: str, field: str, value: str, region: str = None) -> bool:
        """Set a hash field."""
        region = region or self._current_region
        self._put(f'/api/hash/{region}/{key}/{field}', {'value': value})
        return True
    
    def hgetall(self, key: str, region: str = None) -> Dict[str, str]:
        """Get all hash fields and values."""
        region = region or self._current_region
        try:
            result = self._get(f'/api/hash/{region}/{key}')
            return result if isinstance(result, dict) else {}
        except KuberRestException as e:
            if e.status_code == 404:
                return {}
            raise
    
    def hdel(self, key: str, field: str, region: str = None) -> bool:
        """Delete a hash field."""
        region = region or self._current_region
        try:
            self._delete(f'/api/hash/{region}/{key}/{field}')
            return True
        except KuberRestException:
            return False
    
    def hmset(self, key: str, mapping: Dict[str, str], region: str = None) -> bool:
        """Set multiple hash fields."""
        region = region or self._current_region
        self._post(f'/api/hash/{region}/{key}/mset', {'fields': mapping})
        return True
    
    def hkeys(self, key: str, region: str = None) -> List[str]:
        """Get all hash fields."""
        region = region or self._current_region
        try:
            result = self._get(f'/api/hash/{region}/{key}/keys')
            return result if isinstance(result, list) else []
        except KuberRestException:
            return []
    
    # ==================== Bulk Operations ====================
    
    def bulk_import(
        self,
        entries: List[Dict[str, Any]],
        region: str = None
    ) -> Dict[str, Any]:
        """
        Bulk import entries.
        
        Args:
            entries: List of {'key': str, 'value': any, 'ttl': optional int}
            region: Region name (default: current region)
            
        Returns:
            Import result with counts
        """
        region = region or self._current_region
        return self._post(f'/api/bulk/{region}/import', {'entries': entries})
    
    def bulk_export(self, region: str = None, pattern: str = '*') -> List[Dict[str, Any]]:
        """
        Bulk export entries.
        
        Args:
            region: Region name (default: current region)
            pattern: Key pattern to export
            
        Returns:
            List of exported entries
        """
        region = region or self._current_region
        return self._get(f'/api/bulk/{region}/export', {'pattern': pattern})


# Alias for convenience
RestClient = KuberRestClient
