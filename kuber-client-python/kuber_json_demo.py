#!/usr/bin/env python3
# Copyright © 2025-2030, All Rights Reserved
# Ashutosh Sinha | Email: ajsinha@gmail.com
#
# Kuber Distributed Cache - Standalone JSON Operations Demo
# 
# This script demonstrates all JSON-related operations in Kuber:
# a) Set a key with JSON value
# b) Retrieve a key with JSON value  
# c) JSON search using single and multiple attributes
# d) Regex search on JSON attribute values
# e) Search keys using regex pattern
#
# v1.6.5: API Key Authentication Required

import socket
import json
import re
import os
import sys
from typing import Any, Dict, List, Optional

# ============================================================================
# CONFIGURATION - Update these values for your environment
# ============================================================================
KUBER_HOST = os.getenv('KUBER_HOST', 'localhost')
KUBER_PORT = int(os.getenv('KUBER_PORT', '6380'))
KUBER_API_KEY = os.getenv('KUBER_API_KEY', 'kub_admin_sample_key_replace_me')

# ============================================================================
# KUBER CLIENT CLASS
# ============================================================================
class KuberJsonClient:
    """
    Lightweight Kuber client for JSON operations demo.
    Uses Redis protocol with Kuber extensions.
    """
    
    def __init__(self, host: str, port: int, api_key: str):
        if not api_key or not api_key.startswith('kub_'):
            raise ValueError("API key must start with 'kub_'")
        self.host = host
        self.port = port
        self.api_key = api_key
        self.socket: Optional[socket.socket] = None
        self.current_region = 'default'
        self._buffer = b''
        
    def connect(self):
        """Connect and authenticate."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(30.0)
        self.socket.connect((self.host, self.port))
        self._buffer = b''
        
        # Authenticate with API key
        response = self._send_command('AUTH', self.api_key)
        if response != 'OK':
            raise Exception(f"Authentication failed: {response}")
        print(f"✓ Connected and authenticated to {self.host}:{self.port}")
        
    def close(self):
        """Close connection."""
        if self.socket:
            self.socket.close()
            self.socket = None
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        
    def _send_command(self, *args) -> Any:
        """Send command using inline protocol and return response."""
        # Build inline command (server uses TextLineCodecFactory)
        parts = []
        for arg in args:
            arg_str = str(arg)
            # Quote arguments containing spaces, quotes, or newlines
            if ' ' in arg_str or '"' in arg_str or '\n' in arg_str or '\r' in arg_str:
                escaped = arg_str.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
                parts.append(f'"{escaped}"')
            else:
                parts.append(arg_str)
        
        command = ' '.join(parts) + '\r\n'
        self.socket.sendall(command.encode('utf-8'))
        return self._read_response()
    
    def _read_line(self) -> bytes:
        """Read a line from socket (up to CRLF)."""
        while b'\r\n' not in self._buffer:
            chunk = self.socket.recv(4096)
            if not chunk:
                raise ConnectionError("Connection closed")
            self._buffer += chunk
        
        line, self._buffer = self._buffer.split(b'\r\n', 1)
        return line
    
    def _read_bytes(self, length: int) -> bytes:
        """Read exact number of bytes from socket."""
        while len(self._buffer) < length:
            chunk = self.socket.recv(4096)
            if not chunk:
                raise ConnectionError("Connection closed")
            self._buffer += chunk
        
        data = self._buffer[:length]
        self._buffer = self._buffer[length:]
        return data
        
    def _read_response(self) -> Any:
        """Read and parse RESP response."""
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
            return f"ERROR: {data.decode('utf-8')}"
        elif prefix == b':':
            # Integer
            return int(data.decode('utf-8'))
        elif prefix == b'$':
            # Bulk string
            length = int(data.decode('utf-8'))
            if length == -1:
                return None
            bulk = self._read_bytes(length)
            self._read_line()  # Consume trailing CRLF
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
        
    # ==================== Region Operations ====================
    
    def select_region(self, region: str):
        """Select a region."""
        response = self._send_command('RSELECT', region)
        if isinstance(response, str) and 'ERROR' in response:
            # Try to create region
            self._send_command('RCREATE', region, f'Demo region: {region}')
            response = self._send_command('RSELECT', region)
        self.current_region = region
        return response
        
    # ==================== JSON Operations ====================
    
    def json_set(self, key: str, value: Any, path: str = '$') -> str:
        """Set a JSON value."""
        json_str = json.dumps(value) if not isinstance(value, str) else value
        return self._send_command('JSET', key, json_str, path)
        
    def json_get(self, key: str, path: str = '$') -> Optional[Dict]:
        """Get a JSON value."""
        result = self._send_command('JGET', key, path)
        if result is None:
            return None
        if isinstance(result, str):
            if result.startswith('ERROR'):
                return None
            try:
                return json.loads(result)
            except:
                return result
        return result
        
    def json_search(self, query: str) -> List[Dict]:
        """
        Search JSON documents.
        
        Query syntax:
        - field=value           : exact match
        - field=[v1|v2|v3]      : IN clause - match any value (v1.7.7)
        - field!=value          : not equal
        - field>[=]value        : greater than [or equal]
        - field<[=]value        : less than [or equal]
        - field~=regex          : regex match
        
        Multiple conditions (AND logic): field1=value1,field2>value2
        Multiple IN clauses: field1=[a|b],field2=[x|y|z]
        
        Examples:
        - "status=active"                     : exact match
        - "status=[active|pending]"           : IN clause (v1.7.7)
        - "status=[active|pending],country=[USA|UK]" : multiple IN clauses
        - "age>25,department=Engineering"     : multiple conditions
        """
        response = self._send_command('JSEARCH', query)
        return self._parse_search_results(response)
        
    def _parse_search_results(self, response) -> List[Dict]:
        """Parse JSEARCH response into list of key-value pairs."""
        results = []
        if not response:
            return results
        
        # Response is now a list of strings in format "key:json"
        # Key might contain colons (e.g., "prod:1001"), so find where JSON starts
        if isinstance(response, list):
            for item in response:
                if isinstance(item, str):
                    # Find the colon that precedes the JSON (look for :{ or :[)
                    json_start = -1
                    for i, c in enumerate(item):
                        if c == ':' and i + 1 < len(item) and item[i + 1] in '{[':
                            json_start = i
                            break
                    
                    if json_start > 0:
                        key = item[:json_start]
                        json_str = item[json_start + 1:]
                        try:
                            results.append({
                                'key': key,
                                'value': json.loads(json_str)
                            })
                        except json.JSONDecodeError:
                            pass
        return results
        
    # ==================== Key Search Operations ====================
    
    def keys(self, pattern: str = '*') -> List[str]:
        """Search keys using glob pattern."""
        response = self._send_command('KEYS', pattern)
        if isinstance(response, list):
            return response
        return []
        
    def ksearch(self, regex: str, limit: int = 100) -> List[Dict]:
        """Search keys using regex pattern."""
        response = self._send_command('KSEARCH', regex, 'LIMIT', str(limit))
        return self._parse_ksearch_results(response)
        
    def _parse_ksearch_results(self, response) -> List[Dict]:
        """Parse KSEARCH response."""
        results = []
        if not response:
            return results
        
        # Response is now a list of JSON strings
        if isinstance(response, list):
            for item in response:
                if isinstance(item, str):
                    try:
                        results.append(json.loads(item))
                    except json.JSONDecodeError:
                        pass
        return results
        
    def delete(self, key: str) -> str:
        """Delete a key."""
        return self._send_command('DEL', key)


# ============================================================================
# DEMO FUNCTIONS
# ============================================================================

def print_header(title: str):
    """Print section header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)

def print_result(label: str, value: Any):
    """Print formatted result."""
    print(f"  {label}: {json.dumps(value, indent=4) if isinstance(value, (dict, list)) else value}")

def demo_a_set_json(client: KuberJsonClient):
    """Demo a) Set keys with JSON values."""
    print_header("A) SET KEY WITH JSON VALUE")
    
    # Sample JSON documents - Employee records
    employees = [
        {
            "id": "EMP001",
            "name": "John Smith",
            "email": "john.smith@company.com",
            "department": "Engineering",
            "salary": 95000,
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
            "skills": ["SEO", "Content Marketing"],
            "address": {
                "city": "Los Angeles",
                "state": "CA",
                "zip": "90001"
            },
            "active": True
        }
    ]
    
    print("\n  Storing employee records as JSON...")
    for emp in employees:
        key = f"employee:{emp['id']}"
        result = client.json_set(key, emp)
        print(f"    ✓ Stored {key} -> {emp['name']}")
        
    print(f"\n  Total records stored: {len(employees)}")

def demo_b_get_json(client: KuberJsonClient):
    """Demo b) Retrieve keys with JSON values."""
    print_header("B) RETRIEVE KEY WITH JSON VALUE")
    
    # Get single employee
    print("\n  Retrieving employee:EMP001...")
    emp = client.json_get("employee:EMP001")
    print_result("Full JSON", emp)
    
    # Get specific path
    print("\n  Retrieving specific paths...")
    name = client.json_get("employee:EMP001", "$.name")
    print_result("$.name", name)
    
    salary = client.json_get("employee:EMP001", "$.salary")
    print_result("$.salary", salary)
    
    skills = client.json_get("employee:EMP001", "$.skills")
    print_result("$.skills", skills)
    
    city = client.json_get("employee:EMP001", "$.address.city")
    print_result("$.address.city", city)

def demo_c_search_single_attribute(client: KuberJsonClient):
    """Demo c) JSON search with single and multiple attributes."""
    print_header("C) JSON SEARCH - SINGLE ATTRIBUTE")
    
    # Search by department
    print("\n  Search: department=Engineering")
    results = client.json_search("department=Engineering")
    print(f"  Found {len(results)} results:")
    for r in results:
        print(f"    - {r['key']}: {r['value'].get('name', 'N/A')} ({r['value'].get('department', 'N/A')})")
    
    # Search by salary threshold
    print("\n  Search: salary>90000")
    results = client.json_search("salary>90000")
    print(f"  Found {len(results)} results:")
    for r in results:
        print(f"    - {r['key']}: {r['value'].get('name', 'N/A')} (${r['value'].get('salary', 0):,})")
        
    # Search by active status
    print("\n  Search: active=true")
    results = client.json_search("active=true")
    print(f"  Found {len(results)} results:")
    for r in results:
        print(f"    - {r['key']}: {r['value'].get('name', 'N/A')}")

def demo_c_search_multiple_attributes(client: KuberJsonClient):
    """Demo c) JSON search with multiple attributes."""
    print_header("C) JSON SEARCH - MULTIPLE ATTRIBUTES")
    
    # Search by department AND salary
    print("\n  Search: department=Engineering,salary>100000")
    results = client.json_search("department=Engineering,salary>100000")
    print(f"  Found {len(results)} results:")
    for r in results:
        print(f"    - {r['key']}: {r['value'].get('name', 'N/A')} - {r['value'].get('department', 'N/A')} (${r['value'].get('salary', 0):,})")
    
    # Search by department AND active status
    print("\n  Search: department=Engineering,active=true")
    results = client.json_search("department=Engineering,active=true")
    print(f"  Found {len(results)} results:")
    for r in results:
        print(f"    - {r['key']}: {r['value'].get('name', 'N/A')}")
        
    # Search by state AND salary range
    print("\n  Search: address.state=CA,salary>=78000")
    results = client.json_search("address.state=CA,salary>=78000")
    print(f"  Found {len(results)} results (employees in California with salary >= $78,000):")
    for r in results:
        addr = r['value'].get('address', {})
        print(f"    - {r['key']}: {r['value'].get('name', 'N/A')} in {addr.get('city', 'N/A')}, {addr.get('state', 'N/A')}")

def demo_c_in_clause_search(client: KuberJsonClient):
    """Demo c) JSON search with IN clause - v1.7.7 Feature."""
    print_header("C) JSON SEARCH - IN CLAUSE (v1.7.7)")
    
    print("\n  The IN clause allows matching a field against multiple values.")
    print("  Syntax: field=[value1|value2|value3]")
    print("  Logic: Returns records where field equals ANY of the values.")
    
    # IN clause - single attribute with multiple values
    print("\n  Search: department=[Engineering|Sales]")
    print("  (Find employees in Engineering OR Sales departments)")
    results = client.json_search("department=[Engineering|Sales]")
    print(f"  Found {len(results)} results:")
    for r in results:
        print(f"    - {r['key']}: {r['value'].get('name', 'N/A')} ({r['value'].get('department', 'N/A')})")
    
    # IN clause - multiple attributes, each with multiple values
    print("\n  Search: department=[Engineering|Sales],address.state=[CA|NY]")
    print("  (Find employees in (Engineering OR Sales) AND in (California OR New York))")
    results = client.json_search("department=[Engineering|Sales],address.state=[CA|NY]")
    print(f"  Found {len(results)} results:")
    for r in results:
        addr = r['value'].get('address', {})
        print(f"    - {r['key']}: {r['value'].get('name', 'N/A')} - {r['value'].get('department', 'N/A')} in {addr.get('state', 'N/A')}")
    
    # IN clause combined with comparison operator
    print("\n  Search: department=[Engineering|Marketing],salary>80000")
    print("  (Find high-earning employees in Engineering OR Marketing)")
    results = client.json_search("department=[Engineering|Marketing],salary>80000")
    print(f"  Found {len(results)} results:")
    for r in results:
        print(f"    - {r['key']}: {r['value'].get('name', 'N/A')} - {r['value'].get('department', 'N/A')} (${r['value'].get('salary', 0):,})")
    
    # IN clause with three values
    print("\n  Search: address.state=[CA|NY|TX]")
    print("  (Find employees in California, New York, or Texas)")
    results = client.json_search("address.state=[CA|NY|TX]")
    print(f"  Found {len(results)} results:")
    for r in results:
        addr = r['value'].get('address', {})
        print(f"    - {r['key']}: {r['value'].get('name', 'N/A')} in {addr.get('city', 'N/A')}, {addr.get('state', 'N/A')}")
    
    # Building IN clause query programmatically
    print("\n  Building query programmatically:")
    departments = ["Engineering", "Sales", "HR"]
    states = ["CA", "NY"]
    query = f"department=[{'|'.join(departments)}],address.state=[{'|'.join(states)}]"
    print(f"  Query: {query}")
    results = client.json_search(query)
    print(f"  Found {len(results)} results")

def demo_d_regex_search_json(client: KuberJsonClient):
    """Demo d) Regex search on JSON attribute values."""
    print_header("D) REGEX SEARCH ON JSON ATTRIBUTE VALUES")
    
    # Regex search on email domain
    print("\n  Search: email~=.*@company\\.com")
    results = client.json_search("email~=.*@company\\.com")
    print(f"  Found {len(results)} results:")
    for r in results:
        print(f"    - {r['key']}: {r['value'].get('email', 'N/A')}")
    
    # Regex search on name pattern (names starting with J)
    print("\n  Search: name~=^J.*")
    results = client.json_search("name~=^J.*")
    print(f"  Found {len(results)} results (names starting with 'J'):")
    for r in results:
        print(f"    - {r['key']}: {r['value'].get('name', 'N/A')}")
        
    # Regex search on zip code pattern  
    print("\n  Search: address.zip~=^9.*")
    results = client.json_search("address.zip~=^9.*")
    print(f"  Found {len(results)} results (zip codes starting with '9' - West Coast):")
    for r in results:
        addr = r['value'].get('address', {})
        print(f"    - {r['key']}: {r['value'].get('name', 'N/A')} - {addr.get('city', 'N/A')}, {addr.get('zip', 'N/A')}")
        
    # Regex search combining with other conditions
    print("\n  Search: name~=.*son$,department=Engineering")
    results = client.json_search("name~=.*son$,department=Engineering")
    print(f"  Found {len(results)} results (names ending in 'son' in Engineering):")
    for r in results:
        print(f"    - {r['key']}: {r['value'].get('name', 'N/A')}")

def demo_e_key_regex_search(client: KuberJsonClient):
    """Demo e) Search keys using regex."""
    print_header("E) SEARCH KEYS USING REGEX")
    
    # Using KEYS command with glob pattern
    print("\n  KEYS employee:* (glob pattern)")
    keys = client.keys("employee:*")
    print(f"  Found {len(keys)} keys:")
    for k in keys:
        print(f"    - {k}")
        
    # Using KSEARCH command with regex
    print("\n  KSEARCH employee:EMP00[1-3] (regex pattern)")
    results = client.ksearch("employee:EMP00[1-3]")
    print(f"  Found {len(results)} results:")
    for r in results:
        print(f"    - Key: {r.get('key', 'N/A')}")
        
    # More complex regex
    print("\n  KSEARCH .*EMP00[0-9] (any key ending with EMP00X)")
    results = client.ksearch(".*EMP00[0-9]")
    print(f"  Found {len(results)} results:")
    for r in results:
        print(f"    - Key: {r.get('key', 'N/A')}")

def cleanup(client: KuberJsonClient):
    """Clean up demo data."""
    print_header("CLEANUP")
    
    print("\n  Deleting demo keys...")
    for i in range(1, 6):
        key = f"employee:EMP00{i}"
        client.delete(key)
        print(f"    ✓ Deleted {key}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║   KUBER DISTRIBUTED CACHE - JSON OPERATIONS DEMO                     ║
║                                                                      ║
║   Demonstrates:                                                      ║
║   a) Set key with JSON value                                         ║
║   b) Retrieve key with JSON value                                    ║
║   c) JSON search with single/multiple attributes                     ║
║      - IN clause for matching multiple values (v1.7.7)               ║
║   d) Regex search on JSON attribute values                           ║
║   e) Search keys using regex                                         ║
║                                                                      ║
║   v1.7.7 - JSEARCH IN Clause: field=[value1|value2|value3]           ║
╚══════════════════════════════════════════════════════════════════════╝
    """)
    
    print(f"Configuration:")
    print(f"  Host:    {KUBER_HOST}")
    print(f"  Port:    {KUBER_PORT}")
    print(f"  API Key: {KUBER_API_KEY[:12]}...{KUBER_API_KEY[-4:]}")
    
    try:
        with KuberJsonClient(KUBER_HOST, KUBER_PORT, KUBER_API_KEY) as client:
            # Select region for demo
            client.select_region('json-demo')
            print(f"  Region:  json-demo")
            
            # Run all demos
            demo_a_set_json(client)
            demo_b_get_json(client)
            demo_c_search_single_attribute(client)
            demo_c_search_multiple_attributes(client)
            demo_c_in_clause_search(client)  # v1.7.7 - IN clause demo
            demo_d_regex_search_json(client)
            demo_e_key_regex_search(client)
            
            # Cleanup
            cleanup(client)
            
            print_header("ALL DEMOS COMPLETED SUCCESSFULLY")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nTroubleshooting:")
        print("  1. Ensure Kuber server is running")
        print("  2. Check host/port configuration")
        print("  3. Verify API key is valid (generate in Web UI: Admin → API Keys)")
        sys.exit(1)


if __name__ == "__main__":
    main()
