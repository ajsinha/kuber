# Kuber Python Client

Python client library for [Kuber Distributed Cache](https://github.com/ashutosh/kuber).

**v1.6.5**: API Key Authentication Required - All programmatic access now requires an API key.

## Installation

```bash
pip install kuber-client
```

Or install from source:

```bash
cd kuber-client-python
pip install -e .
```

## Quick Start

```python
from kuber import KuberClient

# v1.6.5: API key authentication required
# Generate API keys in Web UI: Admin → API Keys
with KuberClient('localhost', 6380, api_key='kub_your_api_key') as client:
    # String operations
    client.set('key', 'value')
    value = client.get('key')
    
    # With TTL
    from datetime import timedelta
    client.set('temp', 'data', ttl=timedelta(minutes=5))
```

## Demo Scripts

### JSON Operations Demo (kuber_json_demo.py)

A comprehensive standalone demo demonstrating all JSON operations:

```bash
export KUBER_API_KEY=kub_your_key
python3 kuber_json_demo.py
```

Features demonstrated:
- Set keys with JSON values
- Retrieve JSON values and specific paths
- Search by single/multiple attributes
- Regex search on JSON attribute values  
- Key search using glob and regex patterns

## Features

### String Operations

```python
client.set('key', 'value')
client.get('key')
client.set('key', 'value', ttl=3600)  # TTL in seconds
client.setnx('key', 'value')          # Set if not exists
client.mset({'k1': 'v1', 'k2': 'v2'}) # Multiple set
client.mget('k1', 'k2')               # Multiple get
client.incr('counter')
client.decr('counter')
client.append('key', '-suffix')
```

### Key Operations

```python
client.delete('key1', 'key2')
client.exists('key')
client.expire('key', 60)
client.ttl('key')
client.persist('key')
client.type('key')
client.keys('user:*')
client.rename('old', 'new')
```

### Hash Operations

```python
client.hset('hash', 'field', 'value')
client.hget('hash', 'field')
client.hgetall('hash')
client.hdel('hash', 'field')
client.hexists('hash', 'field')
client.hkeys('hash')
client.hvals('hash')
client.hlen('hash')
```

### JSON Operations

```python
# Store Python objects as JSON
client.json_set('user:1', {
    'name': 'John',
    'age': 30,
    'email': 'john@example.com'
})

# Retrieve as Python dict
user = client.json_get('user:1')
print(user['name'])  # John

# Query at specific path
name = client.json_get('user:1', '$.name')

# Search by single attribute
results = client.json_search('department=Engineering')

# Search by multiple attributes (AND)
results = client.json_search('department=Engineering,salary>90000')

# Regex search on JSON attribute values
results = client.json_search('name~=^J.*')           # Names starting with J
results = client.json_search('email~=.*@company\\.com')  # Company emails

# Key regex search
results = client.ksearch('employee:EMP00[1-3]')      # Regex pattern

for doc in results:
    print(f"{doc['key']}: {doc['value']}")
```

### Region Operations

```python
# List all regions
regions = client.list_regions()

# Create a region
client.create_region('sessions', 'User session data')

# Select a region
client.select_region('sessions')

# Current region
print(client.current_region)

# Purge a region
client.purge_region('sessions')

# Delete a region
client.delete_region('sessions')
```

### Server Operations

```python
client.ping()        # PONG
client.info()        # Server info
client.dbsize()      # Number of entries
client.status()      # Node status
client.repl_info()   # Replication info
client.flushdb()     # Clear current region
```

## Error Handling

```python
from kuber import KuberClient, KuberException

try:
    with KuberClient('localhost', 6380, api_key='kub_your_key') as client:
        client.get('nonexistent')
except KuberException as e:
    print(f"Kuber error: {e}")
except ConnectionError as e:
    print(f"Connection failed: {e}")
```

## Configuration

```python
client = KuberClient(
    host='localhost',           # Server hostname
    port=6380,                  # Server port
    api_key='kub_your_key',     # Required API key (starts with 'kub_')
    timeout=30.0,               # Socket timeout (seconds)
    region='default'            # Initial region to select
)
```

## Test Data Generators (v1.7.8)

The `testdata` module provides synthetic data generators for testing and development:

### Trade Data Generator

Generate realistic trade data for CCR (Counterparty Credit Risk) calculations:

```bash
# Generate 200,000 trades (default)
python -m testdata.trade_generator

# Custom options
python -m testdata.trade_generator --count 500000 --output trades.csv --delimiter "|"

# List all 88 available products
python -m testdata.trade_generator --list-products
```

Features:
- **88 Trading Products** across 5 asset classes (IR, FX, Credit, Equity, Commodity)
- **70+ CCR-relevant attributes** including SA-CCR parameters
- **Realistic data distribution** based on typical portfolio composition
- Output formats: CSV (pipe-delimited) or JSON

See [testdata/README.md](testdata/README.md) for full documentation.

## License

Copyright © 2025-2030 Ashutosh Sinha. All Rights Reserved.
