# Kuber Python Client

Python client library for [Kuber Distributed Cache](https://github.com/ashutosh/kuber).

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

# Using context manager (recommended)
with KuberClient('localhost', 6380) as client:
    # String operations
    client.set('key', 'value')
    value = client.get('key')
    
    # With TTL
    from datetime import timedelta
    client.set('temp', 'data', ttl=timedelta(minutes=5))

# Manual connection management
client = KuberClient('localhost', 6380)
client.connect()
try:
    client.set('key', 'value')
finally:
    client.close()
```

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

# Search JSON documents
results = client.json_search('$.age>25')
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
    with KuberClient('localhost', 6380) as client:
        client.get('nonexistent')
except KuberException as e:
    print(f"Kuber error: {e}")
except ConnectionError as e:
    print(f"Connection failed: {e}")
```

## Configuration

```python
client = KuberClient(
    host='localhost',      # Server hostname
    port=6380,             # Server port
    timeout=30.0,          # Socket timeout (seconds)
    password='secret'      # Optional authentication
)
```

## License

Copyright © 2025-2030 Ashutosh Sinha. All Rights Reserved.
