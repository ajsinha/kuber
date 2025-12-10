# Kuber Client Usage Guide

**Version 1.6.5**

Copyright © 2025-2030, All Rights Reserved  
Ashutosh Sinha | Email: ajsinha@gmail.com

**Patent Pending**: Certain architectural patterns and implementations described in this document may be subject to patent applications.

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Authentication Requirements](#2-authentication-requirements)
3. [Python Client - Redis Protocol](#3-python-client---redis-protocol)
4. [Python Client - REST API](#4-python-client---rest-api)
5. [Java Client - Redis Protocol](#5-java-client---redis-protocol)
6. [Java Client - REST API](#6-java-client---rest-api)
7. [Common Operations](#7-common-operations)
8. [JSON Operations](#8-json-operations)
9. [Generic Search API](#9-generic-search-api)
10. [Region Management](#10-region-management)
11. [Attribute Mapping](#11-attribute-mapping)
12. [Error Handling](#12-error-handling)

---

## 1. Introduction

Kuber provides four standalone client libraries for accessing the distributed cache:

| Client | Language | Protocol | Port | Dependencies |
|--------|----------|----------|------|--------------|
| `kuber_redis_standalone.py` | Python | Redis RESP | 6380 | None (stdlib only) |
| `kuber_rest_standalone.py` | Python | HTTP REST | 8080 | None (stdlib only) |
| `KuberClient.java` | Java | Redis RESP | 6380 | Jackson JSON |
| `KuberRestClient.java` | Java | HTTP REST | 8080 | Jackson JSON |

All clients support:
- Basic cache operations (GET, SET, DELETE, etc.)
- Multi-key operations (MGET, MSET)
- Key pattern search (KEYS with wildcards)
- Hash operations
- Region management
- JSON document storage and deep search
- Cross-region operations
- TTL support

---

## 2. Authentication Requirements

**IMPORTANT: All clients REQUIRE authentication. Username and password are mandatory.**

### Why Authentication is Required

- Protects cache data from unauthorized access
- Enables audit logging of operations
- Supports multi-tenant deployments
- Enforces access control policies

### Client Enforcement

Each client validates credentials at construction time:

**Python:**
```python
# This will FAIL - credentials are required
client = KuberRedisClient('localhost', 6380)  # ValueError raised

# This will SUCCEED
client = KuberRedisClient('localhost', 6380, username='admin', password='secret')
```

**Java:**
```java
// This will FAIL - credentials are required
KuberClient client = new KuberClient("localhost", 6380);  // Compile error

// This will SUCCEED
KuberClient client = new KuberClient("localhost", 6380, "admin", "secret");
```

---

## 3. Python Client - Redis Protocol

### 3.1 Installation

The Python Redis client is a single standalone file with no external dependencies.

```bash
# Copy the file to your project
cp kuber_redis_standalone.py your_project/
```

### 3.2 Basic Usage

```python
from kuber_redis_standalone import KuberRedisClient

# Create client - username and password are REQUIRED
with KuberRedisClient(
    host='localhost',
    port=6380,
    username='admin',
    password='secret'
) as client:
    
    # Basic operations
    client.set('key', 'value')
    value = client.get('key')
    print(f"Value: {value}")
    
    # Set with TTL (60 seconds)
    from datetime import timedelta
    client.set('temp_key', 'expires soon', ttl=timedelta(seconds=60))
```

### 3.3 Constructor Parameters

```python
KuberRedisClient(
    host: str = 'localhost',      # Server hostname
    port: int = 6380,             # Redis protocol port
    username: str = None,          # Username (REQUIRED)
    password: str = None,          # Password (REQUIRED)
    timeout: int = 30              # Socket timeout in seconds
)
```

### 3.4 String Operations

```python
with KuberRedisClient('localhost', 6380, username='admin', password='secret') as client:
    
    # SET / GET
    client.set('user:1001', 'John Doe')
    name = client.get('user:1001')  # Returns: 'John Doe'
    
    # SET with TTL
    client.set('session:abc', 'data', ttl=timedelta(minutes=30))
    
    # SETNX - Set if not exists
    result = client.setnx('unique_key', 'first value')  # Returns: True
    result = client.setnx('unique_key', 'second')       # Returns: False
    
    # SETEX - Set with expiration
    client.setex('cache:item', 'data', seconds=300)
    
    # INCR / DECR
    client.set('counter', '0')
    client.incr('counter')           # Returns: 1
    client.incrby('counter', 10)     # Returns: 11
    client.decr('counter')           # Returns: 10
    client.decrby('counter', 5)      # Returns: 5
    
    # APPEND
    client.set('message', 'Hello')
    client.append('message', ' World!')
    print(client.get('message'))     # Returns: 'Hello World!'
    
    # STRLEN
    length = client.strlen('message')  # Returns: 12
```

### 3.5 Multi-Key Operations

```python
with KuberRedisClient('localhost', 6380, username='admin', password='secret') as client:
    
    # MSET - Set multiple keys
    client.mset({
        'user:1:name': 'Alice',
        'user:1:email': 'alice@example.com',
        'user:2:name': 'Bob',
        'user:2:email': 'bob@example.com'
    })
    
    # MGET - Get multiple keys
    names = client.mget('user:1:name', 'user:2:name')
    # Returns: ['Alice', 'Bob']
    
    # MGET with missing keys returns None
    values = client.mget('user:1:name', 'user:99:name', 'user:2:name')
    # Returns: ['Alice', None, 'Bob']
```

### 3.6 Key Operations

```python
with KuberRedisClient('localhost', 6380, username='admin', password='secret') as client:
    
    # Check if key exists
    exists = client.exists('user:1:name')  # Returns: True/False
    
    # Get key type
    key_type = client.type('user:1:name')  # Returns: 'string', 'hash', etc.
    
    # Set expiration
    client.expire('key', 120)  # Expire in 120 seconds
    
    # Get TTL
    ttl = client.ttl('key')  # Returns: seconds remaining, -1 if no expiry
    
    # Remove expiration
    client.persist('key')
    
    # Delete keys
    deleted = client.delete('key1', 'key2', 'key3')  # Returns: count deleted
    
    # Rename key
    client.rename('old_name', 'new_name')
    
    # Find keys by pattern
    keys = client.keys('user:*')           # All user keys
    keys = client.keys('product:*:name')   # Product names
    keys = client.keys('session:[abc]*')   # Sessions starting with a, b, or c
```

### 3.7 Hash Operations

```python
with KuberRedisClient('localhost', 6380, username='admin', password='secret') as client:
    
    # HSET - Set single field
    client.hset('user:profile:1', 'name', 'Alice')
    client.hset('user:profile:1', 'email', 'alice@example.com')
    
    # HMSET - Set multiple fields
    client.hmset('user:profile:2', {
        'name': 'Bob',
        'email': 'bob@example.com',
        'age': '30'
    })
    
    # HGET - Get single field
    name = client.hget('user:profile:1', 'name')  # Returns: 'Alice'
    
    # HMGET - Get multiple fields
    values = client.hmget('user:profile:2', 'name', 'email')
    # Returns: ['Bob', 'bob@example.com']
    
    # HGETALL - Get all fields
    profile = client.hgetall('user:profile:1')
    # Returns: {'name': 'Alice', 'email': 'alice@example.com'}
    
    # HKEYS / HVALS
    fields = client.hkeys('user:profile:1')  # Returns: ['name', 'email']
    values = client.hvals('user:profile:1')  # Returns: ['Alice', 'alice@...']
    
    # HLEN - Number of fields
    count = client.hlen('user:profile:1')  # Returns: 2
    
    # HEXISTS - Check field existence
    exists = client.hexists('user:profile:1', 'name')  # Returns: True
    
    # HDEL - Delete fields
    client.hdel('user:profile:1', 'age')
```

### 3.8 Command Line Usage

```bash
# Run with required authentication
python kuber_redis_standalone.py \
    --host localhost \
    --port 6380 \
    --username admin \
    --password secret

# Short form
python kuber_redis_standalone.py -H localhost -P 6380 -u admin -p secret

# Skip cleanup (keep test data)
python kuber_redis_standalone.py -u admin -p secret --no-cleanup
```

---

## 4. Python Client - REST API

### 4.1 Installation

```bash
# Copy the file to your project
cp kuber_rest_standalone.py your_project/
```

### 4.2 Basic Usage

```python
from kuber_rest_standalone import KuberRestClient

# Create client - username and password are REQUIRED
with KuberRestClient(
    host='localhost',
    port=8080,
    username='admin',
    password='secret'
) as client:
    
    # Basic operations
    client.set('key', 'value')
    value = client.get('key')
    print(f"Value: {value}")
```

### 4.3 Constructor Parameters

```python
KuberRestClient(
    host: str = 'localhost',      # Server hostname
    port: int = 8080,             # HTTP port
    username: str = None,          # Username (REQUIRED)
    password: str = None,          # Password (REQUIRED)
    use_ssl: bool = False,         # Use HTTPS
    timeout: int = 30              # Request timeout in seconds
)
```

### 4.4 Server Operations

```python
with KuberRestClient('localhost', 8080, username='admin', password='secret') as client:
    
    # Health check
    is_alive = client.ping()  # Returns: True/False
    
    # Server info
    info = client.info()      # Returns: dict
    
    # Server status
    status = client.status()  # Returns: dict
    
    # Statistics
    stats = client.stats()    # Returns: dict
```

### 4.5 Cache Operations

```python
with KuberRestClient('localhost', 8080, username='admin', password='secret') as client:
    
    # SET / GET
    client.set('key', 'value')
    value = client.get('key')
    
    # SET with TTL
    client.set('temp', 'data', ttl=timedelta(minutes=30))
    
    # DELETE
    deleted = client.delete('key')
    
    # EXISTS
    exists = client.exists('key')
    
    # TTL
    remaining = client.ttl('key')
    
    # EXPIRE
    client.expire('key', 120)
    
    # MGET / MSET
    client.mset({'k1': 'v1', 'k2': 'v2'})
    values = client.mget(['k1', 'k2'])
    
    # KEYS
    keys = client.keys('user:*')
    
    # DBSIZE
    count = client.dbsize()
```

### 4.6 Bulk Operations

```python
with KuberRestClient('localhost', 8080, username='admin', password='secret') as client:
    
    # Bulk import
    entries = [
        {'key': 'item:1', 'value': {'name': 'Item 1', 'price': 10.99}},
        {'key': 'item:2', 'value': {'name': 'Item 2', 'price': 20.99}, 'ttl': 3600},
        {'key': 'item:3', 'value': {'name': 'Item 3', 'price': 30.99}}
    ]
    result = client.bulk_import(entries, region='products')
    
    # Bulk export
    exported = client.bulk_export('item:*', region='products')
    for entry in exported:
        print(f"Key: {entry['key']}, Value: {entry['value']}")
```

### 4.7 Command Line Usage

```bash
# Run with required authentication
python kuber_rest_standalone.py \
    --host localhost \
    --port 8080 \
    --username admin \
    --password secret

# With SSL
python kuber_rest_standalone.py -u admin -p secret --ssl

# Skip cleanup
python kuber_rest_standalone.py -u admin -p secret --no-cleanup
```

---

## 5. Java Client - Redis Protocol

### 5.1 Dependencies

Add to your `pom.xml`:

```xml
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.15.2</version>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.datatype</groupId>
    <artifactId>jackson-datatype-jsr310</artifactId>
    <version>2.15.2</version>
</dependency>
```

### 5.2 Basic Usage

```java
import com.kuber.client.KuberClient;

// Create client - username and password are REQUIRED
try (KuberClient client = new KuberClient(
        "localhost", 6380, "admin", "secret")) {
    
    // Basic operations
    client.set("key", "value");
    String value = client.get("key");
    System.out.println("Value: " + value);
    
    // Set with TTL
    client.set("temp", "data", Duration.ofMinutes(30));
}
```

### 5.3 Constructor

```java
// Full constructor
public KuberClient(
    String host,           // Server hostname
    int port,              // Redis protocol port (6380)
    String username,       // Username (REQUIRED)
    String password,       // Password (REQUIRED)
    int timeoutMs          // Socket timeout (default: 30000)
) throws IOException;

// Simplified constructor (uses default timeout)
public KuberClient(
    String host, int port, String username, String password
) throws IOException;
```

### 5.4 String Operations

```java
try (KuberClient client = new KuberClient(host, port, username, password)) {
    
    // SET / GET
    client.set("user:1001", "John Doe");
    String name = client.get("user:1001");
    
    // SET with TTL
    client.set("session", "data", Duration.ofMinutes(30));
    
    // SETNX - Set if not exists
    boolean result = client.setNx("unique", "value");  // Returns: true/false
    
    // SETEX - Set with expiration
    client.setEx("cache", "data", 300);  // 300 seconds
    
    // INCR / DECR
    client.set("counter", "0");
    long val = client.incr("counter");       // Returns: 1
    val = client.incrBy("counter", 10);      // Returns: 11
    val = client.decr("counter");            // Returns: 10
    val = client.decrBy("counter", 5);       // Returns: 5
    
    // APPEND
    client.set("msg", "Hello");
    int len = client.append("msg", " World!");
    
    // STRLEN
    int length = client.strlen("msg");
}
```

### 5.5 Multi-Key Operations

```java
try (KuberClient client = new KuberClient(host, port, username, password)) {
    
    // MSET
    Map<String, String> data = new LinkedHashMap<>();
    data.put("user:1:name", "Alice");
    data.put("user:1:email", "alice@example.com");
    data.put("user:2:name", "Bob");
    client.mset(data);
    
    // MGET
    List<String> names = client.mget("user:1:name", "user:2:name");
    // Returns: ["Alice", "Bob"]
}
```

### 5.6 Key Operations

```java
try (KuberClient client = new KuberClient(host, port, username, password)) {
    
    // EXISTS
    boolean exists = client.exists("key");
    
    // TYPE
    String type = client.type("key");  // "string", "hash", etc.
    
    // EXPIRE
    client.expire("key", 120);  // 120 seconds
    
    // TTL
    long ttl = client.ttl("key");
    
    // PERSIST
    client.persist("key");
    
    // DEL
    long deleted = client.del("key1", "key2", "key3");
    
    // RENAME
    client.rename("old_name", "new_name");
    
    // KEYS
    List<String> keys = client.keys("user:*");
}
```

### 5.7 Hash Operations

```java
try (KuberClient client = new KuberClient(host, port, username, password)) {
    
    // HSET
    client.hset("user:profile:1", "name", "Alice");
    client.hset("user:profile:1", "email", "alice@example.com");
    
    // HMSET
    Map<String, String> fields = new LinkedHashMap<>();
    fields.put("name", "Bob");
    fields.put("email", "bob@example.com");
    client.hmset("user:profile:2", fields);
    
    // HGET
    String name = client.hget("user:profile:1", "name");
    
    // HMGET
    List<String> values = client.hmget("user:profile:2", "name", "email");
    
    // HGETALL
    Map<String, String> profile = client.hgetall("user:profile:1");
    
    // HKEYS / HVALS
    List<String> fieldNames = client.hkeys("user:profile:1");
    List<String> fieldValues = client.hvals("user:profile:1");
    
    // HLEN
    int count = client.hlen("user:profile:1");
    
    // HEXISTS
    boolean exists = client.hexists("user:profile:1", "name");
    
    // HDEL
    client.hdel("user:profile:1", "age");
}
```

### 5.8 Command Line Example

```bash
# Compile
mvn compile

# Run example
java -cp target/classes:target/dependency/* \
    com.kuber.client.examples.KuberRedisExample \
    localhost 6380 admin secret
```

---

## 6. Java Client - REST API

### 6.1 Basic Usage

```java
import com.kuber.client.KuberRestClient;

// Create client - username and password are REQUIRED
try (KuberRestClient client = new KuberRestClient(
        "localhost", 8080, "admin", "secret")) {
    
    // Basic operations
    client.set("key", "value");
    String value = client.get("key");
    System.out.println("Value: " + value);
}
```

### 6.2 Constructor

```java
// Full constructor
public KuberRestClient(
    String host,           // Server hostname
    int port,              // HTTP port (8080)
    String username,       // Username (REQUIRED)
    String password,       // Password (REQUIRED)
    boolean useSsl,        // Use HTTPS
    int timeoutMs          // Request timeout (default: 30000)
);

// Simplified constructor
public KuberRestClient(
    String host, int port, String username, String password
);
```

### 6.3 Server Operations

```java
try (KuberRestClient client = new KuberRestClient(host, port, user, pass)) {
    
    // Health check
    boolean alive = client.ping();
    
    // Server info
    JsonNode info = client.info();
    
    // Server status
    JsonNode status = client.status();
    
    // Statistics
    JsonNode stats = client.stats();
}
```

### 6.4 Bulk Operations

```java
try (KuberRestClient client = new KuberRestClient(host, port, user, pass)) {
    
    // Bulk import
    List<Map<String, Object>> entries = new ArrayList<>();
    
    Map<String, Object> entry1 = new HashMap<>();
    entry1.put("key", "item:1");
    entry1.put("value", Map.of("name", "Item 1", "price", 10.99));
    entries.add(entry1);
    
    Map<String, Object> entry2 = new HashMap<>();
    entry2.put("key", "item:2");
    entry2.put("value", Map.of("name", "Item 2", "price", 20.99));
    entry2.put("ttl", 3600);
    entries.add(entry2);
    
    client.bulkImport(entries, "products");
    
    // Bulk export
    List<JsonNode> exported = client.bulkExport("item:*", "products");
    for (JsonNode e : exported) {
        System.out.println(e.get("key") + ": " + e.get("value"));
    }
}
```

### 6.5 Command Line Example

```bash
java -cp target/classes:target/dependency/* \
    com.kuber.client.examples.KuberRestExample \
    localhost 8080 admin secret
```

---

## 7. Common Operations

### 7.1 Connection Management

**Python:**
```python
# Using context manager (recommended)
with KuberRedisClient('localhost', 6380, username='admin', password='secret') as client:
    client.set('key', 'value')
# Connection automatically closed

# Manual management
client = KuberRedisClient('localhost', 6380, username='admin', password='secret')
try:
    client.set('key', 'value')
finally:
    client.close()
```

**Java:**
```java
// Using try-with-resources (recommended)
try (KuberClient client = new KuberClient("localhost", 6380, "admin", "secret")) {
    client.set("key", "value");
}
// Connection automatically closed

// Manual management
KuberClient client = new KuberClient("localhost", 6380, "admin", "secret");
try {
    client.set("key", "value");
} finally {
    client.close();
}
```

### 7.2 TTL Usage

**Python:**
```python
from datetime import timedelta

# Set with TTL using timedelta
client.set('session', 'data', ttl=timedelta(hours=1))
client.set('cache', 'data', ttl=timedelta(minutes=30))
client.set('temp', 'data', ttl=timedelta(seconds=60))

# JSON with TTL
client.json_set('user:1', user_data, ttl=timedelta(days=7))
```

**Java:**
```java
import java.time.Duration;

// Set with TTL using Duration
client.set("session", "data", Duration.ofHours(1));
client.set("cache", "data", Duration.ofMinutes(30));
client.set("temp", "data", Duration.ofSeconds(60));

// JSON with TTL
client.jsonSet("user:1", userData, Duration.ofDays(7));
```

---

## 8. JSON Operations

### 8.1 Storing JSON Documents

**Python:**
```python
# Store JSON in current region
user_data = {
    'name': 'Alice',
    'age': 30,
    'email': 'alice@example.com',
    'roles': ['admin', 'developer']
}
client.json_set('user:1001', user_data)

# Store JSON in specific region
client.json_set('user:1001', user_data, region='users')

# Store JSON with TTL
client.json_set('user:1001', user_data, region='users', ttl=timedelta(days=30))
```

**Java:**
```java
// Create JSON document
Map<String, Object> userData = new LinkedHashMap<>();
userData.put("name", "Alice");
userData.put("age", 30);
userData.put("email", "alice@example.com");
userData.put("roles", Arrays.asList("admin", "developer"));

// Store JSON (Redis client - in current region)
client.jsonSet("user:1001", objectMapper.writeValueAsString(userData));

// Store JSON with TTL
client.jsonSet("user:1001", json, Duration.ofDays(30));

// Store JSON (REST client - with region)
client.jsonSet("user:1001", userData, "users");
client.jsonSet("user:1001", userData, "users", Duration.ofDays(30));
```

### 8.2 Retrieving JSON Documents

**Python:**
```python
# Get entire document
user = client.json_get('user:1001')
print(user['name'])  # 'Alice'

# Get specific path
name = client.json_get('user:1001', path='$.name')

# Get nested path
city = client.json_get('user:1001', path='$.address.city')

# Get from specific region
user = client.json_get('user:1001', region='users')
```

**Java:**
```java
// Get entire document
JsonNode user = client.jsonGet("user:1001");
String name = user.get("name").asText();

// Get specific path
JsonNode nameNode = client.jsonGet("user:1001", "$.name");

// Get typed object (REST client)
User user = client.jsonGet("user:1001", User.class, "users");
```

### 8.3 JSON Deep Search

All clients support powerful JSON search with multiple operators:

| Operator | Syntax | Example |
|----------|--------|---------|
| Equality | `$.field=value` | `$.status=active` |
| Greater than | `$.field>value` | `$.age>30` |
| Less than | `$.field<value` | `$.price<100` |
| Greater or equal | `$.field>=value` | `$.score>=90` |
| Less or equal | `$.field<=value` | `$.quantity<=10` |
| Inequality | `$.field!=value` | `$.type!=deleted` |
| Pattern match | `$.field LIKE %pattern%` | `$.name LIKE %John%` |
| Array contains | `$.array CONTAINS value` | `$.tags CONTAINS urgent` |
| Nested field | `$.parent.child=value` | `$.address.city=NYC` |
| Combined | Multiple comma-separated | `$.age>25,$.status=active` |

**Python:**
```python
# Search by equality
results = client.json_search('$.category=Electronics')

# Search by comparison
results = client.json_search('$.price>100')
results = client.json_search('$.stock<=10')

# Search by inequality
results = client.json_search('$.status!=inactive')

# Search by pattern
results = client.json_search('$.name LIKE %Phone%')

# Search array contains
results = client.json_search('$.tags CONTAINS wireless')

# Search nested fields
results = client.json_search('$.address.city=New York')

# Combined conditions
results = client.json_search('$.in_stock=true,$.price<100')

# Search in specific region
results = client.json_search('$.category=Electronics', region='products')
```

**Java:**
```java
// Search operations
List<JsonNode> results = client.jsonSearch("$.category=Electronics");
results = client.jsonSearch("$.price>100");
results = client.jsonSearch("$.tags CONTAINS wireless");
results = client.jsonSearch("$.in_stock=true,$.price<100");

// REST client - search in specific region
results = client.jsonSearch("$.category=Electronics", "products");
```

---

## 9. Generic Search API

The Generic Search API provides a unified, flexible search endpoint that supports multiple search modes with optional field projection.

### 9.1 Search Modes

| Mode | Parameter | Description |
|------|-----------|-------------|
| Key Lookup | `key` | Exact key lookup |
| Key Pattern | `keypattern` | Regex pattern matching on keys |
| JSON Search | `type="json"` + `values` | Search by JSON attribute conditions |

### 9.2 Field Projection

All search modes support optional field projection to return only specific fields from JSON/Map objects.

**Supports nested paths**: `address.city`, `user.profile.name`

### 9.3 Python Examples

**Python Redis Client:**
```python
# Simple key lookup
results = client.generic_search(key="user:1001")

# Key pattern search
results = client.generic_search(keypattern="user:.*")

# Key pattern with field projection
results = client.generic_search(
    keypattern="user:.*",
    fields=["name", "email", "address.city"]
)

# JSON attribute search
results = client.generic_search(
    search_type="json",
    values=[{"status": "active"}, {"age": "30"}]
)

# JSON search with regex
results = client.generic_search(
    search_type="json",
    values=[{"name": "John.*", "type": "regex"}]
)

# JSON search with IN operator
results = client.generic_search(
    search_type="json",
    values=[{"status": ["active", "pending"]}]
)

# Combined: JSON search with field projection
results = client.generic_search(
    region="users",
    search_type="json",
    values=[{"status": "active"}],
    fields=["name", "email"],
    limit=100
)
```

**Python REST Client:**
```python
# All the same methods work for REST client
results = client.generic_search(
    region="users",
    keypattern="user:.*",
    fields=["name", "email", "address.city"],
    limit=50
)
```

### 9.4 Java Examples

**Java Redis Client:**
```java
// Simple key lookup
List<Map<String, Object>> results = client.genericSearchByKey("user:1001");

// Key lookup with field projection
results = client.genericSearchByKey("user:1001", Arrays.asList("name", "email"));

// Key pattern search
results = client.genericSearchByPattern("user:.*");

// Key pattern with field projection and limit
results = client.genericSearchByPattern(
    "user:.*",
    Arrays.asList("name", "email", "address.city"),
    100
);

// JSON attribute search
List<Map<String, Object>> conditions = Arrays.asList(
    Map.of("status", "active"),
    Map.of("age", "30")
);
results = client.genericSearchByJson(conditions);

// JSON search with field projection
results = client.genericSearchByJson(
    conditions,
    Arrays.asList("name", "email"),
    100
);

// Using GenericSearchRequest for full control
KuberClient.GenericSearchRequest request = new KuberClient.GenericSearchRequest();
request.setKeyPattern("user:.*");
request.setFields(Arrays.asList("name", "email", "address.city"));
request.setLimit(50);
results = client.genericSearch(request);
```

**Java REST Client:**
```java
// Using the GenericSearchRequest
KuberRestClient.GenericSearchRequest request = new KuberRestClient.GenericSearchRequest();
request.setRegion("users");
request.setKeyPattern("user:.*");
request.setFields(Arrays.asList("name", "email"));
request.setLimit(100);
List<JsonNode> results = client.genericSearch(request);

// Convenience methods
results = client.genericSearchByKey("user:1001");
results = client.genericSearchByPattern("user:.*", Arrays.asList("name"), 50, "users");
```

### 9.5 Response Format

All search modes return results in a consistent format:

```json
[
  {
    "key": "user:1001",
    "value": {"name": "John", "email": "john@example.com", "address": {"city": "NYC"}}
  },
  {
    "key": "user:1002", 
    "value": {"name": "Jane", "email": "jane@example.com", "address": {"city": "LA"}}
  }
]
```

With field projection (`fields=["name", "address.city"]`):
```json
[
  {
    "key": "user:1001",
    "value": {"name": "John", "address": {"city": "NYC"}}
  },
  {
    "key": "user:1002",
    "value": {"name": "Jane", "address": {"city": "LA"}}
  }
]
```

### 9.6 KSEARCH - Direct Key Regex Search

The `ksearch` method provides a simpler alternative to generic_search for regex-based key searches. It returns full key-value objects including metadata.

**Response includes**: `key`, `value`, `type`, `ttl`

**Python Redis Client:**
```python
# Search keys matching regex pattern
results = client.ksearch(r'user:\d+', limit=100)
for item in results:
    print(f"Key: {item['key']}, Type: {item['type']}, TTL: {item['ttl']}")
    print(f"Value: {item['value']}")
```

**Python REST Client:**
```python
# Same interface via REST
results = client.ksearch(r'user:\d+', region='users', limit=50)
```

**Java Redis Client:**
```java
// Search keys by regex
List<Map<String, Object>> results = client.ksearch("user:\\d+");

// With limit
results = client.ksearch("user:\\d+", 100);
```

**Java REST Client:**
```java
// Search via REST API
List<Map<String, Object>> results = client.ksearch("user:\\d+", "users", 100);
```

**Redis CLI:**
```bash
KSEARCH user:\d+
KSEARCH user:\d+ LIMIT 50
```

---

## 10. Region Management

### 10.0 Auto-Creation of Regions

**Regions are automatically created when you store data to a non-existent region.** You don't need to explicitly create a region before using it.

```python
# Python - region 'orders' is auto-created on first use
client.set('order:1001', order_data, region='orders')

# Python - region 'analytics' is auto-created
client.json_set('event:1', event_data, region='analytics')
```

```java
// Java - region 'products' is auto-created on first use
client.selectRegion("products");
client.set("item:1001", "Widget");

// Java REST - region 'sessions' is auto-created
client.setJson("session:abc", sessionData, "sessions");
```

This means you can immediately start storing data without any setup:
- Just use any region name and it will be created automatically
- Auto-created regions have the description "Auto-created region"
- The `default` region always exists and cannot be deleted

### 10.1 Working with Regions

**Python:**
```python
# Create regions explicitly (optional - regions are auto-created)
client.create_region('products', 'Product catalog')
client.create_region('orders', 'Customer orders')
client.create_region('sessions', 'User sessions')

# List regions
regions = client.list_regions()
for r in regions:
    print(f"Region: {r['name']} - {r.get('description', '')}")

# Select region (for subsequent operations)
client.select_region('products')
client.set('sku:1001', 'Laptop')  # Stored in 'products' region

# Get region info
info = client.get_region_info('products')

# Purge region (delete all entries)
client.purge_region('sessions')

# Delete region
client.delete_region('temp_region')
```

**Java:**
```java
// Create regions explicitly (optional - regions are auto-created)
client.createRegion("products", "Product catalog");
client.createRegion("orders", "Customer orders");

// List regions
List<String> regions = client.listRegions();  // Redis client
List<Map<String, Object>> regions = client.listRegions();  // REST client

// Select region
client.selectRegion("products");
client.set("sku:1001", "Laptop");  // Stored in 'products' region

// Get region info (REST client)
JsonNode info = client.getRegion("products");

// Purge region
client.purgeRegion("sessions");

// Delete region
client.deleteRegion("temp_region");
```

### 10.2 Cross-Region Operations

**Python (REST client):**
```python
# Store in specific region without switching
client.json_set('prod:1', product_data, region='products')
client.json_set('order:1', order_data, region='orders')

# Search in specific region
product_results = client.json_search('$.price>100', region='products')
order_results = client.json_search('$.status=shipped', region='orders')

# Get from specific region
product = client.json_get('prod:1', region='products')
order = client.json_get('order:1', region='orders')
```

**Java (REST client):**
```java
// Store in specific region
client.jsonSet("prod:1", productData, "products");
client.jsonSet("order:1", orderData, "orders");

// Search in specific region
List<JsonNode> products = client.jsonSearch("$.price>100", "products");
List<JsonNode> orders = client.jsonSearch("$.status=shipped", "orders");

// Get from specific region
JsonNode product = client.jsonGet("prod:1", "$", "products");
```

---

## 11. Attribute Mapping

Attribute mapping transforms JSON attribute names when data is stored. This is useful for normalizing field names from different data sources.

### 11.1 Region-Level Attribute Mapping

Set attribute mapping for a region - all JSON data stored in that region will have attributes renamed.

**Python Redis Client:**
```python
import json

# Set attribute mapping for 'users' region
mapping = {
    "firstName": "first_name",
    "lastName": "last_name", 
    "emailAddress": "email"
}
client._send_command('RSETMAP', 'users', json.dumps(mapping))

# Now when you store JSON, attributes are transformed:
user_data = {"firstName": "John", "lastName": "Doe", "emailAddress": "john@example.com"}
client.json_set('user:1001', user_data, region='users')
# Stored as: {"first_name": "John", "last_name": "Doe", "email": "john@example.com"}

# Get current mapping
result = client._send_command('RGETMAP', 'users')
current_mapping = json.loads(result) if result else {}

# Clear mapping
client._send_command('RCLEARMAP', 'users')
```

**Python REST Client:**
```python
import json

# Set attribute mapping
mapping = {"firstName": "first_name", "lastName": "last_name"}
client._request('PUT', '/api/regions/users/attributemapping', mapping)

# Get mapping
result = client._request('GET', '/api/regions/users/attributemapping')

# Clear mapping
client._request('DELETE', '/api/regions/users/attributemapping')
```

**Java Redis Client:**
```java
// Set attribute mapping
String mappingJson = "{\"firstName\":\"first_name\",\"lastName\":\"last_name\"}";
client.sendCommand("RSETMAP", "users", mappingJson);

// Get mapping
String result = client.sendCommand("RGETMAP", "users");

// Clear mapping
client.sendCommand("RCLEARMAP", "users");
```

**Java REST Client:**
```java
// Set attribute mapping
Map<String, String> mapping = Map.of(
    "firstName", "first_name",
    "lastName", "last_name"
);
client.setAttributeMapping("users", mapping);

// Get mapping
Map<String, String> currentMapping = client.getAttributeMapping("users");

// Clear mapping
client.clearAttributeMapping("users");
```

### 11.2 Autoload Attribute Mapping

When loading data via the Autoload feature, you can provide an attribute mapping file.

Create a file named `<datafile>.metadata.attributemapping.json`:

**Example files:**

`customers.csv`:
```csv
cust_id,firstName,lastName,emailAddr
C001,John,Doe,john@example.com
C002,Jane,Smith,jane@example.com
```

`customers.csv.metadata`:
```
region:customers
key_field:cust_id
```

`customers.csv.metadata.attributemapping.json`:
```json
{
  "firstName": "first_name",
  "lastName": "last_name",
  "emailAddr": "email"
}
```

**Result in cache:**
```json
{"cust_id": "C001", "first_name": "John", "last_name": "Doe", "email": "john@example.com"}
```

### 11.3 Attribute Mapping vs. Region Mapping

| Feature | Region Mapping | Autoload Mapping |
|---------|---------------|------------------|
| Scope | All JSON stored in region | Only during file import |
| Configuration | RSETMAP command or REST API | `.metadata.attributemapping.json` file |
| Persistence | Stored with region config | Used only during import |
| Use case | Normalize all incoming data | Transform specific data files |

---

## 12. Error Handling

### 12.1 Python Error Handling

```python
from kuber_redis_standalone import KuberRedisClient

try:
    with KuberRedisClient('localhost', 6380, username='admin', password='wrong') as client:
        client.set('key', 'value')
except ConnectionError as e:
    print(f"Connection failed: {e}")
except ValueError as e:
    print(f"Invalid parameters: {e}")
except Exception as e:
    print(f"Operation failed: {e}")
```

### 12.2 Java Error Handling

```java
import com.kuber.client.KuberClient;
import com.kuber.client.KuberClient.KuberException;

try (KuberClient client = new KuberClient(host, port, user, pass)) {
    client.set("key", "value");
} catch (IllegalArgumentException e) {
    System.err.println("Invalid parameters: " + e.getMessage());
} catch (KuberException e) {
    System.err.println("Server error: " + e.getMessage());
} catch (IOException e) {
    System.err.println("Connection error: " + e.getMessage());
}
```

### 10.3 Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `ValueError` / `IllegalArgumentException` | Missing username or password | Provide both credentials |
| `ConnectionError` / `IOException` | Server unreachable | Check host, port, network |
| `Authentication failed` | Invalid credentials | Verify username/password |
| `Region not found` | Invalid region name | Create region first |
| `Key not found` | Key doesn't exist | Check key spelling |
| `Permission denied` | Insufficient privileges | Use authorized account |

---

## Appendix: Quick Reference

### Python Redis Client

```python
# Constructor
client = KuberRedisClient(host, port, username=user, password=pass)

# String ops
client.set(key, value, ttl=timedelta(...))
client.get(key)
client.mset({k1: v1, k2: v2})
client.mget(k1, k2, k3)
client.incr(key), client.decr(key)

# Key ops
client.keys(pattern)
client.exists(key)
client.delete(key)
client.expire(key, seconds)
client.ttl(key)

# Hash ops
client.hset(key, field, value)
client.hget(key, field)
client.hmset(key, {f1: v1, f2: v2})
client.hgetall(key)

# JSON ops
client.json_set(key, data, region=region, ttl=timedelta(...))
client.json_get(key, path=path, region=region)
client.json_search(query, region=region)

# Region ops
client.create_region(name, description)
client.select_region(name)
client.list_regions()
client.purge_region(name)
client.delete_region(name)
```

### Java Redis Client

```java
// Constructor
KuberClient client = new KuberClient(host, port, username, password);

// String ops
client.set(key, value, Duration.of...);
client.get(key);
client.mset(Map.of(k1, v1, k2, v2));
client.mget(k1, k2, k3);
client.incr(key); client.decr(key);

// Key ops
client.keys(pattern);
client.exists(key);
client.del(key);
client.expire(key, seconds);
client.ttl(key);

// Hash ops
client.hset(key, field, value);
client.hget(key, field);
client.hmset(key, Map.of(f1, v1, f2, v2));
client.hgetall(key);

// JSON ops
client.jsonSet(key, json, Duration.of...);
client.jsonGet(key, path);
client.jsonSearch(query);

// Region ops
client.createRegion(name, description);
client.selectRegion(name);
client.listRegions();
client.purgeRegion(name);
client.deleteRegion(name);
```

---

*End of Client Usage Guide*
