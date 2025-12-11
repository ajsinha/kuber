# Kuber Client Usage Guide

**Version 1.7.1**

Copyright © 2025-2030, All Rights Reserved  
Ashutosh Sinha | Email: ajsinha@gmail.com

**Patent Pending**: Certain architectural patterns and implementations described in this document may be subject to patent applications.

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Authentication Requirements](#2-authentication-requirements)
3. [Python Client](#3-python-client)
4. [Java Client](#4-java-client)
5. [C# / .NET Client](#5-c--net-client)
6. [Request/Response Messaging](#6-requestresponse-messaging)
7. [Common Operations](#7-common-operations)
8. [JSON Operations](#8-json-operations)
9. [Region Management](#9-region-management)
10. [Error Handling](#10-error-handling)

---

## 1. Introduction

Kuber provides client libraries for three languages, each supporting multiple access patterns:

| Language | Redis Protocol | REST API | Messaging |
|----------|:-------------:|:--------:|:---------:|
| **Python** | ✓ | ✓ | ✓ |
| **Java** | ✓ | ✓ | ✓ |
| **C# / .NET** | ✓ | ✓ | ✓ |

### Access Patterns

| Pattern | Port | Best For |
|---------|------|----------|
| **Redis Protocol** | 6380 | High-performance, low-latency operations |
| **REST API** | 8080 | Web applications, simple integration |
| **Messaging** | N/A | Async processing, decoupled architectures |

### Client Files

| Language | Files | Location |
|----------|-------|----------|
| Python | `kuber_redis_standalone.py`, `kuber_rest_standalone.py`, `examples/messaging_example.py` | `kuber-client-python/` |
| Java | `KuberClient.java`, `KuberRestClient.java`, `KuberMessagingExample.java` | `kuber-client-java/` |
| C# | `KuberClient.cs`, `KuberRestClient.cs`, `KuberMessagingClient.cs` | `kuber-client-csharp/` |

---

## 2. Authentication Requirements

**All API access requires authentication** via one of:

- **Username/Password**: For web UI and basic auth
- **API Key**: For programmatic access (recommended)

### API Key Usage

```bash
# REST API
curl -H "X-API-Key: your-api-key" http://localhost:8080/api/v1/cache/default/key

# Messaging
{
  "api_key": "your-api-key",
  "operation": "GET",
  ...
}
```

---

## 3. Python Client

### 3.1 Redis Protocol Client

**Installation:**
```bash
cp kuber-client-python/kuber_redis_standalone.py your_project/
```

**Basic Usage:**
```python
from kuber_redis_standalone import KuberRedisClient

with KuberRedisClient('localhost', 6380) as client:
    # String operations
    client.set('key', 'value')
    value = client.get('key')
    
    # With TTL
    from datetime import timedelta
    client.set('temp', 'data', ttl=timedelta(minutes=30))
    
    # Multi-key operations
    client.mset({'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})
    values = client.mget('k1', 'k2', 'k3')
    
    # Key operations
    keys = client.keys('user:*')
    exists = client.exists('key')
    client.delete('key')
    
    # Hash operations
    client.hset('user:1', 'name', 'Alice')
    client.hmset('user:1', {'email': 'alice@example.com', 'age': '30'})
    profile = client.hgetall('user:1')
    
    # JSON operations
    client.json_set('product:1', {'name': 'Widget', 'price': 29.99})
    product = client.json_get('product:1')
    results = client.json_search('$.price<50')
    
    # Region operations
    client.create_region('users', 'User data region')
    client.select_region('users')
    client.json_set('user:1', {'name': 'Bob'}, region='users')
```

### 3.2 REST API Client

**Installation:**
```bash
cp kuber-client-python/kuber_rest_standalone.py your_project/
```

**Basic Usage:**
```python
from kuber_rest_standalone import KuberRestClient

with KuberRestClient('localhost', 8080, username='admin', password='secret') as client:
    # Or use API key
    # client = KuberRestClient('localhost', 8080, api_key='your-api-key')
    
    # Basic operations
    client.set('key', 'value')
    value = client.get('key')
    
    # JSON with region
    client.json_set('product:1', {'name': 'Laptop'}, region='products')
    
    # Search
    results = client.json_search('$.price>100', region='products')
    
    # Server info
    info = client.info()
    regions = client.list_regions()
```

### 3.3 Messaging Client

**Location:** `kuber-client-python/examples/messaging_example.py`

```python
import json
import uuid
from datetime import datetime

class KuberMessagingClient:
    def __init__(self, api_key, default_region='default'):
        self.api_key = api_key
        self.default_region = default_region
    
    def build_get_request(self, key, region=None):
        return {
            'api_key': self.api_key,
            'message_id': str(uuid.uuid4()),
            'operation': 'GET',
            'region': region or self.default_region,
            'key': key
        }
    
    def build_set_request(self, key, value, region=None, ttl_seconds=None):
        request = {
            'api_key': self.api_key,
            'message_id': str(uuid.uuid4()),
            'operation': 'SET',
            'region': region or self.default_region,
            'key': key,
            'value': value
        }
        if ttl_seconds:
            request['ttl_seconds'] = ttl_seconds
        return request

# Usage with Kafka
from kafka import KafkaProducer, KafkaConsumer

client = KuberMessagingClient('your-api-key')
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Send request
request = client.build_get_request('user:123', region='users')
producer.send('ccs_cache_request', json.dumps(request).encode())

# Consume response from ccs_cache_response topic
```

---

## 4. Java Client

### 4.1 Redis Protocol Client

**Maven Dependency:**
```xml
<dependency>
    <groupId>com.kuber</groupId>
    <artifactId>kuber-client-java</artifactId>
    <version>1.7.1-SNAPSHOT</version>
</dependency>
```

**Basic Usage:**
```java
import com.kuber.client.KuberClient;

try (KuberClient client = new KuberClient("localhost", 6380)) {
    // String operations
    client.set("key", "value");
    String value = client.get("key");
    
    // With TTL
    client.set("temp", "data", Duration.ofMinutes(30));
    
    // Multi-key operations
    client.mset(Map.of("k1", "v1", "k2", "v2"));
    List<String> values = client.mget("k1", "k2", "k3");
    
    // Key operations
    List<String> keys = client.keys("user:*");
    boolean exists = client.exists("key");
    client.del("key");
    
    // Hash operations
    client.hset("user:1", "name", "Alice");
    client.hmset("user:1", Map.of("email", "alice@example.com", "age", "30"));
    Map<String, String> profile = client.hgetall("user:1");
    
    // JSON operations
    client.jsonSet("product:1", "{\"name\": \"Widget\", \"price\": 29.99}");
    String product = client.jsonGet("product:1");
    List<JsonNode> results = client.jsonSearch("$.price<50");
    
    // Region operations
    client.createRegion("users", "User data region");
    client.selectRegion("users");
}
```

### 4.2 REST API Client

```java
import com.kuber.client.KuberRestClient;

try (KuberRestClient client = new KuberRestClient("localhost", 8080, "admin", "secret")) {
    // Or use API key
    // KuberRestClient client = new KuberRestClient("localhost", 8080, "your-api-key");
    
    // Basic operations
    client.set("key", "value");
    String value = client.get("key");
    
    // JSON with region and TTL
    client.jsonSet("order:1", orderJson, "orders", Duration.ofDays(30));
    
    // Search
    List<JsonNode> results = client.jsonSearch("$.status=shipped", "orders");
    
    // Server info
    JsonNode info = client.info();
    List<String> regions = client.listRegions();
}
```

### 4.3 Messaging Client

**Location:** `kuber-client-java/src/main/java/com/kuber/client/examples/KuberMessagingExample.java`

```java
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

public class RequestBuilder {
    private final String apiKey;
    private final ObjectMapper mapper;
    
    public RequestBuilder(String apiKey) {
        this.apiKey = apiKey;
        this.mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }
    
    public CacheRequest get(String key) {
        return new CacheRequest(apiKey, "GET", key);
    }
    
    public CacheRequest set(String key, String value) {
        CacheRequest req = new CacheRequest(apiKey, "SET", key);
        req.setValue(value);
        return req;
    }
    
    public String toJson(CacheRequest request) throws Exception {
        return mapper.writeValueAsString(request);
    }
}

// Usage with Kafka
RequestBuilder builder = new RequestBuilder("your-api-key");
CacheRequest request = builder.get("user:123").inRegion("users");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("ccs_cache_request", builder.toJson(request)));
```

---

## 5. C# / .NET Client

### 5.1 Installation

**NuGet Package (future):**
```bash
dotnet add package Kuber.Client
```

**Build from Source:**
```bash
cd kuber-client-csharp
dotnet build
```

### 5.2 Redis Protocol Client

Uses StackExchange.Redis for high-performance operations.

```csharp
using Kuber.Client;

// Create client
using var client = new KuberClient("localhost", 6380);

// String operations
await client.SetAsync("key", "value");
var value = await client.GetAsync("key");

// With TTL
await client.SetAsync("temp", "data", TimeSpan.FromMinutes(30));

// Multi-key operations
await client.MSetAsync(new Dictionary<string, string> {
    ["k1"] = "v1",
    ["k2"] = "v2",
    ["k3"] = "v3"
});
var values = await client.MGetAsync("k1", "k2", "k3");

// Key operations
var keys = await client.KeysAsync("user:*");
var exists = await client.ExistsAsync("key");
await client.DeleteAsync("key");

// Hash operations
await client.HSetAsync("user:1", "name", "Alice");
await client.HMSetAsync("user:1", new Dictionary<string, string> {
    ["email"] = "alice@example.com",
    ["age"] = "30"
});
var profile = await client.HGetAllAsync("user:1");

// JSON operations
await client.JsonSetAsync("product:1", new { Name = "Widget", Price = 29.99 });
var product = await client.JsonGetAsync<Product>("product:1");
var results = await client.JsonSearchAsync<Product>("$.price<50");

// Region operations
client.WithRegion("users");
await client.SetAsync("user:1", userData);
```

### 5.3 REST API Client

```csharp
using Kuber.Client;

// Create client with API key
using var client = new KuberRestClient("localhost", 8080, apiKey: "your-api-key");

// Or with username/password
// using var client = new KuberRestClient("localhost", 8080, "admin", "secret");

// Basic operations
await client.SetAsync("key", "value");
var value = await client.GetAsync("key");

// JSON with region
await client.JsonSetAsync("product:1", product, "products");
var result = await client.JsonGetAsync<Product>("product:1", region: "products");

// Search
var products = await client.JsonSearchAsync<Product>("$.price>100", "products");

// Server info
var info = await client.GetInfoAsync();
var regions = await client.ListRegionsAsync();
```

### 5.4 Messaging Client

**Location:** `kuber-client-csharp/src/Kuber.Client/KuberMessagingClient.cs`

```csharp
using Kuber.Client;
using System.Text.Json;

var messagingClient = new KuberMessagingClient("your-api-key");

// Build requests
var getRequest = messagingClient.BuildGetRequest("user:123", "users");
var setRequest = messagingClient.BuildSetRequest("user:456", userData, "users", ttlSeconds: 3600);
var mgetRequest = messagingClient.BuildMGetRequest(new[] { "k1", "k2", "k3" });

// Serialize for sending
var requestJson = JsonSerializer.Serialize(getRequest, new JsonSerializerOptions {
    PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
});

// Send via your message broker (Kafka, RabbitMQ, etc.)
await producer.ProduceAsync("ccs_cache_request", new Message<string, string> {
    Value = requestJson
});

// Parse response
var response = JsonSerializer.Deserialize<CacheResponse>(responseJson);
if (response.Response.Success) {
    var result = response.Response.Result;
}
```

### 5.5 C# Client Features

| Feature | KuberClient | KuberRestClient | KuberMessagingClient |
|---------|:-----------:|:---------------:|:--------------------:|
| Async/Await | ✓ | ✓ | ✓ |
| Strongly-typed JSON | ✓ | ✓ | ✓ |
| Region Support | ✓ | ✓ | ✓ |
| Batch Operations | ✓ | ✓ | ✓ |
| TTL Support | ✓ | ✓ | ✓ |
| IDisposable | ✓ | ✓ | - |

---

## 6. Request/Response Messaging

### 6.1 Overview

Access Kuber via message brokers for async, decoupled architectures:

```
┌─────────┐     ┌──────────────┐     ┌───────┐
│ Client  │────►│ ccs_cache_   │────►│ Kuber │
│ App     │     │ request      │     │Server │
└─────────┘     └──────────────┘     └───┬───┘
     ▲                                    │
     │          ┌──────────────┐          │
     └──────────│ ccs_cache_   │◄─────────┘
                │ response     │
                └──────────────┘
```

### 6.2 Message Format

**Request:**
```json
{
  "api_key": "your-api-key",
  "message_id": "550e8400-e29b-41d4-a716-446655440000",
  "operation": "GET",
  "region": "users",
  "key": "user:123"
}
```

**Response:**
```json
{
  "request_receive_timestamp": "2025-12-10T10:00:00.000Z",
  "response_time": "2025-12-10T10:00:00.005Z",
  "processing_time_ms": 5,
  "request": {
    "api_key": "***",
    "message_id": "550e8400-e29b-41d4-a716-446655440000",
    "operation": "GET",
    "region": "users",
    "key": "user:123"
  },
  "response": {
    "success": true,
    "result": "{\"name\": \"Alice\", \"age\": 30}",
    "error": null,
    "error_code": null,
    "server_message": null
  }
}
```

### 6.3 Supported Operations

| Operation | Required Fields | Optional Fields |
|-----------|-----------------|-----------------|
| GET | key | region |
| SET | key, value | region, ttl_seconds |
| DELETE | key | region |
| MGET | keys (array) | region |
| MSET | entries (object) | region, ttl_seconds |
| KEYS | pattern | region |
| EXISTS | key | region |
| TTL | key | region |
| EXPIRE | key, ttl_seconds | region |
| HGET | key, field | region |
| HSET | key, field, value | region |
| HGETALL | key | region |
| HMSET | key, fields (object) | region |
| JSET | key, value | region, ttl_seconds |
| JGET | key | region, path |
| JSEARCH | query | region, max_results |
| PING | - | - |
| INFO | - | - |
| REGIONS | - | - |

### 6.4 Error Response

```json
{
  "response": {
    "success": false,
    "result": null,
    "error": "Key not found: user:999",
    "error_code": "KEY_NOT_FOUND",
    "server_message": "The requested key does not exist in the cache"
  }
}
```

### 6.5 Batch Response Format

For MGET operations:
```json
{
  "response": {
    "success": true,
    "result": [
      {"key": "user:1", "value": "{\"name\": \"Alice\"}"},
      {"key": "user:2", "value": null},
      {"key": "user:3", "value": "{\"name\": \"Charlie\"}"}
    ],
    "total_count": 3,
    "returned_count": 3
  }
}
```

---

## 7. Common Operations

### 7.1 String Operations

**Python:**
```python
client.set('key', 'value')
client.set('temp', 'data', ttl=timedelta(minutes=30))
value = client.get('key')
client.incr('counter')
client.append('key', ' more data')
```

**Java:**
```java
client.set("key", "value");
client.set("temp", "data", Duration.ofMinutes(30));
String value = client.get("key");
client.incr("counter");
client.append("key", " more data");
```

**C#:**
```csharp
await client.SetAsync("key", "value");
await client.SetAsync("temp", "data", TimeSpan.FromMinutes(30));
var value = await client.GetAsync("key");
await client.IncrAsync("counter");
```

### 7.2 Multi-Key Operations

**Python:**
```python
client.mset({'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})
values = client.mget('k1', 'k2', 'k3')  # Returns: ['v1', 'v2', 'v3']
```

**Java:**
```java
client.mset(Map.of("k1", "v1", "k2", "v2", "k3", "v3"));
List<String> values = client.mget("k1", "k2", "k3");
```

**C#:**
```csharp
await client.MSetAsync(new Dictionary<string, string> {
    ["k1"] = "v1", ["k2"] = "v2", ["k3"] = "v3"
});
var values = await client.MGetAsync("k1", "k2", "k3");
```

### 7.3 Key Operations

**Python:**
```python
keys = client.keys('user:*')
exists = client.exists('key')
client.delete('key1', 'key2')
client.expire('key', 3600)
ttl = client.ttl('key')
```

**Java:**
```java
List<String> keys = client.keys("user:*");
boolean exists = client.exists("key");
client.del("key1", "key2");
client.expire("key", 3600);
long ttl = client.ttl("key");
```

**C#:**
```csharp
var keys = await client.KeysAsync("user:*");
var exists = await client.ExistsAsync("key");
await client.DeleteAsync("key1", "key2");
await client.ExpireAsync("key", TimeSpan.FromHours(1));
var ttl = await client.TtlAsync("key");
```

### 7.4 Hash Operations

**Python:**
```python
client.hset('user:1', 'name', 'Alice')
client.hmset('user:1', {'email': 'alice@example.com', 'age': '30'})
name = client.hget('user:1', 'name')
profile = client.hgetall('user:1')
```

**Java:**
```java
client.hset("user:1", "name", "Alice");
client.hmset("user:1", Map.of("email", "alice@example.com", "age", "30"));
String name = client.hget("user:1", "name");
Map<String, String> profile = client.hgetall("user:1");
```

**C#:**
```csharp
await client.HSetAsync("user:1", "name", "Alice");
await client.HMSetAsync("user:1", new Dictionary<string, string> {
    ["email"] = "alice@example.com", ["age"] = "30"
});
var name = await client.HGetAsync("user:1", "name");
var profile = await client.HGetAllAsync("user:1");
```

---

## 8. JSON Operations

### 8.1 Storing JSON

**Python:**
```python
user = {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'}
client.json_set('user:1', user, region='users')
```

**Java:**
```java
String userJson = "{\"name\": \"Alice\", \"age\": 30, \"email\": \"alice@example.com\"}";
client.jsonSet("user:1", userJson);
// Or with object
client.jsonSet("user:1", objectMapper.writeValueAsString(user));
```

**C#:**
```csharp
var user = new { Name = "Alice", Age = 30, Email = "alice@example.com" };
await client.JsonSetAsync("user:1", user, "users");
```

### 8.2 Retrieving JSON

**Python:**
```python
user = client.json_get('user:1', region='users')
name = client.json_get('user:1', path='$.name')
```

**Java:**
```java
String userJson = client.jsonGet("user:1");
JsonNode user = client.jsonGetAsNode("user:1");
String name = client.jsonGet("user:1", "$.name");
```

**C#:**
```csharp
var user = await client.JsonGetAsync<User>("user:1", "users");
var name = await client.JsonGetAsync<string>("user:1", "users", "$.name");
```

### 8.3 Searching JSON

**Python:**
```python
# Find users over 25
results = client.json_search('$.age>25', region='users')

# Find products under $50
products = client.json_search('$.price<50', region='products')

# Complex query
orders = client.json_search('$.status=shipped AND $.total>100', region='orders')
```

**Java:**
```java
List<JsonNode> results = client.jsonSearch("$.age>25", "users");
List<JsonNode> products = client.jsonSearch("$.price<50", "products");
```

**C#:**
```csharp
var users = await client.JsonSearchAsync<User>("$.age>25", "users");
var products = await client.JsonSearchAsync<Product>("$.price<50", "products");
```

---

## 9. Region Management

### 9.1 Creating Regions

**Python:**
```python
client.create_region('users', 'User profile data')
client.create_region('products', 'Product catalog')
```

**Java:**
```java
client.createRegion("users", "User profile data");
client.createRegion("products", "Product catalog");
```

**C#:**
```csharp
await client.CreateRegionAsync("users", "User profile data");
await client.CreateRegionAsync("products", "Product catalog");
```

### 9.2 Working with Regions

**Python:**
```python
# Select region for subsequent operations
client.select_region('users')
client.set('user:1', 'data')

# Or specify region per-operation
client.json_set('product:1', data, region='products')
```

**Java:**
```java
// Select region
client.selectRegion("users");
client.set("user:1", "data");

// Or specify per-operation
client.jsonSet("product:1", data, "products");
```

**C#:**
```csharp
// Fluent region selection
var usersClient = client.WithRegion("users");
await usersClient.SetAsync("user:1", "data");

// Or specify per-operation
await client.JsonSetAsync("product:1", data, "products");
```

### 9.3 Region Operations

| Operation | Python | Java | C# |
|-----------|--------|------|-----|
| List | `client.list_regions()` | `client.listRegions()` | `await client.ListRegionsAsync()` |
| Purge | `client.purge_region('name')` | `client.purgeRegion("name")` | `await client.PurgeRegionAsync("name")` |
| Delete | `client.delete_region('name')` | `client.deleteRegion("name")` | `await client.DeleteRegionAsync("name")` |

---

## 10. Error Handling

### 10.1 Python

```python
from kuber_redis_standalone import KuberRedisClient

try:
    with KuberRedisClient('localhost', 6380) as client:
        value = client.get('nonexistent')
except ConnectionError as e:
    print(f"Connection failed: {e}")
except ValueError as e:
    print(f"Invalid parameters: {e}")
except Exception as e:
    print(f"Operation failed: {e}")
```

### 10.2 Java

```java
try (KuberClient client = new KuberClient(host, port)) {
    client.set("key", "value");
} catch (IllegalArgumentException e) {
    System.err.println("Invalid parameters: " + e.getMessage());
} catch (KuberException e) {
    System.err.println("Server error: " + e.getMessage());
} catch (IOException e) {
    System.err.println("Connection error: " + e.getMessage());
}
```

### 10.3 C#

```csharp
try {
    using var client = new KuberClient("localhost", 6380);
    await client.SetAsync("key", "value");
}
catch (ArgumentException ex) {
    Console.WriteLine($"Invalid parameters: {ex.Message}");
}
catch (RedisConnectionException ex) {
    Console.WriteLine($"Connection error: {ex.Message}");
}
catch (Exception ex) {
    Console.WriteLine($"Operation failed: {ex.Message}");
}
```

### 10.4 Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| Connection refused | Server not running | Start server, check host/port |
| Authentication failed | Invalid credentials | Verify API key or password |
| Region not found | Invalid region name | Create region first |
| Key not found | Key doesn't exist | Check key spelling, handle null |
| Permission denied | Insufficient privileges | Use authorized account |
| Timeout | Server overloaded | Increase timeout, check server |

---

## Quick Reference

### Python
```python
# Redis Client
client = KuberRedisClient('localhost', 6380)
client.set(key, value, ttl=timedelta(...))
client.get(key)
client.json_set(key, data, region=region)
client.json_search(query, region=region)

# REST Client
client = KuberRestClient('localhost', 8080, api_key='key')
```

### Java
```java
// Redis Client
KuberClient client = new KuberClient("localhost", 6380);
client.set(key, value, Duration.of...);
client.get(key);
client.jsonSet(key, json);
client.jsonSearch(query);

// REST Client
KuberRestClient client = new KuberRestClient("localhost", 8080, apiKey);
```

### C#
```csharp
// Redis Client
using var client = new KuberClient("localhost", 6380);
await client.SetAsync(key, value, TimeSpan.From...);
await client.GetAsync(key);
await client.JsonSetAsync<T>(key, data, region);
await client.JsonSearchAsync<T>(query, region);

// REST Client
using var client = new KuberRestClient("localhost", 8080, apiKey: "key");
```

---

*End of Client Usage Guide*
