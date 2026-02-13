# Kuber Java Client

Java client library for [Kuber Distributed Cache](https://github.com/ashutosh/kuber).

**v2.4.0**: API Key Authentication Required - All programmatic access now requires an API key.

## Installation

### Maven

```xml
<dependency>
    <groupId>com.kuber</groupId>
    <artifactId>kuber-client</artifactId>
    <version>2.4.0</version>
</dependency>
```

### Gradle

```groovy
implementation 'com.kuber:kuber-client:2.4.0'
```

## Quick Start

```java
import com.kuber.client.KuberClient;

// v2.4.0: API key authentication required
// Generate API keys in Web UI: Admin → API Keys
try (KuberClient client = new KuberClient("localhost", 6380, "kub_your_api_key")) {
    // String operations
    client.set("key", "value");
    String value = client.get("key");
    
    // With TTL (seconds)
    client.setex("temp", 300, "data");
}
```

## Demo Scripts

### JSON Operations Demo (KuberJsonDemo.java)

A comprehensive standalone demo demonstrating all JSON operations.
Located in: `src/main/java/com/kuber/client/examples/`

```bash
# Compile (no dependencies needed)
javac KuberJsonDemo.java

# Run with command line arguments
java KuberJsonDemo localhost 6380 kub_your_api_key

# Or with environment variables
export KUBER_API_KEY=kub_your_key
java KuberJsonDemo
```

Features demonstrated:
- Set keys with JSON values
- Retrieve JSON values and specific paths
- Search by single/multiple attributes
- Regex search on JSON attribute values
- Key search using glob and regex patterns

### Other Examples

- **KuberRestExample.java** - REST API client examples
- **KuberRedisExample.java** - Redis protocol client examples

## Features

### String Operations

```java
client.set("key", "value");
client.get("key");
client.setex("key", 3600, "value");  // TTL in seconds
client.setnx("key", "value");        // Set if not exists
client.mset(Map.of("k1", "v1", "k2", "v2"));
client.mget("k1", "k2");
client.incr("counter");
client.decr("counter");
client.append("key", "-suffix");
```

### Key Operations

```java
client.delete("key1", "key2");
client.exists("key");
client.expire("key", 60);
client.ttl("key");
client.persist("key");
client.type("key");
client.keys("user:*");
client.rename("old", "new");
```

### Hash Operations

```java
client.hset("hash", "field", "value");
client.hget("hash", "field");
client.hgetAll("hash");
client.hdel("hash", "field");
client.hexists("hash", "field");
client.hkeys("hash");
client.hvals("hash");
client.hlen("hash");
```

### JSON Operations

```java
// Store JSON document
String json = "{\"name\":\"John\",\"age\":30,\"email\":\"john@example.com\"}";
client.jsonSet("user:1", json);

// Retrieve JSON
String user = client.jsonGet("user:1");

// Query at specific path
String name = client.jsonGet("user:1", "$.name");

// Search by single attribute
List<Map<String, String>> results = client.jsonSearch("department=Engineering");

// Search by multiple attributes (AND)
results = client.jsonSearch("department=Engineering,salary>90000");

// Regex search on JSON attribute values
results = client.jsonSearch("name~=^J.*");           // Names starting with J
results = client.jsonSearch("email~=.*@company\\.com"); // Company emails

// Key regex search
results = client.ksearch("employee:EMP00[1-3]", 100);

for (Map<String, String> doc : results) {
    System.out.println(doc.get("key") + ": " + doc.get("value"));
}
```

### Region Operations

```java
// List all regions
List<String> regions = client.listRegions();

// Create a region
client.createRegion("sessions", "User session data");

// Select a region
client.selectRegion("sessions");

// Purge a region
client.purgeRegion("sessions");

// Delete a region
client.deleteRegion("sessions");
```

### Server Operations

```java
client.ping();        // PONG
client.info();        // Server info
client.dbsize();      // Number of entries
client.status();      // Node status
client.replInfo();    // Replication info
client.flushdb();     // Clear current region
```

## Error Handling

```java
import com.kuber.client.KuberClient;
import com.kuber.client.KuberException;

try (KuberClient client = new KuberClient("localhost", 6380, "kub_your_key")) {
    client.get("nonexistent");
} catch (KuberException e) {
    System.err.println("Kuber error: " + e.getMessage());
} catch (IOException e) {
    System.err.println("Connection failed: " + e.getMessage());
}
```

## Configuration

```java
KuberClient client = new KuberClient(
    "localhost",           // Server hostname
    6380,                  // Server port
    "kub_your_api_key"     // Required API key (starts with 'kub_')
);
```

## REST Client

For REST API access, use `KuberRestClient`:

```java
import com.kuber.client.KuberRestClient;

try (KuberRestClient client = new KuberRestClient("localhost", 8080, "kub_your_api_key")) {
    client.set("key", "value");
    String value = client.get("key");
}
```

## License

Copyright © 2025-2030 Ashutosh Sinha. All Rights Reserved.
