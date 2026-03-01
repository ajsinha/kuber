# Kuber .NET Client Library

[![.NET](https://img.shields.io/badge/.NET-8.0-512BD4)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/License-Proprietary-red.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-2.6.3-blue.svg)](CHANGELOG.md)

Official .NET client library for Kuber Distributed Cache.

## Features

- **Redis Protocol Client** - High-performance access using StackExchange.Redis
- **REST API Client** - HTTP-based access with API key authentication
- **Messaging Client** - Request/Response messaging via message brokers
- **Full Operation Support** - Strings, Hashes, JSON, Keys, TTL, Batch operations
- **Region Support** - Multi-tenant cache isolation
- **Async/Await** - Full async support for all operations

## Installation

### NuGet Package (coming soon)

```bash
dotnet add package Kuber.Client
```

### Build from Source

```bash
cd kuber-client-csharp
dotnet build
```

## Quick Start

### Redis Protocol (Recommended for Performance)

```csharp
using Kuber.Client;

// Connect to Kuber
using var client = new KuberClient("localhost", 6379);

// Basic operations
await client.SetAsync("key", "value");
var value = await client.GetAsync("key");

// With TTL
await client.SetAsync("session", "data", TimeSpan.FromHours(1));

// Batch operations
await client.MSetAsync(new Dictionary<string, string>
{
    ["user:1"] = "Alice",
    ["user:2"] = "Bob"
});

var users = await client.MGetAsync("user:1", "user:2");

// Hash operations
await client.HSetAsync("profile:1", "name", "Alice");
await client.HSetAsync("profile:1", "email", "alice@example.com");
var profile = await client.HGetAllAsync("profile:1");

// JSON operations
await client.JsonSetAsync("product:1", new { Name = "Laptop", Price = 999.99 });
var product = await client.JsonGetAsync<Product>("product:1");
```

### REST API

```csharp
using Kuber.Client;

// Connect with API key
using var client = new KuberRestClient("http://localhost:8080", "your-api-key");

// Basic operations
await client.SetAsync("key", "value");
var value = await client.GetAsync("key");

// With TTL (in seconds)
await client.SetAsync("session", "data", ttlSeconds: 3600);

// JSON operations
await client.JsonSetAsync("order:1", new
{
    OrderId = "ORD-001",
    Items = new[] { "item1", "item2" },
    Total = 99.99
});

// Search JSON documents
var results = await client.JsonSearchAsync("Total > 50");

// Use specific region
var analyticsClient = client.WithRegion("analytics");
await analyticsClient.SetAsync("pageviews", "1000");
```

### Request/Response Messaging

```csharp
using Kuber.Client;

// Create messaging client
var client = new KuberMessagingClient("your-api-key");

// Build requests
var getRequest = client.BuildGet("user:1001");
var setRequest = client.BuildSet("user:1002", new { Name = "Jane" }, ttl: 3600);
var mgetRequest = client.BuildMGet(new List<string> { "key1", "key2", "key3" });

// Serialize to JSON for your message broker
var json = client.ToJson(getRequest);
// Send to broker's request topic...

// Parse response from broker
var response = client.ParseResponse(responseJson);
if (response.Response?.Success == true)
{
    Console.WriteLine($"Result: {response.Response.Result}");
}
```

## API Reference

### KuberClient (Redis Protocol)

| Method | Description |
|--------|-------------|
| `GetAsync(key)` | Get value by key |
| `SetAsync(key, value, expiry?)` | Set key-value pair |
| `DeleteAsync(key)` | Delete a key |
| `ExistsAsync(key)` | Check if key exists |
| `MGetAsync(keys)` | Get multiple keys |
| `MSetAsync(keyValues)` | Set multiple keys |
| `Keys(pattern)` | Find keys by pattern |
| `TtlAsync(key)` | Get remaining TTL |
| `ExpireAsync(key, expiry)` | Set expiration |
| `HGetAsync(key, field)` | Get hash field |
| `HSetAsync(key, field, value)` | Set hash field |
| `HGetAllAsync(key)` | Get all hash fields |
| `HMSetAsync(key, fields)` | Set multiple hash fields |
| `JsonSetAsync<T>(key, doc)` | Store JSON document |
| `JsonGetAsync<T>(key)` | Retrieve JSON document |
| `PingAsync()` | Test connection |
| `WithRegion(region)` | Get client for specific region |

### KuberRestClient (REST API)

Same methods as KuberClient, plus:

| Method | Description |
|--------|-------------|
| `JsonSearchAsync(query)` | Search JSON documents |
| `RegionsAsync()` | List all regions |
| `InfoAsync()` | Get server information |

### KuberMessagingClient (Messaging)

| Method | Description |
|--------|-------------|
| `BuildGet(key)` | Build GET request |
| `BuildSet(key, value, ttl?)` | Build SET request |
| `BuildDelete(key)` | Build DELETE request |
| `BuildMGet(keys)` | Build MGET request |
| `BuildMSet(entries, ttl?)` | Build MSET request |
| `BuildKeys(pattern)` | Build KEYS request |
| `BuildHGet(key, field)` | Build HGET request |
| `BuildHSet(key, field, value)` | Build HSET request |
| `BuildJSet(key, value)` | Build JSET request |
| `BuildJGet(key, path?)` | Build JGET request |
| `BuildJSearch(query)` | Build JSEARCH request |
| `BuildPing()` | Build PING request |
| `ToJson(request)` | Serialize request to JSON |
| `ParseResponse(json)` | Parse response from JSON |

## Examples

Run the interactive examples:

```bash
cd examples
dotnet run
```

Or run specific examples:

```bash
# Redis Protocol example
dotnet run -- redis

# REST API example
dotnet run -- rest

# Messaging example
dotnet run -- messaging

# Messaging with detailed format examples
dotnet run -- messaging --examples
```

## Requirements

- .NET 8.0 or later
- Kuber Server 2.6.3 or later

## Dependencies

- StackExchange.Redis (for Redis protocol)
- System.Text.Json (for JSON serialization)

Optional (for messaging):
- Confluent.Kafka (for Apache Kafka)
- RabbitMQ.Client (for RabbitMQ)

## License

Copyright © 2025-2030, All Rights Reserved  
Ashutosh Sinha | Email: ajsinha@gmail.com  
