# Kuber Generic Search API

**Version 1.7.8**

The Generic Search API provides a powerful, unified endpoint for searching cache data using multiple strategies: key lookups, regex patterns, and JSON attribute filtering.

## Endpoint

```
POST /api/genericsearch
POST /api/v1/genericsearch
POST /api/v2/genericsearch
```

## Authentication

All requests require an API key in the request body:

```json
{
  "apiKey": "your-configured-api-key",
  "region": "your-region",
  ...
}
```

API keys are configured in `application.properties`:
```properties
kuber.security.api-keys=key1,key2,key3
```

---

## Search Modes

### 1. Single Key Lookup

Retrieve a single entry by exact key.

```json
{
  "apiKey": "your-api-key",
  "region": "users",
  "key": "user:12345"
}
```

**Response:**
```json
[
  {
    "key": "user:12345",
    "value": {"name": "John Doe", "email": "john@example.com", "status": "active"}
  }
]
```

---

### 2. Multi-Key Lookup (v1.7.8)

Retrieve multiple entries by exact keys in a single request.

```json
{
  "apiKey": "your-api-key",
  "region": "users",
  "keys": ["user:1", "user:2", "user:3", "user:4", "user:5"]
}
```

**Response:**
```json
[
  {"key": "user:1", "value": {"name": "Alice", "status": "active"}},
  {"key": "user:2", "value": {"name": "Bob", "status": "active"}},
  {"key": "user:3", "value": {"name": "Charlie", "status": "pending"}}
]
```

*Note: Only existing keys are returned. Missing keys are silently omitted.*

---

### 3. Single Key Pattern (Regex)

Search for keys matching a regex pattern.

```json
{
  "apiKey": "your-api-key",
  "region": "sessions",
  "keyPattern": "session:user_123:.*"
}
```

**Response:**
```json
[
  {"key": "session:user_123:abc", "value": {"token": "...", "expires": "..."}},
  {"key": "session:user_123:def", "value": {"token": "...", "expires": "..."}},
  {"key": "session:user_123:ghi", "value": {"token": "...", "expires": "..."}}
]
```

---

### 4. Multi-Pattern Search (v1.7.8)

Search using multiple regex patterns. Returns keys matching ANY pattern (OR logic between patterns).

```json
{
  "apiKey": "your-api-key",
  "region": "logs",
  "keyPatterns": [
    "error:2025-12-22:.*",
    "warning:2025-12-22:.*",
    "critical:.*"
  ],
  "limit": 100
}
```

**Response:**
```json
[
  {"key": "error:2025-12-22:001", "value": {"message": "Connection timeout"}},
  {"key": "warning:2025-12-22:005", "value": {"message": "High memory usage"}},
  {"key": "critical:system:001", "value": {"message": "Database unreachable"}}
]
```

---

### 5. JSON Attribute Search (v1.7.8 Enhanced)

Search JSON documents by attribute values with AND logic across all criteria.

#### 5a. Equality Search

```json
{
  "apiKey": "your-api-key",
  "region": "users",
  "type": "json",
  "criteria": {
    "status": "active",
    "country": "USA"
  }
}
```

*Returns all documents where status="active" AND country="USA"*

#### 5b. Multi-Value Search (IN Operator)

```json
{
  "apiKey": "your-api-key",
  "region": "products",
  "type": "json",
  "criteria": {
    "category": ["electronics", "computers", "accessories"],
    "inStock": true
  }
}
```

*Returns documents where category is one of the listed values AND inStock=true*

#### 5c. Regex Search for Attributes

```json
{
  "apiKey": "your-api-key",
  "region": "users",
  "type": "json",
  "criteria": {
    "email": {"regex": ".*@(company|partner)\\.com"},
    "status": "active"
  }
}
```

*Returns documents where email matches the pattern AND status="active"*

#### 5d. Range Comparisons (Numeric)

```json
{
  "apiKey": "your-api-key",
  "region": "employees",
  "type": "json",
  "criteria": {
    "age": {"gte": 18, "lte": 65},
    "salary": {"gt": 50000},
    "yearsOfService": {"gte": 2}
  }
}
```

*Returns documents matching all numeric conditions*

#### 5e. Combined Search (All Features)

```json
{
  "apiKey": "your-api-key",
  "region": "orders",
  "type": "json",
  "criteria": {
    "status": ["pending", "processing"],
    "customer.email": {"regex": ".*@enterprise\\.com"},
    "total": {"gte": 1000},
    "priority": {"ne": "low"}
  },
  "fields": ["orderId", "customer.name", "total", "status"],
  "limit": 50
}
```

---

## Supported Operators

| Operator | Type | Description | Example |
|----------|------|-------------|---------|
| (direct) | Equality | Exact match | `"status": "active"` |
| (array) | IN | Match any value in list | `"country": ["USA", "UK", "CA"]` |
| `regex` | Pattern | Regex pattern match | `"email": {"regex": ".*@.*\\.org"}` |
| `gt` | Numeric | Greater than | `"age": {"gt": 18}` |
| `gte` | Numeric | Greater than or equal | `"age": {"gte": 18}` |
| `lt` | Numeric | Less than | `"age": {"lt": 65}` |
| `lte` | Numeric | Less than or equal | `"age": {"lte": 65}` |
| `ne` | Any | Not equal | `"status": {"ne": "deleted"}` |
| `eq` | Numeric | Equal (explicit) | `"count": {"eq": 0}` |

---

## Field Projection

Return only specific fields from matching documents:

```json
{
  "apiKey": "your-api-key",
  "region": "users",
  "keys": ["user:1", "user:2", "user:3"],
  "fields": ["name", "email", "address.city", "address.country"]
}
```

**Response:**
```json
[
  {
    "key": "user:1",
    "value": {
      "name": "John Doe",
      "email": "john@example.com",
      "address": {
        "city": "New York",
        "country": "USA"
      }
    }
  }
]
```

*Supports nested paths with dot notation.*

---

## Request Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `apiKey` | String | Yes | API key for authentication |
| `region` | String | Yes | Cache region to search |
| `key` | String | No | Single key for exact lookup |
| `keys` | String[] | No | Multiple keys for batch lookup |
| `keyPattern` | String | No | Regex pattern for key search |
| `keyPatterns` | String[] | No | Multiple regex patterns |
| `type` | String | No | Set to "json" for attribute search |
| `criteria` | Object | No | JSON search criteria (v1.7.8) |
| `values` | Object[] | No | Legacy JSON search format |
| `fields` | String[] | No | Fields to return (projection) |
| `limit` | Integer | No | Max results (default: 1000) |

---

## Response Format

All responses are JSON arrays:

```json
[
  {"key": "key1", "value": {...}},
  {"key": "key2", "value": {...}}
]
```

### Error Responses

```json
[
  {"error": "API key is required. Include 'apiKey' in request body."}
]
```

```json
[
  {"error": "Invalid API key"}
]
```

```json
[
  {"error": "Region does not exist: unknown_region"}
]
```

```json
[
  {"error": "Invalid regex pattern: Unclosed group near index 5"}
]
```

---

## Examples Using curl

### Multi-Key Lookup
```bash
curl -X POST http://localhost:8080/api/genericsearch \
  -H "Content-Type: application/json" \
  -d '{
    "apiKey": "your-api-key",
    "region": "products",
    "keys": ["prod:001", "prod:002", "prod:003"]
  }'
```

### Multi-Pattern Search
```bash
curl -X POST http://localhost:8080/api/genericsearch \
  -H "Content-Type: application/json" \
  -d '{
    "apiKey": "your-api-key",
    "region": "logs",
    "keyPatterns": ["error:.*", "critical:.*"],
    "limit": 50
  }'
```

### JSON Criteria Search
```bash
curl -X POST http://localhost:8080/api/genericsearch \
  -H "Content-Type: application/json" \
  -d '{
    "apiKey": "your-api-key",
    "region": "users",
    "type": "json",
    "criteria": {
      "status": ["active", "verified"],
      "email": {"regex": ".*@company.com"},
      "age": {"gte": 21}
    },
    "fields": ["name", "email", "department"],
    "limit": 100
  }'
```

---

## Examples Using Python

```python
import requests

# Multi-key lookup
response = requests.post('http://localhost:8080/api/genericsearch', json={
    'apiKey': 'your-api-key',
    'region': 'users',
    'keys': ['user:1', 'user:2', 'user:3']
})
results = response.json()

# JSON criteria search
response = requests.post('http://localhost:8080/api/genericsearch', json={
    'apiKey': 'your-api-key',
    'region': 'orders',
    'type': 'json',
    'criteria': {
        'status': 'pending',
        'total': {'gte': 100}
    }
})
orders = response.json()
```

---

## Examples Using Java

```java
// Using OkHttp
OkHttpClient client = new OkHttpClient();

String json = """
{
    "apiKey": "your-api-key",
    "region": "users",
    "type": "json",
    "criteria": {
        "status": "active",
        "email": {"regex": ".*@company\\\\.com"}
    }
}
""";

RequestBody body = RequestBody.create(json, MediaType.parse("application/json"));
Request request = new Request.Builder()
    .url("http://localhost:8080/api/genericsearch")
    .post(body)
    .build();

Response response = client.newCall(request).execute();
String results = response.body().string();
```

---

## Best Practices

1. **Use appropriate search mode**: 
   - Use `keys` for batch lookups of known keys
   - Use `keyPatterns` for discovery of matching keys
   - Use `criteria` for complex attribute-based filtering

2. **Set reasonable limits**: Always set a `limit` for pattern and criteria searches to avoid returning excessive data.

3. **Use field projection**: When you only need specific fields, use the `fields` parameter to reduce response size.

4. **Optimize regex patterns**: Avoid overly broad patterns like `.*` at the start. Prefer anchored patterns when possible.

5. **Index considerations**: JSON criteria searches scan all keys in a region. For high-volume searches, consider using key patterns to narrow the search space first.

---

## Version History

| Version | Changes |
|---------|---------|
| 1.7.8 | Added multi-key, multi-pattern, and enhanced JSON criteria search |
| 1.7.6 | Added field projection support |
| 1.0.0 | Initial generic search with single key/pattern and basic JSON search |

---

**Copyright © 2025-2030, All Rights Reserved**  
Ashutosh Sinha | [ajsinha@gmail.com](mailto:ajsinha@gmail.com)  
*Patent Pending*
