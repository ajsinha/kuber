# Generic Update API

**Version 1.8.2**

The Generic Update API provides a unified SET/UPDATE operation via REST API with intelligent handling of JSON data merging.

## Endpoint

```
POST /api/genericupdate
POST /api/v1/genericupdate
POST /api/v2/genericupdate
```

## Request Format

```json
{
    "apiKey": "kub_xxxxx",
    "region": "myregion",
    "key": "user:123",
    "value": {"name": "John", "age": 30},
    "type": "json",
    "ttl": 3600
}
```

## Request Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `apiKey` | string | **Yes** | API key for authentication |
| `region` | string | **Yes** | Target region name (auto-created if doesn't exist) |
| `key` | string | **Yes** | Cache key to set or update |
| `value` | any | **Yes** | Value to set (scalar, object, or array) |
| `type` | string | No | Set to `"json"` for JSON handling with merge semantics |
| `ttl` | number | No | Time-to-live in seconds (default: -1 = no expiry) |

## Behavior

### When Key Does NOT Exist (CREATE)

| Condition | Action |
|-----------|--------|
| `type` = "json" | Creates new entry as JSON data type |
| `type` ≠ "json" | Creates new entry as string |
| `ttl` provided | Sets expiration to TTL seconds |
| `ttl` not provided | No expiration (TTL = -1) |

### When Key EXISTS (UPDATE)

| Condition | Action |
|-----------|--------|
| `type` ≠ "json" | **Replaces** value entirely |
| `type` = "json" | **Merges** using JUPDATE logic |

### JUPDATE Merge Logic

When `type` = "json" and the key exists:

1. **Existing fields not in request** → Preserved
2. **Fields in request that exist** → Overwritten with new values
3. **New fields in request** → Added to the document
4. **Nested objects** → Deep merged recursively
5. **Arrays** → Replaced entirely (not merged element-by-element)

## Response Format

### Success Response

```json
{
    "success": true,
    "operation": "created|replaced|merged",
    "region": "myregion",
    "key": "user:123",
    "value": {"name": "John", "age": 30},
    "ttl": 3600
}
```

| Field | Description |
|-------|-------------|
| `success` | Always `true` on success |
| `operation` | `created` (new key), `replaced` (non-JSON update), `merged` (JSON update) |
| `region` | The target region |
| `key` | The cache key |
| `value` | The resulting value after operation |
| `ttl` | TTL if provided (omitted if -1) |

### Error Response

```json
{
    "success": false,
    "error": "Error message description"
}
```

## Examples

### Example 1: Create New Entry (Non-JSON)

```bash
curl -X POST http://localhost:8080/api/genericupdate \
  -H "Content-Type: application/json" \
  -d '{
    "apiKey": "kub_xxxxx",
    "region": "counters",
    "key": "page_views",
    "value": 42
  }'
```

Response:
```json
{
    "success": true,
    "operation": "created",
    "region": "counters",
    "key": "page_views",
    "value": "42"
}
```

### Example 2: Create New JSON Entry

```bash
curl -X POST http://localhost:8080/api/genericupdate \
  -H "Content-Type: application/json" \
  -d '{
    "apiKey": "kub_xxxxx",
    "region": "users",
    "key": "user:1",
    "value": {
      "name": "John Doe",
      "email": "john@example.com",
      "age": 30
    },
    "type": "json",
    "ttl": 86400
  }'
```

Response:
```json
{
    "success": true,
    "operation": "created",
    "region": "users",
    "key": "user:1",
    "value": {
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30
    },
    "ttl": 86400
}
```

### Example 3: Merge/Update Existing JSON

Existing value:
```json
{"name": "John Doe", "email": "john@example.com", "age": 30}
```

Request:
```bash
curl -X POST http://localhost:8080/api/genericupdate \
  -H "Content-Type: application/json" \
  -d '{
    "apiKey": "kub_xxxxx",
    "region": "users",
    "key": "user:1",
    "value": {
      "email": "john.new@example.com",
      "city": "New York",
      "verified": true
    },
    "type": "json"
  }'
```

Response (merged result):
```json
{
    "success": true,
    "operation": "merged",
    "region": "users",
    "key": "user:1",
    "value": {
        "name": "John Doe",
        "email": "john.new@example.com",
        "age": 30,
        "city": "New York",
        "verified": true
    }
}
```

### Example 4: Replace Non-JSON Value

```bash
curl -X POST http://localhost:8080/api/genericupdate \
  -H "Content-Type: application/json" \
  -d '{
    "apiKey": "kub_xxxxx",
    "region": "counters",
    "key": "page_views",
    "value": 100
  }'
```

Response:
```json
{
    "success": true,
    "operation": "replaced",
    "region": "counters",
    "key": "page_views",
    "value": "100"
}
```

### Example 5: Deep Merge with Nested Objects

Existing value:
```json
{
    "profile": {
        "name": "John",
        "settings": {
            "theme": "dark",
            "notifications": true
        }
    },
    "lastLogin": "2025-01-01"
}
```

Request:
```bash
curl -X POST http://localhost:8080/api/genericupdate \
  -H "Content-Type: application/json" \
  -d '{
    "apiKey": "kub_xxxxx",
    "region": "users",
    "key": "user:1",
    "value": {
      "profile": {
        "settings": {
          "theme": "light",
          "language": "en"
        }
      }
    },
    "type": "json"
  }'
```

Response (deep merged):
```json
{
    "success": true,
    "operation": "merged",
    "region": "users",
    "key": "user:1",
    "value": {
        "profile": {
            "name": "John",
            "settings": {
                "theme": "light",
                "notifications": true,
                "language": "en"
            }
        },
        "lastLogin": "2025-01-01"
    }
}
```

## Python Client Example

```python
import requests
import json

def generic_update(base_url, api_key, region, key, value, 
                   data_type=None, ttl=None):
    """
    Perform a generic update operation.
    
    Args:
        base_url: Kuber server URL
        api_key: API key for authentication
        region: Target region
        key: Cache key
        value: Value to set (dict, list, or scalar)
        data_type: "json" for JSON merge, None for replacement
        ttl: Time-to-live in seconds (optional)
    
    Returns:
        Response dict with success status and result
    """
    payload = {
        "apiKey": api_key,
        "region": region,
        "key": key,
        "value": value
    }
    
    if data_type:
        payload["type"] = data_type
    if ttl:
        payload["ttl"] = ttl
    
    response = requests.post(
        f"{base_url}/api/genericupdate",
        json=payload
    )
    return response.json()

# Example usage
result = generic_update(
    base_url="http://localhost:8080",
    api_key="kub_xxxxx",
    region="users",
    key="user:123",
    value={"status": "active", "lastSeen": "2025-01-15"},
    data_type="json",
    ttl=3600
)

print(json.dumps(result, indent=2))
```

## Java Client Example

```java
import java.net.http.*;
import java.net.URI;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.*;

public class GenericUpdateExample {
    private static final ObjectMapper mapper = new ObjectMapper();
    
    public static Map<String, Object> genericUpdate(
            String baseUrl, String apiKey, String region, 
            String key, Object value, String type, Long ttl) throws Exception {
        
        Map<String, Object> request = new LinkedHashMap<>();
        request.put("apiKey", apiKey);
        request.put("region", region);
        request.put("key", key);
        request.put("value", value);
        if (type != null) request.put("type", type);
        if (ttl != null) request.put("ttl", ttl);
        
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest httpRequest = HttpRequest.newBuilder()
            .uri(URI.create(baseUrl + "/api/genericupdate"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(request)))
            .build();
        
        HttpResponse<String> response = client.send(httpRequest, 
            HttpResponse.BodyHandlers.ofString());
        
        return mapper.readValue(response.body(), Map.class);
    }
    
    public static void main(String[] args) throws Exception {
        Map<String, Object> user = Map.of(
            "name", "John Doe",
            "email", "john@example.com"
        );
        
        Map<String, Object> result = genericUpdate(
            "http://localhost:8080",
            "kub_xxxxx",
            "users",
            "user:123",
            user,
            "json",
            3600L
        );
        
        System.out.println(mapper.writerWithDefaultPrettyPrinter()
            .writeValueAsString(result));
    }
}
```

## Error Codes

| HTTP Status | Error | Description |
|-------------|-------|-------------|
| 401 | API key is required | Missing `apiKey` in request |
| 401 | Invalid API key | API key validation failed |
| 400 | region is required | Missing `region` in request |
| 400 | key is required | Missing `key` in request |
| 400 | value is required | Missing `value` in request |
| 400 | Failed to create region | Region auto-creation failed |
| 500 | Update failed | Internal error during update |

## Best Practices

1. **Use JSON Type for Partial Updates**: When you only need to update specific fields, use `type: "json"` to merge instead of replacing the entire document.

2. **Set Appropriate TTLs**: Use TTL for session data, caches, and temporary values. Omit TTL for permanent data.

3. **Auto-Created Regions**: Regions are auto-created if they don't exist, but consider pre-creating regions with proper descriptions for better organization.

4. **Handle Response Operations**: Check the `operation` field to understand what happened:
   - `created`: New key was created
   - `replaced`: Existing value was replaced (non-JSON)
   - `merged`: Existing JSON was merged with new values

5. **Error Handling**: Always check the `success` field and handle errors appropriately.

---

**See Also:**
- [Generic Search API](GENERIC_SEARCH_API.md)
- [JSON Operations Guide](JSON_OPERATIONS.md)
- [API Reference](API_REFERENCE.md)

---

**Copyright © 2025-2030, All Rights Reserved**  
Ashutosh Sinha | [ajsinha@gmail.com](mailto:ajsinha@gmail.com)  
*Patent Pending*
