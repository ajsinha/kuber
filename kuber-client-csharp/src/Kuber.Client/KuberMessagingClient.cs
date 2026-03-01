/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber Distributed Cache - .NET Client Library
 * Request/Response Messaging Client
 */

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuber.Client;

/// <summary>
/// Client for Kuber request/response messaging via message brokers.
/// </summary>
/// <remarks>
/// <para>
/// This class provides methods to build request messages for Kuber's
/// request/response messaging system. Messages are serialized as JSON
/// and sent to a message broker (Kafka, RabbitMQ, ActiveMQ, or IBM MQ).
/// </para>
/// <para>
/// Example usage:
/// <code>
/// var client = new KuberMessagingClient("your-api-key");
/// var request = client.BuildGet("user:1001");
/// var json = client.ToJson(request);
/// // Send json to your message broker's request topic
/// </code>
/// </para>
/// </remarks>
public class KuberMessagingClient
{
    private readonly string _apiKey;
    private readonly string _defaultRegion;
    
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = true
    };

    /// <summary>
    /// Creates a new messaging client.
    /// </summary>
    /// <param name="apiKey">API key for authentication with Kuber</param>
    /// <param name="defaultRegion">Default region for operations</param>
    public KuberMessagingClient(string apiKey, string defaultRegion = "default")
    {
        _apiKey = apiKey;
        _defaultRegion = defaultRegion;
    }

    /// <summary>
    /// Generate a unique message ID.
    /// </summary>
    public static string GenerateMessageId() => Guid.NewGuid().ToString();

    /// <summary>
    /// Serialize a request to JSON.
    /// </summary>
    public string ToJson(CacheRequest request) => JsonSerializer.Serialize(request, JsonOptions);

    /// <summary>
    /// Parse a response from JSON.
    /// </summary>
    public CacheResponse? ParseResponse(string json) => JsonSerializer.Deserialize<CacheResponse>(json, JsonOptions);

    // =========================================================================
    // Request Builders
    // =========================================================================

    private CacheRequest CreateBase(string operation, string? region = null)
    {
        return new CacheRequest
        {
            ApiKey = _apiKey,
            MessageId = GenerateMessageId(),
            Operation = operation.ToUpperInvariant(),
            Region = region ?? _defaultRegion
        };
    }

    // String Operations

    /// <summary>Build a GET request.</summary>
    public CacheRequest BuildGet(string key, string? region = null)
    {
        var request = CreateBase("GET", region);
        request.Key = key;
        return request;
    }

    /// <summary>Build a SET request.</summary>
    public CacheRequest BuildSet(string key, object value, long? ttl = null, string? region = null)
    {
        var request = CreateBase("SET", region);
        request.Key = key;
        request.Value = value;
        request.Ttl = ttl;
        return request;
    }

    /// <summary>Build a DELETE request for a single key.</summary>
    public CacheRequest BuildDelete(string key, string? region = null)
    {
        var request = CreateBase("DELETE", region);
        request.Key = key;
        return request;
    }

    /// <summary>Build a DELETE request for multiple keys.</summary>
    public CacheRequest BuildDeleteMulti(List<string> keys, string? region = null)
    {
        var request = CreateBase("DELETE", region);
        request.Keys = keys;
        return request;
    }

    /// <summary>Build an EXISTS request.</summary>
    public CacheRequest BuildExists(string key, string? region = null)
    {
        var request = CreateBase("EXISTS", region);
        request.Key = key;
        return request;
    }

    // Batch Operations

    /// <summary>Build a multi-GET request.</summary>
    public CacheRequest BuildMGet(List<string> keys, string? region = null)
    {
        var request = CreateBase("MGET", region);
        request.Keys = keys;
        return request;
    }

    /// <summary>Build a multi-SET request.</summary>
    public CacheRequest BuildMSet(Dictionary<string, object> entries, long? ttl = null, string? region = null)
    {
        var request = CreateBase("MSET", region);
        request.Entries = entries;
        request.Ttl = ttl;
        return request;
    }

    // Key Operations

    /// <summary>Build a KEYS request.</summary>
    public CacheRequest BuildKeys(string pattern = "*", string? region = null)
    {
        var request = CreateBase("KEYS", region);
        request.Pattern = pattern;
        return request;
    }

    /// <summary>Build a TTL request.</summary>
    public CacheRequest BuildTtl(string key, string? region = null)
    {
        var request = CreateBase("TTL", region);
        request.Key = key;
        return request;
    }

    /// <summary>Build an EXPIRE request.</summary>
    public CacheRequest BuildExpire(string key, long ttl, string? region = null)
    {
        var request = CreateBase("EXPIRE", region);
        request.Key = key;
        request.Ttl = ttl;
        return request;
    }

    // Hash Operations

    /// <summary>Build a HGET request.</summary>
    public CacheRequest BuildHGet(string key, string field, string? region = null)
    {
        var request = CreateBase("HGET", region);
        request.Key = key;
        request.Field = field;
        return request;
    }

    /// <summary>Build a HSET request.</summary>
    public CacheRequest BuildHSet(string key, string field, object value, string? region = null)
    {
        var request = CreateBase("HSET", region);
        request.Key = key;
        request.Field = field;
        request.Value = value;
        return request;
    }

    /// <summary>Build a HGETALL request.</summary>
    public CacheRequest BuildHGetAll(string key, string? region = null)
    {
        var request = CreateBase("HGETALL", region);
        request.Key = key;
        return request;
    }

    /// <summary>Build a HMSET request.</summary>
    public CacheRequest BuildHMSet(string key, Dictionary<string, string> fields, string? region = null)
    {
        var request = CreateBase("HMSET", region);
        request.Key = key;
        request.Fields = fields;
        return request;
    }

    // JSON Operations

    /// <summary>Build a JSET request.</summary>
    public CacheRequest BuildJSet(string key, object value, string? region = null)
    {
        var request = CreateBase("JSET", region);
        request.Key = key;
        request.Value = value;
        return request;
    }

    /// <summary>Build a JGET request.</summary>
    public CacheRequest BuildJGet(string key, string? path = null, string? region = null)
    {
        var request = CreateBase("JGET", region);
        request.Key = key;
        request.Path = path;
        return request;
    }

    /// <summary>Build a JSEARCH request.</summary>
    public CacheRequest BuildJSearch(string query, string? region = null)
    {
        var request = CreateBase("JSEARCH", region);
        request.Query = query;
        return request;
    }

    // Admin Operations

    /// <summary>Build a PING request.</summary>
    public CacheRequest BuildPing() => CreateBase("PING");

    /// <summary>Build an INFO request.</summary>
    public CacheRequest BuildInfo() => CreateBase("INFO");

    /// <summary>Build a REGIONS request.</summary>
    public CacheRequest BuildRegions() => CreateBase("REGIONS");
}

// =========================================================================
// Request/Response DTOs for Messaging
// =========================================================================

/// <summary>
/// Cache request message for messaging system.
/// </summary>
public class CacheRequest
{
    [JsonPropertyName("api_key")]
    public string ApiKey { get; set; } = "";

    [JsonPropertyName("message_id")]
    public string MessageId { get; set; } = "";

    [JsonPropertyName("operation")]
    public string Operation { get; set; } = "";

    [JsonPropertyName("region")]
    public string Region { get; set; } = "default";

    [JsonPropertyName("key")]
    public string? Key { get; set; }

    [JsonPropertyName("value")]
    public object? Value { get; set; }

    [JsonPropertyName("keys")]
    public List<string>? Keys { get; set; }

    [JsonPropertyName("entries")]
    public Dictionary<string, object>? Entries { get; set; }

    [JsonPropertyName("pattern")]
    public string? Pattern { get; set; }

    [JsonPropertyName("ttl")]
    public long? Ttl { get; set; }

    [JsonPropertyName("field")]
    public string? Field { get; set; }

    [JsonPropertyName("fields")]
    public Dictionary<string, string>? Fields { get; set; }

    [JsonPropertyName("path")]
    public string? Path { get; set; }

    [JsonPropertyName("query")]
    public string? Query { get; set; }
}

/// <summary>
/// Cache response message from messaging system.
/// </summary>
public class CacheResponse
{
    [JsonPropertyName("request_receive_timestamp")]
    public string? RequestReceiveTimestamp { get; set; }

    [JsonPropertyName("response_time")]
    public string? ResponseTime { get; set; }

    [JsonPropertyName("processing_time_ms")]
    public long ProcessingTimeMs { get; set; }

    [JsonPropertyName("request")]
    public CacheRequest? Request { get; set; }

    [JsonPropertyName("response")]
    public ResponsePayload? Response { get; set; }
}

/// <summary>
/// Response payload containing result or error.
/// </summary>
public class ResponsePayload
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    [JsonPropertyName("result")]
    public object? Result { get; set; }

    [JsonPropertyName("error")]
    public string Error { get; set; } = "";

    [JsonPropertyName("error_code")]
    public string? ErrorCode { get; set; }

    [JsonPropertyName("server_message")]
    public string ServerMessage { get; set; } = "";

    [JsonPropertyName("total_count")]
    public int? TotalCount { get; set; }

    [JsonPropertyName("returned_count")]
    public int? ReturnedCount { get; set; }
}
