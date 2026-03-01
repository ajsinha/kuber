/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber Distributed Cache - .NET Client Library
 * Redis Protocol Client
 */

using StackExchange.Redis;
using System.Text.Json;

namespace Kuber.Client;

/// <summary>
/// Kuber distributed cache client using Redis protocol.
/// Provides high-performance access to Kuber cache operations.
/// </summary>
/// <remarks>
/// <para>
/// This client uses the StackExchange.Redis library to communicate with Kuber
/// over the Redis protocol. It supports all standard Redis operations plus
/// Kuber-specific extensions for regions and JSON operations.
/// </para>
/// <para>
/// Example usage:
/// <code>
/// using var client = new KuberClient("localhost", 6379);
/// await client.SetAsync("key", "value");
/// var value = await client.GetAsync("key");
/// </code>
/// </para>
/// </remarks>
public class KuberClient : IDisposable
{
    private readonly ConnectionMultiplexer _redis;
    private readonly IDatabase _db;
    private readonly string _region;
    private bool _disposed;

    /// <summary>
    /// Creates a new Kuber client connection.
    /// </summary>
    /// <param name="host">Kuber server hostname</param>
    /// <param name="port">Redis protocol port (default: 6379)</param>
    /// <param name="region">Default region for operations (default: "default")</param>
    /// <param name="password">Optional password for authentication</param>
    public KuberClient(string host = "localhost", int port = 6379, string region = "default", string? password = null)
    {
        var configOptions = new ConfigurationOptions
        {
            EndPoints = { { host, port } },
            AbortOnConnectFail = false,
            ConnectTimeout = 5000,
            SyncTimeout = 5000
        };
        
        if (!string.IsNullOrEmpty(password))
        {
            configOptions.Password = password;
        }

        _redis = ConnectionMultiplexer.Connect(configOptions);
        _db = _redis.GetDatabase();
        _region = region;
    }

    /// <summary>
    /// Creates a new Kuber client with connection string.
    /// </summary>
    /// <param name="connectionString">Redis connection string</param>
    /// <param name="region">Default region for operations</param>
    public KuberClient(string connectionString, string region = "default")
    {
        _redis = ConnectionMultiplexer.Connect(connectionString);
        _db = _redis.GetDatabase();
        _region = region;
    }

    // =========================================================================
    // String Operations
    // =========================================================================

    /// <summary>
    /// Get the value of a key.
    /// </summary>
    public async Task<string?> GetAsync(string key)
    {
        var regionKey = GetRegionKey(key);
        return await _db.StringGetAsync(regionKey);
    }

    /// <summary>
    /// Get the value of a key (synchronous).
    /// </summary>
    public string? Get(string key)
    {
        var regionKey = GetRegionKey(key);
        return _db.StringGet(regionKey);
    }

    /// <summary>
    /// Set the value of a key.
    /// </summary>
    public async Task<bool> SetAsync(string key, string value, TimeSpan? expiry = null)
    {
        var regionKey = GetRegionKey(key);
        return await _db.StringSetAsync(regionKey, value, expiry);
    }

    /// <summary>
    /// Set the value of a key (synchronous).
    /// </summary>
    public bool Set(string key, string value, TimeSpan? expiry = null)
    {
        var regionKey = GetRegionKey(key);
        return _db.StringSet(regionKey, value, expiry);
    }

    /// <summary>
    /// Set multiple keys atomically.
    /// </summary>
    public async Task<bool> MSetAsync(Dictionary<string, string> keyValues)
    {
        var pairs = keyValues
            .Select(kv => new KeyValuePair<RedisKey, RedisValue>(GetRegionKey(kv.Key), kv.Value))
            .ToArray();
        return await _db.StringSetAsync(pairs);
    }

    /// <summary>
    /// Get multiple keys atomically.
    /// </summary>
    public async Task<Dictionary<string, string?>> MGetAsync(params string[] keys)
    {
        var regionKeys = keys.Select(k => (RedisKey)GetRegionKey(k)).ToArray();
        var values = await _db.StringGetAsync(regionKeys);
        
        var result = new Dictionary<string, string?>();
        for (int i = 0; i < keys.Length; i++)
        {
            result[keys[i]] = values[i].HasValue ? values[i].ToString() : null;
        }
        return result;
    }

    /// <summary>
    /// Delete a key.
    /// </summary>
    public async Task<bool> DeleteAsync(string key)
    {
        var regionKey = GetRegionKey(key);
        return await _db.KeyDeleteAsync(regionKey);
    }

    /// <summary>
    /// Delete multiple keys.
    /// </summary>
    public async Task<long> DeleteAsync(params string[] keys)
    {
        var regionKeys = keys.Select(k => (RedisKey)GetRegionKey(k)).ToArray();
        return await _db.KeyDeleteAsync(regionKeys);
    }

    /// <summary>
    /// Check if a key exists.
    /// </summary>
    public async Task<bool> ExistsAsync(string key)
    {
        var regionKey = GetRegionKey(key);
        return await _db.KeyExistsAsync(regionKey);
    }

    // =========================================================================
    // Key Operations
    // =========================================================================

    /// <summary>
    /// Find all keys matching the pattern.
    /// </summary>
    public IEnumerable<string> Keys(string pattern = "*")
    {
        var server = _redis.GetServer(_redis.GetEndPoints().First());
        var regionPattern = GetRegionKey(pattern);
        
        foreach (var key in server.Keys(pattern: regionPattern))
        {
            yield return RemoveRegionPrefix(key.ToString());
        }
    }

    /// <summary>
    /// Get the remaining TTL of a key.
    /// </summary>
    public async Task<TimeSpan?> TtlAsync(string key)
    {
        var regionKey = GetRegionKey(key);
        return await _db.KeyTimeToLiveAsync(regionKey);
    }

    /// <summary>
    /// Set expiration on a key.
    /// </summary>
    public async Task<bool> ExpireAsync(string key, TimeSpan expiry)
    {
        var regionKey = GetRegionKey(key);
        return await _db.KeyExpireAsync(regionKey, expiry);
    }

    /// <summary>
    /// Remove expiration from a key.
    /// </summary>
    public async Task<bool> PersistAsync(string key)
    {
        var regionKey = GetRegionKey(key);
        return await _db.KeyPersistAsync(regionKey);
    }

    // =========================================================================
    // Hash Operations
    // =========================================================================

    /// <summary>
    /// Get a hash field value.
    /// </summary>
    public async Task<string?> HGetAsync(string key, string field)
    {
        var regionKey = GetRegionKey(key);
        return await _db.HashGetAsync(regionKey, field);
    }

    /// <summary>
    /// Set a hash field value.
    /// </summary>
    public async Task<bool> HSetAsync(string key, string field, string value)
    {
        var regionKey = GetRegionKey(key);
        return await _db.HashSetAsync(regionKey, field, value);
    }

    /// <summary>
    /// Get all fields and values of a hash.
    /// </summary>
    public async Task<Dictionary<string, string>> HGetAllAsync(string key)
    {
        var regionKey = GetRegionKey(key);
        var entries = await _db.HashGetAllAsync(regionKey);
        return entries.ToDictionary(
            e => e.Name.ToString(),
            e => e.Value.ToString()
        );
    }

    /// <summary>
    /// Set multiple hash fields.
    /// </summary>
    public async Task HMSetAsync(string key, Dictionary<string, string> fieldValues)
    {
        var regionKey = GetRegionKey(key);
        var entries = fieldValues
            .Select(kv => new HashEntry(kv.Key, kv.Value))
            .ToArray();
        await _db.HashSetAsync(regionKey, entries);
    }

    /// <summary>
    /// Delete hash fields.
    /// </summary>
    public async Task<long> HDelAsync(string key, params string[] fields)
    {
        var regionKey = GetRegionKey(key);
        var redisFields = fields.Select(f => (RedisValue)f).ToArray();
        return await _db.HashDeleteAsync(regionKey, redisFields);
    }

    /// <summary>
    /// Check if a hash field exists.
    /// </summary>
    public async Task<bool> HExistsAsync(string key, string field)
    {
        var regionKey = GetRegionKey(key);
        return await _db.HashExistsAsync(regionKey, field);
    }

    // =========================================================================
    // JSON Operations (Kuber Extension)
    // =========================================================================

    /// <summary>
    /// Set a JSON document.
    /// </summary>
    public async Task<bool> JsonSetAsync<T>(string key, T document, TimeSpan? expiry = null)
    {
        var json = JsonSerializer.Serialize(document);
        return await SetAsync(key, json, expiry);
    }

    /// <summary>
    /// Get a JSON document.
    /// </summary>
    public async Task<T?> JsonGetAsync<T>(string key)
    {
        var json = await GetAsync(key);
        if (string.IsNullOrEmpty(json))
            return default;
        return JsonSerializer.Deserialize<T>(json);
    }

    /// <summary>
    /// Search JSON documents using deep search.
    /// </summary>
    /// <remarks>
    /// <para>Query syntax:</para>
    /// <list type="bullet">
    /// <item>Single value: field=value</item>
    /// <item>IN clause: field=[value1|value2|value3]</item>
    /// <item>Multiple conditions: field1=value1,field2=value2</item>
    /// <item>Operators: =, !=, >, &lt;, >=, &lt;=, ~= (regex)</item>
    /// </list>
    /// <para>Examples:</para>
    /// <code>
    /// // Single value
    /// var results = await client.JsonSearchAsync&lt;User&gt;("status=active");
    /// 
    /// // IN clause - multiple values for one field
    /// var results = await client.JsonSearchAsync&lt;User&gt;("status=[active|pending]");
    /// 
    /// // Multiple attributes with IN clauses
    /// var results = await client.JsonSearchAsync&lt;Trade&gt;("status=[active|pending],country=[USA|UK|CA]");
    /// </code>
    /// </remarks>
    /// <since>1.7.9 - Added IN clause support with [value1|value2|...] syntax</since>
    public async Task<List<KeyValuePair<string, T?>>> JsonSearchAsync<T>(string query)
    {
        var results = new List<KeyValuePair<string, T?>>();
        var redisResults = await _db.ExecuteAsync("JSEARCH", query);
        
        if (redisResults.IsNull)
            return results;
        
        var items = (RedisResult[])redisResults!;
        foreach (var item in items)
        {
            var str = item.ToString();
            if (string.IsNullOrEmpty(str) || !str.Contains(':'))
                continue;
            
            // Find where JSON starts (after the colon before { or [)
            int jsonStart = -1;
            for (int i = 0; i < str.Length - 1; i++)
            {
                if (str[i] == ':' && (str[i + 1] == '{' || str[i + 1] == '['))
                {
                    jsonStart = i;
                    break;
                }
            }
            
            if (jsonStart > 0)
            {
                var key = str.Substring(0, jsonStart);
                var json = str.Substring(jsonStart + 1);
                try
                {
                    var value = JsonSerializer.Deserialize<T>(json);
                    results.Add(new KeyValuePair<string, T?>(key, value));
                }
                catch (JsonException)
                {
                    // Skip malformed JSON
                }
            }
        }
        
        return results;
    }

    /// <summary>
    /// Search JSON documents with IN clause support using a conditions dictionary.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Convenience method for building queries with multiple attribute conditions,
    /// each potentially matching multiple values (IN clause).
    /// </para>
    /// <para>Example:</para>
    /// <code>
    /// var conditions = new Dictionary&lt;string, List&lt;string&gt;&gt;
    /// {
    ///     { "status", new List&lt;string&gt; { "active", "pending" } },
    ///     { "country", new List&lt;string&gt; { "USA", "UK", "CA" } }
    /// };
    /// var results = await client.JsonSearchInAsync&lt;Trade&gt;(conditions);
    /// </code>
    /// </remarks>
    /// <since>1.7.9</since>
    public async Task<List<KeyValuePair<string, T?>>> JsonSearchInAsync<T>(Dictionary<string, List<string>> conditions)
    {
        var query = BuildInClauseQuery(conditions);
        return await JsonSearchAsync<T>(query);
    }

    /// <summary>
    /// Build a query string with IN clause syntax from conditions dictionary.
    /// </summary>
    /// <param name="conditions">Dictionary mapping field names to lists of acceptable values</param>
    /// <returns>Query string in format: field1=[v1|v2],field2=[v3|v4]</returns>
    /// <since>1.7.9</since>
    public static string BuildInClauseQuery(Dictionary<string, List<string>> conditions)
    {
        if (conditions == null || conditions.Count == 0)
            return "";
        
        var parts = new List<string>();
        foreach (var kvp in conditions)
        {
            var field = kvp.Key;
            var values = kvp.Value;
            
            if (string.IsNullOrEmpty(field) || values == null || values.Count == 0)
                continue;
            
            if (values.Count == 1)
            {
                parts.Add($"{field}={values[0]}");
            }
            else
            {
                var valuesStr = string.Join("|", values);
                parts.Add($"{field}=[{valuesStr}]");
            }
        }
        
        return string.Join(",", parts);
    }

    // =========================================================================
    // Admin Operations
    // =========================================================================

    /// <summary>
    /// Ping the server.
    /// </summary>
    public async Task<TimeSpan> PingAsync()
    {
        return await _db.PingAsync();
    }

    /// <summary>
    /// Get server info.
    /// </summary>
    public string Info()
    {
        var server = _redis.GetServer(_redis.GetEndPoints().First());
        return server.Info().SelectMany(g => g).Aggregate("", (acc, kv) => $"{acc}{kv.Key}: {kv.Value}\n");
    }

    // =========================================================================
    // Region Support
    // =========================================================================

    /// <summary>
    /// Get a client scoped to a specific region.
    /// </summary>
    public KuberClient WithRegion(string region)
    {
        return new KuberClient(_redis.Configuration, region);
    }

    private string GetRegionKey(string key)
    {
        if (_region == "default" || string.IsNullOrEmpty(_region))
            return key;
        return $"{_region}:{key}";
    }

    private string RemoveRegionPrefix(string key)
    {
        if (_region == "default" || string.IsNullOrEmpty(_region))
            return key;
        var prefix = $"{_region}:";
        return key.StartsWith(prefix) ? key[prefix.Length..] : key;
    }

    // =========================================================================
    // IDisposable Implementation
    // =========================================================================

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                _redis?.Dispose();
            }
            _disposed = true;
        }
    }
}
