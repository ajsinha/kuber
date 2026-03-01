/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber Distributed Cache - .NET Client Library
 * REST API Client
 */

using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuber.Client;

/// <summary>
/// Kuber distributed cache client using REST API.
/// Provides HTTP-based access to Kuber cache operations.
/// </summary>
/// <remarks>
/// <para>
/// This client communicates with Kuber over HTTP REST API.
/// It requires an API key for authentication.
/// </para>
/// <para>
/// Example usage:
/// <code>
/// using var client = new KuberRestClient("http://localhost:8080", "your-api-key");
/// await client.SetAsync("key", "value");
/// var value = await client.GetAsync("key");
/// </code>
/// </para>
/// </remarks>
public class KuberRestClient : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly string _baseUrl;
    private readonly string _apiKey;
    private readonly string _region;
    private bool _disposed;
    
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// Creates a new Kuber REST client.
    /// </summary>
    /// <param name="baseUrl">Kuber server base URL (e.g., "http://localhost:8080")</param>
    /// <param name="apiKey">API key for authentication</param>
    /// <param name="region">Default region for operations</param>
    public KuberRestClient(string baseUrl, string apiKey, string region = "default")
    {
        _baseUrl = baseUrl.TrimEnd('/');
        _apiKey = apiKey;
        _region = region;
        
        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(_baseUrl),
            Timeout = TimeSpan.FromSeconds(30)
        };
        _httpClient.DefaultRequestHeaders.Add("X-API-Key", _apiKey);
        _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
    }

    // =========================================================================
    // String Operations
    // =========================================================================

    /// <summary>
    /// Get the value of a key.
    /// </summary>
    public async Task<string?> GetAsync(string key)
    {
        var response = await _httpClient.GetAsync($"/api/cache/{_region}/{Uri.EscapeDataString(key)}");
        
        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
            return null;
            
        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<ApiResponse<string>>(JsonOptions);
        return result?.Value;
    }

    /// <summary>
    /// Set the value of a key.
    /// </summary>
    public async Task<bool> SetAsync(string key, string value, int? ttlSeconds = null)
    {
        var url = $"/api/cache/{_region}/{Uri.EscapeDataString(key)}";
        if (ttlSeconds.HasValue)
            url += $"?ttl={ttlSeconds.Value}";
            
        var content = new StringContent(
            JsonSerializer.Serialize(new { value }),
            Encoding.UTF8,
            "application/json"
        );
        
        var response = await _httpClient.PutAsync(url, content);
        response.EnsureSuccessStatusCode();
        return true;
    }

    /// <summary>
    /// Set multiple keys atomically.
    /// </summary>
    public async Task<bool> MSetAsync(Dictionary<string, string> keyValues, int? ttlSeconds = null)
    {
        var url = $"/api/cache/{_region}/batch";
        if (ttlSeconds.HasValue)
            url += $"?ttl={ttlSeconds.Value}";
            
        var content = new StringContent(
            JsonSerializer.Serialize(keyValues),
            Encoding.UTF8,
            "application/json"
        );
        
        var response = await _httpClient.PutAsync(url, content);
        response.EnsureSuccessStatusCode();
        return true;
    }

    /// <summary>
    /// Get multiple keys.
    /// </summary>
    public async Task<Dictionary<string, string?>> MGetAsync(params string[] keys)
    {
        var keysParam = string.Join(",", keys.Select(Uri.EscapeDataString));
        var response = await _httpClient.GetAsync($"/api/cache/{_region}/batch?keys={keysParam}");
        response.EnsureSuccessStatusCode();
        
        var result = await response.Content.ReadFromJsonAsync<Dictionary<string, string?>>(JsonOptions);
        return result ?? new Dictionary<string, string?>();
    }

    /// <summary>
    /// Delete a key.
    /// </summary>
    public async Task<bool> DeleteAsync(string key)
    {
        var response = await _httpClient.DeleteAsync($"/api/cache/{_region}/{Uri.EscapeDataString(key)}");
        return response.IsSuccessStatusCode;
    }

    /// <summary>
    /// Check if a key exists.
    /// </summary>
    public async Task<bool> ExistsAsync(string key)
    {
        var response = await _httpClient.GetAsync($"/api/cache/{_region}/{Uri.EscapeDataString(key)}/exists");
        if (!response.IsSuccessStatusCode)
            return false;
        var result = await response.Content.ReadFromJsonAsync<ApiResponse<bool>>(JsonOptions);
        return result?.Exists ?? false;
    }

    // =========================================================================
    // Key Operations
    // =========================================================================

    /// <summary>
    /// Find all keys matching the pattern.
    /// </summary>
    public async Task<IEnumerable<string>> KeysAsync(string pattern = "*")
    {
        var response = await _httpClient.GetAsync($"/api/cache/{_region}/keys?pattern={Uri.EscapeDataString(pattern)}");
        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<ApiResponse<List<string>>>(JsonOptions);
        return result?.Keys ?? Enumerable.Empty<string>();
    }

    /// <summary>
    /// Find keys matching a regular expression pattern.
    /// This is more powerful than KeysAsync() which uses glob patterns.
    /// Returns only keys (not values) for efficiency.
    /// </summary>
    /// <param name="regexPattern">Java regex pattern (e.g., "^user:\\d+$")</param>
    /// <param name="limit">Maximum number of keys to return (default: 1000)</param>
    /// <returns>List of matching keys</returns>
    /// <example>
    /// // Find all keys starting with 'user:' followed by digits
    /// var keys = await client.KeysRegexAsync(@"^user:\d+$");
    /// 
    /// // Find all email-like keys with limit
    /// var keys = await client.KeysRegexAsync(@".*@.*\.com$", 500);
    /// </example>
    public async Task<IEnumerable<string>> KeysRegexAsync(string regexPattern, int limit = 1000)
    {
        var response = await _httpClient.GetAsync(
            $"/api/cache/{_region}/keys/regex?pattern={Uri.EscapeDataString(regexPattern)}&limit={limit}");
        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<List<string>>(JsonOptions);
        return result ?? Enumerable.Empty<string>();
    }

    /// <summary>
    /// Search keys by regex pattern and return key-value pairs.
    /// Unlike KeysRegexAsync() which returns only keys, this method returns
    /// full details including key, value, type, and TTL.
    /// </summary>
    /// <param name="regexPattern">Java regex pattern (e.g., "^order:\\d+$")</param>
    /// <param name="limit">Maximum number of results (default: 1000)</param>
    /// <returns>List of KeySearchResult with key, value, type, ttl</returns>
    /// <example>
    /// // Search for order keys and get their values
    /// var results = await client.SearchKeysAsync(@"^order:2024.*");
    /// foreach (var r in results)
    /// {
    ///     Console.WriteLine($"Key: {r.Key}, Value: {r.Value}");
    /// }
    /// </example>
    public async Task<IEnumerable<KeySearchResult>> SearchKeysAsync(string regexPattern, int limit = 1000)
    {
        var response = await _httpClient.GetAsync(
            $"/api/cache/{_region}/ksearch?pattern={Uri.EscapeDataString(regexPattern)}&limit={limit}");
        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<List<KeySearchResult>>(JsonOptions);
        return result ?? Enumerable.Empty<KeySearchResult>();
    }

    /// <summary>
    /// Get the remaining TTL of a key in seconds.
    /// </summary>
    public async Task<long?> TtlAsync(string key)
    {
        var response = await _httpClient.GetAsync($"/api/cache/{_region}/{Uri.EscapeDataString(key)}/ttl");
        if (!response.IsSuccessStatusCode)
            return null;
        var result = await response.Content.ReadFromJsonAsync<ApiResponse<long?>>(JsonOptions);
        return result?.Ttl;
    }

    /// <summary>
    /// Set expiration on a key.
    /// </summary>
    public async Task<bool> ExpireAsync(string key, int ttlSeconds)
    {
        var content = new StringContent(
            JsonSerializer.Serialize(new { ttl = ttlSeconds }),
            Encoding.UTF8,
            "application/json"
        );
        var response = await _httpClient.PostAsync(
            $"/api/cache/{_region}/{Uri.EscapeDataString(key)}/expire",
            content
        );
        return response.IsSuccessStatusCode;
    }

    // =========================================================================
    // Hash Operations
    // =========================================================================

    /// <summary>
    /// Get a hash field value.
    /// </summary>
    public async Task<string?> HGetAsync(string key, string field)
    {
        var response = await _httpClient.GetAsync(
            $"/api/cache/{_region}/{Uri.EscapeDataString(key)}/hash/{Uri.EscapeDataString(field)}"
        );
        if (!response.IsSuccessStatusCode)
            return null;
        var result = await response.Content.ReadFromJsonAsync<ApiResponse<string>>(JsonOptions);
        return result?.Value;
    }

    /// <summary>
    /// Set a hash field value.
    /// </summary>
    public async Task<bool> HSetAsync(string key, string field, string value)
    {
        var content = new StringContent(
            JsonSerializer.Serialize(new { value }),
            Encoding.UTF8,
            "application/json"
        );
        var response = await _httpClient.PutAsync(
            $"/api/cache/{_region}/{Uri.EscapeDataString(key)}/hash/{Uri.EscapeDataString(field)}",
            content
        );
        return response.IsSuccessStatusCode;
    }

    /// <summary>
    /// Get all fields and values of a hash.
    /// </summary>
    public async Task<Dictionary<string, string>> HGetAllAsync(string key)
    {
        var response = await _httpClient.GetAsync(
            $"/api/cache/{_region}/{Uri.EscapeDataString(key)}/hash"
        );
        if (!response.IsSuccessStatusCode)
            return new Dictionary<string, string>();
        var result = await response.Content.ReadFromJsonAsync<Dictionary<string, string>>(JsonOptions);
        return result ?? new Dictionary<string, string>();
    }

    // =========================================================================
    // JSON Operations
    // =========================================================================

    /// <summary>
    /// Set a JSON document.
    /// </summary>
    public async Task<bool> JsonSetAsync<T>(string key, T document, int? ttlSeconds = null)
    {
        var url = $"/api/json/{_region}/{Uri.EscapeDataString(key)}";
        if (ttlSeconds.HasValue)
            url += $"?ttl={ttlSeconds.Value}";
            
        var content = new StringContent(
            JsonSerializer.Serialize(document),
            Encoding.UTF8,
            "application/json"
        );
        
        var response = await _httpClient.PutAsync(url, content);
        return response.IsSuccessStatusCode;
    }

    /// <summary>
    /// Get a JSON document.
    /// </summary>
    public async Task<T?> JsonGetAsync<T>(string key, string? path = null)
    {
        var url = $"/api/json/{_region}/{Uri.EscapeDataString(key)}";
        if (!string.IsNullOrEmpty(path))
            url += $"?path={Uri.EscapeDataString(path)}";
            
        var response = await _httpClient.GetAsync(url);
        if (!response.IsSuccessStatusCode)
            return default;
            
        return await response.Content.ReadFromJsonAsync<T>(JsonOptions);
    }

    /// <summary>
    /// Search JSON documents.
    /// </summary>
    public async Task<List<JsonSearchResult>> JsonSearchAsync(string query)
    {
        var response = await _httpClient.GetAsync(
            $"/api/json/{_region}/search?query={Uri.EscapeDataString(query)}"
        );
        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<ApiResponse<List<JsonSearchResult>>>(JsonOptions);
        return result?.Results ?? new List<JsonSearchResult>();
    }

    // =========================================================================
    // Admin Operations
    // =========================================================================

    /// <summary>
    /// Ping the server.
    /// </summary>
    public async Task<bool> PingAsync()
    {
        var response = await _httpClient.GetAsync("/api/ping");
        return response.IsSuccessStatusCode;
    }

    /// <summary>
    /// Get server info.
    /// </summary>
    public async Task<ServerInfo?> InfoAsync()
    {
        var response = await _httpClient.GetAsync("/api/info");
        if (!response.IsSuccessStatusCode)
            return null;
        return await response.Content.ReadFromJsonAsync<ServerInfo>(JsonOptions);
    }

    /// <summary>
    /// List all regions.
    /// </summary>
    public async Task<List<string>> RegionsAsync()
    {
        var response = await _httpClient.GetAsync("/api/regions");
        response.EnsureSuccessStatusCode();
        var result = await response.Content.ReadFromJsonAsync<ApiResponse<List<string>>>(JsonOptions);
        return result?.Regions ?? new List<string>();
    }

    // =========================================================================
    // Region Support
    // =========================================================================

    /// <summary>
    /// Get a client scoped to a specific region.
    /// </summary>
    public KuberRestClient WithRegion(string region)
    {
        return new KuberRestClient(_baseUrl, _apiKey, region);
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
                _httpClient?.Dispose();
            }
            _disposed = true;
        }
    }
}

// =========================================================================
// Response DTOs
// =========================================================================

/// <summary>
/// Generic API response wrapper.
/// </summary>
public class ApiResponse<T>
{
    [JsonPropertyName("value")]
    public T? Value { get; set; }
    
    [JsonPropertyName("keys")]
    public List<string>? Keys { get; set; }
    
    [JsonPropertyName("exists")]
    public bool Exists { get; set; }
    
    [JsonPropertyName("ttl")]
    public long? Ttl { get; set; }
    
    [JsonPropertyName("results")]
    public List<JsonSearchResult>? Results { get; set; }
    
    [JsonPropertyName("regions")]
    public List<string>? Regions { get; set; }
}

/// <summary>
/// JSON search result.
/// </summary>
public class JsonSearchResult
{
    [JsonPropertyName("key")]
    public string Key { get; set; } = "";
    
    [JsonPropertyName("value")]
    public object? Value { get; set; }
}

/// <summary>
/// Key search result including key, value, type, and TTL.
/// Returned by SearchKeysAsync method.
/// </summary>
public class KeySearchResult
{
    [JsonPropertyName("key")]
    public string Key { get; set; } = "";
    
    [JsonPropertyName("value")]
    public object? Value { get; set; }
    
    [JsonPropertyName("type")]
    public string Type { get; set; } = "";
    
    [JsonPropertyName("ttl")]
    public long Ttl { get; set; } = -1;
}

/// <summary>
/// Server information.
/// </summary>
public class ServerInfo
{
    [JsonPropertyName("version")]
    public string Version { get; set; } = "";
    
    [JsonPropertyName("uptime")]
    public long Uptime { get; set; }
    
    [JsonPropertyName("totalKeys")]
    public long TotalKeys { get; set; }
    
    [JsonPropertyName("memoryUsed")]
    public long MemoryUsed { get; set; }
}
