/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber Distributed Cache - .NET Examples
 * Redis Protocol Example
 */

using Kuber.Client;

namespace Kuber.Client.Examples;

/// <summary>
/// Demonstrates using Kuber with Redis protocol via StackExchange.Redis.
/// </summary>
public static class RedisProtocolExample
{
    public static async Task RunAsync()
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║            KUBER REDIS PROTOCOL EXAMPLE - C# CLIENT                 ║");
        Console.WriteLine("║                        Version 1.8.3                                ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        // Connect to Kuber
        Console.WriteLine("Connecting to Kuber at localhost:6379...");
        
        try
        {
            using var client = new KuberClient("localhost", 6379);
            
            // Test connection
            var latency = await client.PingAsync();
            Console.WriteLine($"✓ Connected! Latency: {latency.TotalMilliseconds:F2}ms\n");

            // =================================================================
            // String Operations
            // =================================================================
            Console.WriteLine("═══ STRING OPERATIONS ═══\n");

            // SET
            Console.WriteLine("Setting 'greeting' = 'Hello, Kuber!'");
            await client.SetAsync("greeting", "Hello, Kuber!");
            
            // GET
            var value = await client.GetAsync("greeting");
            Console.WriteLine($"Getting 'greeting': {value}");

            // SET with TTL
            Console.WriteLine("\nSetting 'temp_key' with 60 second TTL");
            await client.SetAsync("temp_key", "temporary value", TimeSpan.FromSeconds(60));
            
            var ttl = await client.TtlAsync("temp_key");
            Console.WriteLine($"TTL of 'temp_key': {ttl?.TotalSeconds} seconds");

            // MSET
            Console.WriteLine("\nBatch setting multiple keys:");
            await client.MSetAsync(new Dictionary<string, string>
            {
                ["user:1:name"] = "Alice",
                ["user:1:email"] = "alice@example.com",
                ["user:2:name"] = "Bob",
                ["user:2:email"] = "bob@example.com"
            });
            Console.WriteLine("  Set user:1:name, user:1:email, user:2:name, user:2:email");

            // MGET
            var users = await client.MGetAsync("user:1:name", "user:1:email", "user:2:name", "nonexistent");
            Console.WriteLine("\nBatch getting keys:");
            foreach (var kv in users)
            {
                Console.WriteLine($"  {kv.Key}: {kv.Value ?? "(null)"}");
            }

            // =================================================================
            // Hash Operations
            // =================================================================
            Console.WriteLine("\n═══ HASH OPERATIONS ═══\n");

            // HSET
            Console.WriteLine("Setting hash fields for 'session:abc123':");
            await client.HSetAsync("session:abc123", "user_id", "1001");
            await client.HSetAsync("session:abc123", "ip", "192.168.1.100");
            await client.HSetAsync("session:abc123", "created", DateTime.UtcNow.ToString("O"));

            // HMSET
            await client.HMSetAsync("session:abc123", new Dictionary<string, string>
            {
                ["browser"] = "Chrome",
                ["os"] = "Windows 11"
            });
            Console.WriteLine("  Set user_id, ip, created, browser, os");

            // HGETALL
            var session = await client.HGetAllAsync("session:abc123");
            Console.WriteLine("\nGetting all hash fields:");
            foreach (var kv in session)
            {
                Console.WriteLine($"  {kv.Key}: {kv.Value}");
            }

            // HGET
            var userId = await client.HGetAsync("session:abc123", "user_id");
            Console.WriteLine($"\nGetting single field 'user_id': {userId}");

            // =================================================================
            // JSON Operations
            // =================================================================
            Console.WriteLine("\n═══ JSON OPERATIONS ═══\n");

            // Store JSON object
            var product = new
            {
                Id = 5001,
                Name = "Gaming Laptop",
                Price = 1299.99,
                Specs = new
                {
                    Cpu = "Intel i9",
                    Ram = "32GB",
                    Storage = "1TB NVMe"
                },
                Tags = new[] { "electronics", "gaming", "laptop" }
            };

            Console.WriteLine("Storing JSON document 'product:5001':");
            await client.JsonSetAsync("product:5001", product);
            Console.WriteLine($"  {System.Text.Json.JsonSerializer.Serialize(product)}");

            // Retrieve JSON object
            var retrieved = await client.JsonGetAsync<dynamic>("product:5001");
            Console.WriteLine($"\nRetrieved JSON: {retrieved}");

            // =================================================================
            // Key Operations
            // =================================================================
            Console.WriteLine("\n═══ KEY OPERATIONS ═══\n");

            // KEYS pattern
            Console.WriteLine("Finding keys matching 'user:*':");
            foreach (var key in client.Keys("user:*"))
            {
                Console.WriteLine($"  {key}");
            }

            // EXISTS
            var exists = await client.ExistsAsync("greeting");
            Console.WriteLine($"\nKey 'greeting' exists: {exists}");

            // DELETE
            Console.WriteLine("\nDeleting 'temp_key'...");
            await client.DeleteAsync("temp_key");
            exists = await client.ExistsAsync("temp_key");
            Console.WriteLine($"Key 'temp_key' exists after delete: {exists}");

            // Cleanup
            Console.WriteLine("\n═══ CLEANUP ═══\n");
            var deleted = await client.DeleteAsync("greeting", "user:1:name", "user:1:email", 
                "user:2:name", "user:2:email", "session:abc123", "product:5001");
            Console.WriteLine($"Deleted {deleted} keys");

            Console.WriteLine("\n✓ Example completed successfully!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n✗ Error: {ex.Message}");
            Console.WriteLine("\nMake sure Kuber server is running on localhost:6379");
            Console.WriteLine("Start with: java -jar kuber-server.jar");
        }
    }
}
