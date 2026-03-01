/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber Distributed Cache - .NET Examples
 * REST API Example
 */

using Kuber.Client;

namespace Kuber.Client.Examples;

/// <summary>
/// Demonstrates using Kuber with REST API via HttpClient.
/// </summary>
public static class RestApiExample
{
    public static async Task RunAsync()
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║              KUBER REST API EXAMPLE - C# CLIENT                     ║");
        Console.WriteLine("║                        Version 1.8.3                                ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        // Configuration
        const string baseUrl = "http://localhost:8080";
        const string apiKey = "your-api-key-here";  // Get from Kuber admin panel

        Console.WriteLine($"Connecting to Kuber at {baseUrl}...");
        Console.WriteLine($"Using API Key: {apiKey[..Math.Min(8, apiKey.Length)]}...\n");

        try
        {
            using var client = new KuberRestClient(baseUrl, apiKey);

            // Test connection
            var isAlive = await client.PingAsync();
            if (!isAlive)
            {
                Console.WriteLine("✗ Failed to connect to Kuber server");
                return;
            }
            Console.WriteLine("✓ Connected!\n");

            // =================================================================
            // String Operations
            // =================================================================
            Console.WriteLine("═══ STRING OPERATIONS ═══\n");

            // SET
            Console.WriteLine("Setting 'greeting' = 'Hello from C#!'");
            await client.SetAsync("greeting", "Hello from C#!");

            // GET
            var value = await client.GetAsync("greeting");
            Console.WriteLine($"Getting 'greeting': {value}");

            // SET with TTL
            Console.WriteLine("\nSetting 'session:token' with 3600 second TTL");
            await client.SetAsync("session:token", "abc123xyz", ttlSeconds: 3600);

            // Check TTL
            var ttl = await client.TtlAsync("session:token");
            Console.WriteLine($"TTL of 'session:token': {ttl} seconds");

            // MSET
            Console.WriteLine("\nBatch setting configuration:");
            await client.MSetAsync(new Dictionary<string, string>
            {
                ["config:app:name"] = "MyApp",
                ["config:app:version"] = "2.6.2",
                ["config:app:env"] = "production"
            });
            Console.WriteLine("  Set config:app:name, config:app:version, config:app:env");

            // MGET
            var configs = await client.MGetAsync("config:app:name", "config:app:version", "config:app:env");
            Console.WriteLine("\nBatch getting configuration:");
            foreach (var kv in configs)
            {
                Console.WriteLine($"  {kv.Key}: {kv.Value ?? "(null)"}");
            }

            // =================================================================
            // Key Operations
            // =================================================================
            Console.WriteLine("\n═══ KEY OPERATIONS ═══\n");

            // KEYS pattern
            Console.WriteLine("Finding keys matching 'config:*':");
            var keys = await client.KeysAsync("config:*");
            foreach (var key in keys)
            {
                Console.WriteLine($"  {key}");
            }

            // EXISTS
            var exists = await client.ExistsAsync("greeting");
            Console.WriteLine($"\nKey 'greeting' exists: {exists}");

            // EXPIRE
            Console.WriteLine("\nSetting 'greeting' to expire in 300 seconds");
            await client.ExpireAsync("greeting", 300);

            // =================================================================
            // Hash Operations
            // =================================================================
            Console.WriteLine("\n═══ HASH OPERATIONS ═══\n");

            // HSET
            Console.WriteLine("Setting hash fields for 'user:profile:1001':");
            await client.HSetAsync("user:profile:1001", "name", "Alice Johnson");
            await client.HSetAsync("user:profile:1001", "email", "alice@example.com");
            await client.HSetAsync("user:profile:1001", "role", "admin");
            Console.WriteLine("  Set name, email, role");

            // HGET
            var name = await client.HGetAsync("user:profile:1001", "name");
            Console.WriteLine($"\nGetting field 'name': {name}");

            // HGETALL
            var profile = await client.HGetAllAsync("user:profile:1001");
            Console.WriteLine("\nGetting all profile fields:");
            foreach (var kv in profile)
            {
                Console.WriteLine($"  {kv.Key}: {kv.Value}");
            }

            // =================================================================
            // JSON Operations
            // =================================================================
            Console.WriteLine("\n═══ JSON OPERATIONS ═══\n");

            // Store complex JSON
            var order = new
            {
                OrderId = "ORD-2025-001",
                Customer = new
                {
                    Id = 1001,
                    Name = "Alice Johnson",
                    Email = "alice@example.com"
                },
                Items = new[]
                {
                    new { ProductId = "P001", Name = "Laptop", Quantity = 1, Price = 999.99 },
                    new { ProductId = "P002", Name = "Mouse", Quantity = 2, Price = 29.99 }
                },
                Total = 1059.97,
                Status = "pending",
                CreatedAt = DateTime.UtcNow
            };

            Console.WriteLine("Storing JSON document 'order:2025-001':");
            await client.JsonSetAsync("order:2025-001", order);
            Console.WriteLine("  Stored order with nested customer and items array");

            // Retrieve JSON
            var retrievedOrder = await client.JsonGetAsync<dynamic>("order:2025-001");
            Console.WriteLine($"\nRetrieved order: {retrievedOrder}");

            // =================================================================
            // Region Operations
            // =================================================================
            Console.WriteLine("\n═══ REGION OPERATIONS ═══\n");

            // List regions
            var regions = await client.RegionsAsync();
            Console.WriteLine("Available regions:");
            foreach (var region in regions)
            {
                Console.WriteLine($"  {region}");
            }

            // Use specific region
            Console.WriteLine("\nUsing 'analytics' region:");
            var analyticsClient = client.WithRegion("analytics");
            await analyticsClient.SetAsync("pageviews:today", "15234");
            var pageviews = await analyticsClient.GetAsync("pageviews:today");
            Console.WriteLine($"  pageviews:today = {pageviews}");

            // =================================================================
            // Server Info
            // =================================================================
            Console.WriteLine("\n═══ SERVER INFO ═══\n");

            var info = await client.InfoAsync();
            if (info != null)
            {
                Console.WriteLine($"Version: {info.Version}");
                Console.WriteLine($"Uptime: {info.Uptime} seconds");
                Console.WriteLine($"Total Keys: {info.TotalKeys}");
                Console.WriteLine($"Memory Used: {info.MemoryUsed / 1024 / 1024} MB");
            }

            // =================================================================
            // Cleanup
            // =================================================================
            Console.WriteLine("\n═══ CLEANUP ═══\n");

            Console.WriteLine("Deleting test keys...");
            await client.DeleteAsync("greeting");
            await client.DeleteAsync("session:token");
            await client.DeleteAsync("config:app:name");
            await client.DeleteAsync("config:app:version");
            await client.DeleteAsync("config:app:env");
            await client.DeleteAsync("user:profile:1001");
            await client.DeleteAsync("order:2025-001");
            await analyticsClient.DeleteAsync("pageviews:today");
            Console.WriteLine("  Cleanup completed");

            Console.WriteLine("\n✓ Example completed successfully!");
        }
        catch (HttpRequestException ex)
        {
            Console.WriteLine($"\n✗ HTTP Error: {ex.Message}");
            Console.WriteLine("\nMake sure:");
            Console.WriteLine("  1. Kuber server is running on localhost:8080");
            Console.WriteLine("  2. You have a valid API key from Admin > API Keys");
            Console.WriteLine("  3. API key authentication is enabled");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\n✗ Error: {ex.Message}");
        }
    }
}
