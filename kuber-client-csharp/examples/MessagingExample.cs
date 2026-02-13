/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 * Patent Pending
 *
 * Kuber Distributed Cache - .NET Examples
 * Request/Response Messaging Example
 *
 * This example demonstrates the message format for Kuber's request/response
 * messaging system. In production, send these messages to your configured
 * message broker (Kafka, RabbitMQ, ActiveMQ, or IBM MQ).
 *
 * Dependencies for production use:
 *   - Confluent.Kafka (for Apache Kafka)
 *   - RabbitMQ.Client (for RabbitMQ)
 *   - Apache.NMS.ActiveMQ (for ActiveMQ)
 */

using System.Text.Json;
using Kuber.Client;

namespace Kuber.Client.Examples;

/// <summary>
/// Demonstrates Kuber request/response messaging patterns.
/// </summary>
public static class MessagingExample
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    public static void Run()
    {
        Console.WriteLine("╔══════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║        KUBER REQUEST/RESPONSE MESSAGING - C# CLIENT                 ║");
        Console.WriteLine("║                        Version 1.8.3                                ║");
        Console.WriteLine("╚══════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        var args = Environment.GetCommandLineArgs();
        if (args.Length > 1 && args[1] == "--examples")
        {
            PrintExampleMessages();
        }
        else
        {
            InteractiveDemo();
            Console.WriteLine("\nRun with --examples flag to see detailed message format examples.");
            Console.WriteLine("\nTo use with a real broker:");
            Console.WriteLine("  1. Install broker client NuGet package:");
            Console.WriteLine("     - Confluent.Kafka for Apache Kafka");
            Console.WriteLine("     - RabbitMQ.Client for RabbitMQ");
            Console.WriteLine("  2. Configure your broker in Kuber's request_response.json");
            Console.WriteLine("  3. Get an API key from Kuber admin panel");
            Console.WriteLine("  4. Implement send/receive logic for your broker");
        }
    }

    private static void InteractiveDemo()
    {
        Console.WriteLine("═══ KUBER MESSAGING CLIENT - INTERACTIVE DEMO ═══\n");
        Console.WriteLine("This demo shows how to build requests for various cache operations.");
        Console.WriteLine("In production, you would send these to your message broker.\n");

        var client = new KuberMessagingClient("demo-api-key-12345");

        Console.WriteLine("--- Building Sample Requests ---\n");

        // 1. Store user data
        Console.WriteLine("1. Storing user data with TTL:");
        var setRequest = client.BuildSet("user:john_doe", new
        {
            name = "John Doe",
            email = "john@example.com",
            role = "admin"
        }, ttl: 3600);
        Console.WriteLine($"   Topic: kuber_request");
        Console.WriteLine($"   Message: {client.ToJson(setRequest)}");

        // 2. Batch store configuration
        Console.WriteLine("\n2. Batch storing configuration:");
        var msetRequest = client.BuildMSet(new Dictionary<string, object>
        {
            ["app:config:theme"] = "dark",
            ["app:config:lang"] = "en-US",
            ["app:config:version"] = "2.4.0"
        });
        Console.WriteLine($"   Topic: kuber_request");
        Console.WriteLine($"   Message: {client.ToJson(msetRequest)}");

        // 3. Retrieve multiple users
        Console.WriteLine("\n3. Batch retrieving users:");
        var mgetRequest = client.BuildMGet(new List<string>
        {
            "user:john_doe",
            "user:jane_doe",
            "user:bob_smith"
        });
        Console.WriteLine($"   Topic: kuber_request");
        Console.WriteLine($"   Message: {client.ToJson(mgetRequest)}");

        // 4. Search JSON documents
        Console.WriteLine("\n4. Searching JSON documents:");
        var searchRequest = client.BuildJSearch("category = 'electronics' AND price < 1000");
        Console.WriteLine($"   Topic: kuber_request");
        Console.WriteLine($"   Message: {client.ToJson(searchRequest)}");

        // 5. Health check
        Console.WriteLine("\n5. Health check (PING):");
        var pingRequest = client.BuildPing();
        Console.WriteLine($"   Topic: kuber_request");
        Console.WriteLine($"   Message: {client.ToJson(pingRequest)}");

        Console.WriteLine("\n" + new string('-', 70));
        Console.WriteLine("Response Topic: kuber_response");
        Console.WriteLine("Responses include the original request for correlation via message_id");
        Console.WriteLine(new string('-', 70));
    }

    private static void PrintExampleMessages()
    {
        Console.WriteLine(new string('=', 70));
        Console.WriteLine("KUBER REQUEST/RESPONSE MESSAGING - MESSAGE FORMAT EXAMPLES");
        Console.WriteLine(new string('=', 70));

        var client = new KuberMessagingClient("your-api-key-here");

        // GET operation
        Console.WriteLine("\n--- GET Operation ---");
        var getRequest = client.BuildGet("user:1001");
        Console.WriteLine("Request:");
        Console.WriteLine(client.ToJson(getRequest));
        Console.WriteLine("\nExpected Response:");
        Console.WriteLine(JsonSerializer.Serialize(new
        {
            request_receive_timestamp = "2025-01-15T10:30:00.123Z",
            response_time = "2025-01-15T10:30:00.125Z",
            processing_time_ms = 2,
            request = getRequest,
            response = new
            {
                success = true,
                result = "{\"name\": \"John\", \"email\": \"john@example.com\"}",
                error = ""
            }
        }, JsonOptions));

        // SET operation
        Console.WriteLine("\n--- SET Operation ---");
        var setRequest = client.BuildSet("user:1002", new { name = "Jane", email = "jane@example.com" }, ttl: 3600);
        Console.WriteLine("Request:");
        Console.WriteLine(client.ToJson(setRequest));

        // MGET operation
        Console.WriteLine("\n--- MGET (Batch GET) Operation ---");
        var mgetRequest = client.BuildMGet(new List<string> { "user:1001", "user:1002", "user:1003" });
        Console.WriteLine("Request:");
        Console.WriteLine(client.ToJson(mgetRequest));
        Console.WriteLine("\nExpected Response (key-value pairs format):");
        Console.WriteLine(@"{
  ""response"": {
    ""success"": true,
    ""result"": [
      {""key"": ""user:1001"", ""value"": ""{\""name\"": \""John\""}""},
      {""key"": ""user:1002"", ""value"": ""{\""name\"": \""Jane\""}""},
      {""key"": ""user:1003"", ""value"": null}
    ],
    ""error"": """"
  }
}");

        // MSET operation
        Console.WriteLine("\n--- MSET (Batch SET) Operation ---");
        var msetRequest = client.BuildMSet(new Dictionary<string, object>
        {
            ["config:theme"] = "dark",
            ["config:language"] = "en",
            ["config:timezone"] = "UTC"
        }, ttl: 86400);
        Console.WriteLine("Request:");
        Console.WriteLine(client.ToJson(msetRequest));

        // KEYS operation
        Console.WriteLine("\n--- KEYS Operation ---");
        var keysRequest = client.BuildKeys("user:*");
        Console.WriteLine("Request:");
        Console.WriteLine(client.ToJson(keysRequest));

        // Hash operations
        Console.WriteLine("\n--- Hash Operations ---");
        var hsetRequest = client.BuildHSet("session:abc123", "last_active", "2025-01-15T10:30:00Z");
        Console.WriteLine("HSET Request:");
        Console.WriteLine(client.ToJson(hsetRequest));

        var hmsetRequest = client.BuildHMSet("session:abc123", new Dictionary<string, string>
        {
            ["user_id"] = "1001",
            ["ip_address"] = "192.168.1.100",
            ["user_agent"] = "Mozilla/5.0"
        });
        Console.WriteLine("\nHMSET Request:");
        Console.WriteLine(client.ToJson(hmsetRequest));

        // JSON operations
        Console.WriteLine("\n--- JSON Operations ---");
        var jsetRequest = client.BuildJSet("product:5001", new
        {
            name = "Laptop",
            price = 999.99,
            specs = new
            {
                cpu = "Intel i7",
                ram = "16GB",
                storage = "512GB SSD"
            },
            tags = new[] { "electronics", "computers" }
        });
        Console.WriteLine("JSET Request:");
        Console.WriteLine(client.ToJson(jsetRequest));

        var jgetRequest = client.BuildJGet("product:5001", path: "$.specs.cpu");
        Console.WriteLine("\nJGET with JSONPath Request:");
        Console.WriteLine(client.ToJson(jgetRequest));

        var jsearchRequest = client.BuildJSearch("price > 500 AND tags CONTAINS 'electronics'");
        Console.WriteLine("\nJSEARCH Request:");
        Console.WriteLine(client.ToJson(jsearchRequest));

        // Error response example
        Console.WriteLine("\n--- Error Response Example ---");
        Console.WriteLine(@"{
  ""request_receive_timestamp"": ""2025-01-15T10:30:05.000Z"",
  ""response_time"": ""2025-01-15T10:30:05.001Z"",
  ""processing_time_ms"": 1,
  ""request"": {
    ""api_key"": ""invalid-key"",
    ""message_id"": ""msg-12345"",
    ""operation"": ""GET"",
    ""key"": ""test""
  },
  ""response"": {
    ""success"": false,
    ""result"": null,
    ""error"": ""Invalid or expired API key"",
    ""error_code"": ""AUTHENTICATION_FAILED""
  }
}");

        Console.WriteLine("\n" + new string('=', 70));
        Console.WriteLine("END OF EXAMPLES");
        Console.WriteLine(new string('=', 70));
    }
}
