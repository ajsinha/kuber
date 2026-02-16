/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber Comprehensive Cache Operations via Kafka (v2.5.0)
 *
 * Demonstrates every cache operation through Kafka request/response messaging:
 *   - String operations:  SET, GET, DELETE, EXISTS, KEYS, TTL, EXPIRE, MGET, MSET
 *   - JSON operations:    JSET, JGET, JUPDATE, JREMOVE, JSEARCH
 *   - Hash operations:    HSET, HGET, HGETALL
 *   - Admin operations:   PING, INFO, REGIONS
 *
 * Prerequisites:
 *   - dotnet add package Confluent.Kafka
 *   - Kafka running (default: localhost:9092)
 *   - Kuber server running with request/response messaging enabled
 *   - Valid API key from Kuber Admin → API Keys
 */

using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Nodes;
using Confluent.Kafka;

namespace Kuber.Client.Examples;

public class KuberCacheOpsViaKafka
{
    // ── Configuration ─────────────────────────────────────────────────────────
    private const string BootstrapServers = "localhost:9092";
    private const string RequestTopic     = "kuber_cache_request";
    private const string ResponseTopic    = "kuber_cache_response";
    private const string ApiKey           = "YOUR_API_KEY";   // from /admin/apikeys
    private const string Region           = "employees";       // target cache region
    private const int    TimeoutMs        = 10_000;            // response wait timeout

    // ── Internal state ────────────────────────────────────────────────────────
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    private IProducer<string, string>? _producer;
    private IConsumer<string, string>? _consumer;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonNode>> _pending = new();
    private CancellationTokenSource _cts = new();
    private Task? _listenerTask;

    // ══════════════════════════════════════════════════════════════════════════
    //  MAIN
    // ══════════════════════════════════════════════════════════════════════════

    public static async Task Main(string[] args)
    {
        var client = new KuberCacheOpsViaKafka();
        try
        {
            client.Start();

            // ── Admin Operations ──────────────────────────────────────────
            client.Banner("ADMIN OPERATIONS");
            await client.DemoPingAsync();
            await client.DemoInfoAsync();
            await client.DemoRegionsAsync();

            // ── String Operations ─────────────────────────────────────────
            client.Banner("STRING OPERATIONS (SET / GET / DELETE / EXISTS / KEYS / TTL / EXPIRE)");
            await client.DemoStringOpsAsync();

            // ── Batch Operations ──────────────────────────────────────────
            client.Banner("BATCH OPERATIONS (MSET / MGET)");
            await client.DemoBatchOpsAsync();

            // ── JSON Document Operations ──────────────────────────────────
            client.Banner("JSON OPERATIONS (JSET / JGET / JUPDATE / JREMOVE / JSEARCH)");
            await client.DemoJsonOpsAsync();

            // ── Hash Operations ───────────────────────────────────────────
            client.Banner("HASH OPERATIONS (HSET / HGET / HGETALL)");
            await client.DemoHashOpsAsync();

            // ── Cleanup ───────────────────────────────────────────────────
            client.Banner("CLEANUP");
            await client.DemoCleanupAsync();
        }
        finally
        {
            client.Stop();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  ADMIN DEMOS
    // ══════════════════════════════════════════════════════════════════════════

    private async Task DemoPingAsync()
    {
        Section("PING - Health Check");
        PrintResponse(await SendAndWaitAsync("PING"));
    }

    private async Task DemoInfoAsync()
    {
        Section("INFO - Server Information");
        PrintResponse(await SendAndWaitAsync("INFO"));
    }

    private async Task DemoRegionsAsync()
    {
        Section("REGIONS - List All Regions");
        PrintResponse(await SendAndWaitAsync("REGIONS"));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  STRING OPERATION DEMOS
    // ══════════════════════════════════════════════════════════════════════════

    private async Task DemoStringOpsAsync()
    {
        Section("SET - Store string value");
        PrintResponse(await SendAndWaitAsync("SET", Region, "emp:1001",
            new { value = "John Doe - Engineering Manager", ttl = 3600 }));

        Section("GET - Retrieve value");
        PrintResponse(await SendAndWaitAsync("GET", Region, "emp:1001"));

        Section("EXISTS - Check key exists");
        PrintResponse(await SendAndWaitAsync("EXISTS", Region, "emp:1001"));

        Section("TTL - Check remaining TTL");
        PrintResponse(await SendAndWaitAsync("TTL", Region, "emp:1001"));

        Section("EXPIRE - Set new TTL (7200s)");
        PrintResponse(await SendAndWaitAsync("EXPIRE", Region, "emp:1001",
            new { ttl = 7200 }));

        Section("KEYS - Find keys matching 'emp:*'");
        PrintResponse(await SendAndWaitAsync("KEYS", Region, null,
            new { pattern = "emp:*" }));

        Section("DELETE - Remove key");
        PrintResponse(await SendAndWaitAsync("DELETE", Region, "emp:1001"));

        Section("GET - Verify deletion (expect null)");
        PrintResponse(await SendAndWaitAsync("GET", Region, "emp:1001"));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  BATCH OPERATION DEMOS
    // ══════════════════════════════════════════════════════════════════════════

    private async Task DemoBatchOpsAsync()
    {
        Section("MSET - Batch store 3 entries");
        PrintResponse(await SendAndWaitAsync("MSET", Region, null, new
        {
            entries = new Dictionary<string, string>
            {
                ["dept:engineering"] = "Software Engineering",
                ["dept:marketing"]   = "Digital Marketing",
                ["dept:finance"]     = "Corporate Finance"
            }
        }));

        Section("MGET - Batch retrieve 3 entries (+ 1 nonexistent)");
        PrintResponse(await SendAndWaitAsync("MGET", Region, null, new
        {
            keys = new[] { "dept:engineering", "dept:marketing", "dept:finance", "dept:nonexistent" }
        }));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  JSON DOCUMENT OPERATION DEMOS
    // ══════════════════════════════════════════════════════════════════════════

    private async Task DemoJsonOpsAsync()
    {
        Section("JSET - Store employee JSON document (E001)");
        PrintResponse(await SendAndWaitAsync("JSET", Region, "employee/E001", new
        {
            value = new
            {
                name = "Jane Smith",
                email = "jane.smith@acme.com",
                department = "Engineering",
                title = "Senior Software Engineer",
                salary = 145000,
                active = true,
                address = new { city = "San Francisco", state = "CA", zip = "94105" },
                skills = new[] { "Java", "Kafka", "Kubernetes", "PostgreSQL" }
            }
        }));

        Section("JSET - Store second employee (E002)");
        PrintResponse(await SendAndWaitAsync("JSET", Region, "employee/E002", new
        {
            value = new
            {
                name = "Bob Johnson",
                email = "bob.johnson@acme.com",
                department = "Engineering",
                title = "DevOps Engineer",
                salary = 135000,
                active = true,
                address = new { city = "Seattle", state = "WA", zip = "98101" },
                skills = new[] { "Docker", "Kafka", "Terraform", "AWS" }
            }
        }));

        Section("JSET - Store marketing employee (E003)");
        PrintResponse(await SendAndWaitAsync("JSET", Region, "employee/E003", new
        {
            value = new
            {
                name = "Alice Chen",
                email = "alice.chen@acme.com",
                department = "Marketing",
                title = "Marketing Director",
                salary = 160000,
                active = true,
                address = new { city = "New York", state = "NY", zip = "10001" },
                skills = new[] { "Analytics", "SEO", "Content Strategy" }
            }
        }));

        Section("JGET - Retrieve full employee document (E001)");
        PrintResponse(await SendAndWaitAsync("JGET", Region, "employee/E001"));

        Section("JGET - Retrieve only the address (JSONPath: $.address)");
        PrintResponse(await SendAndWaitAsync("JGET", Region, "employee/E001",
            new { path = "$.address" }));

        Section("JUPDATE - Promote employee (update title + salary, add field)");
        PrintResponse(await SendAndWaitAsync("JUPDATE", Region, "employee/E001", new
        {
            value = new
            {
                title = "Staff Software Engineer",
                salary = 175000,
                promotion_date = "2026-02-16"
            }
        }));

        Section("JGET - Verify updated employee");
        PrintResponse(await SendAndWaitAsync("JGET", Region, "employee/E001"));

        Section("JREMOVE - Remove 'promotion_date' attribute");
        PrintResponse(await SendAndWaitAsync("JREMOVE", Region, "employee/E001",
            new { keys = new[] { "promotion_date" } }));

        Section("JSEARCH - Find all Engineering department employees");
        PrintResponse(await SendAndWaitAsync("JSEARCH", Region, null,
            new { query = "$.department == 'Engineering'" }));

        Section("JSEARCH - Find employees in California");
        PrintResponse(await SendAndWaitAsync("JSEARCH", Region, null,
            new { query = "$.address.state == 'CA'" }));

        Section("JSEARCH - Find employees with salary > 150000");
        PrintResponse(await SendAndWaitAsync("JSEARCH", Region, null,
            new { query = "$.salary > 150000" }));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  HASH OPERATION DEMOS
    // ══════════════════════════════════════════════════════════════════════════

    private async Task DemoHashOpsAsync()
    {
        Section("HSET - Set hash fields for config:app");
        var fields = new (string field, string value)[]
        {
            ("version", "2.5.0"),
            ("max_connections", "500"),
            ("log_level", "INFO"),
            ("feature_flags", "{\"dark_mode\":true,\"beta_api\":false}")
        };
        foreach (var (field, value) in fields)
        {
            var resp = await SendAndWaitAsync("HSET", Region, "config:app",
                new { field, value });
            var ok = resp?["success"]?.GetValue<bool>() ?? false;
            Console.WriteLine($"   Set field '{field}': {(ok ? "OK" : "FAIL")}");
        }
        Console.WriteLine();

        Section("HGET - Get 'version' field from config:app");
        PrintResponse(await SendAndWaitAsync("HGET", Region, "config:app",
            new { field = "version" }));

        Section("HGETALL - Get all fields from config:app");
        PrintResponse(await SendAndWaitAsync("HGETALL", Region, "config:app"));
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  CLEANUP
    // ══════════════════════════════════════════════════════════════════════════

    private async Task DemoCleanupAsync()
    {
        Section("DELETE - Clean up all demo keys");
        foreach (var key in new[]
        {
            "emp:1001", "dept:engineering", "dept:marketing", "dept:finance",
            "employee/E001", "employee/E002", "employee/E003", "config:app"
        })
        {
            await SendAndWaitAsync("DELETE", Region, key);
            Console.WriteLine($"   Deleted: {key}");
        }
        Console.WriteLine();
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  KAFKA INFRASTRUCTURE
    // ══════════════════════════════════════════════════════════════════════════

    private void Start()
    {
        Console.WriteLine("╔═══════════════════════════════════════════════════════════════════════╗");
        Console.WriteLine("║  Kuber Cache Operations via Kafka — Comprehensive Demo (v2.5.0)       ║");
        Console.WriteLine($"║  Region:   {Region,-57}  ║");
        Console.WriteLine($"║  Request:  {RequestTopic,-55}  ║");
        Console.WriteLine($"║  Response: {ResponseTopic,-55}  ║");
        Console.WriteLine("╚═══════════════════════════════════════════════════════════════════════╝");
        Console.WriteLine();

        // Create producer
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            Acks = Acks.All
        }).Build();

        // Create consumer
        _consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
            GroupId = $"kuber-csharp-demo-{Guid.NewGuid()}",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true
        }).Build();
        _consumer.Subscribe(ResponseTopic);

        // Start response listener
        _cts = new CancellationTokenSource();
        _listenerTask = Task.Run(() => ResponseListener(_cts.Token));

        Thread.Sleep(2000);
        Console.WriteLine($"✅ Connected to Kafka. Consumer subscribed to: {ResponseTopic}\n");
    }

    private void Stop()
    {
        _cts.Cancel();
        _listenerTask?.Wait(3000);
        _consumer?.Close();
        _producer?.Dispose();
        Console.WriteLine("\n✅ Kafka clients shut down.");
    }

    /// <summary>
    /// Background task polling Kafka for responses and completing pending futures.
    /// </summary>
    private void ResponseListener(CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = _consumer!.Consume(TimeSpan.FromMilliseconds(100));
                if (result == null) continue;

                try
                {
                    var resp = JsonNode.Parse(result.Message.Value);
                    if (resp == null) continue;

                    string? messageId = resp["request"]?["message_id"]?.GetValue<string>()
                                     ?? resp["message_id"]?.GetValue<string>();

                    if (messageId != null && _pending.TryRemove(messageId, out var tcs))
                    {
                        tcs.SetResult(resp);
                    }
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"  Error parsing response: {ex.Message}");
                }
            }
        }
        catch (OperationCanceledException) { /* normal shutdown */ }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  REQUEST BUILDER & SENDER
    // ══════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Build request JSON, publish to Kafka, and wait for the correlated response.
    /// </summary>
    private async Task<JsonNode> SendAndWaitAsync(string operation, string? region = null,
        string? key = null, object? extra = null)
    {
        var messageId = Guid.NewGuid().ToString();

        // Build request as a dictionary, then serialize
        var request = new Dictionary<string, object?>
        {
            ["api_key"]    = ApiKey,
            ["message_id"] = messageId,
            ["operation"]  = operation.ToUpper()
        };
        if (region != null) request["region"] = region;
        if (key != null) request["key"] = key;

        // Merge extra fields using reflection
        if (extra != null)
        {
            foreach (var prop in extra.GetType().GetProperties())
            {
                var name = prop.Name;
                // Convert PascalCase to snake_case for common fields
                request[name] = prop.GetValue(extra);
            }
        }

        var json = JsonSerializer.Serialize(request, JsonOpts);

        // Register pending
        var tcs = new TaskCompletionSource<JsonNode>();
        _pending[messageId] = tcs;

        // Publish
        await _producer!.ProduceAsync(RequestTopic,
            new Message<string, string> { Key = messageId, Value = json });
        _producer.Flush(TimeSpan.FromSeconds(5));

        // Wait for response
        using var cts = new CancellationTokenSource(TimeoutMs);
        cts.Token.Register(() =>
        {
            if (_pending.TryRemove(messageId, out var removed))
                removed.SetException(new TimeoutException(
                    $"Timeout waiting for {operation} (message_id={messageId})"));
        });

        return await tcs.Task;
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  OUTPUT HELPERS
    // ══════════════════════════════════════════════════════════════════════════

    private static void PrintResponse(JsonNode? resp)
    {
        if (resp == null) { Console.WriteLine("   (null response)\n"); return; }

        var success = resp["success"]?.GetValue<bool>() ?? false;
        Console.WriteLine($"   Status: {(success ? "✅ SUCCESS" : "❌ FAIL")}");

        if (resp["result"] != null)
        {
            var pretty = resp["result"]!.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
            foreach (var line in pretty.Split('\n'))
                Console.WriteLine($"   {line}");
        }

        if (resp["error"] != null)
            Console.WriteLine($"   Error: {resp["error"]}");

        Console.WriteLine();
    }

    private void Banner(string title)
    {
        Console.WriteLine($"\n{new string('═', 70)}");
        Console.WriteLine($"  {title}");
        Console.WriteLine($"{new string('═', 70)}\n");
    }

    private static void Section(string title)
    {
        Console.WriteLine($"── {title} ──");
    }
}
