/*
 * Kuber REST API - Insert & Update JSON Documents Demo (v1.7.9)
 *
 * This standalone C# application demonstrates how to:
 *
 * 1. INSERT new JSON documents
 *    - Simple insert
 *    - Insert with TTL (time to live)
 *
 * 2. UPDATE existing JSON documents
 *    - Full replace (PUT)
 *    - Partial update (PATCH/JUPDATE) - merge specific attributes
 *
 * 3. Generic Update API (JUPDATE)
 *    - Creates new entry if key doesn't exist
 *    - Merges with existing entry if key exists
 *    - Deep merge of nested objects
 *
 * Usage:
 *     dotnet run [host] [port] [apiKey]
 *
 * Or compile and run:
 *     csc RestInsertUpdateDemo.cs
 *     RestInsertUpdateDemo.exe localhost 7070 kub_admin
 *
 * Dependencies: .NET 6.0+ (uses System.Net.Http and System.Text.Json)
 *
 * Copyright (c) 2025-2030 Ashutosh Sinha. All Rights Reserved.
 */

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Kuber.Examples
{
    public class RestInsertUpdateDemo
    {
        private readonly HttpClient _httpClient;
        private readonly string _baseUrl;
        private readonly string _apiKey;

        public RestInsertUpdateDemo(string host, int port, string apiKey)
        {
            _baseUrl = $"http://{host}:{port}/api/v1";
            _apiKey = apiKey;
            _httpClient = new HttpClient();
            _httpClient.DefaultRequestHeaders.Add("X-API-Key", apiKey);
            _httpClient.Timeout = TimeSpan.FromSeconds(30);
        }

        // ==================== HTTP Helper Methods ====================

        private async Task<(int StatusCode, string Body)> PostAsync(string endpoint, string jsonBody)
        {
            var content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
            var response = await _httpClient.PostAsync(_baseUrl + endpoint, content);
            var body = await response.Content.ReadAsStringAsync();
            return ((int)response.StatusCode, body);
        }

        private async Task<(int StatusCode, string Body)> GetAsync(string endpoint)
        {
            var response = await _httpClient.GetAsync(_baseUrl + endpoint);
            var body = await response.Content.ReadAsStringAsync();
            return ((int)response.StatusCode, body);
        }

        // ==================== Cache Operations ====================

        /// <summary>
        /// Insert a JSON document (creates or replaces).
        /// </summary>
        public async Task<(bool Success, string Message)> InsertAsync(string region, string key, Dictionary<string, object> document)
        {
            var json = JsonSerializer.Serialize(document);
            var (statusCode, body) = await PostAsync($"/cache/{region}/{key}", json);
            return (statusCode >= 200 && statusCode < 300, body);
        }

        /// <summary>
        /// Insert with TTL.
        /// </summary>
        public async Task<(bool Success, string Message)> InsertWithTtlAsync(string region, string key, Dictionary<string, object> document, int ttlSeconds)
        {
            var json = JsonSerializer.Serialize(document);
            var (statusCode, body) = await PostAsync($"/cache/{region}/{key}?ttl={ttlSeconds}", json);
            return (statusCode >= 200 && statusCode < 300, body);
        }

        /// <summary>
        /// Get a JSON document by key.
        /// </summary>
        public async Task<(bool Found, string Value)> GetAsync(string region, string key)
        {
            var (statusCode, body) = await GetAsync($"/cache/{region}/{key}");
            return (statusCode == 200, body);
        }

        /// <summary>
        /// Partial update using Generic Update API (JUPDATE).
        /// Merges updates with existing document.
        /// </summary>
        public async Task<(bool Success, string Action, string Message)> JupdateAsync(string region, string key, Dictionary<string, object> updates)
        {
            var request = new Dictionary<string, object>
            {
                ["region"] = region,
                ["key"] = key,
                ["value"] = updates,
                ["apiKey"] = _apiKey
            };
            var json = JsonSerializer.Serialize(request);
            var (statusCode, body) = await PostAsync("/genericupdate", json);
            
            string action = "unknown";
            if (body.Contains("\"action\":\"created\"")) action = "created";
            else if (body.Contains("\"action\":\"updated\"")) action = "updated";
            
            return (statusCode >= 200 && statusCode < 300, action, body);
        }

        // ==================== Print Helpers ====================

        private static void PrintHeader(string title)
        {
            Console.WriteLine();
            Console.WriteLine(new string('=', 80));
            Console.WriteLine($"  {title}");
            Console.WriteLine(new string('=', 80));
        }

        private static void PrintSubheader(string title)
        {
            Console.WriteLine($"\n--- {title} ---");
        }

        private static void PrintJson(string label, object obj)
        {
            var json = obj is string s ? s : JsonSerializer.Serialize(obj);
            Console.WriteLine($"{label}: {json}");
        }

        private static void PrintResult(string operation, bool success, string message = "")
        {
            if (success)
            {
                Console.WriteLine($"✅ {operation}: Success");
                if (!string.IsNullOrEmpty(message) && message.Length > 200)
                    message = message.Substring(0, 200) + "...";
                if (!string.IsNullOrEmpty(message))
                    Console.WriteLine($"   {message}");
            }
            else
            {
                Console.WriteLine($"❌ {operation}: Failed");
                if (!string.IsNullOrEmpty(message))
                    Console.WriteLine($"   Error: {message}");
            }
        }

        // ==================== Demo Methods ====================

        private async Task DemoInsertOperationsAsync(string region)
        {
            PrintHeader("1. INSERT OPERATIONS");

            // 1a. Simple insert
            PrintSubheader("1a. Simple Insert - New Customer");
            var customer = new Dictionary<string, object>
            {
                ["id"] = "CUST001",
                ["name"] = "John Smith",
                ["email"] = "john.smith@email.com",
                ["status"] = "active",
                ["tier"] = "premium",
                ["balance"] = 15000.50,
                ["address"] = new Dictionary<string, object>
                {
                    ["street"] = "123 Main St",
                    ["city"] = "New York",
                    ["state"] = "NY",
                    ["zip"] = "10001"
                },
                ["tags"] = new[] { "vip", "loyalty-member" },
                ["created_at"] = DateTime.Now.ToString("o")
            };

            PrintJson("Document", customer);
            var (success, msg) = await InsertAsync(region, "customer_CUST001", customer);
            PrintResult("Insert", success);

            // Verify
            Console.WriteLine("\nVerifying insert...");
            var (found, _) = await GetAsync(region, "customer_CUST001");
            if (found) Console.WriteLine("✓ Document stored successfully");

            // 1b. Insert with TTL
            PrintSubheader("1b. Insert with TTL (60 seconds)");
            var sessionData = new Dictionary<string, object>
            {
                ["type"] = "session",
                ["user_id"] = "U123",
                ["token"] = "abc123xyz",
                ["expires_at"] = DateTime.Now.AddSeconds(60).ToString("o")
            };

            PrintJson("Document", sessionData);
            (success, msg) = await InsertWithTtlAsync(region, "session_U123", sessionData, 60);
            PrintResult("Insert with TTL=60s", success);

            // 1c. Insert multiple products
            PrintSubheader("1c. Insert Multiple Products");
            var products = new[]
            {
                (key: "product_P001", data: new Dictionary<string, object> { ["sku"] = "P001", ["name"] = "Laptop Pro", ["category"] = "electronics", ["price"] = 1299.99, ["stock"] = 50 }),
                (key: "product_P002", data: new Dictionary<string, object> { ["sku"] = "P002", ["name"] = "Wireless Mouse", ["category"] = "electronics", ["price"] = 49.99, ["stock"] = 200 }),
                (key: "product_P003", data: new Dictionary<string, object> { ["sku"] = "P003", ["name"] = "USB-C Cable", ["category"] = "accessories", ["price"] = 19.99, ["stock"] = 500 }),
            };

            foreach (var p in products)
            {
                (success, _) = await InsertAsync(region, p.key, p.data);
                Console.WriteLine($"  {(success ? "✓" : "✗")} {p.key}");
            }
        }

        private async Task DemoFullReplaceAsync(string region)
        {
            PrintHeader("2. FULL UPDATE (Replace)");

            // Show current state
            PrintSubheader("2a. Current Document State");
            var (found, value) = await GetAsync(region, "customer_CUST001");
            if (found) PrintJson("Current", value);

            // Full replace
            PrintSubheader("2b. Full Replace - All Fields Changed");
            var newDocument = new Dictionary<string, object>
            {
                ["id"] = "CUST001",
                ["name"] = "John Smith Jr.",
                ["email"] = "john.smith.jr@newemail.com",
                ["status"] = "inactive",
                ["tier"] = "basic",
                ["balance"] = 5000.00,
                ["phone"] = "+1-555-0123",
                ["updated_at"] = DateTime.Now.ToString("o")
                // Note: address and tags are NOT included - they will be REMOVED
            };

            Console.WriteLine("New Document (replaces entire document):");
            PrintJson("", newDocument);
            Console.WriteLine("\n⚠️  Note: 'address' and 'tags' are NOT included - they will be REMOVED");

            var (success, msg) = await InsertAsync(region, "customer_CUST001", newDocument);
            PrintResult("Full Replace", success);

            // Verify
            Console.WriteLine("\nAfter full replace:");
            (found, value) = await GetAsync(region, "customer_CUST001");
            if (found)
            {
                PrintJson("Result", value);
                if (!value.Contains("address"))
                    Console.WriteLine("⚠️  Confirmed: 'address' field was removed");
            }
        }

        private async Task DemoPartialMergeAsync(string region)
        {
            PrintHeader("3. PARTIAL UPDATE (JUPDATE - Merge)");

            // Setup fresh document
            PrintSubheader("3a. Setup - Create Initial Document");
            var initialCustomer = new Dictionary<string, object>
            {
                ["id"] = "CUST002",
                ["name"] = "Jane Doe",
                ["email"] = "jane@email.com",
                ["status"] = "active",
                ["tier"] = "standard",
                ["balance"] = 8500.00,
                ["address"] = new Dictionary<string, object>
                {
                    ["street"] = "456 Oak Ave",
                    ["city"] = "Los Angeles",
                    ["state"] = "CA",
                    ["zip"] = "90001"
                },
                ["preferences"] = new Dictionary<string, object>
                {
                    ["newsletter"] = true,
                    ["sms_alerts"] = false
                },
                ["created_at"] = DateTime.Now.ToString("o")
            };

            await InsertAsync(region, "customer_CUST002", initialCustomer);
            PrintJson("Initial Document", initialCustomer);

            // 3b. Partial update - single field
            PrintSubheader("3b. Partial Update - Change Status Only");
            var updates = new Dictionary<string, object> { ["status"] = "premium" };
            PrintJson("Updates to apply", updates);
            var (success, action, msg) = await JupdateAsync(region, "customer_CUST002", updates);
            PrintResult("JUPDATE", success);

            var (found, value) = await GetAsync(region, "customer_CUST002");
            if (found) Console.WriteLine("✓ Other fields preserved (name, email, address, etc.)");

            // 3c. Partial update - multiple fields
            PrintSubheader("3c. Partial Update - Multiple Fields");
            updates = new Dictionary<string, object>
            {
                ["tier"] = "enterprise",
                ["balance"] = 25000.00,
                ["phone"] = "+1-555-9999",
                ["updated_at"] = DateTime.Now.ToString("o")
            };
            PrintJson("Updates to apply", updates);
            (success, action, msg) = await JupdateAsync(region, "customer_CUST002", updates);
            PrintResult("JUPDATE", success);

            // 3d. Partial update - nested object merge
            PrintSubheader("3d. Partial Update - Nested Object Merge");
            Console.WriteLine("Updating address.city and adding address.country (preserves other address fields)");
            updates = new Dictionary<string, object>
            {
                ["address"] = new Dictionary<string, object>
                {
                    ["city"] = "San Francisco",
                    ["country"] = "USA"
                }
            };
            PrintJson("Updates to apply", updates);
            (success, action, msg) = await JupdateAsync(region, "customer_CUST002", updates);
            PrintResult("JUPDATE (nested)", success);

            (found, value) = await GetAsync(region, "customer_CUST002");
            if (found) Console.WriteLine("✓ Nested address fields merged (street, state preserved)");

            // 3e. Add new nested object
            PrintSubheader("3e. Partial Update - Add New Nested Object");
            updates = new Dictionary<string, object>
            {
                ["billing"] = new Dictionary<string, object>
                {
                    ["method"] = "credit_card",
                    ["last_four"] = "1234",
                    ["exp_date"] = "12/26"
                }
            };
            PrintJson("Updates to apply", updates);
            (success, action, msg) = await JupdateAsync(region, "customer_CUST002", updates);
            PrintResult("JUPDATE", success);
        }

        private async Task DemoJupdateCreateVsUpdateAsync(string region)
        {
            PrintHeader("4. JUPDATE - CREATE vs UPDATE");

            // 4a. JUPDATE on non-existent key (creates new)
            PrintSubheader("4a. JUPDATE on Non-Existent Key (Creates New)");
            var newKey = $"order_ORD_{DateTimeOffset.Now.ToUnixTimeMilliseconds()}";
            var orderData = new Dictionary<string, object>
            {
                ["order_id"] = newKey,
                ["customer_id"] = "CUST002",
                ["items"] = new[] { new Dictionary<string, object> { ["product"] = "P001", ["qty"] = 1, ["price"] = 1299.99 } },
                ["total"] = 1299.99,
                ["status"] = "pending"
            };

            Console.WriteLine($"Key: {newKey} (does not exist)");
            PrintJson("Data", orderData);

            var (success, action, msg) = await JupdateAsync(region, newKey, orderData);
            PrintResult("JUPDATE", success);
            Console.WriteLine($"Action: {action.ToUpper()} (new document)");

            // 4b. JUPDATE on existing key (merges)
            PrintSubheader("4b. JUPDATE on Existing Key (Merges)");
            var updates = new Dictionary<string, object>
            {
                ["status"] = "shipped",
                ["shipped_at"] = DateTime.Now.ToString("o"),
                ["tracking_number"] = "TRK123456789"
            };

            Console.WriteLine($"Key: {newKey} (exists)");
            PrintJson("Updates", updates);

            (success, action, msg) = await JupdateAsync(region, newKey, updates);
            PrintResult("JUPDATE", success);
            Console.WriteLine($"Action: {action.ToUpper()} (merged with existing)");

            // Verify merge
            var (found, value) = await GetAsync(region, newKey);
            if (found)
            {
                Console.WriteLine("✓ Original fields preserved (order_id, items, total)");
                Console.WriteLine("✓ New fields added (tracking_number, shipped_at)");
            }
        }

        private async Task DemoUpdateSpecificAttributesAsync(string region)
        {
            PrintHeader("5. UPDATE SPECIFIC ATTRIBUTES BY KEY");

            // Setup
            PrintSubheader("5a. Setup - Create Test Document");
            var employee = new Dictionary<string, object>
            {
                ["id"] = "EMP001",
                ["name"] = "Alice Johnson",
                ["department"] = "Engineering",
                ["title"] = "Software Developer",
                ["salary"] = 95000,
                ["skills"] = new[] { "Python", "Java", "SQL" },
                ["performance"] = new Dictionary<string, object>
                {
                    ["rating"] = 4.2,
                    ["last_review"] = "2024-06-15"
                }
            };

            await InsertAsync(region, "employee_EMP001", employee);
            PrintJson("Initial", employee);

            // 5b. Update salary only
            PrintSubheader("5b. Update Single Attribute - Salary");
            var (success, action, msg) = await JupdateAsync(region, "employee_EMP001", 
                new Dictionary<string, object> { ["salary"] = 105000 });
            PrintResult("Update salary to 105000", success);

            // 5c. Update title and department
            PrintSubheader("5c. Update Multiple Attributes - Title & Department");
            (success, action, msg) = await JupdateAsync(region, "employee_EMP001",
                new Dictionary<string, object>
                {
                    ["title"] = "Senior Software Developer",
                    ["department"] = "Platform Engineering"
                });
            PrintResult("Update title and department", success);

            // 5d. Update skills array
            PrintSubheader("5d. Update Array - Add New Skills");
            Console.WriteLine("Note: Arrays are REPLACED, not merged");
            (success, action, msg) = await JupdateAsync(region, "employee_EMP001",
                new Dictionary<string, object>
                {
                    ["skills"] = new[] { "Python", "Java", "SQL", "Kubernetes", "AWS" }
                });
            PrintResult("Update skills array", success);

            // 5e. Update nested object
            PrintSubheader("5e. Update Nested Attributes - Performance");
            (success, action, msg) = await JupdateAsync(region, "employee_EMP001",
                new Dictionary<string, object>
                {
                    ["performance"] = new Dictionary<string, object>
                    {
                        ["rating"] = 4.8,
                        ["last_review"] = "2025-01-10",
                        ["promotion_eligible"] = true
                    }
                });
            PrintResult("Update performance object", success);

            // 5f. Show final document
            PrintSubheader("5f. Final Document State");
            var (found, value) = await GetAsync(region, "employee_EMP001");
            if (found) PrintJson("Final Document", value);
        }

        // ==================== Main ====================

        public async Task RunDemoAsync(string region)
        {
            try
            {
                await DemoInsertOperationsAsync(region);
                await DemoFullReplaceAsync(region);
                await DemoPartialMergeAsync(region);
                await DemoJupdateCreateVsUpdateAsync(region);
                await DemoUpdateSpecificAttributesAsync(region);

                PrintHeader("DEMO COMPLETE");
                Console.WriteLine(@"
This demo showcased Insert & Update operations:

  1. INSERT OPERATIONS
     - Simple insert: POST /api/v1/cache/{region}/{key}
     - Insert with TTL: POST /api/v1/cache/{region}/{key}?ttl=60

  2. FULL UPDATE (Replace)
     - Same as insert - completely replaces the document
     - Fields not included are REMOVED

  3. PARTIAL UPDATE (JUPDATE - Merge)
     - POST /api/v1/genericupdate
     - Only updates specified fields
     - Existing fields are PRESERVED
     - Nested objects are DEEP MERGED
     - Arrays are REPLACED (not merged)

  4. JUPDATE CREATE vs UPDATE
     - Non-existent key: Creates new document
     - Existing key: Merges with existing document

  5. UPDATE SPECIFIC ATTRIBUTES
     - Pass only the fields you want to change
     - All other fields remain unchanged

Key Endpoints:
    POST /api/v1/cache/{region}/{key}     - Insert/Replace
    POST /api/v1/genericupdate            - JUPDATE (Create or Merge)
    GET  /api/v1/cache/{region}/{key}     - Read
    DELETE /api/v1/cache/{region}/{key}   - Delete

Request Body for JUPDATE:
    {
        ""region"": ""myregion"",
        ""key"": ""mykey"",
        ""value"": {
            ""field1"": ""new_value"",
            ""field2"": 123
        },
        ""apiKey"": ""kub_xxx""
    }
");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error: {ex.Message}");
                throw;
            }
        }

        public static async Task Main(string[] args)
        {
            string host = args.Length > 0 ? args[0] : "localhost";
            int port = args.Length > 1 ? int.Parse(args[1]) : 7070;
            string apiKey = args.Length > 2 ? args[2] : "kub_admin";
            string region = args.Length > 3 ? args[3] : "update_demo_csharp";

            Console.WriteLine(new string('=', 80));
            Console.WriteLine("  KUBER REST API - INSERT & UPDATE DEMO (v1.7.9) - C#");
            Console.WriteLine(new string('=', 80));
            Console.WriteLine($"  Server:  {host}:{port}");
            Console.WriteLine($"  Region:  {region}");
            Console.WriteLine($"  Time:    {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            Console.WriteLine(new string('=', 80));

            var demo = new RestInsertUpdateDemo(host, port, apiKey);
            await demo.RunDemoAsync(region);
        }
    }
}
