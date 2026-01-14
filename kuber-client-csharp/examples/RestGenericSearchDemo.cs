/*
 * Kuber REST API - Generic Search Demo (v1.7.9)
 *
 * This standalone C# application demonstrates ALL search capabilities of the Kuber
 * Generic Search API including:
 *
 * 1. Single key lookup
 * 2. Multi-key lookup
 * 3. Single key pattern (regex) search
 * 4. Multi-pattern (regex) search
 * 5. JSON attribute search with:
 *    - Simple equality
 *    - IN clause (list of values)
 *    - Regex matching
 *    - Comparison operators (gt, gte, lt, lte, eq, ne)
 *    - Combined AND logic across multiple attributes
 * 6. Field projection (select specific fields)
 *
 * Usage:
 *     dotnet run [host] [port] [apiKey]
 *     
 * Or compile and run:
 *     csc RestGenericSearchDemo.cs
 *     RestGenericSearchDemo.exe localhost 7070 kub_admin
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
    public class RestGenericSearchDemo
    {
        private readonly HttpClient _httpClient;
        private readonly string _baseUrl;
        private readonly string _apiKey;

        public RestGenericSearchDemo(string host, int port, string apiKey)
        {
            _baseUrl = $"http://{host}:{port}/api/v1";
            _apiKey = apiKey;
            _httpClient = new HttpClient();
            _httpClient.DefaultRequestHeaders.Add("X-API-Key", apiKey);
            _httpClient.Timeout = TimeSpan.FromSeconds(30);
        }

        // ==================== HTTP Helper Methods ====================

        private async Task<string> PostAsync(string endpoint, string jsonBody)
        {
            var content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
            var response = await _httpClient.PostAsync(_baseUrl + endpoint, content);
            return await response.Content.ReadAsStringAsync();
        }

        // ==================== Generic Search ====================

        public async Task<string> GenericSearchAsync(Dictionary<string, object> request)
        {
            request["apiKey"] = _apiKey;
            var json = JsonSerializer.Serialize(request);
            return await PostAsync("/genericsearch", json);
        }

        public async Task PutJsonAsync(string region, string key, Dictionary<string, object> value)
        {
            var json = JsonSerializer.Serialize(value);
            await PostAsync($"/cache/{region}/{key}", json);
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

        private static void PrintRequest(Dictionary<string, object> request)
        {
            var display = new Dictionary<string, object>(request);
            display.Remove("apiKey");
            Console.WriteLine($"Request: {JsonSerializer.Serialize(display)}");
        }

        private static void PrintResult(string result)
        {
            if (string.IsNullOrEmpty(result))
            {
                Console.WriteLine("❌ No response");
                return;
            }

            // Count results
            int count = 0;
            int idx = 0;
            while ((idx = result.IndexOf("\"key\":", idx + 1)) != -1) count++;

            if (result.StartsWith("["))
            {
                Console.WriteLine($"✅ Results: {count}");
                Console.WriteLine($"  {(result.Length > 500 ? result.Substring(0, 500) + "..." : result)}");
            }
            else
            {
                Console.WriteLine($"Response: {result}");
            }
        }

        // ==================== Setup Test Data ====================

        private async Task SetupTestDataAsync(string region)
        {
            PrintHeader("SETUP: Inserting Test Data");
            Console.WriteLine($"\nInserting customers into region '{region}'...");

            var customers = new[]
            {
                new { key = "customer_C001", data = new Dictionary<string, object> { ["id"] = "C001", ["name"] = "John Smith", ["email"] = "john@gmail.com", ["status"] = "active", ["city"] = "NYC", ["tier"] = "premium", ["age"] = 35, ["balance"] = 15000.50 } },
                new { key = "customer_C002", data = new Dictionary<string, object> { ["id"] = "C002", ["name"] = "Jane Doe", ["email"] = "jane@yahoo.com", ["status"] = "active", ["city"] = "LA", ["tier"] = "standard", ["age"] = 28, ["balance"] = 5200.00 } },
                new { key = "customer_C003", data = new Dictionary<string, object> { ["id"] = "C003", ["name"] = "Bob Wilson", ["email"] = "bob@gmail.com", ["status"] = "pending", ["city"] = "NYC", ["tier"] = "premium", ["age"] = 42, ["balance"] = 32000.75 } },
                new { key = "customer_C004", data = new Dictionary<string, object> { ["id"] = "C004", ["name"] = "Alice Brown", ["email"] = "alice@outlook.com", ["status"] = "inactive", ["city"] = "Chicago", ["tier"] = "basic", ["age"] = 31, ["balance"] = 1500.25 } },
                new { key = "customer_C005", data = new Dictionary<string, object> { ["id"] = "C005", ["name"] = "Charlie Davis", ["email"] = "charlie@gmail.com", ["status"] = "active", ["city"] = "Boston", ["tier"] = "premium", ["age"] = 45, ["balance"] = 48000.00 } },
                new { key = "customer_C006", data = new Dictionary<string, object> { ["id"] = "C006", ["name"] = "Eva Martinez", ["email"] = "eva@yahoo.com", ["status"] = "pending", ["city"] = "Miami", ["tier"] = "standard", ["age"] = 29, ["balance"] = 7800.50 } },
                new { key = "customer_C007", data = new Dictionary<string, object> { ["id"] = "C007", ["name"] = "Frank Johnson", ["email"] = "frank@company.com", ["status"] = "active", ["city"] = "NYC", ["tier"] = "enterprise", ["age"] = 52, ["balance"] = 125000.00 } },
                new { key = "customer_C008", data = new Dictionary<string, object> { ["id"] = "C008", ["name"] = "Grace Lee", ["email"] = "grace@gmail.com", ["status"] = "trial", ["city"] = "Seattle", ["tier"] = "basic", ["age"] = 24, ["balance"] = 500.00 } },
            };

            foreach (var c in customers)
            {
                await PutJsonAsync(region, c.key, c.data);
                Console.WriteLine($"  ✓ {c.key}");
            }

            Console.WriteLine("\nInserting orders...");
            var orders = new[]
            {
                new { key = "order_ORD001", data = new Dictionary<string, object> { ["orderId"] = "ORD001", ["customerId"] = "C001", ["status"] = "shipped", ["region"] = "US", ["priority"] = "high", ["amount"] = 450.00, ["items"] = 3 } },
                new { key = "order_ORD002", data = new Dictionary<string, object> { ["orderId"] = "ORD002", ["customerId"] = "C002", ["status"] = "delivered", ["region"] = "US", ["priority"] = "normal", ["amount"] = 125.50, ["items"] = 1 } },
                new { key = "order_ORD003", data = new Dictionary<string, object> { ["orderId"] = "ORD003", ["customerId"] = "C003", ["status"] = "pending", ["region"] = "CA", ["priority"] = "urgent", ["amount"] = 890.00, ["items"] = 5 } },
            };

            foreach (var o in orders)
            {
                await PutJsonAsync(region, o.key, o.data);
                Console.WriteLine($"  ✓ {o.key}");
            }

            Console.WriteLine("\n✅ Test data setup complete!");
        }

        // ==================== Demo Methods ====================

        private async Task DemoKeyLookupsAsync(string region)
        {
            PrintHeader("1. KEY-BASED LOOKUPS");

            // 1a. Single key lookup
            PrintSubheader("1a. Single Key Lookup");
            var req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["key"] = "customer_C001"
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 1b. Multi-key lookup
            PrintSubheader("1b. Multi-Key Lookup (3 keys)");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["keys"] = new[] { "customer_C001", "customer_C003", "customer_C005" }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 1c. Multi-key lookup with field projection
            PrintSubheader("1c. Multi-Key Lookup with Field Projection");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["keys"] = new[] { "customer_C001", "customer_C002", "customer_C003" },
                ["fields"] = new[] { "name", "email", "status" }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));
        }

        private async Task DemoPatternSearchesAsync(string region)
        {
            PrintHeader("2. REGEX PATTERN SEARCHES");

            // 2a. Single pattern - All customers
            PrintSubheader("2a. Single Pattern - All customers");
            var req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["keypattern"] = "customer_.*"
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 2b. Single pattern - Orders only
            PrintSubheader("2b. Single Pattern - Orders only");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["keypattern"] = "order_.*"
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 2c. Multi-pattern search
            PrintSubheader("2c. Multi-Pattern - Customers C001-C003 OR Orders");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["keypatterns"] = new[] { "customer_C00[1-3]", "order_.*" }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));
        }

        private async Task DemoJsonEqualitySearchAsync(string region)
        {
            PrintHeader("3. JSON SEARCH - EQUALITY");

            // 3a. Single attribute equality
            PrintSubheader("3a. Single Attribute - status = 'active'");
            var req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object> { ["status"] = "active" }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 3b. Multiple attributes (AND logic)
            PrintSubheader("3b. Multiple Attributes (AND) - status='active' AND city='NYC'");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object> { ["status"] = "active", ["city"] = "NYC" }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 3c. Three attributes (AND logic)
            PrintSubheader("3c. Three Attributes (AND) - status='active' AND city='NYC' AND tier='premium'");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object> { ["status"] = "active", ["city"] = "NYC", ["tier"] = "premium" }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));
        }

        private async Task DemoJsonInClauseAsync(string region)
        {
            PrintHeader("4. JSON SEARCH - IN CLAUSE (List of Values)");

            // 4a. Single attribute with list
            PrintSubheader("4a. Single IN Clause - status IN ['active', 'pending']");
            var req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object> { ["status"] = new[] { "active", "pending" } }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 4b. Single attribute with longer list
            PrintSubheader("4b. Single IN Clause - tier IN ['premium', 'enterprise']");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object> { ["tier"] = new[] { "premium", "enterprise" } }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 4c. Multiple IN clauses
            PrintSubheader("4c. Multiple IN Clauses - status IN [...] AND city IN [...]");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object>
                {
                    ["status"] = new[] { "active", "pending" },
                    ["city"] = new[] { "NYC", "LA", "SF" }
                }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 4d. IN clause combined with equality
            PrintSubheader("4d. IN Clause + Equality - tier IN [...] AND status='active'");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object>
                {
                    ["tier"] = new[] { "premium", "enterprise" },
                    ["status"] = "active"
                }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));
        }

        private async Task DemoJsonRegexSearchAsync(string region)
        {
            PrintHeader("5. JSON SEARCH - REGEX MATCHING");

            // 5a. Email regex - Gmail users
            PrintSubheader("5a. Regex - Gmail users (email matches '.*@gmail.com')");
            var req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object>
                {
                    ["email"] = new Dictionary<string, object> { ["regex"] = ".*@gmail\\.com" }
                }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 5b. Regex with other conditions
            PrintSubheader("5b. Regex + Equality - Gmail users who are active");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object>
                {
                    ["email"] = new Dictionary<string, object> { ["regex"] = ".*@gmail\\.com" },
                    ["status"] = "active"
                }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 5c. Name pattern regex
            PrintSubheader("5c. Regex - Names starting with 'J'");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object>
                {
                    ["name"] = new Dictionary<string, object> { ["regex"] = "^J.*" }
                }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));
        }

        private async Task DemoJsonComparisonOperatorsAsync(string region)
        {
            PrintHeader("6. JSON SEARCH - COMPARISON OPERATORS");

            // 6a. Greater than
            PrintSubheader("6a. Greater Than - balance > 20000");
            var req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object>
                {
                    ["balance"] = new Dictionary<string, object> { ["gt"] = 20000 }
                }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 6b. Less than or equal
            PrintSubheader("6b. Less Than or Equal - age <= 30");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object>
                {
                    ["age"] = new Dictionary<string, object> { ["lte"] = 30 }
                }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 6c. Range query (between)
            PrintSubheader("6c. Range Query - age >= 30 AND age <= 45");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object>
                {
                    ["age"] = new Dictionary<string, object> { ["gte"] = 30, ["lte"] = 45 }
                }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 6d. Comparison + IN clause
            PrintSubheader("6d. Comparison + IN Clause - balance > 10000 AND status IN ['active', 'pending']");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object>
                {
                    ["balance"] = new Dictionary<string, object> { ["gt"] = 10000 },
                    ["status"] = new[] { "active", "pending" }
                }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));
        }

        private async Task DemoComplexSearchesAsync(string region)
        {
            PrintHeader("7. COMPLEX COMBINED SEARCHES");

            // 7a. All operators combined
            PrintSubheader("7a. All Operators Combined");
            Console.WriteLine("    - status IN ['active', 'pending']");
            Console.WriteLine("    - city IN ['NYC', 'LA', 'SF']");
            Console.WriteLine("    - tier = 'premium'");
            Console.WriteLine("    - balance > 10000");
            var req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object>
                {
                    ["status"] = new[] { "active", "pending" },
                    ["city"] = new[] { "NYC", "LA", "SF" },
                    ["tier"] = "premium",
                    ["balance"] = new Dictionary<string, object> { ["gt"] = 10000 }
                }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 7b. Orders search - complex
            PrintSubheader("7b. Orders Search - Multiple Conditions");
            Console.WriteLine("    - status IN ['shipped', 'delivered']");
            Console.WriteLine("    - region IN ['US', 'CA']");
            Console.WriteLine("    - amount > 200");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object>
                {
                    ["status"] = new[] { "shipped", "delivered" },
                    ["region"] = new[] { "US", "CA" },
                    ["amount"] = new Dictionary<string, object> { ["gt"] = 200 }
                }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));
        }

        private async Task DemoFieldProjectionAsync(string region)
        {
            PrintHeader("8. FIELD PROJECTION (Select Specific Fields)");

            // 8a. Key lookup with projection
            PrintSubheader("8a. Key Lookup - Return only name, email, status");
            var req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["key"] = "customer_C001",
                ["fields"] = new[] { "name", "email", "status" }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 8b. JSON search with projection
            PrintSubheader("8b. JSON Search with Projection - Active users, return name and balance only");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object> { ["status"] = "active" },
                ["fields"] = new[] { "name", "balance" }
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));
        }

        private async Task DemoLimitResultsAsync(string region)
        {
            PrintHeader("9. LIMITING RESULTS");

            // 9a. Limit to 3 results
            PrintSubheader("9a. All Customers, Limit 3");
            var req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["keypattern"] = "customer_.*",
                ["limit"] = 3
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));

            // 9b. JSON search with limit
            PrintSubheader("9b. Active Users, Limit 2");
            req = new Dictionary<string, object>
            {
                ["region"] = region,
                ["type"] = "json",
                ["attributes"] = new Dictionary<string, object> { ["status"] = "active" },
                ["limit"] = 2
            };
            PrintRequest(req);
            PrintResult(await GenericSearchAsync(req));
        }

        // ==================== Main ====================

        public async Task RunDemoAsync(string region)
        {
            try
            {
                await SetupTestDataAsync(region);
                await DemoKeyLookupsAsync(region);
                await DemoPatternSearchesAsync(region);
                await DemoJsonEqualitySearchAsync(region);
                await DemoJsonInClauseAsync(region);
                await DemoJsonRegexSearchAsync(region);
                await DemoJsonComparisonOperatorsAsync(region);
                await DemoComplexSearchesAsync(region);
                await DemoFieldProjectionAsync(region);
                await DemoLimitResultsAsync(region);

                PrintHeader("DEMO COMPLETE");
                Console.WriteLine(@"
This demo showcased all Generic Search API capabilities:

  1. KEY LOOKUPS
     - Single key: {""key"": ""abc""}
     - Multi-key: {""keys"": [""a"", ""b"", ""c""]}

  2. PATTERN SEARCHES (Regex)
     - Single pattern: {""keypattern"": ""user_.*""}
     - Multi-pattern: {""keypatterns"": [""user_.*"", ""admin_.*""]}

  3. JSON ATTRIBUTE SEARCH
     - Equality: {""attributes"": {""status"": ""active""}}
     - IN clause: {""attributes"": {""status"": [""active"", ""pending""]}}
     - Regex: {""attributes"": {""email"": {""regex"": "".*@gmail.com""}}}
     - Comparisons: {""attributes"": {""age"": {""gt"": 25, ""lte"": 50}}}

  4. COMBINING CRITERIA (AND logic)
     - All criteria in ""attributes"" are ANDed together
     - Each list value within an attribute uses OR (IN clause)

  5. FIELD PROJECTION
     - Return specific fields: {""fields"": [""name"", ""email""]}

  6. LIMITING RESULTS
     - Max results: {""limit"": 100}

API Endpoint: POST /api/v1/genericsearch
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
            string region = args.Length > 3 ? args[3] : "search_demo_csharp";

            Console.WriteLine(new string('=', 80));
            Console.WriteLine("  KUBER REST API - GENERIC SEARCH DEMO (v1.7.9) - C#");
            Console.WriteLine(new string('=', 80));
            Console.WriteLine($"  Server:  {host}:{port}");
            Console.WriteLine($"  Region:  {region}");
            Console.WriteLine($"  Time:    {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            Console.WriteLine(new string('=', 80));

            var demo = new RestGenericSearchDemo(host, port, apiKey);
            await demo.RunDemoAsync(region);
        }
    }
}
