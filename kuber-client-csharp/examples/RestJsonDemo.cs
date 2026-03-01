/*
 * Kuber REST API - JSON Operations Demo (v2.6.2)
 *
 * This standalone C# application demonstrates all JSON-related operations
 * using the Kuber REST API (HTTP/JSON protocol):
 *
 *   a) Store JSON documents           PUT  /api/v1/json/{region}/{key}
 *   b) Retrieve JSON documents        GET  /api/v1/json/{region}/{key}
 *   c) Retrieve at JSONPath           GET  with ?path=$.field
 *   d) Update JSON documents          PUT  (full replace)
 *   e-j) Search with operators        POST /api/v1/json/{region}/search
 *       - Equality, Comparison, Inequality, Pattern, Contains, AND
 *   k) Store with TTL                 PUT  with ttl parameter
 *   l) Cross-region operations        Multiple regions
 *   m) Delete JSON documents          DELETE /api/v1/json/{region}/{key}
 *
 * Usage:
 *     dotnet run [host] [port] [apiKey]
 *
 * Or compile and run:
 *     csc RestJsonDemo.cs
 *     RestJsonDemo.exe localhost 8080 kub_admin_sample_key_replace_me
 *
 * Dependencies: .NET 6.0+ (uses System.Net.Http and System.Text.Json)
 *
 * Copyright (c) 2025-2030 Ashutosh Sinha. All Rights Reserved.
 *
 * @version 2.6.2
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Kuber.Examples
{
    public class RestJsonDemo
    {
        private readonly HttpClient _httpClient;
        private readonly string _baseUrl;
        private readonly string _apiKey;

        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        public RestJsonDemo(string host, int port, string apiKey)
        {
            _baseUrl = $"http://{host}:{port}/api/v1";
            _apiKey = apiKey;
            _httpClient = new HttpClient();
            _httpClient.DefaultRequestHeaders.Add("X-API-Key", apiKey);
            _httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
            _httpClient.Timeout = TimeSpan.FromSeconds(30);
        }

        // ==================== HTTP Helper Methods ====================

        private async Task<(int StatusCode, string Body)> RequestAsync(
            string method, string endpoint, string? jsonBody = null)
        {
            var request = new HttpRequestMessage(
                new HttpMethod(method), _baseUrl + endpoint);

            if (jsonBody != null)
            {
                request.Content = new StringContent(jsonBody, Encoding.UTF8, "application/json");
            }

            var response = await _httpClient.SendAsync(request);
            var body = await response.Content.ReadAsStringAsync();
            return ((int)response.StatusCode, body);
        }

        // ==================== Server Operations ====================

        public async Task<bool> PingAsync()
        {
            var (code, body) = await RequestAsync("GET", "/ping");
            return code >= 200 && code < 300 && body.Contains("\"status\":\"OK\"");
        }

        public async Task<string> ServerVersionAsync()
        {
            var (code, body) = await RequestAsync("GET", "/info");
            if (code >= 200 && code < 300)
                return ExtractJsonString(body, "version") ?? "N/A";
            return "N/A";
        }

        // ==================== Region Operations ====================

        public async Task<bool> CreateRegionAsync(string name, string description)
        {
            var json = JsonSerializer.Serialize(new { name, description });
            var (code, _) = await RequestAsync("POST", "/regions", json);
            return code >= 200 && code < 300;
        }

        public async Task<int> RegionSizeAsync(string region)
        {
            var (code, body) = await RequestAsync("GET", $"/cache/{Encode(region)}/size");
            if (code >= 200 && code < 300)
                return ExtractJsonInt(body, "size");
            return -1;
        }

        // ==================== JSON Operations ====================

        /// <summary>
        /// Store a JSON document.
        /// </summary>
        public async Task<bool> JsonSetAsync(string region, string key, string jsonValue, int ttlSeconds = -1)
        {
            var body = $"{{\"value\":{jsonValue}";
            if (ttlSeconds > 0)
                body += $",\"ttl\":{ttlSeconds}";
            body += "}";

            var (code, _) = await RequestAsync("PUT",
                $"/json/{Encode(region)}/{Encode(key)}", body);
            return code >= 200 && code < 300;
        }

        /// <summary>
        /// Get a JSON document (full or at specific JSONPath).
        /// </summary>
        public async Task<string?> JsonGetAsync(string region, string key, string? path = null)
        {
            var endpoint = $"/json/{Encode(region)}/{Encode(key)}";
            if (path != null && path != "$")
                endpoint += $"?path={Encode(path)}";

            var (code, body) = await RequestAsync("GET", endpoint);
            return code >= 200 && code < 300 ? body : null;
        }

        /// <summary>
        /// Search JSON documents using query operators.
        /// </summary>
        public async Task<List<Dictionary<string, string>>> JsonSearchAsync(string region, string query)
        {
            var body = $"{{\"query\":\"{EscapeJson(query)}\"}}";
            var (code, responseBody) = await RequestAsync("POST",
                $"/json/{Encode(region)}/search", body);

            var results = new List<Dictionary<string, string>>();
            if (code >= 200 && code < 300 && responseBody.Contains("\"results\""))
            {
                // Parse results using JsonDocument
                try
                {
                    using var doc = JsonDocument.Parse(responseBody);
                    if (doc.RootElement.TryGetProperty("results", out var resultsArray))
                    {
                        foreach (var item in resultsArray.EnumerateArray())
                        {
                            var result = new Dictionary<string, string>();
                            if (item.TryGetProperty("key", out var keyProp))
                                result["key"] = keyProp.GetString() ?? "";
                            if (item.TryGetProperty("value", out var valueProp))
                                result["value"] = valueProp.GetRawText();
                            results.Add(result);
                        }
                    }
                }
                catch (JsonException) { /* ignore parse errors */ }
            }
            return results;
        }

        /// <summary>
        /// Delete a JSON document.
        /// </summary>
        public async Task<bool> JsonDeleteAsync(string region, string key)
        {
            var (code, _) = await RequestAsync("DELETE",
                $"/json/{Encode(region)}/{Encode(key)}");
            return code >= 200 && code < 300;
        }

        // ==================== Helper Methods ====================

        private static string Encode(string s) => WebUtility.UrlEncode(s) ?? s;

        private static string EscapeJson(string s) =>
            s.Replace("\\", "\\\\").Replace("\"", "\\\"")
             .Replace("\n", "\\n").Replace("\r", "\\r");

        private static string? ExtractJsonString(string json, string key)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.TryGetProperty(key, out var prop) &&
                    prop.ValueKind == JsonValueKind.String)
                    return prop.GetString();
            }
            catch (JsonException) { }
            return null;
        }

        private static int ExtractJsonInt(string json, string key)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.TryGetProperty(key, out var prop) &&
                    prop.ValueKind == JsonValueKind.Number)
                    return prop.GetInt32();
            }
            catch (JsonException) { }
            return 0;
        }

        private static string ExtractFieldFromJson(string json, string field)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.TryGetProperty(field, out var prop))
                {
                    return prop.ValueKind switch
                    {
                        JsonValueKind.String => prop.GetString() ?? "",
                        JsonValueKind.Number => prop.GetRawText(),
                        JsonValueKind.True => "true",
                        JsonValueKind.False => "false",
                        _ => prop.GetRawText()
                    };
                }
            }
            catch (JsonException) { }
            return "";
        }

        // ==================== Display Helpers ====================

        private static void PrintHeader(string title)
        {
            Console.WriteLine();
            Console.WriteLine("========================================================================");
            Console.WriteLine($"  {title}");
            Console.WriteLine("========================================================================");
        }

        private static void PrintSubHeader(string title)
        {
            Console.WriteLine($"\n  --- {title} ---");
        }

        private static void PrintResults(List<Dictionary<string, string>> results, params string[] fields)
        {
            Console.WriteLine($"    Found {results.Count} result(s):");
            foreach (var r in results)
            {
                var key = r.GetValueOrDefault("key", "N/A");
                var value = r.GetValueOrDefault("value", "{}");

                var line = $"      - {key}: ";
                if (fields.Length > 0)
                {
                    var parts = new List<string>();
                    foreach (var field in fields)
                    {
                        var val = ExtractFieldFromJson(value, field);
                        if (!string.IsNullOrEmpty(val))
                            parts.Add($"{field}={val}");
                    }
                    line += string.Join(", ", parts);
                }
                else
                {
                    var name = ExtractFieldFromJson(value, "name");
                    if (string.IsNullOrEmpty(name))
                        name = ExtractFieldFromJson(value, "product_name");
                    line += !string.IsNullOrEmpty(name) ? name : value[..Math.Min(80, value.Length)];
                }
                Console.WriteLine(line);
            }
        }

        // ==================== Sample Data ====================

        private static readonly string[] EmployeeKeys = {
            "employee:EMP001", "employee:EMP002", "employee:EMP003",
            "employee:EMP004", "employee:EMP005"
        };

        private static readonly string[] EmployeeNames = {
            "John Smith", "Jane Doe", "Bob Johnson", "Alice Brown", "Charlie Wilson"
        };

        private static readonly string[] ProductKeys = {
            "product:PROD001", "product:PROD002", "product:PROD003",
            "product:PROD004", "product:PROD005"
        };

        private static readonly string[] ProductNames = {
            "Mechanical Keyboard Pro", "Wireless Noise-Cancelling Headphones",
            "Ergonomic Office Chair", "USB-C Hub Adapter", "Standing Desk Converter"
        };

        private static string BuildEmployeeJson(string id, string name, string email,
            string department, int salary, string level, string skills,
            string city, string state, string zip, bool active)
        {
            return $@"{{""id"":""{id}"",""name"":""{name}"",""email"":""{email}"",""department"":""{department}"",""salary"":{salary},""level"":""{level}"",""skills"":{skills},""address"":{{""city"":""{city}"",""state"":""{state}"",""zip"":""{zip}""}},""active"":{active.ToString().ToLower()}}}";
        }

        private static string[] BuildEmployees()
        {
            return new[]
            {
                BuildEmployeeJson("EMP001", "John Smith", "john.smith@company.com",
                    "Engineering", 95000, "Senior",
                    @"[""Python"",""Java"",""Kubernetes""]",
                    "San Francisco", "CA", "94102", true),
                BuildEmployeeJson("EMP002", "Jane Doe", "jane.doe@company.com",
                    "Engineering", 105000, "Lead",
                    @"[""JavaScript"",""React"",""Node.js""]",
                    "New York", "NY", "10001", true),
                BuildEmployeeJson("EMP003", "Bob Johnson", "bob.j@company.com",
                    "Sales", 85000, "Senior",
                    @"[""CRM"",""Negotiation""]",
                    "Chicago", "IL", "60601", true),
                BuildEmployeeJson("EMP004", "Alice Brown", "alice.brown@company.com",
                    "Engineering", 115000, "Principal",
                    @"[""Python"",""Machine Learning"",""TensorFlow""]",
                    "Seattle", "WA", "98101", false),
                BuildEmployeeJson("EMP005", "Charlie Wilson", "charlie.w@company.com",
                    "Marketing", 78000, "Junior",
                    @"[""SEO"",""Content Marketing""]",
                    "Los Angeles", "CA", "90001", true)
            };
        }

        private static string BuildProductJson(string id, string name, string category,
            string subcategory, string brand, double price, bool inStock,
            int stockCount, string tags, string specs)
        {
            return $@"{{""product_id"":""{id}"",""product_name"":""{name}"",""category"":""{category}"",""subcategory"":""{subcategory}"",""brand"":""{brand}"",""price"":{price},""in_stock"":{inStock.ToString().ToLower()},""stock_count"":{stockCount},""tags"":{tags},""specs"":{specs}}}";
        }

        private static string[] BuildProducts()
        {
            return new[]
            {
                BuildProductJson("PROD001", "Mechanical Keyboard Pro", "Electronics", "Peripherals",
                    "TechGear", 149.99, true, 230,
                    @"[""gaming"",""mechanical"",""rgb""]",
                    @"{""switch_type"":""Cherry MX Blue"",""backlighting"":""RGB"",""connectivity"":""USB-C""}"),
                BuildProductJson("PROD002", "Wireless Noise-Cancelling Headphones", "Electronics", "Audio",
                    "SoundTech", 299.99, true, 85,
                    @"[""wireless"",""noise-cancelling"",""bluetooth""]",
                    @"{""driver_size"":""40mm"",""battery_life"":""30h"",""connectivity"":""Bluetooth 5.2""}"),
                BuildProductJson("PROD003", "Ergonomic Office Chair", "Furniture", "Chairs",
                    "ComfortPlus", 449.00, true, 42,
                    @"[""ergonomic"",""office"",""adjustable""]",
                    @"{""material"":""Mesh"",""lumbar_support"":true,""weight_capacity"":""300lbs""}"),
                BuildProductJson("PROD004", "USB-C Hub Adapter", "Electronics", "Accessories",
                    "TechGear", 39.99, false, 0,
                    @"[""usb-c"",""hub"",""adapter""]",
                    @"{""ports"":""7-in-1"",""hdmi"":true,""power_delivery"":""100W""}"),
                BuildProductJson("PROD005", "Standing Desk Converter", "Furniture", "Desks",
                    "ErgoRise", 279.00, true, 118,
                    @"[""standing-desk"",""ergonomic"",""adjustable""]",
                    @"{""surface_area"":""36x24 inches"",""height_range"":""6-17 inches"",""weight_capacity"":""35lbs""}")
            };
        }

        // ==================== Demo Functions ====================

        private async Task DemoA_StoreJson(string region)
        {
            PrintHeader("A) STORE JSON DOCUMENTS (PUT /api/v1/json/{region}/{key})");

            var employees = BuildEmployees();
            Console.WriteLine("\n  Storing 5 employee records...");
            for (int i = 0; i < employees.Length; i++)
            {
                var ok = await JsonSetAsync(region, EmployeeKeys[i], employees[i]);
                Console.WriteLine($"    [{(ok ? "OK" : "FAIL")}] {EmployeeKeys[i]} -> {EmployeeNames[i]}");
            }
            Console.WriteLine($"\n  Region size: {await RegionSizeAsync(region)} entries");
        }

        private async Task DemoB_RetrieveJson(string region)
        {
            PrintHeader("B) RETRIEVE JSON DOCUMENTS (GET /api/v1/json/{region}/{key})");

            PrintSubHeader("Full Document Retrieval");
            var emp = await JsonGetAsync(region, "employee:EMP001");
            if (emp != null)
            {
                Console.WriteLine("    employee:EMP001:");
                Console.WriteLine($"      {emp}");
            }

            var emp3 = await JsonGetAsync(region, "employee:EMP003");
            if (emp3 != null)
            {
                var name = ExtractFieldFromJson(emp3, "name");
                var dept = ExtractFieldFromJson(emp3, "department");
                Console.WriteLine($"\n    employee:EMP003 -> {name}, {dept}");
            }

            var missing = await JsonGetAsync(region, "employee:EMP999");
            Console.WriteLine($"\n    employee:EMP999 (missing) -> {missing ?? "null"}");
        }

        private async Task DemoC_RetrieveJsonPath(string region)
        {
            PrintHeader("C) RETRIEVE JSON AT SPECIFIC PATH (GET with ?path=)");

            var key = "employee:EMP001";
            var paths = new (string Path, string Label)[]
            {
                ("$.name", "Name"),
                ("$.salary", "Salary"),
                ("$.department", "Department"),
                ("$.skills", "Skills"),
                ("$.address", "Address (nested)"),
                ("$.address.city", "City (deep path)"),
                ("$.address.state", "State (deep path)"),
                ("$.active", "Active status"),
            };

            Console.WriteLine($"\n  Querying paths on '{key}':\n");
            foreach (var (path, label) in paths)
            {
                var value = await JsonGetAsync(region, key, path);
                Console.WriteLine($"    {label,-22} ({path,-22}) -> {value}");
            }
        }

        private async Task DemoD_UpdateJson(string region)
        {
            PrintHeader("D) UPDATE JSON DOCUMENTS (PUT - full replace)");

            var key = "employee:EMP005";
            var original = await JsonGetAsync(region, key);
            if (original != null)
            {
                var name = ExtractFieldFromJson(original, "name");
                var salary = ExtractFieldFromJson(original, "salary");
                var dept = ExtractFieldFromJson(original, "department");
                Console.WriteLine($"\n  Original: {name} - salary=${salary}, dept={dept}");
            }

            // Build updated version
            var updated = BuildEmployeeJson("EMP005", "Charlie Wilson", "charlie.w@company.com",
                "Engineering", 92000, "Senior",
                @"[""SEO"",""Content Marketing"",""Python"",""Data Analytics""]",
                "Los Angeles", "CA", "90001", true);

            var ok = await JsonSetAsync(region, key, updated);
            Console.WriteLine($"\n  Updated (PUT replace): {(ok ? "OK" : "FAIL")}");

            var after = await JsonGetAsync(region, key);
            if (after != null)
            {
                var newSalary = ExtractFieldFromJson(after, "salary");
                var newDept = ExtractFieldFromJson(after, "department");
                Console.WriteLine($"  After:    Charlie Wilson - salary=${newSalary}, dept={newDept}");
            }

            // Restore original
            var employees = BuildEmployees();
            await JsonSetAsync(region, key, employees[4]);
            Console.WriteLine("\n  Restored original values for subsequent demos.");
        }

        private async Task DemoE_SearchEquality(string region)
        {
            PrintHeader("E) JSON SEARCH - EQUALITY ($.field=value)");

            PrintSubHeader("Search: $.department=Engineering");
            PrintResults(await JsonSearchAsync(region, "$.department=Engineering"), "name", "department", "salary");

            PrintSubHeader("Search: $.active=true");
            PrintResults(await JsonSearchAsync(region, "$.active=true"), "name", "active");

            PrintSubHeader("Search: $.address.state=CA");
            PrintResults(await JsonSearchAsync(region, "$.address.state=CA"), "name");

            PrintSubHeader("Search: $.level=Senior");
            PrintResults(await JsonSearchAsync(region, "$.level=Senior"), "name", "department", "level");
        }

        private async Task DemoF_SearchComparison(string region)
        {
            PrintHeader("F) JSON SEARCH - COMPARISON (>, <, >=, <=)");

            PrintSubHeader("Search: $.salary>100000");
            PrintResults(await JsonSearchAsync(region, "$.salary>100000"), "name", "salary");

            PrintSubHeader("Search: $.salary<90000");
            PrintResults(await JsonSearchAsync(region, "$.salary<90000"), "name", "salary");

            PrintSubHeader("Search: $.salary>=95000");
            PrintResults(await JsonSearchAsync(region, "$.salary>=95000"), "name", "salary");

            PrintSubHeader("Search: $.salary<=85000");
            PrintResults(await JsonSearchAsync(region, "$.salary<=85000"), "name", "salary");
        }

        private async Task DemoG_SearchInequality(string region)
        {
            PrintHeader("G) JSON SEARCH - INEQUALITY ($.field!=value)");

            PrintSubHeader("Search: $.department!=Engineering");
            PrintResults(await JsonSearchAsync(region, "$.department!=Engineering"), "name", "department");

            PrintSubHeader("Search: $.active!=true");
            PrintResults(await JsonSearchAsync(region, "$.active!=true"), "name", "active");
        }

        private async Task DemoH_SearchPattern(string region)
        {
            PrintHeader("H) JSON SEARCH - PATTERN MATCH ($.field LIKE %pattern%)");

            PrintSubHeader("Search: $.name LIKE %son%");
            PrintResults(await JsonSearchAsync(region, "$.name LIKE %son%"), "name");

            PrintSubHeader("Search: $.email LIKE %@company.com");
            PrintResults(await JsonSearchAsync(region, "$.email LIKE %@company.com"), "name", "email");

            PrintSubHeader("Search: $.address.zip LIKE 9%");
            PrintResults(await JsonSearchAsync(region, "$.address.zip LIKE 9%"), "name");
        }

        private async Task DemoI_SearchContains(string region)
        {
            PrintHeader("I) JSON SEARCH - ARRAY CONTAINS ($.array CONTAINS value)");

            PrintSubHeader("Search: $.skills CONTAINS Python");
            PrintResults(await JsonSearchAsync(region, "$.skills CONTAINS Python"), "name");

            PrintSubHeader("Search: $.skills CONTAINS CRM");
            PrintResults(await JsonSearchAsync(region, "$.skills CONTAINS CRM"), "name");

            PrintSubHeader("Search: $.skills CONTAINS JavaScript");
            PrintResults(await JsonSearchAsync(region, "$.skills CONTAINS JavaScript"), "name");
        }

        private async Task DemoJ_SearchCombined(string region)
        {
            PrintHeader("J) JSON SEARCH - COMBINED CONDITIONS (AND logic)");

            PrintSubHeader("Search: $.department=Engineering,$.salary>100000");
            PrintResults(await JsonSearchAsync(region, "$.department=Engineering,$.salary>100000"),
                "name", "department", "salary");

            PrintSubHeader("Search: $.active=true,$.address.state=CA");
            PrintResults(await JsonSearchAsync(region, "$.active=true,$.address.state=CA"),
                "name", "active");

            PrintSubHeader("Search: $.department=Engineering,$.active=true,$.salary>=95000");
            PrintResults(await JsonSearchAsync(region, "$.department=Engineering,$.active=true,$.salary>=95000"),
                "name", "department", "salary", "active");

            PrintSubHeader("Search: $.address.state!=CA,$.level=Senior");
            PrintResults(await JsonSearchAsync(region, "$.address.state!=CA,$.level=Senior"),
                "name", "level");
        }

        private async Task DemoK_JsonWithTtl(string region)
        {
            PrintHeader("K) STORE JSON WITH TTL (time-to-live)");

            var session = @"{""session_id"":""sess:ABC123"",""user"":""jsmith"",""ip"":""192.168.1.100"",""login_time"":""2025-01-15T10:30:00Z"",""permissions"":[""read"",""write"",""admin""]}";

            Console.WriteLine("\n  Storing session with 60-second TTL...");
            var ok = await JsonSetAsync(region, "session:ABC123", session, 60);
            Console.WriteLine($"    Store: {(ok ? "OK" : "FAIL")}");

            var retrieved = await JsonGetAsync(region, "session:ABC123");
            if (retrieved != null)
            {
                var user = ExtractFieldFromJson(retrieved, "user");
                Console.WriteLine($"    Verify: user={user}");
            }

            var apiCache = @"{""cached_at"":""2025-01-15T10:30:00Z"",""data"":{""rates"":{""USD"":1.0,""EUR"":0.85,""GBP"":0.73}},""source"":""exchange-api""}";

            Console.WriteLine("\n  Storing API cache entry with 300-second TTL...");
            ok = await JsonSetAsync(region, "api_cache:exchange_rates", apiCache, 300);
            Console.WriteLine($"    Store: {(ok ? "OK" : "FAIL")}");

            retrieved = await JsonGetAsync(region, "api_cache:exchange_rates");
            if (retrieved != null)
            {
                var source = ExtractFieldFromJson(retrieved, "source");
                Console.WriteLine($"    Verify: source={source}");
            }
        }

        private async Task<string> DemoL_CrossRegion()
        {
            PrintHeader("L) CROSS-REGION JSON OPERATIONS");

            var productsRegion = "products_json_rest_demo";
            await CreateRegionAsync(productsRegion, "Product catalog for JSON REST demo");

            var products = BuildProducts();
            Console.WriteLine($"\n  Storing {products.Length} products in '{productsRegion}' region...");
            for (int i = 0; i < products.Length; i++)
            {
                var ok = await JsonSetAsync(productsRegion, ProductKeys[i], products[i]);
                Console.WriteLine($"    [{(ok ? "OK" : "FAIL")}] {ProductKeys[i]} -> {ProductNames[i]}");
            }

            PrintSubHeader("Search: $.category=Electronics");
            PrintResults(await JsonSearchAsync(productsRegion, "$.category=Electronics"),
                "product_name", "price");

            PrintSubHeader("Search: $.price>200");
            PrintResults(await JsonSearchAsync(productsRegion, "$.price>200"),
                "product_name", "price");

            PrintSubHeader("Search: $.in_stock=true,$.price<300");
            PrintResults(await JsonSearchAsync(productsRegion, "$.in_stock=true,$.price<300"),
                "product_name", "price", "in_stock");

            PrintSubHeader("Search: $.specs.backlighting=RGB");
            PrintResults(await JsonSearchAsync(productsRegion, "$.specs.backlighting=RGB"),
                "product_name");

            PrintSubHeader("Search: $.brand LIKE %Tech%");
            PrintResults(await JsonSearchAsync(productsRegion, "$.brand LIKE %Tech%"),
                "product_name", "brand");

            PrintSubHeader("Search: $.tags CONTAINS gaming");
            PrintResults(await JsonSearchAsync(productsRegion, "$.tags CONTAINS gaming"),
                "product_name");

            PrintSubHeader("Search: $.stock_count>=100");
            PrintResults(await JsonSearchAsync(productsRegion, "$.stock_count>=100"),
                "product_name", "stock_count");

            return productsRegion;
        }

        private async Task DemoM_DeleteJson(string region, string productsRegion)
        {
            PrintHeader("M) DELETE JSON DOCUMENTS (DELETE /api/v1/json/{region}/{key})");

            Console.WriteLine("\n  Deleting employee records...");
            foreach (var key in EmployeeKeys)
            {
                var ok = await JsonDeleteAsync(region, key);
                Console.WriteLine($"    [{(ok ? "OK" : "FAIL")}] Deleted {key}");
            }

            foreach (var key in new[] { "session:ABC123", "api_cache:exchange_rates" })
            {
                var ok = await JsonDeleteAsync(region, key);
                Console.WriteLine($"    [{(ok ? "OK" : "FAIL")}] Deleted {key}");
            }

            Console.WriteLine($"\n  Deleting product records from '{productsRegion}'...");
            foreach (var key in ProductKeys)
            {
                var ok = await JsonDeleteAsync(productsRegion, key);
                Console.WriteLine($"    [{(ok ? "OK" : "FAIL")}] Deleted {key}");
            }

            Console.WriteLine($"\n  Region '{region}' size after cleanup: {await RegionSizeAsync(region)}");
            Console.WriteLine($"  Region '{productsRegion}' size after cleanup: {await RegionSizeAsync(productsRegion)}");
        }

        private async Task DemoN_SlashKeys(string region)
        {
            PrintHeader("N) KEYS WITH FORWARD SLASHES (employee/EMP001)");

            var slashDocs = new (string key, string json)[]
            {
                ("employee/EMP001", "{\"name\":\"Alice Walker\",\"dept\":\"Engineering\",\"level\":\"L5\"}"),
                ("employee/EMP002", "{\"name\":\"Bob Chen\",\"dept\":\"Marketing\",\"level\":\"L4\"}"),
                ("department/eng/lead", "{\"name\":\"Carol Davis\",\"role\":\"Director\",\"reports\":42}"),
                ("config/app/v2.1/settings", "{\"theme\":\"dark\",\"locale\":\"en-US\",\"timeout\":30}")
            };

            Console.WriteLine("\n  Storing documents with slash keys...");
            foreach (var (key, json) in slashDocs)
            {
                var ok = await JsonSetAsync(region, key, json);
                Console.WriteLine($"    [{(ok ? "OK" : "FAIL")}] {key}");
            }

            Console.WriteLine("\n  Retrieving documents by slash key...");
            foreach (var (key, _) in slashDocs)
            {
                var result = await JsonGetAsync(region, key);
                Console.WriteLine($"    [{(result != null ? "OK" : "FAIL")}] {key} -> {(result != null ? result[..Math.Min(80, result.Length)] + "..." : "NOT FOUND")}");
            }

            Console.WriteLine("\n  JSONPath on slash key 'employee/EMP001' ($.dept)...");
            var dept = await JsonGetAsync(region, "employee/EMP001", "$.dept");
            Console.WriteLine($"    Department: {dept}");

            Console.WriteLine("\n  Deleting slash-key documents...");
            foreach (var (key, _) in slashDocs)
            {
                var ok = await JsonDeleteAsync(region, key);
                Console.WriteLine($"    [{(ok ? "OK" : "FAIL")}] Deleted {key}");
            }
        }

        // ==================== Main ====================

        public static async Task Main(string[] args)
        {
            var host = args.Length > 0 ? args[0] : "localhost";
            var port = args.Length > 1 ? int.Parse(args[1]) : 8080;
            var apiKey = args.Length > 2 ? args[2] : "kub_admin_sample_key_replace_me";

            Console.WriteLine();
            Console.WriteLine("+======================================================================+");
            Console.WriteLine("|                                                                      |");
            Console.WriteLine("|   KUBER DISTRIBUTED CACHE - JSON OPERATIONS VIA REST API (C#)        |");
            Console.WriteLine("|                                                                      |");
            Console.WriteLine("|   Demonstrates all JSON operations through the REST/HTTP API:        |");
            Console.WriteLine("|                                                                      |");
            Console.WriteLine("|   a) Store JSON documents           PUT  /api/v1/json/{r}/{k}       |");
            Console.WriteLine("|   b) Retrieve JSON documents        GET  /api/v1/json/{r}/{k}       |");
            Console.WriteLine("|   c) Retrieve at JSONPath           GET  with ?path=$.field          |");
            Console.WriteLine("|   d) Update JSON documents          PUT  (full replace)              |");
            Console.WriteLine("|   e-j) Search with operators        POST /api/v1/json/{r}/search    |");
            Console.WriteLine("|       - Equality, Comparison, Inequality, Pattern, Contains, AND     |");
            Console.WriteLine("|   k) Store with TTL                 PUT  with ttl parameter          |");
            Console.WriteLine("|   l) Cross-region operations        Multiple regions                 |");
            Console.WriteLine("|   m) Delete JSON documents          DELETE /api/v1/json/{r}/{k}     |");
            Console.WriteLine("|   n) Keys with forward slashes      employee/EMP001 etc.            |");
            Console.WriteLine("|                                                                      |");
            Console.WriteLine("|   v2.6.2                                                             |");
            Console.WriteLine("+======================================================================+");
            Console.WriteLine();

            Console.WriteLine("Configuration:");
            Console.WriteLine($"  Host:    {host}");
            Console.WriteLine($"  Port:    {port}");
            Console.WriteLine($"  API Key: {apiKey[..Math.Min(12, apiKey.Length)]}...{apiKey[Math.Max(0, apiKey.Length - 4)..]}");

            var region = "employees_json_rest_demo";

            try
            {
                var demo = new RestJsonDemo(host, port, apiKey);

                if (!await demo.PingAsync())
                {
                    Console.WriteLine("\n  ✗ Cannot connect to Kuber server");
                    Environment.Exit(1);
                }
                Console.WriteLine($"\n  ✓ Connected to Kuber server at {host}:{port}");
                Console.WriteLine($"  ✓ Server version: {await demo.ServerVersionAsync()}");

                await demo.CreateRegionAsync(region, "Employee data for JSON REST demo");
                Console.WriteLine($"  ✓ Region: {region}");

                // Run all demos
                await demo.DemoA_StoreJson(region);
                await demo.DemoB_RetrieveJson(region);
                await demo.DemoC_RetrieveJsonPath(region);
                await demo.DemoD_UpdateJson(region);
                await demo.DemoE_SearchEquality(region);
                await demo.DemoF_SearchComparison(region);
                await demo.DemoG_SearchInequality(region);
                await demo.DemoH_SearchPattern(region);
                await demo.DemoI_SearchContains(region);
                await demo.DemoJ_SearchCombined(region);
                await demo.DemoK_JsonWithTtl(region);
                var productsRegion = await demo.DemoL_CrossRegion();
                await demo.DemoM_DeleteJson(region, productsRegion);
                await demo.DemoN_SlashKeys(region);

                PrintHeader("ALL DEMOS COMPLETED SUCCESSFULLY");
                Console.WriteLine();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\n  ✗ Error: {ex.Message}");
                Console.WriteLine("\n  Troubleshooting:");
                Console.WriteLine("    1. Ensure Kuber server is running");
                Console.WriteLine("    2. Check host/port (default: localhost:8080)");
                Console.WriteLine("    3. Verify API key starts with 'kub_'");
                Console.WriteLine("    4. Check REST API is enabled on the server");
                Environment.Exit(1);
            }
        }
    }
}
