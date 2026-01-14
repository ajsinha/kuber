/*
 * Kuber REST API - Generic Search Demo (v1.7.9)
 *
 * This standalone Java application demonstrates ALL search capabilities of the Kuber
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
 *     javac RestGenericSearchDemo.java
 *     java RestGenericSearchDemo [host] [port] [apiKey]
 *
 * Example:
 *     java RestGenericSearchDemo localhost 7070 kub_admin
 *
 * Dependencies: None (uses only JDK classes)
 *
 * Copyright (c) 2025-2030 Ashutosh Sinha. All Rights Reserved.
 */

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class RestGenericSearchDemo {

    private final String baseUrl;
    private final String apiKey;

    public RestGenericSearchDemo(String host, int port, String apiKey) {
        this.baseUrl = String.format("http://%s:%d/api/v1", host, port);
        this.apiKey = apiKey;
    }

    // ==================== HTTP Helper Methods ====================

    private String doPost(String endpoint, String jsonBody) throws IOException {
        URL url = new URL(baseUrl + endpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("X-API-Key", apiKey);
        conn.setDoOutput(true);
        conn.setConnectTimeout(30000);
        conn.setReadTimeout(30000);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = conn.getResponseCode();
        InputStream is = (responseCode >= 200 && responseCode < 300) 
            ? conn.getInputStream() 
            : conn.getErrorStream();

        if (is == null) return "";

        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
            return response.toString();
        }
    }

    private String doGet(String endpoint) throws IOException {
        URL url = new URL(baseUrl + endpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("X-API-Key", apiKey);
        conn.setConnectTimeout(30000);
        conn.setReadTimeout(30000);

        int responseCode = conn.getResponseCode();
        InputStream is = (responseCode >= 200 && responseCode < 300) 
            ? conn.getInputStream() 
            : conn.getErrorStream();

        if (is == null) return "";

        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
            return response.toString();
        }
    }

    // ==================== Generic Search ====================

    public String genericSearch(String jsonRequest) throws IOException {
        return doPost("/genericsearch", jsonRequest);
    }

    public void putJson(String region, String key, String jsonValue) throws IOException {
        doPost("/cache/" + region + "/" + key, jsonValue);
    }

    // ==================== JSON Builder Helpers ====================

    /**
     * Simple JSON object builder (no external dependencies).
     */
    static class JsonBuilder {
        private final StringBuilder sb = new StringBuilder();
        private boolean first = true;

        public JsonBuilder() {
            sb.append("{");
        }

        public JsonBuilder add(String key, String value) {
            addComma();
            sb.append("\"").append(key).append("\":\"").append(escapeJson(value)).append("\"");
            return this;
        }

        public JsonBuilder add(String key, int value) {
            addComma();
            sb.append("\"").append(key).append("\":").append(value);
            return this;
        }

        public JsonBuilder add(String key, double value) {
            addComma();
            sb.append("\"").append(key).append("\":").append(value);
            return this;
        }

        public JsonBuilder add(String key, boolean value) {
            addComma();
            sb.append("\"").append(key).append("\":").append(value);
            return this;
        }

        public JsonBuilder addArray(String key, String... values) {
            addComma();
            sb.append("\"").append(key).append("\":[");
            for (int i = 0; i < values.length; i++) {
                if (i > 0) sb.append(",");
                sb.append("\"").append(escapeJson(values[i])).append("\"");
            }
            sb.append("]");
            return this;
        }

        public JsonBuilder addRaw(String key, String rawJson) {
            addComma();
            sb.append("\"").append(key).append("\":").append(rawJson);
            return this;
        }

        public JsonBuilder addObject(String key, JsonBuilder nested) {
            addComma();
            sb.append("\"").append(key).append("\":").append(nested.build());
            return this;
        }

        private void addComma() {
            if (!first) sb.append(",");
            first = false;
        }

        private String escapeJson(String s) {
            return s.replace("\\", "\\\\").replace("\"", "\\\"");
        }

        public String build() {
            return sb.toString() + "}";
        }
    }

    // ==================== Print Helpers ====================

    private static void printHeader(String title) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("  " + title);
        System.out.println("=".repeat(80));
    }

    private static void printSubheader(String title) {
        System.out.println("\n--- " + title + " ---");
    }

    private static void printRequest(String request) {
        // Remove apiKey for cleaner display
        String display = request.replaceAll(",?\"apiKey\":\"[^\"]*\"", "");
        System.out.println("Request: " + display);
    }

    private static void printResult(String result) {
        if (result == null || result.isEmpty()) {
            System.out.println("❌ No response");
            return;
        }
        
        // Count results (simple parsing)
        int count = 0;
        int idx = 0;
        while ((idx = result.indexOf("\"key\":", idx)) != -1) {
            count++;
            idx++;
        }
        
        if (result.startsWith("[")) {
            System.out.println("✅ Results: " + count);
            // Truncate if too long
            if (result.length() > 500) {
                System.out.println("  " + result.substring(0, 500) + "...");
            } else {
                System.out.println("  " + result);
            }
        } else {
            System.out.println("Response: " + result);
        }
    }

    // ==================== Setup Test Data ====================

    private void setupTestData(String region) throws IOException {
        printHeader("SETUP: Inserting Test Data");
        
        System.out.println("\nInserting customers into region '" + region + "'...");
        
        // Customer 1
        String c1 = new JsonBuilder()
            .add("id", "C001").add("name", "John Smith").add("email", "john@gmail.com")
            .add("status", "active").add("city", "NYC").add("tier", "premium")
            .add("age", 35).add("balance", 15000.50).build();
        putJson(region, "customer_C001", c1);
        System.out.println("  ✓ customer_C001");

        // Customer 2
        String c2 = new JsonBuilder()
            .add("id", "C002").add("name", "Jane Doe").add("email", "jane@yahoo.com")
            .add("status", "active").add("city", "LA").add("tier", "standard")
            .add("age", 28).add("balance", 5200.00).build();
        putJson(region, "customer_C002", c2);
        System.out.println("  ✓ customer_C002");

        // Customer 3
        String c3 = new JsonBuilder()
            .add("id", "C003").add("name", "Bob Wilson").add("email", "bob@gmail.com")
            .add("status", "pending").add("city", "NYC").add("tier", "premium")
            .add("age", 42).add("balance", 32000.75).build();
        putJson(region, "customer_C003", c3);
        System.out.println("  ✓ customer_C003");

        // Customer 4
        String c4 = new JsonBuilder()
            .add("id", "C004").add("name", "Alice Brown").add("email", "alice@outlook.com")
            .add("status", "inactive").add("city", "Chicago").add("tier", "basic")
            .add("age", 31).add("balance", 1500.25).build();
        putJson(region, "customer_C004", c4);
        System.out.println("  ✓ customer_C004");

        // Customer 5
        String c5 = new JsonBuilder()
            .add("id", "C005").add("name", "Charlie Davis").add("email", "charlie@gmail.com")
            .add("status", "active").add("city", "Boston").add("tier", "premium")
            .add("age", 45).add("balance", 48000.00).build();
        putJson(region, "customer_C005", c5);
        System.out.println("  ✓ customer_C005");

        // Customer 6
        String c6 = new JsonBuilder()
            .add("id", "C006").add("name", "Eva Martinez").add("email", "eva@yahoo.com")
            .add("status", "pending").add("city", "Miami").add("tier", "standard")
            .add("age", 29).add("balance", 7800.50).build();
        putJson(region, "customer_C006", c6);
        System.out.println("  ✓ customer_C006");

        // Customer 7
        String c7 = new JsonBuilder()
            .add("id", "C007").add("name", "Frank Johnson").add("email", "frank@company.com")
            .add("status", "active").add("city", "NYC").add("tier", "enterprise")
            .add("age", 52).add("balance", 125000.00).build();
        putJson(region, "customer_C007", c7);
        System.out.println("  ✓ customer_C007");

        // Customer 8
        String c8 = new JsonBuilder()
            .add("id", "C008").add("name", "Grace Lee").add("email", "grace@gmail.com")
            .add("status", "trial").add("city", "Seattle").add("tier", "basic")
            .add("age", 24).add("balance", 500.00).build();
        putJson(region, "customer_C008", c8);
        System.out.println("  ✓ customer_C008");

        // Orders
        System.out.println("\nInserting orders...");
        String o1 = new JsonBuilder()
            .add("orderId", "ORD001").add("customerId", "C001").add("status", "shipped")
            .add("region", "US").add("priority", "high").add("amount", 450.00).add("items", 3).build();
        putJson(region, "order_ORD001", o1);
        System.out.println("  ✓ order_ORD001");

        String o2 = new JsonBuilder()
            .add("orderId", "ORD002").add("customerId", "C002").add("status", "delivered")
            .add("region", "US").add("priority", "normal").add("amount", 125.50).add("items", 1).build();
        putJson(region, "order_ORD002", o2);
        System.out.println("  ✓ order_ORD002");

        String o3 = new JsonBuilder()
            .add("orderId", "ORD003").add("customerId", "C003").add("status", "pending")
            .add("region", "CA").add("priority", "urgent").add("amount", 890.00).add("items", 5).build();
        putJson(region, "order_ORD003", o3);
        System.out.println("  ✓ order_ORD003");

        System.out.println("\n✅ Test data setup complete!");
    }

    // ==================== Demo Methods ====================

    private void demoKeyLookups(String region) throws IOException {
        printHeader("1. KEY-BASED LOOKUPS");

        // 1a. Single key lookup
        printSubheader("1a. Single Key Lookup");
        String req = new JsonBuilder()
            .add("region", region)
            .add("key", "customer_C001")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 1b. Multi-key lookup
        printSubheader("1b. Multi-Key Lookup (3 keys)");
        req = new JsonBuilder()
            .add("region", region)
            .addArray("keys", "customer_C001", "customer_C003", "customer_C005")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 1c. Multi-key lookup with field projection
        printSubheader("1c. Multi-Key Lookup with Field Projection");
        req = new JsonBuilder()
            .add("region", region)
            .addArray("keys", "customer_C001", "customer_C002", "customer_C003")
            .addArray("fields", "name", "email", "status")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));
    }

    private void demoPatternSearches(String region) throws IOException {
        printHeader("2. REGEX PATTERN SEARCHES");

        // 2a. Single pattern - All customers
        printSubheader("2a. Single Pattern - All customers");
        String req = new JsonBuilder()
            .add("region", region)
            .add("keypattern", "customer_.*")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 2b. Single pattern - Orders only
        printSubheader("2b. Single Pattern - Orders only");
        req = new JsonBuilder()
            .add("region", region)
            .add("keypattern", "order_.*")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 2c. Multi-pattern search
        printSubheader("2c. Multi-Pattern - Customers C001-C003 OR Orders");
        req = new JsonBuilder()
            .add("region", region)
            .addArray("keypatterns", "customer_C00[1-3]", "order_.*")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));
    }

    private void demoJsonEqualitySearch(String region) throws IOException {
        printHeader("3. JSON SEARCH - EQUALITY");

        // 3a. Single attribute equality
        printSubheader("3a. Single Attribute - status = 'active'");
        String req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"status\":\"active\"}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 3b. Multiple attributes (AND logic)
        printSubheader("3b. Multiple Attributes (AND) - status='active' AND city='NYC'");
        req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"status\":\"active\",\"city\":\"NYC\"}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 3c. Three attributes (AND logic)
        printSubheader("3c. Three Attributes (AND) - status='active' AND city='NYC' AND tier='premium'");
        req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"status\":\"active\",\"city\":\"NYC\",\"tier\":\"premium\"}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));
    }

    private void demoJsonInClause(String region) throws IOException {
        printHeader("4. JSON SEARCH - IN CLAUSE (List of Values)");

        // 4a. Single attribute with list
        printSubheader("4a. Single IN Clause - status IN ['active', 'pending']");
        String req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"status\":[\"active\",\"pending\"]}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 4b. Single attribute with longer list
        printSubheader("4b. Single IN Clause - tier IN ['premium', 'enterprise']");
        req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"tier\":[\"premium\",\"enterprise\"]}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 4c. Multiple IN clauses
        printSubheader("4c. Multiple IN Clauses - status IN [...] AND city IN [...]");
        req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"status\":[\"active\",\"pending\"],\"city\":[\"NYC\",\"LA\",\"SF\"]}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 4d. IN clause combined with equality
        printSubheader("4d. IN Clause + Equality - tier IN [...] AND status='active'");
        req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"tier\":[\"premium\",\"enterprise\"],\"status\":\"active\"}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));
    }

    private void demoJsonRegexSearch(String region) throws IOException {
        printHeader("5. JSON SEARCH - REGEX MATCHING");

        // 5a. Email regex - Gmail users
        printSubheader("5a. Regex - Gmail users (email matches '.*@gmail.com')");
        String req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"email\":{\"regex\":\".*@gmail\\\\.com\"}}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 5b. Regex with other conditions
        printSubheader("5b. Regex + Equality - Gmail users who are active");
        req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"email\":{\"regex\":\".*@gmail\\\\.com\"},\"status\":\"active\"}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 5c. Name pattern regex
        printSubheader("5c. Regex - Names starting with 'J'");
        req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"name\":{\"regex\":\"^J.*\"}}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));
    }

    private void demoJsonComparisonOperators(String region) throws IOException {
        printHeader("6. JSON SEARCH - COMPARISON OPERATORS");

        // 6a. Greater than
        printSubheader("6a. Greater Than - balance > 20000");
        String req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"balance\":{\"gt\":20000}}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 6b. Less than or equal
        printSubheader("6b. Less Than or Equal - age <= 30");
        req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"age\":{\"lte\":30}}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 6c. Range query (between)
        printSubheader("6c. Range Query - age >= 30 AND age <= 45");
        req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"age\":{\"gte\":30,\"lte\":45}}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 6d. Comparison + other conditions
        printSubheader("6d. Comparison + IN Clause - balance > 10000 AND status IN ['active', 'pending']");
        req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"balance\":{\"gt\":10000},\"status\":[\"active\",\"pending\"]}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));
    }

    private void demoComplexSearches(String region) throws IOException {
        printHeader("7. COMPLEX COMBINED SEARCHES");

        // 7a. All operators combined
        printSubheader("7a. All Operators Combined");
        System.out.println("    - status IN ['active', 'pending']");
        System.out.println("    - city IN ['NYC', 'LA', 'SF']");
        System.out.println("    - tier = 'premium'");
        System.out.println("    - balance > 10000");
        String req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"status\":[\"active\",\"pending\"],\"city\":[\"NYC\",\"LA\",\"SF\"],\"tier\":\"premium\",\"balance\":{\"gt\":10000}}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 7b. Orders search - complex
        printSubheader("7b. Orders Search - Multiple Conditions");
        System.out.println("    - status IN ['shipped', 'delivered']");
        System.out.println("    - region IN ['US', 'CA']");
        System.out.println("    - amount > 200");
        req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"status\":[\"shipped\",\"delivered\"],\"region\":[\"US\",\"CA\"],\"amount\":{\"gt\":200}}")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));
    }

    private void demoFieldProjection(String region) throws IOException {
        printHeader("8. FIELD PROJECTION (Select Specific Fields)");

        // 8a. Key lookup with projection
        printSubheader("8a. Key Lookup - Return only name, email, status");
        String req = new JsonBuilder()
            .add("region", region)
            .add("key", "customer_C001")
            .addArray("fields", "name", "email", "status")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 8b. JSON search with projection
        printSubheader("8b. JSON Search with Projection - Active users, return name and balance only");
        req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"status\":\"active\"}")
            .addArray("fields", "name", "balance")
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));
    }

    private void demoLimitResults(String region) throws IOException {
        printHeader("9. LIMITING RESULTS");

        // 9a. Limit to 3 results
        printSubheader("9a. All Customers, Limit 3");
        String req = new JsonBuilder()
            .add("region", region)
            .add("keypattern", "customer_.*")
            .add("limit", 3)
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));

        // 9b. JSON search with limit
        printSubheader("9b. Active Users, Limit 2");
        req = new JsonBuilder()
            .add("region", region)
            .add("type", "json")
            .addRaw("attributes", "{\"status\":\"active\"}")
            .add("limit", 2)
            .add("apiKey", apiKey).build();
        printRequest(req);
        printResult(genericSearch(req));
    }

    // ==================== Main ====================

    public void runDemo(String region) {
        try {
            setupTestData(region);
            demoKeyLookups(region);
            demoPatternSearches(region);
            demoJsonEqualitySearch(region);
            demoJsonInClause(region);
            demoJsonRegexSearch(region);
            demoJsonComparisonOperators(region);
            demoComplexSearches(region);
            demoFieldProjection(region);
            demoLimitResults(region);

            printHeader("DEMO COMPLETE");
            System.out.println("""

This demo showcased all Generic Search API capabilities:

  1. KEY LOOKUPS
     - Single key: {"key": "abc"}
     - Multi-key: {"keys": ["a", "b", "c"]}

  2. PATTERN SEARCHES (Regex)
     - Single pattern: {"keypattern": "user_.*"}
     - Multi-pattern: {"keypatterns": ["user_.*", "admin_.*"]}

  3. JSON ATTRIBUTE SEARCH
     - Equality: {"attributes": {"status": "active"}}
     - IN clause: {"attributes": {"status": ["active", "pending"]}}
     - Regex: {"attributes": {"email": {"regex": ".*@gmail.com"}}}
     - Comparisons: {"attributes": {"age": {"gt": 25, "lte": 50}}}

  4. COMBINING CRITERIA (AND logic)
     - All criteria in "attributes" are ANDed together
     - Each list value within an attribute uses OR (IN clause)

  5. FIELD PROJECTION
     - Return specific fields: {"fields": ["name", "email"]}

  6. LIMITING RESULTS
     - Max results: {"limit": 100}

API Endpoint: POST /api/v1/genericsearch
            """);

        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 7070;
        String apiKey = args.length > 2 ? args[2] : "kub_admin";
        String region = args.length > 3 ? args[3] : "search_demo_java";

        System.out.println("=".repeat(80));
        System.out.println("  KUBER REST API - GENERIC SEARCH DEMO (v1.7.9) - Java");
        System.out.println("=".repeat(80));
        System.out.println("  Server:  " + host + ":" + port);
        System.out.println("  Region:  " + region);
        System.out.println("  Time:    " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.println("=".repeat(80));

        RestGenericSearchDemo demo = new RestGenericSearchDemo(host, port, apiKey);
        demo.runDemo(region);
    }
}
