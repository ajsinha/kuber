/*
 * Kuber REST API - JSON Operations Demo (v2.5.0)
 *
 * This standalone Java application demonstrates all JSON-related operations
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
 *     javac RestJsonDemo.java
 *     java RestJsonDemo [host] [port] [apiKey]
 *
 * Example:
 *     java RestJsonDemo localhost 8080 kub_admin_sample_key_replace_me
 *
 * Dependencies: None (uses only JDK classes)
 *
 * Copyright (c) 2025-2030 Ashutosh Sinha. All Rights Reserved.
 *
 * @version 2.5.0
 */

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class RestJsonDemo {

    private final String baseUrl;
    private final String apiKey;

    public RestJsonDemo(String host, int port, String apiKey) {
        this.baseUrl = String.format("http://%s:%d/api/v1", host, port);
        this.apiKey = apiKey;
    }

    // ==================== HTTP Helper Methods ====================

    private HttpResponse doRequest(String method, String endpoint) throws IOException {
        return doRequest(method, endpoint, null);
    }

    private HttpResponse doRequest(String method, String endpoint, String jsonBody) throws IOException {
        URL url = new URL(baseUrl + endpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestProperty("X-API-Key", apiKey);
        conn.setConnectTimeout(30000);
        conn.setReadTimeout(30000);

        if (jsonBody != null) {
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
            }
        }

        int responseCode = conn.getResponseCode();
        InputStream is = (responseCode >= 200 && responseCode < 300)
            ? conn.getInputStream()
            : conn.getErrorStream();

        String body = "";
        if (is != null) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    response.append(line);
                }
                body = response.toString();
            }
        }
        return new HttpResponse(responseCode, body);
    }

    // Simple HTTP response wrapper
    record HttpResponse(int statusCode, String body) {
        boolean isSuccess() { return statusCode >= 200 && statusCode < 300; }
    }

    // ==================== Server Operations ====================

    public boolean ping() throws IOException {
        HttpResponse resp = doRequest("GET", "/ping");
        return resp.isSuccess() && resp.body().contains("\"status\":\"OK\"");
    }

    public String serverVersion() throws IOException {
        HttpResponse resp = doRequest("GET", "/info");
        if (resp.isSuccess()) {
            return extractJsonString(resp.body(), "version");
        }
        return "N/A";
    }

    // ==================== Region Operations ====================

    public boolean createRegion(String name, String description) throws IOException {
        String json = new JsonBuilder()
            .add("name", name)
            .add("description", description)
            .build();
        HttpResponse resp = doRequest("POST", "/regions", json);
        return resp.isSuccess();
    }

    public int regionSize(String region) throws IOException {
        HttpResponse resp = doRequest("GET", "/cache/" + region + "/size");
        if (resp.isSuccess()) {
            return extractJsonInt(resp.body(), "size");
        }
        return -1;
    }

    // ==================== JSON Operations ====================

    /**
     * Store a JSON document.
     */
    public boolean jsonSet(String region, String key, String jsonValue) throws IOException {
        return jsonSet(region, key, jsonValue, -1);
    }

    /**
     * Store a JSON document with optional TTL.
     */
    public boolean jsonSet(String region, String key, String jsonValue, int ttlSeconds) throws IOException {
        String body = "{\"value\":" + jsonValue;
        if (ttlSeconds > 0) {
            body += ",\"ttl\":" + ttlSeconds;
        }
        body += "}";
        HttpResponse resp = doRequest("PUT", "/json/" + encode(region) + "/" + encode(key), body);
        return resp.isSuccess();
    }

    /**
     * Get a JSON document (full).
     */
    public String jsonGet(String region, String key) throws IOException {
        return jsonGet(region, key, null);
    }

    /**
     * Get a JSON document at a specific JSONPath.
     */
    public String jsonGet(String region, String key, String path) throws IOException {
        String endpoint = "/json/" + encode(region) + "/" + encode(key);
        if (path != null && !path.equals("$")) {
            endpoint += "?path=" + encode(path);
        }
        HttpResponse resp = doRequest("GET", endpoint);
        if (resp.isSuccess()) {
            return resp.body();
        }
        return null;
    }

    /**
     * Search JSON documents.
     */
    public List<Map<String, String>> jsonSearch(String region, String query) throws IOException {
        String body = "{\"query\":\"" + escapeJson(query) + "\"}";
        HttpResponse resp = doRequest("POST", "/json/" + encode(region) + "/search", body);

        List<Map<String, String>> results = new ArrayList<>();
        if (resp.isSuccess() && resp.body().contains("\"results\"")) {
            // Parse results array - simple extraction
            String resultsJson = extractJsonArray(resp.body(), "results");
            if (resultsJson != null) {
                // Parse each result object
                for (String item : splitJsonArray(resultsJson)) {
                    Map<String, String> result = new HashMap<>();
                    result.put("key", extractJsonString(item, "key"));
                    // Extract the value as raw JSON
                    int valueStart = item.indexOf("\"value\":");
                    if (valueStart >= 0) {
                        result.put("value", item.substring(valueStart + 8).replaceAll("}\\s*$", ""));
                    }
                    results.add(result);
                }
            }
        }
        return results;
    }

    /**
     * Delete a JSON document.
     */
    public boolean jsonDelete(String region, String key) throws IOException {
        HttpResponse resp = doRequest("DELETE", "/json/" + encode(region) + "/" + encode(key));
        return resp.isSuccess();
    }

    // ==================== JSON Builder Helper ====================

    static class JsonBuilder {
        private final StringBuilder sb = new StringBuilder();
        private boolean first = true;

        public JsonBuilder() { sb.append("{"); }

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

        public JsonBuilder addRaw(String key, String rawJson) {
            addComma();
            sb.append("\"").append(key).append("\":").append(rawJson);
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

        private void addComma() {
            if (!first) sb.append(",");
            first = false;
        }

        public String build() {
            sb.append("}");
            return sb.toString();
        }
    }

    // ==================== JSON Parsing Helpers ====================

    private static String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private static String encode(String s) {
        try { return URLEncoder.encode(s, "UTF-8"); }
        catch (Exception e) { return s; }
    }

    static String extractJsonString(String json, String key) {
        String search = "\"" + key + "\":\"";
        int start = json.indexOf(search);
        if (start < 0) return null;
        start += search.length();
        int end = json.indexOf("\"", start);
        return end > start ? json.substring(start, end) : null;
    }

    static int extractJsonInt(String json, String key) {
        String search = "\"" + key + "\":";
        int start = json.indexOf(search);
        if (start < 0) return 0;
        start += search.length();
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
            end++;
        }
        try { return Integer.parseInt(json.substring(start, end)); }
        catch (NumberFormatException e) { return 0; }
    }

    static String extractJsonArray(String json, String key) {
        String search = "\"" + key + "\":[";
        int start = json.indexOf(search);
        if (start < 0) return null;
        start += search.length() - 1; // include the '['
        int depth = 0;
        int end = start;
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '[') depth++;
            else if (c == ']') { depth--; if (depth == 0) { end = i + 1; break; } }
        }
        return json.substring(start, end);
    }

    static List<String> splitJsonArray(String arrayJson) {
        List<String> items = new ArrayList<>();
        if (arrayJson == null || arrayJson.length() < 2) return items;
        // Remove outer brackets
        String inner = arrayJson.substring(1, arrayJson.length() - 1).trim();
        if (inner.isEmpty()) return items;

        int depth = 0;
        int start = 0;
        for (int i = 0; i < inner.length(); i++) {
            char c = inner.charAt(i);
            if (c == '{' || c == '[') depth++;
            else if (c == '}' || c == ']') depth--;
            else if (c == ',' && depth == 0) {
                items.add(inner.substring(start, i).trim());
                start = i + 1;
            }
        }
        if (start < inner.length()) {
            items.add(inner.substring(start).trim());
        }
        return items;
    }

    // Extract a simple value from JSON (string, number, boolean)
    static String extractJsonValue(String json, String key) {
        // Try string first
        String str = extractJsonString(json, key);
        if (str != null) return str;
        // Try numeric/boolean
        String search = "\"" + key + "\":";
        int start = json.indexOf(search);
        if (start < 0) return null;
        start += search.length();
        int end = start;
        while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}') {
            end++;
        }
        return json.substring(start, end).trim();
    }

    // ==================== Display Helpers ====================

    static void printHeader(String title) {
        System.out.println();
        System.out.println("========================================================================");
        System.out.println("  " + title);
        System.out.println("========================================================================");
    }

    static void printSubHeader(String title) {
        System.out.println("\n  --- " + title + " ---");
    }

    static void printResults(List<Map<String, String>> results, String... fields) {
        System.out.println("    Found " + results.size() + " result(s):");
        for (Map<String, String> r : results) {
            String key = r.get("key");
            String value = r.getOrDefault("value", "{}");
            StringBuilder line = new StringBuilder("      - " + key + ": ");
            if (fields.length > 0) {
                List<String> parts = new ArrayList<>();
                for (String field : fields) {
                    String val = extractJsonValue(value, field);
                    if (val != null) parts.add(field + "=" + val);
                }
                line.append(String.join(", ", parts));
            } else {
                String name = extractJsonString(value, "name");
                if (name == null) name = extractJsonString(value, "product_name");
                line.append(name != null ? name : value.substring(0, Math.min(80, value.length())));
            }
            System.out.println(line);
        }
    }

    // ==================== Sample Data ====================

    private static final String[] EMPLOYEE_KEYS = {
        "employee:EMP001", "employee:EMP002", "employee:EMP003",
        "employee:EMP004", "employee:EMP005"
    };

    private static final String[] EMPLOYEE_NAMES = {
        "John Smith", "Jane Doe", "Bob Johnson", "Alice Brown", "Charlie Wilson"
    };

    private static String buildEmployee(String id, String name, String email,
                                         String department, int salary, String level,
                                         String skills, String city, String state,
                                         String zip, boolean active) {
        return new JsonBuilder()
            .add("id", id)
            .add("name", name)
            .add("email", email)
            .add("department", department)
            .add("salary", salary)
            .add("level", level)
            .addRaw("skills", skills)
            .addRaw("address", new JsonBuilder()
                .add("city", city).add("state", state).add("zip", zip).build())
            .add("active", active)
            .build();
    }

    private static String[] buildEmployees() {
        return new String[] {
            buildEmployee("EMP001", "John Smith", "john.smith@company.com",
                "Engineering", 95000, "Senior",
                "[\"Python\",\"Java\",\"Kubernetes\"]",
                "San Francisco", "CA", "94102", true),
            buildEmployee("EMP002", "Jane Doe", "jane.doe@company.com",
                "Engineering", 105000, "Lead",
                "[\"JavaScript\",\"React\",\"Node.js\"]",
                "New York", "NY", "10001", true),
            buildEmployee("EMP003", "Bob Johnson", "bob.j@company.com",
                "Sales", 85000, "Senior",
                "[\"CRM\",\"Negotiation\"]",
                "Chicago", "IL", "60601", true),
            buildEmployee("EMP004", "Alice Brown", "alice.brown@company.com",
                "Engineering", 115000, "Principal",
                "[\"Python\",\"Machine Learning\",\"TensorFlow\"]",
                "Seattle", "WA", "98101", false),
            buildEmployee("EMP005", "Charlie Wilson", "charlie.w@company.com",
                "Marketing", 78000, "Junior",
                "[\"SEO\",\"Content Marketing\"]",
                "Los Angeles", "CA", "90001", true)
        };
    }

    private static String buildProduct(String id, String name, String category,
                                        String subcategory, String brand,
                                        double price, boolean inStock, int stockCount,
                                        String tags, String specs) {
        return new JsonBuilder()
            .add("product_id", id)
            .add("product_name", name)
            .add("category", category)
            .add("subcategory", subcategory)
            .add("brand", brand)
            .add("price", price)
            .add("in_stock", inStock)
            .add("stock_count", stockCount)
            .addRaw("tags", tags)
            .addRaw("specs", specs)
            .build();
    }

    private static final String[] PRODUCT_KEYS = {
        "product:PROD001", "product:PROD002", "product:PROD003",
        "product:PROD004", "product:PROD005"
    };

    private static String[] buildProducts() {
        return new String[] {
            buildProduct("PROD001", "Mechanical Keyboard Pro", "Electronics", "Peripherals",
                "TechGear", 149.99, true, 230,
                "[\"gaming\",\"mechanical\",\"rgb\"]",
                new JsonBuilder().add("switch_type","Cherry MX Blue")
                    .add("backlighting","RGB").add("connectivity","USB-C").build()),
            buildProduct("PROD002", "Wireless Noise-Cancelling Headphones", "Electronics", "Audio",
                "SoundTech", 299.99, true, 85,
                "[\"wireless\",\"noise-cancelling\",\"bluetooth\"]",
                new JsonBuilder().add("driver_size","40mm")
                    .add("battery_life","30h").add("connectivity","Bluetooth 5.2").build()),
            buildProduct("PROD003", "Ergonomic Office Chair", "Furniture", "Chairs",
                "ComfortPlus", 449.00, true, 42,
                "[\"ergonomic\",\"office\",\"adjustable\"]",
                new JsonBuilder().add("material","Mesh")
                    .add("lumbar_support", true).add("weight_capacity","300lbs").build()),
            buildProduct("PROD004", "USB-C Hub Adapter", "Electronics", "Accessories",
                "TechGear", 39.99, false, 0,
                "[\"usb-c\",\"hub\",\"adapter\"]",
                new JsonBuilder().add("ports","7-in-1")
                    .add("hdmi", true).add("power_delivery","100W").build()),
            buildProduct("PROD005", "Standing Desk Converter", "Furniture", "Desks",
                "ErgoRise", 279.00, true, 118,
                "[\"standing-desk\",\"ergonomic\",\"adjustable\"]",
                new JsonBuilder().add("surface_area","36x24 inches")
                    .add("height_range","6-17 inches").add("weight_capacity","35lbs").build())
        };
    }

    // ==================== Demo Functions ====================

    private void demoA_StoreJson(String region) throws IOException {
        printHeader("A) STORE JSON DOCUMENTS (PUT /api/v1/json/{region}/{key})");

        String[] employees = buildEmployees();
        System.out.println("\n  Storing 5 employee records...");
        for (int i = 0; i < employees.length; i++) {
            boolean ok = jsonSet(region, EMPLOYEE_KEYS[i], employees[i]);
            System.out.printf("    [%s] %s -> %s%n",
                ok ? "OK" : "FAIL", EMPLOYEE_KEYS[i], EMPLOYEE_NAMES[i]);
        }
        System.out.println("\n  Region size: " + regionSize(region) + " entries");
    }

    private void demoB_RetrieveJson(String region) throws IOException {
        printHeader("B) RETRIEVE JSON DOCUMENTS (GET /api/v1/json/{region}/{key})");

        printSubHeader("Full Document Retrieval");
        String emp = jsonGet(region, "employee:EMP001");
        if (emp != null) {
            System.out.println("    employee:EMP001:");
            System.out.println("      " + emp);
        }

        String emp3 = jsonGet(region, "employee:EMP003");
        if (emp3 != null) {
            String name = extractJsonString(emp3, "name");
            String dept = extractJsonString(emp3, "department");
            System.out.printf("%n    employee:EMP003 -> %s, %s%n", name, dept);
        }

        String missing = jsonGet(region, "employee:EMP999");
        System.out.printf("%n    employee:EMP999 (missing) -> %s%n", missing);
    }

    private void demoC_RetrieveJsonPath(String region) throws IOException {
        printHeader("C) RETRIEVE JSON AT SPECIFIC PATH (GET with ?path=)");

        String key = "employee:EMP001";
        String[][] paths = {
            {"$.name", "Name"},
            {"$.salary", "Salary"},
            {"$.department", "Department"},
            {"$.skills", "Skills"},
            {"$.address", "Address (nested)"},
            {"$.address.city", "City (deep path)"},
            {"$.address.state", "State (deep path)"},
            {"$.active", "Active status"},
        };

        System.out.printf("%n  Querying paths on '%s':%n%n", key);
        for (String[] p : paths) {
            String value = jsonGet(region, key, p[0]);
            System.out.printf("    %-22s (%-22s) -> %s%n", p[1], p[0], value);
        }
    }

    private void demoD_UpdateJson(String region) throws IOException {
        printHeader("D) UPDATE JSON DOCUMENTS (PUT - full replace)");

        String key = "employee:EMP005";
        String original = jsonGet(region, key);
        String name = extractJsonString(original, "name");
        String salary = extractJsonValue(original, "salary");
        String dept = extractJsonString(original, "department");
        System.out.printf("%n  Original: %s - salary=$%s, dept=%s%n", name, salary, dept);

        // Build updated version
        String updated = buildEmployee("EMP005", "Charlie Wilson", "charlie.w@company.com",
            "Engineering", 92000, "Senior",
            "[\"SEO\",\"Content Marketing\",\"Python\",\"Data Analytics\"]",
            "Los Angeles", "CA", "90001", true);

        boolean ok = jsonSet(region, key, updated);
        System.out.printf("%n  Updated (PUT replace): %s%n", ok ? "OK" : "FAIL");

        String after = jsonGet(region, key);
        String newSalary = extractJsonValue(after, "salary");
        String newDept = extractJsonString(after, "department");
        System.out.printf("  After:    %s - salary=$%s, dept=%s%n", name, newSalary, newDept);

        // Restore original
        String[] employees = buildEmployees();
        jsonSet(region, key, employees[4]);
        System.out.println("\n  Restored original values for subsequent demos.");
    }

    private void demoE_SearchEquality(String region) throws IOException {
        printHeader("E) JSON SEARCH - EQUALITY ($.field=value)");

        printSubHeader("Search: $.department=Engineering");
        printResults(jsonSearch(region, "$.department=Engineering"), "name", "department", "salary");

        printSubHeader("Search: $.active=true");
        printResults(jsonSearch(region, "$.active=true"), "name", "active");

        printSubHeader("Search: $.address.state=CA");
        printResults(jsonSearch(region, "$.address.state=CA"), "name");

        printSubHeader("Search: $.level=Senior");
        printResults(jsonSearch(region, "$.level=Senior"), "name", "department", "level");
    }

    private void demoF_SearchComparison(String region) throws IOException {
        printHeader("F) JSON SEARCH - COMPARISON (>, <, >=, <=)");

        printSubHeader("Search: $.salary>100000");
        printResults(jsonSearch(region, "$.salary>100000"), "name", "salary");

        printSubHeader("Search: $.salary<90000");
        printResults(jsonSearch(region, "$.salary<90000"), "name", "salary");

        printSubHeader("Search: $.salary>=95000");
        printResults(jsonSearch(region, "$.salary>=95000"), "name", "salary");

        printSubHeader("Search: $.salary<=85000");
        printResults(jsonSearch(region, "$.salary<=85000"), "name", "salary");
    }

    private void demoG_SearchInequality(String region) throws IOException {
        printHeader("G) JSON SEARCH - INEQUALITY ($.field!=value)");

        printSubHeader("Search: $.department!=Engineering");
        printResults(jsonSearch(region, "$.department!=Engineering"), "name", "department");

        printSubHeader("Search: $.active!=true");
        printResults(jsonSearch(region, "$.active!=true"), "name", "active");
    }

    private void demoH_SearchPattern(String region) throws IOException {
        printHeader("H) JSON SEARCH - PATTERN MATCH ($.field LIKE %pattern%)");

        printSubHeader("Search: $.name LIKE %son%");
        printResults(jsonSearch(region, "$.name LIKE %son%"), "name");

        printSubHeader("Search: $.email LIKE %@company.com");
        printResults(jsonSearch(region, "$.email LIKE %@company.com"), "name", "email");

        printSubHeader("Search: $.address.zip LIKE 9%");
        printResults(jsonSearch(region, "$.address.zip LIKE 9%"), "name");
    }

    private void demoI_SearchContains(String region) throws IOException {
        printHeader("I) JSON SEARCH - ARRAY CONTAINS ($.array CONTAINS value)");

        printSubHeader("Search: $.skills CONTAINS Python");
        printResults(jsonSearch(region, "$.skills CONTAINS Python"), "name");

        printSubHeader("Search: $.skills CONTAINS CRM");
        printResults(jsonSearch(region, "$.skills CONTAINS CRM"), "name");

        printSubHeader("Search: $.skills CONTAINS JavaScript");
        printResults(jsonSearch(region, "$.skills CONTAINS JavaScript"), "name");
    }

    private void demoJ_SearchCombined(String region) throws IOException {
        printHeader("J) JSON SEARCH - COMBINED CONDITIONS (AND logic)");

        printSubHeader("Search: $.department=Engineering,$.salary>100000");
        printResults(jsonSearch(region, "$.department=Engineering,$.salary>100000"), "name", "department", "salary");

        printSubHeader("Search: $.active=true,$.address.state=CA");
        printResults(jsonSearch(region, "$.active=true,$.address.state=CA"), "name", "active");

        printSubHeader("Search: $.department=Engineering,$.active=true,$.salary>=95000");
        printResults(jsonSearch(region, "$.department=Engineering,$.active=true,$.salary>=95000"), "name", "department", "salary", "active");

        printSubHeader("Search: $.address.state!=CA,$.level=Senior");
        printResults(jsonSearch(region, "$.address.state!=CA,$.level=Senior"), "name", "level");
    }

    private void demoK_JsonWithTtl(String region) throws IOException {
        printHeader("K) STORE JSON WITH TTL (time-to-live)");

        String session = new JsonBuilder()
            .add("session_id", "sess:ABC123")
            .add("user", "jsmith")
            .add("ip", "192.168.1.100")
            .add("login_time", "2025-01-15T10:30:00Z")
            .addRaw("permissions", "[\"read\",\"write\",\"admin\"]")
            .build();

        System.out.println("\n  Storing session with 60-second TTL...");
        boolean ok = jsonSet(region, "session:ABC123", session, 60);
        System.out.printf("    Store: %s%n", ok ? "OK" : "FAIL");

        String retrieved = jsonGet(region, "session:ABC123");
        String user = extractJsonString(retrieved, "user");
        System.out.printf("    Verify: user=%s%n", user);

        String apiCache = new JsonBuilder()
            .add("cached_at", "2025-01-15T10:30:00Z")
            .addRaw("data", "{\"rates\":{\"USD\":1.0,\"EUR\":0.85,\"GBP\":0.73}}")
            .add("source", "exchange-api")
            .build();

        System.out.println("\n  Storing API cache entry with 300-second TTL...");
        ok = jsonSet(region, "api_cache:exchange_rates", apiCache, 300);
        System.out.printf("    Store: %s%n", ok ? "OK" : "FAIL");

        retrieved = jsonGet(region, "api_cache:exchange_rates");
        String source = extractJsonString(retrieved, "source");
        System.out.printf("    Verify: source=%s%n", source);
    }

    private String demoL_CrossRegion() throws IOException {
        printHeader("L) CROSS-REGION JSON OPERATIONS");

        String productsRegion = "products_json_rest_demo";
        createRegion(productsRegion, "Product catalog for JSON REST demo");

        String[] products = buildProducts();
        String[] productNames = {
            "Mechanical Keyboard Pro", "Wireless Noise-Cancelling Headphones",
            "Ergonomic Office Chair", "USB-C Hub Adapter", "Standing Desk Converter"
        };

        System.out.printf("%n  Storing %d products in '%s' region...%n", products.length, productsRegion);
        for (int i = 0; i < products.length; i++) {
            boolean ok = jsonSet(productsRegion, PRODUCT_KEYS[i], products[i]);
            System.out.printf("    [%s] %s -> %s%n", ok ? "OK" : "FAIL", PRODUCT_KEYS[i], productNames[i]);
        }

        printSubHeader("Search: $.category=Electronics");
        printResults(jsonSearch(productsRegion, "$.category=Electronics"), "product_name", "price");

        printSubHeader("Search: $.price>200");
        printResults(jsonSearch(productsRegion, "$.price>200"), "product_name", "price");

        printSubHeader("Search: $.in_stock=true,$.price<300");
        printResults(jsonSearch(productsRegion, "$.in_stock=true,$.price<300"), "product_name", "price", "in_stock");

        printSubHeader("Search: $.specs.backlighting=RGB");
        printResults(jsonSearch(productsRegion, "$.specs.backlighting=RGB"), "product_name");

        printSubHeader("Search: $.brand LIKE %Tech%");
        printResults(jsonSearch(productsRegion, "$.brand LIKE %Tech%"), "product_name", "brand");

        printSubHeader("Search: $.tags CONTAINS gaming");
        printResults(jsonSearch(productsRegion, "$.tags CONTAINS gaming"), "product_name");

        printSubHeader("Search: $.stock_count>=100");
        printResults(jsonSearch(productsRegion, "$.stock_count>=100"), "product_name", "stock_count");

        return productsRegion;
    }

    private void demoM_DeleteJson(String region, String productsRegion) throws IOException {
        printHeader("M) DELETE JSON DOCUMENTS (DELETE /api/v1/json/{region}/{key})");

        System.out.println("\n  Deleting employee records...");
        for (String key : EMPLOYEE_KEYS) {
            boolean ok = jsonDelete(region, key);
            System.out.printf("    [%s] Deleted %s%n", ok ? "OK" : "FAIL", key);
        }

        for (String key : new String[]{"session:ABC123", "api_cache:exchange_rates"}) {
            boolean ok = jsonDelete(region, key);
            System.out.printf("    [%s] Deleted %s%n", ok ? "OK" : "FAIL", key);
        }

        System.out.printf("%n  Deleting product records from '%s'...%n", productsRegion);
        for (String key : PRODUCT_KEYS) {
            boolean ok = jsonDelete(productsRegion, key);
            System.out.printf("    [%s] Deleted %s%n", ok ? "OK" : "FAIL", key);
        }

        System.out.printf("%n  Region '%s' size after cleanup: %d%n", region, regionSize(region));
        System.out.printf("  Region '%s' size after cleanup: %d%n", productsRegion, regionSize(productsRegion));
    }

    // ==================== Main ====================

    private void demoN_SlashKeys(String region) throws IOException {
        printHeader("N) KEYS WITH FORWARD SLASHES (employee/EMP001)");

        String[][] slashDocs = {
            {"employee/EMP001", "{\"name\":\"Alice Walker\",\"dept\":\"Engineering\",\"level\":\"L5\"}"},
            {"employee/EMP002", "{\"name\":\"Bob Chen\",\"dept\":\"Marketing\",\"level\":\"L4\"}"},
            {"department/eng/lead", "{\"name\":\"Carol Davis\",\"role\":\"Director\",\"reports\":42}"},
            {"config/app/v2.1/settings", "{\"theme\":\"dark\",\"locale\":\"en-US\",\"timeout\":30}"}
        };

        System.out.println("\n  Storing documents with slash keys...");
        for (String[] doc : slashDocs) {
            boolean ok = jsonSet(doc[0], parseJson(doc[1]), region, -1);
            System.out.println("    [" + (ok ? "OK" : "FAIL") + "] " + doc[0]);
        }

        System.out.println("\n  Retrieving documents by slash key...");
        for (String[] doc : slashDocs) {
            String result = jsonGet(doc[0], region, "$");
            System.out.println("    [" + (result != null ? "OK" : "FAIL") + "] " + doc[0] +
                " -> " + (result != null ? result.substring(0, Math.min(80, result.length())) + "..." : "NOT FOUND"));
        }

        System.out.println("\n  JSONPath on slash key 'employee/EMP001' ($.dept)...");
        String dept = jsonGet("employee/EMP001", region, "$.dept");
        System.out.println("    Department: " + dept);

        System.out.println("\n  Deleting slash-key documents...");
        for (String[] doc : slashDocs) {
            boolean ok = jsonDelete(doc[0], region, "$");
            System.out.println("    [" + (ok ? "OK" : "FAIL") + "] Deleted " + doc[0]);
        }
    }

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        String apiKey = args.length > 2 ? args[2] : "kub_admin_sample_key_replace_me";

        System.out.println();
        System.out.println("+======================================================================+");
        System.out.println("|                                                                      |");
        System.out.println("|   KUBER DISTRIBUTED CACHE - JSON OPERATIONS VIA REST API (Java)      |");
        System.out.println("|                                                                      |");
        System.out.println("|   Demonstrates all JSON operations through the REST/HTTP API:        |");
        System.out.println("|                                                                      |");
        System.out.println("|   a) Store JSON documents           PUT  /api/v1/json/{r}/{k}       |");
        System.out.println("|   b) Retrieve JSON documents        GET  /api/v1/json/{r}/{k}       |");
        System.out.println("|   c) Retrieve at JSONPath           GET  with ?path=$.field          |");
        System.out.println("|   d) Update JSON documents          PUT  (full replace)              |");
        System.out.println("|   e-j) Search with operators        POST /api/v1/json/{r}/search    |");
        System.out.println("|       - Equality, Comparison, Inequality, Pattern, Contains, AND     |");
        System.out.println("|   k) Store with TTL                 PUT  with ttl parameter          |");
        System.out.println("|   l) Cross-region operations        Multiple regions                 |");
        System.out.println("|   m) Delete JSON documents          DELETE /api/v1/json/{r}/{k}     |");
        System.out.println("|   n) Keys with forward slashes      employee/EMP001 etc.            |");
        System.out.println("|                                                                      |");
        System.out.println("|   v2.5.0                                                             |");
        System.out.println("+======================================================================+");
        System.out.println();

        System.out.println("Configuration:");
        System.out.println("  Host:    " + host);
        System.out.println("  Port:    " + port);
        System.out.println("  API Key: " + apiKey.substring(0, Math.min(12, apiKey.length())) + "..." +
            apiKey.substring(Math.max(0, apiKey.length() - 4)));

        String region = "employees_json_rest_demo";

        try {
            RestJsonDemo demo = new RestJsonDemo(host, port, apiKey);

            if (!demo.ping()) {
                System.out.println("\n  ✗ Cannot connect to Kuber server");
                System.exit(1);
            }
            System.out.println("\n  ✓ Connected to Kuber server at " + host + ":" + port);
            System.out.println("  ✓ Server version: " + demo.serverVersion());

            demo.createRegion(region, "Employee data for JSON REST demo");
            System.out.println("  ✓ Region: " + region);

            // Run all demos
            demo.demoA_StoreJson(region);
            demo.demoB_RetrieveJson(region);
            demo.demoC_RetrieveJsonPath(region);
            demo.demoD_UpdateJson(region);
            demo.demoE_SearchEquality(region);
            demo.demoF_SearchComparison(region);
            demo.demoG_SearchInequality(region);
            demo.demoH_SearchPattern(region);
            demo.demoI_SearchContains(region);
            demo.demoJ_SearchCombined(region);
            demo.demoK_JsonWithTtl(region);
            String productsRegion = demo.demoL_CrossRegion();
            demo.demoM_DeleteJson(region, productsRegion);
            demo.demoN_SlashKeys(region);

            printHeader("ALL DEMOS COMPLETED SUCCESSFULLY");
            System.out.println();

        } catch (Exception e) {
            System.out.println("\n  ✗ Error: " + e.getMessage());
            System.out.println("\n  Troubleshooting:");
            System.out.println("    1. Ensure Kuber server is running");
            System.out.println("    2. Check host/port (default: localhost:8080)");
            System.out.println("    3. Verify API key starts with 'kub_'");
            System.out.println("    4. Check REST API is enabled on the server");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
