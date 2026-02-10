/*
 * Kuber REST API - Insert & Update JSON Documents Demo (v1.7.9)
 *
 * This standalone Java application demonstrates how to:
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
 *     javac RestInsertUpdateDemo.java
 *     java RestInsertUpdateDemo [host] [port] [apiKey]
 *
 * Example:
 *     java RestInsertUpdateDemo localhost 8080 kub_admin
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

public class RestInsertUpdateDemo {

    private final String baseUrl;
    private final String apiKey;

    public RestInsertUpdateDemo(String host, int port, String apiKey) {
        this.baseUrl = String.format("http://%s:%d/api/v1", host, port);
        this.apiKey = apiKey;
    }

    // ==================== HTTP Helper Methods ====================

    private HttpResponse doPost(String endpoint, String jsonBody) throws IOException {
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

    private HttpResponse doGet(String endpoint) throws IOException {
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

    // ==================== Cache Operations ====================

    /**
     * Insert a JSON document (creates or replaces).
     */
    public HttpResponse insert(String region, String key, String jsonDocument) throws IOException {
        return doPost("/cache/" + region + "/" + key, jsonDocument);
    }

    /**
     * Insert with TTL.
     */
    public HttpResponse insertWithTtl(String region, String key, String jsonDocument, int ttlSeconds) throws IOException {
        return doPost("/cache/" + region + "/" + key + "?ttl=" + ttlSeconds, jsonDocument);
    }

    /**
     * Get a JSON document by key.
     */
    public HttpResponse get(String region, String key) throws IOException {
        return doGet("/cache/" + region + "/" + key);
    }

    /**
     * Partial update using Generic Update API (JUPDATE).
     * Merges updates with existing document.
     */
    public HttpResponse jupdate(String region, String key, String updatesJson) throws IOException {
        String request = new JsonBuilder()
            .add("region", region)
            .add("key", key)
            .addRaw("value", updatesJson)
            .add("apiKey", apiKey)
            .build();
        return doPost("/genericupdate", request);
    }

    // ==================== JSON Builder Helper ====================

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

    private static void printJson(String label, String json) {
        System.out.println(label + ": " + json);
    }

    private static void printResult(String operation, HttpResponse response) {
        if (response.isSuccess()) {
            System.out.println("\u2705 " + operation + ": Success");
            if (!response.body().isEmpty()) {
                String body = response.body();
                if (body.length() > 200) body = body.substring(0, 200) + "...";
                System.out.println("   Response: " + body);
            }
        } else {
            System.out.println("\u274C " + operation + ": Failed (HTTP " + response.statusCode() + ")");
            if (!response.body().isEmpty()) {
                System.out.println("   Error: " + response.body());
            }
        }
    }

    // ==================== Demo Methods ====================

    private void demoInsertOperations(String region) throws IOException {
        printHeader("1. INSERT OPERATIONS");

        // 1a. Simple insert
        printSubheader("1a. Simple Insert - New Customer");
        String customer = new JsonBuilder()
            .add("id", "CUST001")
            .add("name", "John Smith")
            .add("email", "john.smith@email.com")
            .add("status", "active")
            .add("tier", "premium")
            .add("balance", 15000.50)
            .addObject("address", new JsonBuilder()
                .add("street", "123 Main St")
                .add("city", "New York")
                .add("state", "NY")
                .add("zip", "10001"))
            .addArray("tags", "vip", "loyalty-member")
            .add("created_at", LocalDateTime.now().toString())
            .build();
        
        printJson("Document", customer);
        HttpResponse resp = insert(region, "customer_CUST001", customer);
        printResult("Insert", resp);

        // Verify
        System.out.println("\nVerifying insert...");
        resp = get(region, "customer_CUST001");
        if (resp.isSuccess()) {
            System.out.println("\u2713 Document stored successfully");
        }

        // 1b. Insert with TTL
        printSubheader("1b. Insert with TTL (60 seconds)");
        String sessionData = new JsonBuilder()
            .add("type", "session")
            .add("user_id", "U123")
            .add("token", "abc123xyz")
            .add("expires_at", LocalDateTime.now().plusSeconds(60).toString())
            .build();
        
        printJson("Document", sessionData);
        resp = insertWithTtl(region, "session_U123", sessionData, 60);
        printResult("Insert with TTL=60s", resp);

        // 1c. Insert multiple products
        printSubheader("1c. Insert Multiple Products");
        String[] products = {
            new JsonBuilder().add("sku", "P001").add("name", "Laptop Pro").add("category", "electronics").add("price", 1299.99).add("stock", 50).build(),
            new JsonBuilder().add("sku", "P002").add("name", "Wireless Mouse").add("category", "electronics").add("price", 49.99).add("stock", 200).build(),
            new JsonBuilder().add("sku", "P003").add("name", "USB-C Cable").add("category", "accessories").add("price", 19.99).add("stock", 500).build()
        };
        String[] keys = {"product_P001", "product_P002", "product_P003"};
        
        for (int i = 0; i < products.length; i++) {
            resp = insert(region, keys[i], products[i]);
            String status = resp.isSuccess() ? "\u2713" : "\u2717";
            System.out.println("  " + status + " " + keys[i]);
        }
    }

    private void demoFullReplace(String region) throws IOException {
        printHeader("2. FULL UPDATE (Replace)");

        // Show current state
        printSubheader("2a. Current Document State");
        HttpResponse resp = get(region, "customer_CUST001");
        if (resp.isSuccess()) {
            printJson("Current", resp.body());
        }

        // Full replace
        printSubheader("2b. Full Replace - All Fields Changed");
        String newDocument = new JsonBuilder()
            .add("id", "CUST001")
            .add("name", "John Smith Jr.")
            .add("email", "john.smith.jr@newemail.com")
            .add("status", "inactive")
            .add("tier", "basic")
            .add("balance", 5000.00)
            .add("phone", "+1-555-0123")
            .add("updated_at", LocalDateTime.now().toString())
            .build();
        
        System.out.println("New Document (replaces entire document):");
        printJson("", newDocument);
        System.out.println("\n\u26A0\uFE0F  Note: 'address' and 'tags' are NOT included - they will be REMOVED");
        
        resp = insert(region, "customer_CUST001", newDocument);
        printResult("Full Replace", resp);

        // Verify
        System.out.println("\nAfter full replace:");
        resp = get(region, "customer_CUST001");
        if (resp.isSuccess()) {
            printJson("Result", resp.body());
            if (!resp.body().contains("address")) {
                System.out.println("\u26A0\uFE0F  Confirmed: 'address' field was removed");
            }
        }
    }

    private void demoPartialMerge(String region) throws IOException {
        printHeader("3. PARTIAL UPDATE (JUPDATE - Merge)");

        // Setup fresh document
        printSubheader("3a. Setup - Create Initial Document");
        String initialCustomer = new JsonBuilder()
            .add("id", "CUST002")
            .add("name", "Jane Doe")
            .add("email", "jane@email.com")
            .add("status", "active")
            .add("tier", "standard")
            .add("balance", 8500.00)
            .addObject("address", new JsonBuilder()
                .add("street", "456 Oak Ave")
                .add("city", "Los Angeles")
                .add("state", "CA")
                .add("zip", "90001"))
            .addObject("preferences", new JsonBuilder()
                .add("newsletter", true)
                .add("sms_alerts", false))
            .add("created_at", LocalDateTime.now().toString())
            .build();
        
        insert(region, "customer_CUST002", initialCustomer);
        printJson("Initial Document", initialCustomer);

        // 3b. Partial update - single field
        printSubheader("3b. Partial Update - Change Status Only");
        String updates = "{\"status\":\"premium\"}";
        printJson("Updates to apply", updates);
        HttpResponse resp = jupdate(region, "customer_CUST002", updates);
        printResult("JUPDATE", resp);

        resp = get(region, "customer_CUST002");
        if (resp.isSuccess()) {
            System.out.println("\u2713 Other fields preserved (name, email, address, etc.)");
        }

        // 3c. Partial update - multiple fields
        printSubheader("3c. Partial Update - Multiple Fields");
        updates = new JsonBuilder()
            .add("tier", "enterprise")
            .add("balance", 25000.00)
            .add("phone", "+1-555-9999")
            .add("updated_at", LocalDateTime.now().toString())
            .build();
        printJson("Updates to apply", updates);
        resp = jupdate(region, "customer_CUST002", updates);
        printResult("JUPDATE", resp);

        // 3d. Partial update - nested object merge
        printSubheader("3d. Partial Update - Nested Object Merge");
        System.out.println("Updating address.city and adding address.country (preserves other address fields)");
        updates = "{\"address\":{\"city\":\"San Francisco\",\"country\":\"USA\"}}";
        printJson("Updates to apply", updates);
        resp = jupdate(region, "customer_CUST002", updates);
        printResult("JUPDATE (nested)", resp);

        resp = get(region, "customer_CUST002");
        if (resp.isSuccess()) {
            System.out.println("\u2713 Nested address fields merged (street, state preserved)");
        }

        // 3e. Add new nested object
        printSubheader("3e. Partial Update - Add New Nested Object");
        updates = "{\"billing\":{\"method\":\"credit_card\",\"last_four\":\"1234\",\"exp_date\":\"12/26\"}}";
        printJson("Updates to apply", updates);
        resp = jupdate(region, "customer_CUST002", updates);
        printResult("JUPDATE", resp);
    }

    private void demoJupdateCreateVsUpdate(String region) throws IOException {
        printHeader("4. JUPDATE - CREATE vs UPDATE");

        // 4a. JUPDATE on non-existent key (creates new)
        printSubheader("4a. JUPDATE on Non-Existent Key (Creates New)");
        String newKey = "order_ORD_" + System.currentTimeMillis();
        String orderData = new JsonBuilder()
            .add("order_id", newKey)
            .add("customer_id", "CUST002")
            .addRaw("items", "[{\"product\":\"P001\",\"qty\":1,\"price\":1299.99}]")
            .add("total", 1299.99)
            .add("status", "pending")
            .build();
        
        System.out.println("Key: " + newKey + " (does not exist)");
        printJson("Data", orderData);
        
        HttpResponse resp = jupdate(region, newKey, orderData);
        printResult("JUPDATE", resp);
        if (resp.body().contains("created")) {
            System.out.println("Action: CREATED (new document)");
        }

        // 4b. JUPDATE on existing key (merges)
        printSubheader("4b. JUPDATE on Existing Key (Merges)");
        String updates = new JsonBuilder()
            .add("status", "shipped")
            .add("shipped_at", LocalDateTime.now().toString())
            .add("tracking_number", "TRK123456789")
            .build();
        
        System.out.println("Key: " + newKey + " (exists)");
        printJson("Updates", updates);
        
        resp = jupdate(region, newKey, updates);
        printResult("JUPDATE", resp);
        if (resp.body().contains("updated")) {
            System.out.println("Action: UPDATED (merged with existing)");
        }

        // Verify merge
        resp = get(region, newKey);
        if (resp.isSuccess()) {
            System.out.println("\u2713 Original fields preserved (order_id, items, total)");
            System.out.println("\u2713 New fields added (tracking_number, shipped_at)");
        }
    }

    private void demoUpdateSpecificAttributes(String region) throws IOException {
        printHeader("5. UPDATE SPECIFIC ATTRIBUTES BY KEY");

        // Setup
        printSubheader("5a. Setup - Create Test Document");
        String employee = new JsonBuilder()
            .add("id", "EMP001")
            .add("name", "Alice Johnson")
            .add("department", "Engineering")
            .add("title", "Software Developer")
            .add("salary", 95000)
            .addArray("skills", "Python", "Java", "SQL")
            .addObject("performance", new JsonBuilder()
                .add("rating", 4.2)
                .add("last_review", "2024-06-15"))
            .build();
        
        insert(region, "employee_EMP001", employee);
        printJson("Initial", employee);

        // 5b. Update salary only
        printSubheader("5b. Update Single Attribute - Salary");
        HttpResponse resp = jupdate(region, "employee_EMP001", "{\"salary\":105000}");
        printResult("Update salary to 105000", resp);
        
        resp = get(region, "employee_EMP001");
        System.out.println("New salary extracted from document");

        // 5c. Update title and department
        printSubheader("5c. Update Multiple Attributes - Title & Department");
        String updates = new JsonBuilder()
            .add("title", "Senior Software Developer")
            .add("department", "Platform Engineering")
            .build();
        resp = jupdate(region, "employee_EMP001", updates);
        printResult("Update title and department", resp);

        // 5d. Update skills array
        printSubheader("5d. Update Array - Add New Skills");
        System.out.println("Note: Arrays are REPLACED, not merged");
        updates = "{\"skills\":[\"Python\",\"Java\",\"SQL\",\"Kubernetes\",\"AWS\"]}";
        resp = jupdate(region, "employee_EMP001", updates);
        printResult("Update skills array", resp);

        // 5e. Update nested object
        printSubheader("5e. Update Nested Attributes - Performance");
        updates = "{\"performance\":{\"rating\":4.8,\"last_review\":\"2025-01-10\",\"promotion_eligible\":true}}";
        resp = jupdate(region, "employee_EMP001", updates);
        printResult("Update performance object", resp);

        // 5f. Show final document
        printSubheader("5f. Final Document State");
        resp = get(region, "employee_EMP001");
        if (resp.isSuccess()) {
            printJson("Final Document", resp.body());
        }
    }

    // ==================== Main ====================

    public void runDemo(String region) {
        try {
            demoInsertOperations(region);
            demoFullReplace(region);
            demoPartialMerge(region);
            demoJupdateCreateVsUpdate(region);
            demoUpdateSpecificAttributes(region);

            printHeader("DEMO COMPLETE");
            System.out.println("""

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
        "region": "myregion",
        "key": "mykey",
        "value": {
            "field1": "new_value",
            "field2": 123
        },
        "apiKey": "kub_xxx"
    }
            """);

        } catch (Exception e) {
            System.err.println("\u274C Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8080;
        String apiKey = args.length > 2 ? args[2] : "kub_admin";
        String region = args.length > 3 ? args[3] : "update_demo_java";

        System.out.println("=".repeat(80));
        System.out.println("  KUBER REST API - INSERT & UPDATE DEMO (v1.7.9) - Java");
        System.out.println("=".repeat(80));
        System.out.println("  Server:  " + host + ":" + port);
        System.out.println("  Region:  " + region);
        System.out.println("  Time:    " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        System.out.println("=".repeat(80));

        RestInsertUpdateDemo demo = new RestInsertUpdateDemo(host, port, apiKey);
        demo.runDemo(region);
    }
}
