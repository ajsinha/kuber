/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 * Patent Pending: Certain architectural patterns and implementations described in
 * this module may be subject to patent applications.
 */
package com.kuber.client.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuber.client.KuberRestClient;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * Comprehensive examples demonstrating all capabilities of the Kuber Java REST client
 * using pure HTTP REST API endpoints.
 * 
 * v2.1.0: API Key authentication required.
 * 
 * Features demonstrated:
 * - Server operations (ping, info, status, stats)
 * - Basic key-value operations (GET, SET, MGET, MSET)
 * - Key pattern search (KEYS)
 * - Hash operations (HGET, HSET, HMSET, HGETALL)
 * - Region management (CREATE, SELECT, LIST, PURGE, DELETE)
 * - JSON operations with specific region storage
 * - Deep JSON search with multiple operators
 * - Cross-region operations
 * - Bulk import/export
 * 
 * Usage:
 *   java -cp kuber-client.jar com.kuber.client.examples.KuberRestExample [host] [port] [api-key]
 */
public class KuberRestExample {
    
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: KuberRestExample <host> <port> <api-key>");
            System.err.println("Example: KuberRestExample localhost 8080 kub_your_api_key_here");
            System.exit(1);
        }
        
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String apiKey = args[2];
        
        if (!apiKey.startsWith("kub_")) {
            System.err.println("Error: API key must start with 'kub_'");
            System.exit(1);
        }
        
        System.out.println("""
            ╔══════════════════════════════════════════════════════════════════════╗
            ║   KUBER JAVA CLIENT - HTTP REST API (v2.4.0)                         ║
            ║                                                                      ║
            ║   Protocol: HTTP REST (API Key Authentication)                       ║
            ╚══════════════════════════════════════════════════════════════════════╝
            """);
        System.out.printf("Connecting to: http://%s:%d%n", host, port);
        System.out.println("Authentication: API Key\n");
        
        try (KuberRestClient client = new KuberRestClient(host, port, apiKey)) {
            // Run all examples
            example1_ServerOperations(client);
            example2_BasicOperations(client);
            example3_MgetMsetOperations(client);
            example4_KeyPatternSearch(client);
            example5_RegionOperations(client);
            example6_HashOperations(client);
            example7_JsonOperationsWithRegions(client);
            example8_JsonDeepSearch(client);
            example9_CrossRegionSearch(client);
            example10_BulkOperations(client);
            
            // Cleanup
            cleanup(client);
            
            printSection("ALL EXAMPLES COMPLETED SUCCESSFULLY");
            System.out.println("""
                
                This REST client demonstrated:
                ✓ Server operations (ping, info, status, stats)
                ✓ Basic key-value operations (GET, SET, DELETE, EXISTS)
                ✓ Multi-key operations (MGET, MSET)
                ✓ Key pattern search (KEYS with wildcards)
                ✓ Region management (CREATE, SELECT, LIST, PURGE, DELETE)
                ✓ Hash operations (HGET, HSET, HMSET, HGETALL, HKEYS)
                ✓ JSON storage in specific regions with TTL
                ✓ Deep JSON search with multiple operators
                ✓ Cross-region JSON search
                ✓ Bulk import/export operations
                
                NOTE: This client uses pure HTTP REST API.
                      No Redis protocol or dependencies required.
                """);
            
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    // ==================== Example 1: Server Operations ====================
    
    private static void example1_ServerOperations(KuberRestClient client) throws IOException {
        printSection("1. SERVER OPERATIONS");
        
        // Ping
        boolean isAlive = client.ping();
        printResult("PING", isAlive ? "OK" : "FAILED");
        
        // Info
        JsonNode info = client.info();
        printResult("INFO", info != null ? info.toString().substring(0, Math.min(200, info.toString().length())) + "..." : "null");
        
        // Status
        JsonNode status = client.status();
        printResult("STATUS", status != null ? status.toString().substring(0, Math.min(200, status.toString().length())) + "..." : "null");
        
        // Stats
        JsonNode stats = client.stats();
        printResult("STATS", stats != null ? stats.toString().substring(0, Math.min(200, stats.toString().length())) + "..." : "null");
    }
    
    // ==================== Example 2: Basic Operations ====================
    
    private static void example2_BasicOperations(KuberRestClient client) throws IOException {
        printSection("2. BASIC KEY-VALUE OPERATIONS (GET, SET)");
        
        // Simple SET and GET
        client.set("greeting", "Hello, Kuber REST!");
        String value = client.get("greeting");
        printResult("SET/GET 'greeting'", value);
        
        // SET with TTL
        client.set("temp_key", "expires in 60 seconds", Duration.ofSeconds(60));
        printResult("SET with TTL (60s)", client.get("temp_key"));
        printResult("TTL remaining", String.valueOf(client.ttl("temp_key")));
        
        // SET with Duration
        client.set("session_data", "user session", Duration.ofHours(1));
        printResult("SET with Duration (1 hour)", "TTL: " + client.ttl("session_data") + " seconds");
        
        // Check EXISTS
        printResult("EXISTS 'greeting'", String.valueOf(client.exists("greeting")));
        printResult("EXISTS 'nonexistent'", String.valueOf(client.exists("nonexistent")));
        
        // DELETE
        client.set("to_delete", "delete me");
        printResult("GET before DELETE", client.get("to_delete"));
        boolean deleted = client.delete("to_delete");
        printResult("DELETE result", String.valueOf(deleted));
        printResult("GET after DELETE", String.valueOf(client.get("to_delete")));
        
        // EXPIRE
        client.set("expire_test", "will expire");
        client.expire("expire_test", 120);
        printResult("TTL after EXPIRE(120)", String.valueOf(client.ttl("expire_test")));
        
        // DBSIZE
        printResult("DBSIZE (default region)", String.valueOf(client.dbSize()));
    }
    
    // ==================== Example 3: MGET/MSET Operations ====================
    
    private static void example3_MgetMsetOperations(KuberRestClient client) throws IOException {
        printSection("3. MULTI-KEY OPERATIONS (MGET, MSET)");
        
        // MSET - Set multiple keys at once
        Map<String, String> userData = new LinkedHashMap<>();
        userData.put("user:1:name", "Alice");
        userData.put("user:1:email", "alice@example.com");
        userData.put("user:1:role", "admin");
        userData.put("user:2:name", "Bob");
        userData.put("user:2:email", "bob@example.com");
        userData.put("user:2:role", "developer");
        userData.put("user:3:name", "Charlie");
        userData.put("user:3:email", "charlie@example.com");
        userData.put("user:3:role", "analyst");
        
        client.mset(userData);
        System.out.println("  MSET: Set 9 keys for 3 users");
        
        // MGET - Get multiple keys at once
        List<String> names = client.mget(Arrays.asList("user:1:name", "user:2:name", "user:3:name"), "default");
        printResult("MGET all names", names.toString());
        
        List<String> emails = client.mget(Arrays.asList("user:1:email", "user:2:email", "user:3:email"), "default");
        printResult("MGET all emails", emails.toString());
        
        // MGET with some missing keys
        List<String> mixed = client.mget(Arrays.asList("user:1:name", "user:99:name", "user:2:name"), "default");
        printResult("MGET with missing key (user:99)", mixed.toString());
    }
    
    // ==================== Example 4: Key Pattern Search ====================
    
    private static void example4_KeyPatternSearch(KuberRestClient client) throws IOException {
        printSection("4. KEY PATTERN SEARCH (KEYS)");
        
        // Create test data with various patterns
        Map<String, String> testData = new LinkedHashMap<>();
        testData.put("product:electronics:1001", "Laptop");
        testData.put("product:electronics:1002", "Smartphone");
        testData.put("product:electronics:1003", "Tablet");
        testData.put("product:furniture:2001", "Chair");
        testData.put("product:furniture:2002", "Desk");
        testData.put("order:2024:001", "Order Jan");
        testData.put("order:2024:002", "Order Feb");
        testData.put("order:2025:001", "Order New Year");
        testData.put("session:abc123", "Session 1");
        testData.put("session:def456", "Session 2");
        
        client.mset(testData);
        System.out.println("  Created test keys: product:*, order:*, session:*");
        
        // Find all keys
        List<String> allKeys = client.keys("*");
        printResult("KEYS * (all keys count)", String.valueOf(allKeys.size()));
        
        // Find product keys
        List<String> productKeys = client.keys("product:*");
        printResult("KEYS product:*", productKeys.toString());
        
        // Find electronics products only
        List<String> electronicsKeys = client.keys("product:electronics:*");
        printResult("KEYS product:electronics:*", electronicsKeys.toString());
        
        // Find order keys
        List<String> orderKeys = client.keys("order:*");
        printResult("KEYS order:*", orderKeys.toString());
        
        // Find session keys
        List<String> sessionKeys = client.keys("session:*");
        printResult("KEYS session:*", sessionKeys.toString());
    }
    
    // ==================== Example 5: Region Operations ====================
    
    private static void example5_RegionOperations(KuberRestClient client) throws IOException {
        printSection("5. REGION OPERATIONS (CREATE, SELECT, LIST)");
        
        // List existing regions
        List<Map<String, Object>> regions = client.listRegions();
        List<String> regionNames = new ArrayList<>();
        for (Map<String, Object> r : regions) {
            regionNames.add((String) r.get("name"));
        }
        printResult("Initial regions", regionNames.toString());
        
        // Create custom regions
        client.createRegion("inventory_rest_java", "Product inventory data");
        client.createRegion("customers_rest_java", "Customer information");
        client.createRegion("analytics_rest_java", "Analytics and metrics");
        System.out.println("  Created 3 new regions: inventory_rest_java, customers_rest_java, analytics_rest_java");
        
        // List regions again
        regions = client.listRegions();
        regionNames.clear();
        for (Map<String, Object> r : regions) {
            regionNames.add((String) r.get("name"));
        }
        printResult("Regions after creation", regionNames.toString());
        
        // Select and use inventory region
        client.selectRegion("inventory_rest_java");
        System.out.println("\n  Selected region: " + client.getCurrentRegion());
        
        // Store data in inventory region
        client.set("sku:1001", "Laptop - Stock: 50");
        client.set("sku:1002", "Mouse - Stock: 200");
        client.set("sku:1003", "Keyboard - Stock: 150");
        
        printResult("DBSIZE in inventory_rest_java", String.valueOf(client.dbSize()));
        printResult("KEYS in inventory_rest_java", client.keys("*").toString());
        
        // Select customers region
        client.selectRegion("customers_rest_java");
        System.out.println("\n  Selected region: " + client.getCurrentRegion());
        
        // Store data in customers region
        client.set("cust:C001", "Acme Corp");
        client.set("cust:C002", "TechStart Inc");
        
        printResult("DBSIZE in customers_rest_java", String.valueOf(client.dbSize()));
        
        // Verify isolation
        String laptop = client.get("sku:1001");
        printResult("GET sku:1001 from customers (isolation test)", String.valueOf(laptop));
        
        // Get region info
        JsonNode regionInfo = client.getRegion("inventory_rest_java");
        printResult("\nRegion info for inventory_rest_java", 
            regionInfo != null ? regionInfo.toString() : "null");
        
        // Switch back to default
        client.selectRegion("default");
        System.out.println("\n  Back to region: " + client.getCurrentRegion());
    }
    
    // ==================== Example 6: Hash Operations ====================
    
    private static void example6_HashOperations(KuberRestClient client) throws IOException {
        printSection("6. HASH OPERATIONS (HGET, HSET, HMSET, HGETALL)");
        
        // HSET - Set individual fields
        client.hset("user:profile:1", "name", "Alice Johnson");
        client.hset("user:profile:1", "email", "alice@example.com");
        client.hset("user:profile:1", "age", "28");
        client.hset("user:profile:1", "city", "New York");
        System.out.println("  HSET: Set 4 fields for user:profile:1");
        
        // HGET - Get individual fields
        printResult("HGET name", client.hget("user:profile:1", "name"));
        printResult("HGET email", client.hget("user:profile:1", "email"));
        
        // HMSET - Set multiple fields at once
        Map<String, String> profile2 = new LinkedHashMap<>();
        profile2.put("name", "Bob Smith");
        profile2.put("email", "bob@example.com");
        profile2.put("age", "35");
        profile2.put("city", "San Francisco");
        
        client.hmset("user:profile:2", profile2);
        System.out.println("\n  HMSET: Set 4 fields for user:profile:2");
        
        // HMGET - Get multiple fields
        List<String> fields = client.hmget("user:profile:2", "name", "email", "city");
        printResult("HMGET name, email, city", fields.toString());
        
        // HGETALL - Get all fields
        Map<String, String> allFields = client.hgetall("user:profile:1");
        printResult("HGETALL user:profile:1", allFields.toString());
        
        // HKEYS - Get all field names
        List<String> keys = client.hkeys("user:profile:1");
        printResult("HKEYS", keys.toString());
        
        // HDEL - Delete field
        client.hdel("user:profile:1", "age");
        printResult("After HDEL 'age', remaining keys", client.hkeys("user:profile:1").toString());
    }
    
    // ==================== Example 7: JSON Operations with Regions ====================
    
    private static void example7_JsonOperationsWithRegions(KuberRestClient client) throws IOException {
        printSection("7. JSON OPERATIONS WITH REGION STORAGE");
        
        // Create a products region for JSON documents
        client.createRegion("products_json_rest_java", "Product catalog as JSON");
        
        System.out.println("\n  Storing products in 'products_json_rest_java' region:");
        
        // Store JSON products using Maps
        Map<String, Object> product1 = new LinkedHashMap<>();
        product1.put("id", "prod:1001");
        product1.put("name", "Wireless Headphones");
        product1.put("brand", "AudioTech");
        product1.put("price", 79.99);
        product1.put("category", "Electronics");
        product1.put("in_stock", true);
        product1.put("stock_count", 150);
        product1.put("tags", Arrays.asList("wireless", "bluetooth", "audio"));
        
        client.jsonSet("prod:1001", product1, "products_json_rest_java");
        System.out.println("    Stored: prod:1001 - Wireless Headphones");
        
        Map<String, Object> product2 = new LinkedHashMap<>();
        product2.put("id", "prod:1002");
        product2.put("name", "Mechanical Keyboard");
        product2.put("brand", "KeyMaster");
        product2.put("price", 149.99);
        product2.put("category", "Electronics");
        product2.put("in_stock", true);
        product2.put("stock_count", 75);
        product2.put("tags", Arrays.asList("mechanical", "keyboard", "gaming"));
        
        client.jsonSet("prod:1002", product2, "products_json_rest_java");
        System.out.println("    Stored: prod:1002 - Mechanical Keyboard");
        
        Map<String, Object> product3 = new LinkedHashMap<>();
        product3.put("id", "prod:1003");
        product3.put("name", "Ergonomic Office Chair");
        product3.put("brand", "ComfortPlus");
        product3.put("price", 349.99);
        product3.put("category", "Furniture");
        product3.put("in_stock", false);
        product3.put("stock_count", 0);
        product3.put("tags", Arrays.asList("ergonomic", "office", "chair"));
        
        client.jsonSet("prod:1003", product3, "products_json_rest_java");
        System.out.println("    Stored: prod:1003 - Ergonomic Office Chair");
        
        Map<String, Object> product4 = new LinkedHashMap<>();
        product4.put("id", "prod:2001");
        product4.put("name", "USB-C Hub");
        product4.put("brand", "ConnectPro");
        product4.put("price", 45.99);
        product4.put("category", "Electronics");
        product4.put("in_stock", true);
        product4.put("stock_count", 300);
        product4.put("tags", Arrays.asList("usb-c", "hub", "laptop"));
        
        client.jsonSet("prod:2001", product4, "products_json_rest_java");
        System.out.println("    Stored: prod:2001 - USB-C Hub");
        
        // Retrieve JSON
        System.out.println("\n  Retrieving products:");
        JsonNode headphones = client.jsonGet("prod:1001", "$", "products_json_rest_java");
        printResult("GET prod:1001", headphones != null ? headphones.toPrettyString() : "null");
        
        // Create orders region with TTL
        client.createRegion("orders_json_rest_java", "Customer orders");
        
        Map<String, Object> shipping1 = new LinkedHashMap<>();
        shipping1.put("method", "express");
        shipping1.put("address", "123 Main St, NYC");
        
        Map<String, Object> order1 = new LinkedHashMap<>();
        order1.put("order_id", "ord:10001");
        order1.put("customer_id", "cust:C001");
        order1.put("status", "shipped");
        order1.put("total", 225.98);
        order1.put("shipping", shipping1);
        
        client.jsonSet("ord:10001", order1, "orders_json_rest_java", Duration.ofDays(30));
        System.out.println("\n  Stored: ord:10001 - shipped - $225.98 (30-day TTL)");
        
        Map<String, Object> shipping2 = new LinkedHashMap<>();
        shipping2.put("method", "standard");
        shipping2.put("address", "456 Oak Ave, SF");
        
        Map<String, Object> order2 = new LinkedHashMap<>();
        order2.put("order_id", "ord:10002");
        order2.put("customer_id", "cust:C002");
        order2.put("status", "pending");
        order2.put("total", 149.99);
        order2.put("shipping", shipping2);
        
        client.jsonSet("ord:10002", order2, "orders_json_rest_java", Duration.ofDays(30));
        System.out.println("  Stored: ord:10002 - pending - $149.99 (30-day TTL)");
        
        Map<String, Object> shipping3 = new LinkedHashMap<>();
        shipping3.put("method", "express");
        shipping3.put("address", "123 Main St, NYC");
        
        Map<String, Object> order3 = new LinkedHashMap<>();
        order3.put("order_id", "ord:10003");
        order3.put("customer_id", "cust:C001");
        order3.put("status", "delivered");
        order3.put("total", 395.98);
        order3.put("shipping", shipping3);
        
        client.jsonSet("ord:10003", order3, "orders_json_rest_java", Duration.ofDays(30));
        System.out.println("  Stored: ord:10003 - delivered - $395.98 (30-day TTL)");
    }
    
    // ==================== Example 8: JSON Deep Search ====================
    
    private static void example8_JsonDeepSearch(KuberRestClient client) throws IOException {
        printSection("8. JSON DEEP SEARCH");
        
        // Search by equality
        System.out.println("\n  === SEARCH BY EQUALITY ===");
        
        List<JsonNode> results = client.jsonSearch("$.category=Electronics", "products_json_rest_java");
        printResult("$.category=Electronics", results.size() + " results");
        for (JsonNode doc : results) {
            JsonNode value = doc.get("value");
            if (value != null) {
                System.out.printf("    - %s ($%s)%n", 
                    value.path("name").asText(), 
                    value.path("price").asText());
            }
        }
        
        results = client.jsonSearch("$.in_stock=true", "products_json_rest_java");
        printResult("$.in_stock=true", results.size() + " results");
        for (JsonNode doc : results) {
            JsonNode value = doc.get("value");
            if (value != null) {
                System.out.printf("    - %s (Stock: %s)%n", 
                    value.path("name").asText(), 
                    value.path("stock_count").asText());
            }
        }
        
        // Search by comparison
        System.out.println("\n  === SEARCH BY COMPARISON ===");
        
        results = client.jsonSearch("$.price>100", "products_json_rest_java");
        printResult("$.price>100", results.size() + " results");
        for (JsonNode doc : results) {
            JsonNode value = doc.get("value");
            if (value != null) {
                System.out.printf("    - %s ($%s)%n", 
                    value.path("name").asText(), 
                    value.path("price").asText());
            }
        }
        
        results = client.jsonSearch("$.stock_count>=100", "products_json_rest_java");
        printResult("$.stock_count>=100", results.size() + " results");
        for (JsonNode doc : results) {
            JsonNode value = doc.get("value");
            if (value != null) {
                System.out.printf("    - %s (Stock: %s)%n", 
                    value.path("name").asText(), 
                    value.path("stock_count").asText());
            }
        }
        
        // Search with inequality
        System.out.println("\n  === SEARCH WITH INEQUALITY ===");
        
        results = client.jsonSearch("$.category!=Electronics", "products_json_rest_java");
        printResult("$.category!=Electronics", results.size() + " results");
        for (JsonNode doc : results) {
            JsonNode value = doc.get("value");
            if (value != null) {
                System.out.printf("    - %s (Category: %s)%n", 
                    value.path("name").asText(), 
                    value.path("category").asText());
            }
        }
        
        // Search with pattern matching
        System.out.println("\n  === SEARCH WITH PATTERN MATCHING ===");
        
        results = client.jsonSearch("$.name LIKE %Keyboard%", "products_json_rest_java");
        printResult("$.name LIKE %Keyboard%", results.size() + " results");
        for (JsonNode doc : results) {
            JsonNode value = doc.get("value");
            if (value != null) {
                System.out.printf("    - %s%n", value.path("name").asText());
            }
        }
        
        // Search array contains
        System.out.println("\n  === SEARCH ARRAY CONTAINS ===");
        
        results = client.jsonSearch("$.tags CONTAINS wireless", "products_json_rest_java");
        printResult("$.tags CONTAINS wireless", results.size() + " results");
        for (JsonNode doc : results) {
            JsonNode value = doc.get("value");
            if (value != null) {
                System.out.printf("    - %s (Tags: %s)%n", 
                    value.path("name").asText(), 
                    value.path("tags").toString());
            }
        }
        
        // Combined conditions
        System.out.println("\n  === COMBINED CONDITIONS ===");
        
        results = client.jsonSearch("$.in_stock=true,$.price<100", "products_json_rest_java");
        printResult("$.in_stock=true,$.price<100", results.size() + " results");
        for (JsonNode doc : results) {
            JsonNode value = doc.get("value");
            if (value != null) {
                System.out.printf("    - %s ($%s, In Stock)%n", 
                    value.path("name").asText(), 
                    value.path("price").asText());
            }
        }
    }
    
    // ==================== Example 9: Cross-Region Search ====================
    
    private static void example9_CrossRegionSearch(KuberRestClient client) throws IOException {
        printSection("9. CROSS-REGION JSON SEARCH");
        
        // Search in products region
        System.out.println("\n  === SEARCH IN 'products_json_rest_java' REGION ===");
        
        List<JsonNode> results = client.jsonSearch("$.category=Electronics", "products_json_rest_java");
        printResult("$.category=Electronics", results.size() + " results");
        for (JsonNode doc : results) {
            JsonNode value = doc.get("value");
            if (value != null) {
                System.out.printf("    - %s%n", value.path("name").asText());
            }
        }
        
        // Search in orders region
        System.out.println("\n  === SEARCH IN 'orders_json_rest_java' REGION ===");
        
        results = client.jsonSearch("$.status=shipped", "orders_json_rest_java");
        printResult("$.status=shipped", results.size() + " results");
        for (JsonNode doc : results) {
            JsonNode value = doc.get("value");
            if (value != null) {
                System.out.printf("    - Customer %s, Total: $%s%n", 
                    value.path("customer_id").asText(), 
                    value.path("total").asText());
            }
        }
        
        results = client.jsonSearch("$.customer_id=cust:C001", "orders_json_rest_java");
        printResult("$.customer_id=cust:C001", results.size() + " results");
        for (JsonNode doc : results) {
            JsonNode value = doc.get("value");
            if (value != null) {
                System.out.printf("    - Status: %s, Total: $%s%n", 
                    value.path("status").asText(), 
                    value.path("total").asText());
            }
        }
        
        results = client.jsonSearch("$.total>200", "orders_json_rest_java");
        printResult("$.total>200", results.size() + " results");
        for (JsonNode doc : results) {
            JsonNode value = doc.get("value");
            if (value != null) {
                System.out.printf("    - $%s%n", value.path("total").asText());
            }
        }
        
        results = client.jsonSearch("$.shipping.method=express", "orders_json_rest_java");
        printResult("$.shipping.method=express", results.size() + " results");
        for (JsonNode doc : results) {
            JsonNode value = doc.get("value");
            if (value != null) {
                System.out.printf("    - %s%n", value.path("shipping").path("address").asText());
            }
        }
    }
    
    // ==================== Example 10: Bulk Operations ====================
    
    private static void example10_BulkOperations(KuberRestClient client) throws IOException {
        printSection("10. BULK OPERATIONS (IMPORT, EXPORT)");
        
        // Create a region for bulk demo
        client.createRegion("bulk_demo_rest_java", "Bulk operations demo");
        
        // Bulk import
        List<Map<String, Object>> entries = new ArrayList<>();
        
        for (int i = 1; i <= 5; i++) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("key", "item:" + i);
            
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("name", "Item " + i);
            value.put("price", 10.99 * i);
            entry.put("value", value);
            
            if (i == 3) {
                entry.put("ttl", 3600);  // 1 hour TTL
            }
            
            entries.add(entry);
        }
        
        JsonNode result = client.bulkImport(entries, "bulk_demo_rest_java");
        printResult("Bulk import 5 items", result != null ? result.toString() : "success");
        
        // Verify import
        printResult("DBSIZE after import", String.valueOf(client.dbSize("bulk_demo_rest_java")));
        printResult("KEYS after import", client.keys("*", "bulk_demo_rest_java").toString());
        
        // Bulk export
        List<JsonNode> exported = client.bulkExport("item:*", "bulk_demo_rest_java");
        printResult("Bulk export item:*", exported.size() + " items");
        for (int i = 0; i < Math.min(3, exported.size()); i++) {
            JsonNode entry = exported.get(i);
            System.out.printf("    - %s: %s%n", 
                entry.path("key").asText(), 
                entry.path("value").toString());
        }
        if (exported.size() > 3) {
            System.out.printf("    ... and %d more%n", exported.size() - 3);
        }
    }
    
    // ==================== Cleanup ====================
    
    private static void cleanup(KuberRestClient client) throws IOException {
        printSection("CLEANUP");
        
        String[] regionsToClean = {
            "inventory_rest_java", "customers_rest_java", "analytics_rest_java",
            "products_json_rest_java", "orders_json_rest_java", "bulk_demo_rest_java"
        };
        
        for (String region : regionsToClean) {
            try {
                client.purgeRegion(region);
                client.deleteRegion(region);
                System.out.println("  Cleaned up region: " + region);
            } catch (Exception e) {
                System.out.println("  Warning for " + region + ": " + e.getMessage());
            }
        }
        
        // Clean default region
        client.selectRegion("default");
        for (String key : client.keys("*")) {
            client.delete(key);
        }
        System.out.println("  Cleaned default region");
    }
    
    // ==================== Helper Methods ====================
    
    private static void printSection(String title) {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("  " + title);
        System.out.println("=".repeat(70) + "\n");
    }
    
    private static void printResult(String description, String result) {
        System.out.println("  " + description + ": " + result);
    }
}
