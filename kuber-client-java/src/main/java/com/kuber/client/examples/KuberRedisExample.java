/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.client.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kuber.client.KuberClient;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * Comprehensive examples demonstrating all capabilities of the Kuber Java client
 * using Redis protocol with Kuber extensions.
 * 
 * v2.1.0: API Key authentication required.
 * 
 * Features demonstrated:
 * - Basic string operations (GET, SET, MGET, MSET, INCR, DECR)
 * - Key pattern search (KEYS, SCAN)
 * - Hash operations (HGET, HSET, HMSET, HGETALL)
 * - Region management (CREATE, SELECT, LIST, PURGE, DELETE)
 * - JSON operations with specific region storage
 * - Deep JSON search with multiple operators
 * - Cross-region operations
 * 
 * Usage:
 *   java -cp kuber-client.jar com.kuber.client.examples.KuberRedisExample [host] [port] [api-key]
 */
public class KuberRedisExample {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: KuberRedisExample <host> <port> <api-key>");
            System.err.println("Example: KuberRedisExample localhost 6380 kub_your_api_key_here");
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
            ║   KUBER JAVA CLIENT - REDIS PROTOCOL WITH EXTENSIONS (v2.6.3)        ║
            ║                                                                      ║
            ║   Protocol: Redis RESP with Kuber Extensions (API Key Auth)          ║
            ╚══════════════════════════════════════════════════════════════════════╝
            """);
        System.out.printf("Connecting to: %s:%d%n", host, port);
        System.out.println("Authentication: API Key\n");
        
        try (KuberClient client = new KuberClient(host, port, apiKey)) {
            System.out.println("  Authentication successful!\n");
            
            // Run all examples
            example1_BasicStringOperations(client);
            example2_MgetMsetOperations(client);
            example3_KeyPatternSearch(client);
            example4_HashOperations(client);
            example5_RegionOperations(client);
            example6_JsonOperationsWithRegions(client);
            example7_JsonDeepSearch(client);
            example8_CrossRegionSearch(client);
            example9_ServerInformation(client);
            example10_KeyManagement(client);
            
            // Cleanup
            cleanup(client);
            
            printSection("ALL EXAMPLES COMPLETED SUCCESSFULLY");
            
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    // ==================== Example 1: Basic String Operations ====================
    
    private static void example1_BasicStringOperations(KuberClient client) throws IOException {
        printSection("1. BASIC STRING OPERATIONS (GET, SET, MGET, MSET)");
        
        // Simple SET and GET
        client.set("greeting", "Hello, Kuber!");
        String value = client.get("greeting");
        printResult("SET/GET 'greeting'", value);
        
        // SET with TTL
        client.set("temp_key", "expires in 60 seconds", Duration.ofSeconds(60));
        printResult("SET with TTL (60s)", client.get("temp_key"));
        printResult("TTL remaining", String.valueOf(client.ttl("temp_key")));
        
        // SET with Duration
        client.set("session_data", "user session", Duration.ofHours(1));
        printResult("SET with Duration (1 hour)", "TTL: " + client.ttl("session_data") + " seconds");
        
        // SETNX - Set only if not exists
        boolean result1 = client.setNx("unique_key", "first value");
        boolean result2 = client.setNx("unique_key", "second value (should fail)");
        printResult("SETNX first attempt", String.valueOf(result1));
        printResult("SETNX second attempt", String.valueOf(result2));
        printResult("Final value", client.get("unique_key"));
        
        // SETEX - Set with expiration
        client.setEx("cache_item", "cached data", 300);
        printResult("SETEX (5 minutes TTL)", client.get("cache_item"));
        
        // INCR/DECR operations
        client.set("counter", "0");
        client.incr("counter");
        client.incr("counter");
        client.incrBy("counter", 10);
        printResult("After INCR, INCR, INCRBY(10)", client.get("counter"));
        
        client.decr("counter");
        client.decrBy("counter", 5);
        printResult("After DECR, DECRBY(5)", client.get("counter"));
        
        // APPEND
        client.set("message", "Hello");
        client.append("message", " World!");
        printResult("After APPEND", client.get("message"));
        
        // STRLEN
        printResult("String length of 'message'", String.valueOf(client.strlen("message")));
    }
    
    // ==================== Example 2: MGET/MSET Operations ====================
    
    private static void example2_MgetMsetOperations(KuberClient client) throws IOException {
        printSection("2. MULTI-KEY OPERATIONS (MGET, MSET)");
        
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
        List<String> names = client.mget("user:1:name", "user:2:name", "user:3:name");
        printResult("MGET all names", names.toString());
        
        List<String> emails = client.mget("user:1:email", "user:2:email", "user:3:email");
        printResult("MGET all emails", emails.toString());
        
        // MGET with some missing keys
        List<String> mixed = client.mget("user:1:name", "user:99:name", "user:2:name");
        printResult("MGET with missing key (user:99)", mixed.toString());
        
        // Get all properties for one user
        List<String> user1Props = client.mget("user:1:name", "user:1:email", "user:1:role");
        printResult("MGET user:1 all properties", user1Props.toString());
    }
    
    // ==================== Example 3: Key Pattern Search ====================
    
    private static void example3_KeyPatternSearch(KuberClient client) throws IOException {
        printSection("3. KEY PATTERN SEARCH (KEYS)");
        
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
        testData.put("session:xyz789", "Session 3");
        
        client.mset(testData);
        System.out.println("  Created test keys: product:*, order:*, session:*");
        
        // Find product keys
        List<String> productKeys = client.keys("product:*");
        printResult("KEYS product:*", productKeys.toString());
        
        // Find electronics products only
        List<String> electronicsKeys = client.keys("product:electronics:*");
        printResult("KEYS product:electronics:*", electronicsKeys.toString());
        
        // Find order keys
        List<String> orderKeys = client.keys("order:*");
        printResult("KEYS order:*", orderKeys.toString());
        
        // Find 2024 orders
        List<String> order2024 = client.keys("order:2024:*");
        printResult("KEYS order:2024:*", order2024.toString());
        
        // Find session keys
        List<String> sessionKeys = client.keys("session:*");
        printResult("KEYS session:*", sessionKeys.toString());
    }
    
    // ==================== Example 4: Hash Operations ====================
    
    private static void example4_HashOperations(KuberClient client) throws IOException {
        printSection("4. HASH OPERATIONS (HGET, HSET, HMSET, HGETALL)");
        
        // HSET - Set individual fields
        client.hset("user:profile:1", "name", "Alice Johnson");
        client.hset("user:profile:1", "email", "alice@example.com");
        client.hset("user:profile:1", "age", "28");
        client.hset("user:profile:1", "city", "New York");
        client.hset("user:profile:1", "department", "Engineering");
        System.out.println("  HSET: Set 5 fields for user:profile:1");
        
        // HGET - Get individual fields
        printResult("HGET name", client.hget("user:profile:1", "name"));
        printResult("HGET email", client.hget("user:profile:1", "email"));
        printResult("HGET age", client.hget("user:profile:1", "age"));
        
        // HMSET - Set multiple fields at once
        Map<String, String> profile2 = new LinkedHashMap<>();
        profile2.put("name", "Bob Smith");
        profile2.put("email", "bob@example.com");
        profile2.put("age", "35");
        profile2.put("city", "San Francisco");
        profile2.put("department", "Product");
        
        client.hmset("user:profile:2", profile2);
        System.out.println("\n  HMSET: Set 5 fields for user:profile:2");
        
        // HGETALL - Get all fields
        Map<String, String> allFields = client.hgetall("user:profile:1");
        printResult("HGETALL user:profile:1", allFields.toString());
        
        // HKEYS - Get all field names
        List<String> keys = client.hkeys("user:profile:1");
        printResult("HKEYS", keys.toString());
        
        // HVALS - Get all values
        List<String> vals = client.hvals("user:profile:1");
        printResult("HVALS", vals.toString());
        
        // HLEN - Get hash length
        printResult("HLEN", String.valueOf(client.hlen("user:profile:1")));
        
        // HEXISTS - Check field existence
        printResult("HEXISTS 'name'", String.valueOf(client.hexists("user:profile:1", "name")));
        printResult("HEXISTS 'salary'", String.valueOf(client.hexists("user:profile:1", "salary")));
        
        // HDEL - Delete fields
        client.hdel("user:profile:1", "age");
        printResult("After HDEL 'age', remaining keys", client.hkeys("user:profile:1").toString());
    }
    
    // ==================== Example 5: Region Operations ====================
    
    private static void example5_RegionOperations(KuberClient client) throws IOException {
        printSection("5. REGION OPERATIONS (CREATE, SELECT, LIST)");
        
        // List existing regions
        List<String> regions = client.listRegions();
        printResult("Initial regions", regions.toString());
        
        // Create custom regions
        client.createRegion("inventory_java", "Product inventory data");
        client.createRegion("customers_java", "Customer information");
        client.createRegion("analytics_java", "Analytics and metrics");
        System.out.println("  Created 3 new regions: inventory_java, customers_java, analytics_java");
        
        // List regions again
        regions = client.listRegions();
        printResult("Regions after creation", regions.toString());
        
        // Select and use inventory region
        client.selectRegion("inventory_java");
        System.out.println("\n  Selected region: " + client.getCurrentRegion());
        
        // Store data in inventory region
        client.set("sku:1001", "Laptop - Stock: 50");
        client.set("sku:1002", "Mouse - Stock: 200");
        client.set("sku:1003", "Keyboard - Stock: 150");
        
        printResult("DBSIZE in inventory_java region", String.valueOf(client.dbSize()));
        printResult("KEYS in inventory_java", client.keys("*").toString());
        
        // Select customers region
        client.selectRegion("customers_java");
        System.out.println("\n  Selected region: " + client.getCurrentRegion());
        
        // Store data in customers region
        client.set("cust:C001", "Acme Corp");
        client.set("cust:C002", "TechStart Inc");
        
        printResult("DBSIZE in customers_java region", String.valueOf(client.dbSize()));
        printResult("KEYS in customers_java", client.keys("*").toString());
        
        // Verify isolation - inventory keys not visible in customers
        String laptop = client.get("sku:1001");
        printResult("GET sku:1001 from customers_java region", laptop);  // Should be null
        
        // Switch back to default
        client.selectRegion("default");
        System.out.println("\n  Back to region: " + client.getCurrentRegion());
    }
    
    // ==================== Example 6: JSON Operations with Regions ====================
    
    private static void example6_JsonOperationsWithRegions(KuberClient client) throws IOException {
        printSection("6. JSON OPERATIONS WITH REGION STORAGE (JSET, JGET)");
        
        // Create a products region for JSON documents
        client.createRegion("products_json_java", "Product catalog as JSON");
        client.selectRegion("products_json_java");
        
        System.out.println("\n  Storing products in 'products_json_java' region:");
        
        // Store JSON products
        String product1 = """
            {
                "id": "prod:1001",
                "name": "Wireless Headphones",
                "brand": "AudioTech",
                "price": 79.99,
                "category": "Electronics",
                "in_stock": true,
                "stock_count": 150,
                "tags": ["wireless", "bluetooth", "audio"]
            }
            """;
        client.jsonSet("prod:1001", product1);
        System.out.println("    Stored: prod:1001 - Wireless Headphones");
        
        String product2 = """
            {
                "id": "prod:1002",
                "name": "Mechanical Keyboard",
                "brand": "KeyMaster",
                "price": 149.99,
                "category": "Electronics",
                "in_stock": true,
                "stock_count": 75,
                "tags": ["mechanical", "keyboard", "gaming"]
            }
            """;
        client.jsonSet("prod:1002", product2);
        System.out.println("    Stored: prod:1002 - Mechanical Keyboard");
        
        String product3 = """
            {
                "id": "prod:1003",
                "name": "Ergonomic Office Chair",
                "brand": "ComfortPlus",
                "price": 349.99,
                "category": "Furniture",
                "in_stock": false,
                "stock_count": 0,
                "tags": ["ergonomic", "office", "chair"]
            }
            """;
        client.jsonSet("prod:1003", product3);
        System.out.println("    Stored: prod:1003 - Ergonomic Office Chair");
        
        String product4 = """
            {
                "id": "prod:2001",
                "name": "USB-C Hub",
                "brand": "ConnectPro",
                "price": 45.99,
                "category": "Electronics",
                "in_stock": true,
                "stock_count": 300,
                "tags": ["usb-c", "hub", "laptop"]
            }
            """;
        client.jsonSet("prod:2001", product4);
        System.out.println("    Stored: prod:2001 - USB-C Hub");
        
        // Retrieve JSON
        System.out.println("\n  Retrieving products from 'products_json_java' region:");
        JsonNode headphones = client.jsonGet("prod:1001");
        printResult("GET prod:1001", headphones != null ? headphones.toPrettyString() : "null");
        
        // Create orders region with TTL
        client.createRegion("orders_json_java", "Customer orders");
        client.selectRegion("orders_json_java");
        
        String order1 = """
            {
                "order_id": "ord:10001",
                "customer_id": "cust:C001",
                "status": "shipped",
                "total": 225.98,
                "shipping": {
                    "method": "express",
                    "address": "123 Main St, NYC"
                }
            }
            """;
        client.jsonSet("ord:10001", order1, Duration.ofDays(30));
        System.out.println("\n  Stored: ord:10001 - shipped - $225.98 (30-day TTL)");
        
        String order2 = """
            {
                "order_id": "ord:10002",
                "customer_id": "cust:C002",
                "status": "pending",
                "total": 149.99,
                "shipping": {
                    "method": "standard",
                    "address": "456 Oak Ave, SF"
                }
            }
            """;
        client.jsonSet("ord:10002", order2, Duration.ofDays(30));
        System.out.println("  Stored: ord:10002 - pending - $149.99 (30-day TTL)");
        
        String order3 = """
            {
                "order_id": "ord:10003",
                "customer_id": "cust:C001",
                "status": "delivered",
                "total": 395.98,
                "shipping": {
                    "method": "express",
                    "address": "123 Main St, NYC"
                }
            }
            """;
        client.jsonSet("ord:10003", order3, Duration.ofDays(30));
        System.out.println("  Stored: ord:10003 - delivered - $395.98 (30-day TTL)");
        
        client.selectRegion("default");
    }
    
    // ==================== Example 7: JSON Deep Search ====================
    
    private static void example7_JsonDeepSearch(KuberClient client) throws IOException {
        printSection("7. JSON DEEP SEARCH (JSEARCH with multiple operators)");
        
        client.selectRegion("products_json_java");
        
        // Search by equality
        System.out.println("\n  === SEARCH BY EQUALITY ===");
        
        List<JsonNode> results = client.jsonSearch("$.category=Electronics");
        printResult("$.category=Electronics", results.size() + " results");
        for (JsonNode doc : results) {
            System.out.printf("    - %s (%s)%n", 
                doc.path("name").asText(), 
                "$" + doc.path("price").asText());
        }
        
        results = client.jsonSearch("$.in_stock=true");
        printResult("$.in_stock=true", results.size() + " results");
        for (JsonNode doc : results) {
            System.out.printf("    - %s (Stock: %s)%n", 
                doc.path("name").asText(), 
                doc.path("stock_count").asText());
        }
        
        // Search by comparison
        System.out.println("\n  === SEARCH BY COMPARISON ===");
        
        results = client.jsonSearch("$.price>100");
        printResult("$.price>100", results.size() + " results");
        for (JsonNode doc : results) {
            System.out.printf("    - %s ($%s)%n", 
                doc.path("name").asText(), 
                doc.path("price").asText());
        }
        
        results = client.jsonSearch("$.stock_count>=100");
        printResult("$.stock_count>=100", results.size() + " results");
        for (JsonNode doc : results) {
            System.out.printf("    - %s (Stock: %s)%n", 
                doc.path("name").asText(), 
                doc.path("stock_count").asText());
        }
        
        // Search with inequality
        System.out.println("\n  === SEARCH WITH INEQUALITY ===");
        
        results = client.jsonSearch("$.category!=Electronics");
        printResult("$.category!=Electronics", results.size() + " results");
        for (JsonNode doc : results) {
            System.out.printf("    - %s (Category: %s)%n", 
                doc.path("name").asText(), 
                doc.path("category").asText());
        }
        
        // Search with pattern matching
        System.out.println("\n  === SEARCH WITH PATTERN MATCHING (LIKE) ===");
        
        results = client.jsonSearch("$.name LIKE %Keyboard%");
        printResult("$.name LIKE %Keyboard%", results.size() + " results");
        for (JsonNode doc : results) {
            System.out.printf("    - %s%n", doc.path("name").asText());
        }
        
        // Search array contains
        System.out.println("\n  === SEARCH ARRAY CONTAINS ===");
        
        results = client.jsonSearch("$.tags CONTAINS wireless");
        printResult("$.tags CONTAINS wireless", results.size() + " results");
        for (JsonNode doc : results) {
            System.out.printf("    - %s (Tags: %s)%n", 
                doc.path("name").asText(), 
                doc.path("tags").toString());
        }
        
        // Combined conditions
        System.out.println("\n  === COMBINED CONDITIONS ===");
        
        results = client.jsonSearch("$.in_stock=true,$.price<100");
        printResult("$.in_stock=true,$.price<100", results.size() + " results");
        for (JsonNode doc : results) {
            System.out.printf("    - %s ($%s, In Stock)%n", 
                doc.path("name").asText(), 
                doc.path("price").asText());
        }
        
        client.selectRegion("default");
    }
    
    // ==================== Example 8: Cross-Region Search ====================
    
    private static void example8_CrossRegionSearch(KuberClient client) throws IOException {
        printSection("8. CROSS-REGION JSON SEARCH");
        
        // Search in products region
        System.out.println("\n  === SEARCH IN 'products_json_java' REGION ===");
        client.selectRegion("products_json_java");
        
        List<JsonNode> results = client.jsonSearch("$.category=Electronics");
        printResult("$.category=Electronics", results.size() + " results");
        for (JsonNode doc : results) {
            System.out.printf("    - %s%n", doc.path("name").asText());
        }
        
        // Search in orders region
        System.out.println("\n  === SEARCH IN 'orders_json_java' REGION ===");
        client.selectRegion("orders_json_java");
        
        results = client.jsonSearch("$.status=shipped");
        printResult("$.status=shipped", results.size() + " results");
        for (JsonNode doc : results) {
            System.out.printf("    - Customer %s, Total: $%s%n", 
                doc.path("customer_id").asText(), 
                doc.path("total").asText());
        }
        
        results = client.jsonSearch("$.customer_id=cust:C001");
        printResult("$.customer_id=cust:C001", results.size() + " results");
        for (JsonNode doc : results) {
            System.out.printf("    - Status: %s, Total: $%s%n", 
                doc.path("status").asText(), 
                doc.path("total").asText());
        }
        
        results = client.jsonSearch("$.total>200");
        printResult("$.total>200", results.size() + " results");
        for (JsonNode doc : results) {
            System.out.printf("    - $%s%n", doc.path("total").asText());
        }
        
        results = client.jsonSearch("$.shipping.method=express");
        printResult("$.shipping.method=express", results.size() + " results");
        for (JsonNode doc : results) {
            System.out.printf("    - %s%n", doc.path("shipping").path("address").asText());
        }
        
        client.selectRegion("default");
    }
    
    // ==================== Example 9: Server Information ====================
    
    private static void example9_ServerInformation(KuberClient client) throws IOException {
        printSection("9. SERVER INFORMATION");
        
        // PING
        printResult("PING", client.ping());
        
        // INFO
        String info = client.info();
        System.out.println("\n  INFO (first 500 chars):");
        System.out.println("    " + info.substring(0, Math.min(500, info.length())) + "...");
        
        // STATUS
        String status = client.status();
        printResult("\n  STATUS", status.substring(0, Math.min(300, status.length())) + "...");
        
        // DBSIZE for each region
        client.selectRegion("default");
        printResult("DBSIZE (default)", String.valueOf(client.dbSize()));
        
        client.selectRegion("products_json_java");
        printResult("DBSIZE (products_json_java)", String.valueOf(client.dbSize()));
        
        client.selectRegion("orders_json_java");
        printResult("DBSIZE (orders_json_java)", String.valueOf(client.dbSize()));
        
        // REPL_INFO
        String repl = client.replInfo();
        printResult("REPLINFO", repl != null && repl.length() > 0 ? 
            repl.substring(0, Math.min(200, repl.length())) : "N/A");
        
        client.selectRegion("default");
    }
    
    // ==================== Example 10: Key Management ====================
    
    private static void example10_KeyManagement(KuberClient client) throws IOException {
        printSection("10. KEY MANAGEMENT (EXISTS, DELETE, EXPIRE, RENAME)");
        
        client.selectRegion("default");
        
        // EXISTS
        client.set("exists_test_1", "value1");
        client.set("exists_test_2", "value2");
        printResult("EXISTS exists_test_1", String.valueOf(client.exists("exists_test_1")));
        printResult("EXISTS nonexistent_key", String.valueOf(client.exists("nonexistent_key")));
        
        // TYPE
        printResult("TYPE of string key", client.type("exists_test_1"));
        client.hset("hash_key", "field", "value");
        printResult("TYPE of hash key", client.type("hash_key"));
        
        // EXPIRE and TTL
        client.set("expire_test", "will expire");
        printResult("TTL before EXPIRE", String.valueOf(client.ttl("expire_test")));
        client.expire("expire_test", 120);
        printResult("TTL after EXPIRE(120)", String.valueOf(client.ttl("expire_test")));
        
        // PERSIST
        client.persist("expire_test");
        printResult("TTL after PERSIST", String.valueOf(client.ttl("expire_test")));
        
        // RENAME
        client.set("old_name", "some data");
        client.rename("old_name", "new_name");
        printResult("GET 'new_name' after RENAME", client.get("new_name"));
        printResult("GET 'old_name' after RENAME", String.valueOf(client.get("old_name")));
        
        // DELETE
        client.set("to_delete_1", "value1");
        client.set("to_delete_2", "value2");
        long deleted = client.del("to_delete_1", "to_delete_2", "nonexistent");
        printResult("DEL (3 keys, 2 existed)", String.valueOf(deleted));
    }
    
    // ==================== Cleanup ====================
    
    private static void cleanup(KuberClient client) throws IOException {
        printSection("CLEANUP");
        
        String[] regionsToClean = {
            "inventory_java", "customers_java", "analytics_java",
            "products_json_java", "orders_json_java"
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
        client.flushDb();
        System.out.println("  Flushed default region");
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
