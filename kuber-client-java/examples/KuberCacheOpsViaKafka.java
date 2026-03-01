/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber Comprehensive Cache Operations via Kafka (v2.6.3)
 *
 * Demonstrates every cache operation through Kafka request/response messaging:
 *   - String operations:  SET, GET, DELETE, EXISTS, KEYS, TTL, EXPIRE, MGET, MSET
 *   - JSON operations:    JSET, JGET, JUPDATE, JREMOVE, JSEARCH
 *   - Hash operations:    HSET, HGET, HGETALL
 *   - Admin operations:   PING, INFO, REGIONS
 *
 * Prerequisites:
 *   - Kafka running (default: localhost:9092)
 *   - Kuber server running with request/response messaging enabled
 *   - Valid API key from Kuber Admin → API Keys
 *
 * Dependencies (pom.xml):
 *   <dependency>
 *       <groupId>org.apache.kafka</groupId>
 *       <artifactId>kafka-clients</artifactId>
 *       <version>3.6.0</version>
 *   </dependency>
 *   <dependency>
 *       <groupId>com.fasterxml.jackson.core</groupId>
 *       <artifactId>jackson-databind</artifactId>
 *       <version>2.17.0</version>
 *   </dependency>
 *
 * @version 2.6.3
 */
package com.kuber.client.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class KuberCacheOpsViaKafka {

    // ── Configuration ─────────────────────────────────────────────────────────
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String REQUEST_TOPIC     = "kuber_cache_request";
    private static final String RESPONSE_TOPIC    = "kuber_cache_response";
    private static final String API_KEY           = "YOUR_API_KEY";   // from /admin/apikeys
    private static final String REGION            = "employees";       // target cache region
    private static final long   TIMEOUT_MS        = 10_000;            // response wait timeout

    // ── Internal state ────────────────────────────────────────────────────────
    private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Map<String, CompletableFuture<JsonNode>> pending = new ConcurrentHashMap<>();
    private ExecutorService consumerThread;

    // ══════════════════════════════════════════════════════════════════════════
    //  MAIN
    // ══════════════════════════════════════════════════════════════════════════
    public static void main(String[] args) throws Exception {
        KuberCacheOpsViaKafka client = new KuberCacheOpsViaKafka();
        try {
            client.start();

            // ── Admin Operations ──────────────────────────────────────────
            client.banner("ADMIN OPERATIONS");
            client.demoPing();
            client.demoInfo();
            client.demoRegions();

            // ── String Operations ─────────────────────────────────────────
            client.banner("STRING OPERATIONS (SET / GET / DELETE / EXISTS / KEYS / TTL / EXPIRE)");
            client.demoStringOps();

            // ── Batch Operations ──────────────────────────────────────────
            client.banner("BATCH OPERATIONS (MSET / MGET)");
            client.demoBatchOps();

            // ── JSON Document Operations ──────────────────────────────────
            client.banner("JSON OPERATIONS (JSET / JGET / JUPDATE / JREMOVE / JSEARCH)");
            client.demoJsonOps();

            // ── Hash Operations ───────────────────────────────────────────
            client.banner("HASH OPERATIONS (HSET / HGET / HGETALL)");
            client.demoHashOps();

            // ── Cleanup ───────────────────────────────────────────────────
            client.banner("CLEANUP");
            client.demoCleanup();

        } finally {
            client.stop();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  ADMIN DEMOS
    // ══════════════════════════════════════════════════════════════════════════

    private void demoPing() throws Exception {
        section("PING - Health Check");
        JsonNode resp = sendAndWait("PING", null, null, null);
        printResponse(resp);
    }

    private void demoInfo() throws Exception {
        section("INFO - Server Information");
        JsonNode resp = sendAndWait("INFO", null, null, null);
        printResponse(resp);
    }

    private void demoRegions() throws Exception {
        section("REGIONS - List All Regions");
        JsonNode resp = sendAndWait("REGIONS", null, null, null);
        printResponse(resp);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  STRING OPERATION DEMOS
    // ══════════════════════════════════════════════════════════════════════════

    private void demoStringOps() throws Exception {
        // SET - store a plain string value
        section("SET - Store string value");
        ObjectNode params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "emp:1001");
        params.put("value", "John Doe - Engineering Manager");
        params.put("ttl", 3600);
        JsonNode resp = sendAndWait("SET", params);
        printResponse(resp);

        // GET - retrieve the value
        section("GET - Retrieve value");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "emp:1001");
        resp = sendAndWait("GET", params);
        printResponse(resp);

        // EXISTS - check key existence
        section("EXISTS - Check key exists");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "emp:1001");
        resp = sendAndWait("EXISTS", params);
        printResponse(resp);

        // TTL - check remaining time-to-live
        section("TTL - Check remaining TTL");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "emp:1001");
        resp = sendAndWait("TTL", params);
        printResponse(resp);

        // EXPIRE - update TTL to 7200 seconds
        section("EXPIRE - Set new TTL (7200s)");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "emp:1001");
        params.put("ttl", 7200);
        resp = sendAndWait("EXPIRE", params);
        printResponse(resp);

        // KEYS - pattern-based key search
        section("KEYS - Find keys matching 'emp:*'");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("pattern", "emp:*");
        resp = sendAndWait("KEYS", params);
        printResponse(resp);

        // DELETE - remove a key
        section("DELETE - Remove key");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "emp:1001");
        resp = sendAndWait("DELETE", params);
        printResponse(resp);

        // GET deleted key - should return null
        section("GET - Verify deletion (expect null)");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "emp:1001");
        resp = sendAndWait("GET", params);
        printResponse(resp);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  BATCH OPERATION DEMOS
    // ══════════════════════════════════════════════════════════════════════════

    private void demoBatchOps() throws Exception {
        // MSET - store multiple key-value pairs at once
        section("MSET - Batch store 3 entries");
        ObjectNode params = mapper.createObjectNode();
        params.put("region", REGION);
        ObjectNode entries = mapper.createObjectNode();
        entries.put("dept:engineering", "Software Engineering");
        entries.put("dept:marketing", "Digital Marketing");
        entries.put("dept:finance", "Corporate Finance");
        params.set("entries", entries);
        JsonNode resp = sendAndWait("MSET", params);
        printResponse(resp);

        // MGET - retrieve multiple keys at once
        section("MGET - Batch retrieve 3 entries");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        ArrayNode keys = mapper.createArrayNode();
        keys.add("dept:engineering");
        keys.add("dept:marketing");
        keys.add("dept:finance");
        keys.add("dept:nonexistent");  // this one should return null
        params.set("keys", keys);
        resp = sendAndWait("MGET", params);
        printResponse(resp);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  JSON DOCUMENT OPERATION DEMOS
    // ══════════════════════════════════════════════════════════════════════════

    private void demoJsonOps() throws Exception {
        // JSET - store a JSON document
        section("JSET - Store employee JSON document");
        ObjectNode params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "employee/E001");
        ObjectNode employee = mapper.createObjectNode();
        employee.put("name", "Jane Smith");
        employee.put("email", "jane.smith@acme.com");
        employee.put("department", "Engineering");
        employee.put("title", "Senior Software Engineer");
        employee.put("salary", 145000);
        employee.put("active", true);
        ObjectNode address = mapper.createObjectNode();
        address.put("city", "San Francisco");
        address.put("state", "CA");
        address.put("zip", "94105");
        employee.set("address", address);
        ArrayNode skills = mapper.createArrayNode();
        skills.add("Java").add("Kafka").add("Kubernetes").add("PostgreSQL");
        employee.set("skills", skills);
        params.set("value", employee);
        JsonNode resp = sendAndWait("JSET", params);
        printResponse(resp);

        // JSET - store another employee
        section("JSET - Store second employee");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "employee/E002");
        employee = mapper.createObjectNode();
        employee.put("name", "Bob Johnson");
        employee.put("email", "bob.johnson@acme.com");
        employee.put("department", "Engineering");
        employee.put("title", "DevOps Engineer");
        employee.put("salary", 135000);
        employee.put("active", true);
        address = mapper.createObjectNode();
        address.put("city", "Seattle");
        address.put("state", "WA");
        address.put("zip", "98101");
        employee.set("address", address);
        skills = mapper.createArrayNode();
        skills.add("Docker").add("Kafka").add("Terraform").add("AWS");
        employee.set("skills", skills);
        params.set("value", employee);
        resp = sendAndWait("JSET", params);
        printResponse(resp);

        // JSET - store a third employee in a different department
        section("JSET - Store marketing employee");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "employee/E003");
        employee = mapper.createObjectNode();
        employee.put("name", "Alice Chen");
        employee.put("email", "alice.chen@acme.com");
        employee.put("department", "Marketing");
        employee.put("title", "Marketing Director");
        employee.put("salary", 160000);
        employee.put("active", true);
        address = mapper.createObjectNode();
        address.put("city", "New York");
        address.put("state", "NY");
        address.put("zip", "10001");
        employee.set("address", address);
        skills = mapper.createArrayNode();
        skills.add("Analytics").add("SEO").add("Content Strategy");
        employee.set("skills", skills);
        params.set("value", employee);
        resp = sendAndWait("JSET", params);
        printResponse(resp);

        // JGET - retrieve full JSON document
        section("JGET - Retrieve full employee document");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "employee/E001");
        resp = sendAndWait("JGET", params);
        printResponse(resp);

        // JGET with JSONPath - retrieve specific field
        section("JGET - Retrieve only the address (JSONPath: $.address)");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "employee/E001");
        params.put("path", "$.address");
        resp = sendAndWait("JGET", params);
        printResponse(resp);

        // JUPDATE - deep merge update (add new fields, update existing)
        section("JUPDATE - Promote employee (update title + salary, add promotion date)");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "employee/E001");
        ObjectNode updates = mapper.createObjectNode();
        updates.put("title", "Staff Software Engineer");
        updates.put("salary", 175000);
        updates.put("promotion_date", "2026-02-16");
        params.set("value", updates);
        resp = sendAndWait("JUPDATE", params);
        printResponse(resp);

        // JGET - verify the update
        section("JGET - Verify updated employee");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "employee/E001");
        resp = sendAndWait("JGET", params);
        printResponse(resp);

        // JREMOVE - remove specific attributes
        section("JREMOVE - Remove 'promotion_date' attribute");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "employee/E001");
        ArrayNode removeKeys = mapper.createArrayNode();
        removeKeys.add("promotion_date");
        params.set("keys", removeKeys);
        resp = sendAndWait("JREMOVE", params);
        printResponse(resp);

        // JSEARCH - search JSON documents by field value
        section("JSEARCH - Find all Engineering department employees");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("query", "$.department == 'Engineering'");
        resp = sendAndWait("JSEARCH", params);
        printResponse(resp);

        // JSEARCH - search by nested field
        section("JSEARCH - Find employees in California");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("query", "$.address.state == 'CA'");
        resp = sendAndWait("JSEARCH", params);
        printResponse(resp);

        // JSEARCH - numeric comparison
        section("JSEARCH - Find employees with salary > 150000");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("query", "$.salary > 150000");
        resp = sendAndWait("JSEARCH", params);
        printResponse(resp);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  HASH OPERATION DEMOS
    // ══════════════════════════════════════════════════════════════════════════

    private void demoHashOps() throws Exception {
        // HSET - set a field in a hash
        section("HSET - Set hash fields for config:app");
        for (String[] kv : new String[][] {
            {"version", "2.6.3"},
            {"max_connections", "500"},
            {"log_level", "INFO"},
            {"feature_flags", "{\"dark_mode\":true,\"beta_api\":false}"}
        }) {
            ObjectNode params = mapper.createObjectNode();
            params.put("region", REGION);
            params.put("key", "config:app");
            params.put("field", kv[0]);
            params.put("value", kv[1]);
            JsonNode resp = sendAndWait("HSET", params);
            System.out.println("   Set field '" + kv[0] + "': " +
                (resp.has("success") && resp.get("success").asBoolean() ? "OK" : "FAIL"));
        }

        // HGET - get a single field
        section("HGET - Get 'version' field from config:app");
        ObjectNode params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "config:app");
        params.put("field", "version");
        JsonNode resp = sendAndWait("HGET", params);
        printResponse(resp);

        // HGETALL - get all fields
        section("HGETALL - Get all fields from config:app");
        params = mapper.createObjectNode();
        params.put("region", REGION);
        params.put("key", "config:app");
        resp = sendAndWait("HGETALL", params);
        printResponse(resp);
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  CLEANUP
    // ══════════════════════════════════════════════════════════════════════════

    private void demoCleanup() throws Exception {
        section("DELETE - Clean up all demo keys");
        for (String key : new String[] {
            "emp:1001", "dept:engineering", "dept:marketing", "dept:finance",
            "employee/E001", "employee/E002", "employee/E003", "config:app"
        }) {
            ObjectNode params = mapper.createObjectNode();
            params.put("region", REGION);
            params.put("key", key);
            sendAndWait("DELETE", params);
            System.out.println("   Deleted: " + key);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  KAFKA INFRASTRUCTURE
    // ══════════════════════════════════════════════════════════════════════════

    private void start() {
        System.out.println("╔═══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║  Kuber Cache Operations via Kafka — Comprehensive Demo (v2.6.3)       ║");
        System.out.println("║  Region: " + padRight(REGION, 57) + "  ║");
        System.out.println("║  Request:  " + padRight(REQUEST_TOPIC, 55) + "  ║");
        System.out.println("║  Response: " + padRight(RESPONSE_TOPIC, 55) + "  ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════════════╝\n");

        // Create producer
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producer = new KafkaProducer<>(prodProps);

        // Create consumer
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kuber-java-demo-" + UUID.randomUUID());
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumer = new KafkaConsumer<>(consProps);
        consumer.subscribe(Collections.singletonList(RESPONSE_TOPIC));

        // Start response listener
        consumerThread = Executors.newSingleThreadExecutor();
        consumerThread.submit(this::responseListener);

        // Wait for consumer to be ready
        try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
        System.out.println("✅ Connected to Kafka. Consumer subscribed to: " + RESPONSE_TOPIC + "\n");
    }

    private void stop() {
        running.set(false);
        if (producer != null) producer.close(Duration.ofSeconds(5));
        if (consumer != null) consumer.wakeup();
        if (consumerThread != null) consumerThread.shutdownNow();
        System.out.println("\n✅ Kafka clients shut down.");
    }

    /**
     * Background thread that polls Kafka for responses and completes pending futures.
     */
    private void responseListener() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JsonNode response = mapper.readTree(record.value());
                        // Extract message_id from the nested request object
                        String messageId = null;
                        if (response.has("request") && response.get("request").has("message_id")) {
                            messageId = response.get("request").get("message_id").asText();
                        } else if (response.has("message_id")) {
                            messageId = response.get("message_id").asText();
                        }
                        if (messageId != null) {
                            CompletableFuture<JsonNode> future = pending.remove(messageId);
                            if (future != null) {
                                future.complete(response);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("Error parsing response: " + e.getMessage());
                    }
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException ignored) {
            // Normal shutdown
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  REQUEST BUILDER & SENDER
    // ══════════════════════════════════════════════════════════════════════════

    /**
     * Build a request JSON, publish to Kafka, and wait for the correlated response.
     */
    private JsonNode sendAndWait(String operation, ObjectNode params) throws Exception {
        String region = params != null && params.has("region") ? params.get("region").asText() : null;
        String key    = params != null && params.has("key")    ? params.get("key").asText()    : null;
        return sendAndWait(operation, region, key, params);
    }

    private JsonNode sendAndWait(String operation, String region, String key, ObjectNode extra) throws Exception {
        String messageId = UUID.randomUUID().toString();

        // Build request
        ObjectNode request = mapper.createObjectNode();
        request.put("api_key", API_KEY);
        request.put("message_id", messageId);
        request.put("operation", operation.toUpperCase());
        if (region != null) request.put("region", region);
        if (key != null) request.put("key", key);

        // Merge any extra fields (value, ttl, pattern, keys, entries, field, path, query, etc.)
        if (extra != null) {
            extra.fieldNames().forEachRemaining(f -> {
                if (!f.equals("region") && !f.equals("key")) {
                    request.set(f, extra.get(f));
                }
            });
        }

        // Register pending future
        CompletableFuture<JsonNode> future = new CompletableFuture<>();
        pending.put(messageId, future);

        // Publish
        String json = mapper.writeValueAsString(request);
        producer.send(new ProducerRecord<>(REQUEST_TOPIC, messageId, json)).get(5, TimeUnit.SECONDS);
        producer.flush();

        // Wait for response
        try {
            return future.get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            pending.remove(messageId);
            throw new RuntimeException("Timeout waiting for response to " + operation + " (message_id=" + messageId + ")");
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    //  OUTPUT HELPERS
    // ══════════════════════════════════════════════════════════════════════════

    private void printResponse(JsonNode resp) throws Exception {
        boolean success = resp.has("success") && resp.get("success").asBoolean();
        System.out.println("   Status: " + (success ? "✅ SUCCESS" : "❌ FAIL"));
        if (resp.has("result")) {
            String pretty = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(resp.get("result"));
            // indent each line for readability
            for (String line : pretty.split("\n")) {
                System.out.println("   " + line);
            }
        }
        if (resp.has("error")) {
            System.out.println("   Error: " + resp.get("error").asText());
        }
        System.out.println();
    }

    private void banner(String title) {
        System.out.println("\n══════════════════════════════════════════════════════════════════════");
        System.out.println("  " + title);
        System.out.println("══════════════════════════════════════════════════════════════════════\n");
    }

    private void section(String title) {
        System.out.println("── " + title + " ──");
    }

    private static String padRight(String s, int n) {
        return String.format("%-" + n + "s", s);
    }
}
