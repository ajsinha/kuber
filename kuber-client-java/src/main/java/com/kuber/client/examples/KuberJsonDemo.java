/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Kuber Distributed Cache - Standalone JSON Operations Demo
 * 
 * This demonstrates all JSON-related operations in Kuber:
 * a) Set a key with JSON value
 * b) Retrieve a key with JSON value  
 * c) JSON search using single and multiple attributes
 * d) Regex search on JSON attribute values
 * e) Search keys using regex pattern
 *
 * v1.6.5 - API Key Authentication Required
 * 
 * Compile: javac KuberJsonDemo.java
 * Run: java KuberJsonDemo [host] [port] [api-key]
 */

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.regex.*;

public class KuberJsonDemo {
    
    // ========================================================================
    // CONFIGURATION
    // ========================================================================
    private static String HOST = "localhost";
    private static int PORT = 6380;
    private static String API_KEY = "kub_admin_sample_key_replace_me";
    
    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;
    private String currentRegion = "default";
    
    // ========================================================================
    // CONNECTION & PROTOCOL
    // ========================================================================
    
    public void connect() throws IOException {
        socket = new Socket(HOST, PORT);
        socket.setSoTimeout(30000);
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        
        // Authenticate
        String response = sendCommand("AUTH", API_KEY);
        if (!response.contains("OK")) {
            throw new IOException("Authentication failed: " + response);
        }
        System.out.println("✓ Connected and authenticated to " + HOST + ":" + PORT);
    }
    
    public void close() {
        try {
            if (socket != null) socket.close();
        } catch (IOException e) {
            // Ignore
        }
    }
    
    private String sendCommand(String... args) throws IOException {
        // Build RESP command
        StringBuilder cmd = new StringBuilder();
        cmd.append("*").append(args.length).append("\r\n");
        for (String arg : args) {
            cmd.append("$").append(arg.length()).append("\r\n");
            cmd.append(arg).append("\r\n");
        }
        
        writer.write(cmd.toString());
        writer.flush();
        
        return readResponse();
    }
    
    private String readResponse() throws IOException {
        StringBuilder response = new StringBuilder();
        String line = reader.readLine();
        
        if (line == null || line.isEmpty()) {
            return "";
        }
        
        char type = line.charAt(0);
        String content = line.substring(1);
        
        switch (type) {
            case '+': // Simple string
                return content;
            case '-': // Error
                return "ERROR: " + content;
            case ':': // Integer
                return content;
            case '$': // Bulk string
                int length = Integer.parseInt(content);
                if (length == -1) {
                    return null;
                }
                char[] buffer = new char[length];
                int read = 0;
                while (read < length) {
                    read += reader.read(buffer, read, length - read);
                }
                reader.readLine(); // Consume trailing \r\n
                return new String(buffer);
            case '*': // Array
                int count = Integer.parseInt(content);
                if (count == -1 || count == 0) {
                    return "[]";
                }
                List<String> items = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    items.add(readResponse());
                }
                return items.toString();
            default:
                return line;
        }
    }
    
    // ========================================================================
    // REGION OPERATIONS
    // ========================================================================
    
    public void selectRegion(String region) throws IOException {
        String response = sendCommand("RSELECT", region);
        if (response.contains("ERROR")) {
            // Try to create region
            sendCommand("RCREATE", region, "Demo region: " + region);
            response = sendCommand("RSELECT", region);
        }
        currentRegion = region;
    }
    
    // ========================================================================
    // JSON OPERATIONS
    // ========================================================================
    
    public String jsonSet(String key, String jsonValue) throws IOException {
        return sendCommand("JSET", key, jsonValue, "$");
    }
    
    public String jsonGet(String key) throws IOException {
        return sendCommand("JGET", key, "$");
    }
    
    public String jsonGet(String key, String path) throws IOException {
        return sendCommand("JGET", key, path);
    }
    
    /**
     * Search JSON documents.
     * 
     * Query syntax:
     * - field=value           : exact match
     * - field=[v1|v2|v3]      : IN clause - match any value (v1.7.8)
     * - field!=value          : not equal
     * - field>[=]value        : greater than [or equal]
     * - field<[=]value        : less than [or equal]
     * - field~=regex          : regex match
     * 
     * Multiple conditions (AND logic): field1=value1,field2>value2
     * Multiple IN clauses: field1=[a|b],field2=[x|y|z]
     * 
     * Examples:
     * - "status=active"                     : exact match
     * - "status=[active|pending]"           : IN clause (v1.7.8)
     * - "status=[active|pending],country=[USA|UK]" : multiple IN clauses
     * - "age>25,department=Engineering"     : multiple conditions
     */
    public List<Map<String, String>> jsonSearch(String query) throws IOException {
        String response = sendCommand("JSEARCH", query);
        return parseSearchResults(response);
    }
    
    private List<Map<String, String>> parseSearchResults(String response) {
        List<Map<String, String>> results = new ArrayList<>();
        
        if (response == null || response.equals("[]") || response.isEmpty()) {
            return results;
        }
        
        // Parse array format: [key1:json1, key2:json2, ...]
        String content = response;
        if (content.startsWith("[") && content.endsWith("]")) {
            content = content.substring(1, content.length() - 1);
        }
        
        // Split by ", " but be careful with JSON content
        int depth = 0;
        int start = 0;
        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            if (c == '{' || c == '[') depth++;
            else if (c == '}' || c == ']') depth--;
            else if (c == ',' && depth == 0) {
                String item = content.substring(start, i).trim();
                parseResultItem(item, results);
                start = i + 1;
            }
        }
        // Last item
        if (start < content.length()) {
            String item = content.substring(start).trim();
            parseResultItem(item, results);
        }
        
        return results;
    }
    
    private void parseResultItem(String item, List<Map<String, String>> results) {
        // Format: key:{"json":"value"}
        // Key might contain colons (e.g., "prod:1001"), so find where JSON starts
        // JSON always starts with { or [
        int jsonStart = -1;
        for (int i = 0; i < item.length() - 1; i++) {
            if (item.charAt(i) == ':' && (item.charAt(i + 1) == '{' || item.charAt(i + 1) == '[')) {
                jsonStart = i;
                break;
            }
        }
        if (jsonStart > 0) {
            Map<String, String> result = new HashMap<>();
            result.put("key", item.substring(0, jsonStart));
            result.put("value", item.substring(jsonStart + 1));
            results.add(result);
        }
    }
    
    // ========================================================================
    // KEY SEARCH OPERATIONS
    // ========================================================================
    
    public List<String> keys(String pattern) throws IOException {
        String response = sendCommand("KEYS", pattern);
        return parseStringArray(response);
    }
    
    public List<String> ksearch(String regex, int limit) throws IOException {
        String response = sendCommand("KSEARCH", regex, "LIMIT", String.valueOf(limit));
        return parseStringArray(response);
    }
    
    private List<String> parseStringArray(String response) {
        List<String> results = new ArrayList<>();
        if (response == null || response.equals("[]") || response.isEmpty()) {
            return results;
        }
        
        String content = response;
        if (content.startsWith("[") && content.endsWith("]")) {
            content = content.substring(1, content.length() - 1);
        }
        
        for (String item : content.split(", ")) {
            String trimmed = item.trim();
            if (!trimmed.isEmpty() && !trimmed.equals("null")) {
                results.add(trimmed);
            }
        }
        return results;
    }
    
    public String delete(String key) throws IOException {
        return sendCommand("DEL", key);
    }
    
    // ========================================================================
    // DEMO METHODS
    // ========================================================================
    
    private static void printHeader(String title) {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("  " + title);
        System.out.println("=".repeat(70));
    }
    
    private static void printResult(String label, String value) {
        System.out.println("  " + label + ": " + value);
    }
    
    public void demoA_SetJson() throws IOException {
        printHeader("A) SET KEY WITH JSON VALUE");
        
        String[][] employees = {
            {"EMP001", """
                {"id":"EMP001","name":"John Smith","email":"john.smith@company.com",
                 "department":"Engineering","salary":95000,"skills":["Python","Java","Kubernetes"],
                 "address":{"city":"San Francisco","state":"CA","zip":"94102"},"active":true}"""},
            {"EMP002", """
                {"id":"EMP002","name":"Jane Doe","email":"jane.doe@company.com",
                 "department":"Engineering","salary":105000,"skills":["JavaScript","React","Node.js"],
                 "address":{"city":"New York","state":"NY","zip":"10001"},"active":true}"""},
            {"EMP003", """
                {"id":"EMP003","name":"Bob Johnson","email":"bob.j@company.com",
                 "department":"Sales","salary":85000,"skills":["CRM","Negotiation"],
                 "address":{"city":"Chicago","state":"IL","zip":"60601"},"active":true}"""},
            {"EMP004", """
                {"id":"EMP004","name":"Alice Brown","email":"alice.brown@company.com",
                 "department":"Engineering","salary":115000,"skills":["Python","Machine Learning","TensorFlow"],
                 "address":{"city":"Seattle","state":"WA","zip":"98101"},"active":false}"""},
            {"EMP005", """
                {"id":"EMP005","name":"Charlie Wilson","email":"charlie.w@company.com",
                 "department":"Marketing","salary":78000,"skills":["SEO","Content Marketing"],
                 "address":{"city":"Los Angeles","state":"CA","zip":"90001"},"active":true}"""}
        };
        
        System.out.println("\n  Storing employee records as JSON...");
        for (String[] emp : employees) {
            String key = "employee:" + emp[0];
            String json = emp[1].replaceAll("\\s+", "");
            jsonSet(key, json);
            System.out.println("    ✓ Stored " + key);
        }
        System.out.println("\n  Total records stored: " + employees.length);
    }
    
    public void demoB_GetJson() throws IOException {
        printHeader("B) RETRIEVE KEY WITH JSON VALUE");
        
        System.out.println("\n  Retrieving employee:EMP001...");
        String emp = jsonGet("employee:EMP001");
        printResult("Full JSON", emp);
        
        System.out.println("\n  Retrieving specific paths...");
        printResult("$.name", jsonGet("employee:EMP001", "$.name"));
        printResult("$.salary", jsonGet("employee:EMP001", "$.salary"));
        printResult("$.skills", jsonGet("employee:EMP001", "$.skills"));
        printResult("$.address.city", jsonGet("employee:EMP001", "$.address.city"));
    }
    
    public void demoC_SearchSingleAttribute() throws IOException {
        printHeader("C) JSON SEARCH - SINGLE ATTRIBUTE");
        
        System.out.println("\n  Search: department=Engineering");
        List<Map<String, String>> results = jsonSearch("department=Engineering");
        System.out.println("  Found " + results.size() + " results:");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
        
        System.out.println("\n  Search: salary>90000");
        results = jsonSearch("salary>90000");
        System.out.println("  Found " + results.size() + " results:");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
        
        System.out.println("\n  Search: active=true");
        results = jsonSearch("active=true");
        System.out.println("  Found " + results.size() + " results:");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
    }
    
    public void demoC_SearchMultipleAttributes() throws IOException {
        printHeader("C) JSON SEARCH - MULTIPLE ATTRIBUTES");
        
        System.out.println("\n  Search: department=Engineering,salary>100000");
        List<Map<String, String>> results = jsonSearch("department=Engineering,salary>100000");
        System.out.println("  Found " + results.size() + " results:");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
        
        System.out.println("\n  Search: department=Engineering,active=true");
        results = jsonSearch("department=Engineering,active=true");
        System.out.println("  Found " + results.size() + " results:");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
        
        System.out.println("\n  Search: address.state=CA,salary>=78000");
        results = jsonSearch("address.state=CA,salary>=78000");
        System.out.println("  Found " + results.size() + " results (employees in California with salary >= $78,000):");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
    }
    
    public void demoC_InClauseSearch() throws IOException {
        printHeader("C) JSON SEARCH - IN CLAUSE (v1.7.8)");
        
        System.out.println("\n  The IN clause allows matching a field against multiple values.");
        System.out.println("  Syntax: field=[value1|value2|value3]");
        System.out.println("  Logic: Returns records where field equals ANY of the values.");
        
        // IN clause - single attribute with multiple values
        System.out.println("\n  Search: department=[Engineering|Sales]");
        System.out.println("  (Find employees in Engineering OR Sales departments)");
        List<Map<String, String>> results = jsonSearch("department=[Engineering|Sales]");
        System.out.println("  Found " + results.size() + " results:");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
        
        // IN clause - multiple attributes, each with multiple values
        System.out.println("\n  Search: department=[Engineering|Sales],address.state=[CA|NY]");
        System.out.println("  (Find employees in (Engineering OR Sales) AND in (California OR New York))");
        results = jsonSearch("department=[Engineering|Sales],address.state=[CA|NY]");
        System.out.println("  Found " + results.size() + " results:");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
        
        // IN clause combined with comparison operator
        System.out.println("\n  Search: department=[Engineering|Marketing],salary>80000");
        System.out.println("  (Find high-earning employees in Engineering OR Marketing)");
        results = jsonSearch("department=[Engineering|Marketing],salary>80000");
        System.out.println("  Found " + results.size() + " results:");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
        
        // IN clause with three values
        System.out.println("\n  Search: address.state=[CA|NY|TX]");
        System.out.println("  (Find employees in California, New York, or Texas)");
        results = jsonSearch("address.state=[CA|NY|TX]");
        System.out.println("  Found " + results.size() + " results:");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
        
        // Building IN clause query programmatically
        System.out.println("\n  Building query programmatically:");
        String[] departments = {"Engineering", "Sales", "HR"};
        String[] states = {"CA", "NY"};
        String query = "department=[" + String.join("|", departments) + "],address.state=[" + String.join("|", states) + "]";
        System.out.println("  Query: " + query);
        results = jsonSearch(query);
        System.out.println("  Found " + results.size() + " results");
    }
    
    public void demoD_RegexSearchJson() throws IOException {
        printHeader("D) REGEX SEARCH ON JSON ATTRIBUTE VALUES");
        
        System.out.println("\n  Search: email~=.*@company\\\\.com");
        List<Map<String, String>> results = jsonSearch("email~=.*@company\\.com");
        System.out.println("  Found " + results.size() + " results:");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
        
        System.out.println("\n  Search: name~=^J.*");
        results = jsonSearch("name~=^J.*");
        System.out.println("  Found " + results.size() + " results (names starting with 'J'):");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
        
        System.out.println("\n  Search: address.zip~=^9.*");
        results = jsonSearch("address.zip~=^9.*");
        System.out.println("  Found " + results.size() + " results (zip codes starting with '9' - West Coast):");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
        
        System.out.println("\n  Search: name~=.*son$,department=Engineering");
        results = jsonSearch("name~=.*son$,department=Engineering");
        System.out.println("  Found " + results.size() + " results (names ending in 'son' in Engineering):");
        for (Map<String, String> r : results) {
            System.out.println("    - " + r.get("key"));
        }
    }
    
    public void demoE_KeyRegexSearch() throws IOException {
        printHeader("E) SEARCH KEYS USING REGEX");
        
        System.out.println("\n  KEYS employee:* (glob pattern)");
        List<String> keys = keys("employee:*");
        System.out.println("  Found " + keys.size() + " keys:");
        for (String k : keys) {
            System.out.println("    - " + k);
        }
        
        System.out.println("\n  KSEARCH employee:EMP00[1-3] (regex pattern)");
        List<String> results = ksearch("employee:EMP00[1-3]", 100);
        System.out.println("  Found " + results.size() + " results:");
        for (String r : results) {
            System.out.println("    - " + r);
        }
        
        System.out.println("\n  KSEARCH .*EMP00[0-9] (any key ending with EMP00X)");
        results = ksearch(".*EMP00[0-9]", 100);
        System.out.println("  Found " + results.size() + " results:");
        for (String r : results) {
            System.out.println("    - " + r);
        }
    }
    
    public void cleanup() throws IOException {
        printHeader("CLEANUP");
        
        System.out.println("\n  Deleting demo keys...");
        for (int i = 1; i <= 5; i++) {
            String key = "employee:EMP00" + i;
            delete(key);
            System.out.println("    ✓ Deleted " + key);
        }
    }
    
    // ========================================================================
    // MAIN
    // ========================================================================
    
    public static void main(String[] args) {
        System.out.println("""
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║   KUBER DISTRIBUTED CACHE - JSON OPERATIONS DEMO (Java)              ║
║                                                                      ║
║   Demonstrates:                                                      ║
║   a) Set key with JSON value                                         ║
║   b) Retrieve key with JSON value                                    ║
║   c) JSON search with single/multiple attributes                     ║
║      - IN clause for matching multiple values (v1.7.8)               ║
║   d) Regex search on JSON attribute values                           ║
║   e) Search keys using regex                                         ║
║                                                                      ║
║   v1.7.8 - JSEARCH IN Clause: field=[value1|value2|value3]           ║
╚══════════════════════════════════════════════════════════════════════╝
        """);
        
        // Parse command line arguments
        if (args.length >= 1) HOST = args[0];
        if (args.length >= 2) PORT = Integer.parseInt(args[1]);
        if (args.length >= 3) API_KEY = args[2];
        
        // Also check environment variables
        String envHost = System.getenv("KUBER_HOST");
        String envPort = System.getenv("KUBER_PORT");
        String envApiKey = System.getenv("KUBER_API_KEY");
        
        if (envHost != null) HOST = envHost;
        if (envPort != null) PORT = Integer.parseInt(envPort);
        if (envApiKey != null) API_KEY = envApiKey;
        
        System.out.println("Configuration:");
        System.out.println("  Host:    " + HOST);
        System.out.println("  Port:    " + PORT);
        System.out.println("  API Key: " + API_KEY.substring(0, 12) + "..." + 
                          API_KEY.substring(API_KEY.length() - 4));
        
        KuberJsonDemo demo = new KuberJsonDemo();
        
        try {
            demo.connect();
            demo.selectRegion("json-demo");
            System.out.println("  Region:  json-demo");
            
            // Run all demos
            demo.demoA_SetJson();
            demo.demoB_GetJson();
            demo.demoC_SearchSingleAttribute();
            demo.demoC_SearchMultipleAttributes();
            demo.demoC_InClauseSearch();  // v1.7.8 - IN clause demo
            demo.demoD_RegexSearchJson();
            demo.demoE_KeyRegexSearch();
            
            // Cleanup
            demo.cleanup();
            
            printHeader("ALL DEMOS COMPLETED SUCCESSFULLY");
            
        } catch (Exception e) {
            System.err.println("\n❌ Error: " + e.getMessage());
            System.err.println("\nTroubleshooting:");
            System.err.println("  1. Ensure Kuber server is running");
            System.err.println("  2. Check host/port configuration");
            System.err.println("  3. Verify API key is valid (generate in Web UI: Admin → API Keys)");
            System.err.println("\nUsage: java KuberJsonDemo [host] [port] [api-key]");
            e.printStackTrace();
            System.exit(1);
        } finally {
            demo.close();
        }
    }
}
