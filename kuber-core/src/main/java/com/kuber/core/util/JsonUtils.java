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
package com.kuber.core.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kuber.core.exception.KuberException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Utility class for JSON operations including path queries and searches.
 */
public final class JsonUtils {
    
    private static final ObjectMapper objectMapper;
    
    static {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }
    
    private JsonUtils() {
        // Prevent instantiation
    }
    
    /**
     * Get the shared ObjectMapper instance
     */
    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }
    
    /**
     * Parse JSON string to JsonNode
     */
    public static JsonNode parse(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new KuberException(KuberException.ErrorCode.JSON_PARSE_ERROR, 
                    "Failed to parse JSON: " + e.getMessage(), e);
        }
    }
    
    /**
     * Parse JSON string to specified type using TypeReference
     */
    public static <T> T fromJson(String json, TypeReference<T> typeReference) {
        try {
            return objectMapper.readValue(json, typeReference);
        } catch (JsonProcessingException e) {
            throw new KuberException(KuberException.ErrorCode.JSON_PARSE_ERROR, 
                    "Failed to parse JSON: " + e.getMessage(), e);
        }
    }
    
    /**
     * Convert object to JSON string
     */
    public static String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new KuberException(KuberException.ErrorCode.JSON_PARSE_ERROR, 
                    "Failed to serialize to JSON: " + e.getMessage(), e);
        }
    }
    
    /**
     * Convert object to pretty JSON string
     */
    public static String toPrettyJson(Object obj) {
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new KuberException(KuberException.ErrorCode.JSON_PARSE_ERROR, 
                    "Failed to serialize to JSON: " + e.getMessage(), e);
        }
    }
    
    /**
     * Get value at JSON path (simplified JSONPath-like syntax)
     * Supports: $.field, $.field.subfield, $.array[0], $.array[*]
     */
    public static JsonNode getPath(JsonNode root, String path) {
        if (root == null || path == null || path.isEmpty()) {
            return null;
        }
        
        // Remove leading $ if present
        if (path.startsWith("$")) {
            path = path.substring(1);
        }
        if (path.startsWith(".")) {
            path = path.substring(1);
        }
        
        if (path.isEmpty()) {
            return root;
        }
        
        String[] parts = splitPath(path);
        JsonNode current = root;
        
        for (String part : parts) {
            if (current == null) {
                return null;
            }
            
            // Handle array index
            if (part.contains("[")) {
                int bracketStart = part.indexOf('[');
                int bracketEnd = part.indexOf(']');
                String fieldName = part.substring(0, bracketStart);
                String indexStr = part.substring(bracketStart + 1, bracketEnd);
                
                if (!fieldName.isEmpty()) {
                    current = current.get(fieldName);
                }
                
                if (current != null && current.isArray()) {
                    if ("*".equals(indexStr)) {
                        // Return all elements - caller should handle
                        return current;
                    } else {
                        int index = Integer.parseInt(indexStr);
                        current = current.get(index);
                    }
                } else {
                    return null;
                }
            } else {
                current = current.get(part);
            }
        }
        
        return current;
    }
    
    /**
     * Set value at JSON path
     */
    public static JsonNode setPath(JsonNode root, String path, JsonNode value) {
        if (root == null || !root.isObject()) {
            throw new KuberException(KuberException.ErrorCode.JSON_PATH_ERROR, 
                    "Root must be an object");
        }
        
        // Remove leading $ if present
        if (path.startsWith("$")) {
            path = path.substring(1);
        }
        if (path.startsWith(".")) {
            path = path.substring(1);
        }
        
        if (path.isEmpty()) {
            return value;
        }
        
        String[] parts = splitPath(path);
        ObjectNode current = (ObjectNode) root;
        
        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i];
            JsonNode next = current.get(part);
            
            if (next == null || !next.isObject()) {
                ObjectNode newNode = objectMapper.createObjectNode();
                current.set(part, newNode);
                current = newNode;
            } else {
                current = (ObjectNode) next;
            }
        }
        
        String lastPart = parts[parts.length - 1];
        current.set(lastPart, value);
        
        return root;
    }
    
    /**
     * Delete value at JSON path
     */
    public static JsonNode deletePath(JsonNode root, String path) {
        if (root == null || !root.isObject()) {
            return root;
        }
        
        // Remove leading $ if present
        if (path.startsWith("$")) {
            path = path.substring(1);
        }
        if (path.startsWith(".")) {
            path = path.substring(1);
        }
        
        if (path.isEmpty()) {
            return objectMapper.createObjectNode();
        }
        
        String[] parts = splitPath(path);
        ObjectNode current = (ObjectNode) root;
        
        for (int i = 0; i < parts.length - 1; i++) {
            JsonNode next = current.get(parts[i]);
            if (next == null || !next.isObject()) {
                return root;
            }
            current = (ObjectNode) next;
        }
        
        current.remove(parts[parts.length - 1]);
        return root;
    }
    
    /**
     * Search JSON values using operators
     * Supports: =, !=, >, <, >=, <=, ~= (regex), contains
     */
    public static boolean matchesQuery(JsonNode json, String field, String operator, String value) {
        JsonNode fieldValue = getPath(json, field);
        if (fieldValue == null) {
            return "!=".equals(operator);
        }
        
        String actualValue = fieldValue.isTextual() ? fieldValue.asText() : fieldValue.toString();
        
        switch (operator) {
            case "=":
            case "==":
                return actualValue.equals(value);
                
            case "!=":
            case "<>":
                return !actualValue.equals(value);
                
            case ">":
                return compareNumbers(actualValue, value) > 0;
                
            case "<":
                return compareNumbers(actualValue, value) < 0;
                
            case ">=":
                return compareNumbers(actualValue, value) >= 0;
                
            case "<=":
                return compareNumbers(actualValue, value) <= 0;
                
            case "~=":
            case "regex":
                try {
                    Pattern pattern = Pattern.compile(value);
                    return pattern.matcher(actualValue).matches();
                } catch (Exception e) {
                    return false;
                }
                
            case "contains":
                if (fieldValue.isArray()) {
                    for (JsonNode element : fieldValue) {
                        if (element.asText().equals(value)) {
                            return true;
                        }
                    }
                    return false;
                }
                return actualValue.contains(value);
                
            case "startsWith":
                return actualValue.startsWith(value);
                
            case "endsWith":
                return actualValue.endsWith(value);
                
            case "exists":
                return "true".equalsIgnoreCase(value);
                
            default:
                return false;
        }
    }
    
    /**
     * Check if JSON matches all query conditions
     */
    public static boolean matchesAllQueries(JsonNode json, List<QueryCondition> conditions) {
        for (QueryCondition condition : conditions) {
            if (!matchesQuery(json, condition.getField(), condition.getOperator(), condition.getValue())) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Check if JSON matches any query condition
     */
    public static boolean matchesAnyQuery(JsonNode json, List<QueryCondition> conditions) {
        for (QueryCondition condition : conditions) {
            if (matchesQuery(json, condition.getField(), condition.getOperator(), condition.getValue())) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Parse query string into conditions
     * Format: field1=value1,field2>value2,field3~=pattern
     */
    public static List<QueryCondition> parseQuery(String query) {
        List<QueryCondition> conditions = new ArrayList<>();
        
        if (query == null || query.isEmpty()) {
            return conditions;
        }
        
        String[] parts = query.split(",");
        for (String part : parts) {
            QueryCondition condition = parseCondition(part.trim());
            if (condition != null) {
                conditions.add(condition);
            }
        }
        
        return conditions;
    }
    
    /**
     * Parse a single condition
     */
    private static QueryCondition parseCondition(String condition) {
        String[] operators = {"~=", ">=", "<=", "!=", "<>", "==", "=", ">", "<"};
        
        for (String op : operators) {
            int idx = condition.indexOf(op);
            if (idx > 0) {
                String field = condition.substring(0, idx).trim();
                String value = condition.substring(idx + op.length()).trim();
                return new QueryCondition(field, op, value);
            }
        }
        
        return null;
    }
    
    /**
     * Split JSON path into parts
     */
    private static String[] splitPath(String path) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inBracket = false;
        
        for (char c : path.toCharArray()) {
            if (c == '[') {
                inBracket = true;
                current.append(c);
            } else if (c == ']') {
                inBracket = false;
                current.append(c);
            } else if (c == '.' && !inBracket) {
                if (current.length() > 0) {
                    parts.add(current.toString());
                    current = new StringBuilder();
                }
            } else {
                current.append(c);
            }
        }
        
        if (current.length() > 0) {
            parts.add(current.toString());
        }
        
        return parts.toArray(new String[0]);
    }
    
    /**
     * Compare two values as numbers
     */
    private static int compareNumbers(String a, String b) {
        try {
            double da = Double.parseDouble(a);
            double db = Double.parseDouble(b);
            return Double.compare(da, db);
        } catch (NumberFormatException e) {
            return a.compareTo(b);
        }
    }
    
    /**
     * Query condition holder
     */
    public static class QueryCondition {
        private final String field;
        private final String operator;
        private final String value;
        
        public QueryCondition(String field, String operator, String value) {
            this.field = field;
            this.operator = operator;
            this.value = value;
        }
        
        public String getField() {
            return field;
        }
        
        public String getOperator() {
            return operator;
        }
        
        public String getValue() {
            return value;
        }
    }
    
    /**
     * Apply attribute mapping to a JSON node.
     * Renames attributes in the JSON according to the provided mapping.
     * Supports nested attributes using dot notation (e.g., "address.street" -> "addr.str").
     * 
     * @param json The JSON node to transform
     * @param attributeMapping Map of source attribute names to target attribute names
     * @return New JSON node with renamed attributes
     */
    public static JsonNode applyAttributeMapping(JsonNode json, Map<String, String> attributeMapping) {
        if (json == null || attributeMapping == null || attributeMapping.isEmpty()) {
            return json;
        }
        
        if (json.isObject()) {
            ObjectNode result = objectMapper.createObjectNode();
            Iterator<Map.Entry<String, JsonNode>> fields = json.fields();
            
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                JsonNode fieldValue = field.getValue();
                
                // Check if this field should be renamed
                String newName = attributeMapping.getOrDefault(fieldName, fieldName);
                
                // Recursively apply mapping to nested objects/arrays
                JsonNode transformedValue = applyAttributeMapping(fieldValue, attributeMapping);
                result.set(newName, transformedValue);
            }
            
            // Handle nested dot-notation mappings (e.g., "address.city" -> "addr.city")
            for (Map.Entry<String, String> mapping : attributeMapping.entrySet()) {
                String sourcePath = mapping.getKey();
                String targetPath = mapping.getValue();
                
                if (sourcePath.contains(".")) {
                    // This is a nested path mapping
                    JsonNode sourceValue = getPath(result, sourcePath);
                    if (sourceValue != null) {
                        // Delete from source path
                        deletePath(result, sourcePath);
                        // Set at target path
                        setPath(result, targetPath, sourceValue);
                    }
                }
            }
            
            return result;
        } else if (json.isArray()) {
            ArrayNode result = objectMapper.createArrayNode();
            for (JsonNode element : json) {
                result.add(applyAttributeMapping(element, attributeMapping));
            }
            return result;
        }
        
        return json;
    }
    
    /**
     * Apply attribute mapping to a JSON string.
     * 
     * @param jsonString The JSON string to transform
     * @param attributeMapping Map of source attribute names to target attribute names
     * @return Transformed JSON string
     */
    public static String applyAttributeMapping(String jsonString, Map<String, String> attributeMapping) {
        if (jsonString == null || jsonString.isEmpty() || 
            attributeMapping == null || attributeMapping.isEmpty()) {
            return jsonString;
        }
        
        try {
            JsonNode json = parse(jsonString);
            JsonNode transformed = applyAttributeMapping(json, attributeMapping);
            return toJson(transformed);
        } catch (Exception e) {
            // If parsing fails, return original string
            return jsonString;
        }
    }
    
    /**
     * Check if a string appears to be valid JSON
     */
    public static boolean isValidJson(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        str = str.trim();
        return (str.startsWith("{") && str.endsWith("}")) || 
               (str.startsWith("[") && str.endsWith("]"));
    }
}
