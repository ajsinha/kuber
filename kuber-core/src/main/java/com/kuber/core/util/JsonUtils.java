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
                    // Use find() for partial match, not matches() which requires full string match
                    return pattern.matcher(actualValue).find();
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
     * Check if JSON matches all query conditions.
     * Supports IN clause for matching against multiple values.
     * 
     * @since 1.7.9 - Added IN clause support
     */
    public static boolean matchesAllQueries(JsonNode json, List<QueryCondition> conditions) {
        for (QueryCondition condition : conditions) {
            if (condition.isInClause()) {
                // IN clause: match if field equals ANY of the values
                if (!matchesInClause(json, condition.getField(), condition.getOperator(), condition.getValues())) {
                    return false;
                }
            } else {
                if (!matchesQuery(json, condition.getField(), condition.getOperator(), condition.getValue())) {
                    return false;
                }
            }
        }
        return true;
    }
    
    /**
     * Check if JSON field matches any of the values in an IN clause.
     * 
     * @param json The JSON node to check
     * @param field The field path to check
     * @param operator The operator (typically "=" or "==")
     * @param values List of values to match against
     * @return true if field value matches any of the values
     * @since 1.7.9
     */
    public static boolean matchesInClause(JsonNode json, String field, String operator, List<String> values) {
        JsonNode fieldValue = getPath(json, field);
        if (fieldValue == null || fieldValue.isNull() || fieldValue.isMissingNode()) {
            // For != operator with IN, null means NOT in list, so true
            return "!=".equals(operator) || "<>".equals(operator);
        }
        
        String actualValue = fieldValue.isTextual() ? fieldValue.asText() : fieldValue.toString();
        
        // For equality operators, check if actual value is in the list
        if ("=".equals(operator) || "==".equals(operator)) {
            for (String value : values) {
                if (actualValue.equals(value)) {
                    return true;
                }
            }
            return false;
        }
        
        // For not-equals operators, check if actual value is NOT in the list
        if ("!=".equals(operator) || "<>".equals(operator)) {
            for (String value : values) {
                if (actualValue.equals(value)) {
                    return false;
                }
            }
            return true;
        }
        
        // For regex operator, check if actual value matches any pattern
        if ("~=".equals(operator) || "regex".equals(operator)) {
            for (String pattern : values) {
                try {
                    if (Pattern.compile(pattern).matcher(actualValue).matches()) {
                        return true;
                    }
                } catch (Exception e) {
                    // Invalid pattern, skip
                }
            }
            return false;
        }
        
        // For comparison operators, not typical with IN clause but support anyway
        // Returns true if comparison is true for ANY value
        for (String value : values) {
            if (matchesQuery(json, field, operator, value)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Check if JSON matches any query condition.
     * Supports IN clause for matching against multiple values.
     * 
     * @since 1.7.9 - Added IN clause support
     */
    public static boolean matchesAnyQuery(JsonNode json, List<QueryCondition> conditions) {
        for (QueryCondition condition : conditions) {
            if (condition.isInClause()) {
                if (matchesInClause(json, condition.getField(), condition.getOperator(), condition.getValues())) {
                    return true;
                }
            } else {
                if (matchesQuery(json, condition.getField(), condition.getOperator(), condition.getValue())) {
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * Parse query string into conditions.
     * 
     * Format: field1=value1,field2>value2,field3~=pattern
     * 
     * Supports IN clause for matching multiple values:
     *   field=[value1|value2|value3]
     * 
     * Examples:
     *   - "status=active" - single attribute, single value
     *   - "status=[active|pending]" - single attribute, IN clause with multiple values
     *   - "status=active,country=USA" - multiple attributes, single values each
     *   - "status=[active|pending],country=[USA|UK|CA]" - multiple attributes, each with IN clause
     *   - "age>=18,age<=65" - numeric range comparison
     *   - "email~=.*@company\\.com" - regex matching
     * 
     * @since 1.7.9 - Added IN clause support with [value1|value2|...] syntax
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
     * Parse a single condition.
     * 
     * Supports:
     * - Simple conditions: field=value, field>10, field~=pattern
     * - IN clause: field=[value1|value2|value3]
     * 
     * @since 1.7.9 - Added IN clause support with [value1|value2|...] syntax
     */
    private static QueryCondition parseCondition(String condition) {
        String[] operators = {"~=", ">=", "<=", "!=", "<>", "==", "=", ">", "<"};
        
        for (String op : operators) {
            int idx = condition.indexOf(op);
            if (idx > 0) {
                String field = condition.substring(0, idx).trim();
                String value = condition.substring(idx + op.length()).trim();
                
                // Strip surrounding quotes from value if present
                value = stripQuotes(value);
                
                // Check for IN clause syntax: [value1|value2|value3]
                if (value.startsWith("[") && value.endsWith("]")) {
                    String innerValues = value.substring(1, value.length() - 1);
                    String[] parts = innerValues.split("\\|");
                    List<String> valueList = new ArrayList<>();
                    for (String part : parts) {
                        String trimmed = stripQuotes(part.trim());
                        if (!trimmed.isEmpty()) {
                            valueList.add(trimmed);
                        }
                    }
                    if (!valueList.isEmpty()) {
                        return new QueryCondition(field, op, valueList);
                    }
                }
                
                return new QueryCondition(field, op, value);
            }
        }
        
        return null;
    }
    
    /**
     * Strip surrounding single or double quotes from a value.
     */
    private static String stripQuotes(String value) {
        if (value == null || value.length() < 2) {
            return value;
        }
        if ((value.startsWith("\"") && value.endsWith("\"")) ||
            (value.startsWith("'") && value.endsWith("'"))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
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
     * Query condition holder.
     * Supports single value matching and IN clause with multiple values.
     * 
     * Syntax:
     * - Single value: field=value, field>value, field~=pattern
     * - Multiple values (IN): field=[value1|value2|value3]
     * 
     * @since 1.7.9 - Added support for IN clause with multiple values
     */
    public static class QueryCondition {
        private final String field;
        private final String operator;
        private final String value;           // Single value (for backward compatibility)
        private final List<String> values;    // Multiple values for IN clause
        private final boolean isInClause;
        
        /**
         * Constructor for single value condition.
         */
        public QueryCondition(String field, String operator, String value) {
            this.field = field;
            this.operator = operator;
            this.value = value;
            this.values = null;
            this.isInClause = false;
        }
        
        /**
         * Constructor for IN clause with multiple values.
         */
        public QueryCondition(String field, String operator, List<String> values) {
            this.field = field;
            this.operator = operator;
            this.value = values.isEmpty() ? "" : values.get(0);
            this.values = new ArrayList<>(values);
            this.isInClause = true;
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
        
        public List<String> getValues() {
            return values;
        }
        
        public boolean isInClause() {
            return isInClause;
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
