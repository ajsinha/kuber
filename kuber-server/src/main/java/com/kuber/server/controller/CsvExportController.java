/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.server.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.kuber.core.model.CacheEntry;
import com.kuber.core.util.JsonUtils;
import com.kuber.server.cache.CacheService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Controller for exporting cache data as CSV files.
 */
@RestController
@RequestMapping("/api/export")
@RequiredArgsConstructor
@Slf4j
public class CsvExportController {

    private final CacheService cacheService;
    
    private static final DateTimeFormatter FILENAME_FORMATTER = 
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    
    /**
     * Export all entries in a region as CSV.
     */
    @GetMapping("/region/{region}")
    public ResponseEntity<byte[]> exportRegion(
            @PathVariable String region,
            @RequestParam(required = false) Integer limit) {
        int effectiveLimit = getEffectiveLimit(limit);
        log.info("Exporting region '{}' as CSV with limit {}", region, effectiveLimit);
        
        StringBuilder csv = new StringBuilder();
        csv.append("key,value,type,ttl\n");
        
        Set<String> keys = cacheService.keys(region, "*", effectiveLimit);
        
        for (String key : keys) {
            String value = cacheService.get(region, key);
            String type = cacheService.type(region, key);
            long ttl = cacheService.ttl(region, key);
            
            csv.append(escapeCsvField(key)).append(",");
            csv.append(escapeCsvField(value != null ? value : "")).append(",");
            csv.append(type).append(",");
            csv.append(ttl).append("\n");
        }
        
        String filename = String.format("kuber_%s_%s.csv", 
                region, LocalDateTime.now().format(FILENAME_FORMATTER));
        
        return buildCsvResponse(csv.toString(), filename);
    }
    
    /**
     * Export keys matching a pattern as CSV.
     */
    @GetMapping("/keys")
    public ResponseEntity<byte[]> exportKeys(
            @RequestParam(defaultValue = "default") String region,
            @RequestParam(defaultValue = "*") String pattern,
            @RequestParam(required = false) Integer limit) {
        int effectiveLimit = getEffectiveLimit(limit);
        log.info("Exporting keys matching '{}' from region '{}' as CSV with limit {}", pattern, region, effectiveLimit);
        
        StringBuilder csv = new StringBuilder();
        csv.append("key,value,type,ttl\n");
        
        Set<String> keys = cacheService.keys(region, pattern, effectiveLimit);
        
        for (String key : keys) {
            String value = cacheService.get(region, key);
            String type = cacheService.type(region, key);
            long ttl = cacheService.ttl(region, key);
            
            csv.append(escapeCsvField(key)).append(",");
            csv.append(escapeCsvField(value != null ? value : "")).append(",");
            csv.append(type).append(",");
            csv.append(ttl).append("\n");
        }
        
        String filename = String.format("kuber_keys_%s_%s.csv", 
                region, LocalDateTime.now().format(FILENAME_FORMATTER));
        
        return buildCsvResponse(csv.toString(), filename);
    }
    
    /**
     * Export key search results (regex) as CSV.
     */
    @GetMapping("/ksearch")
    public ResponseEntity<byte[]> exportKeySearch(
            @RequestParam(defaultValue = "default") String region,
            @RequestParam(defaultValue = ".*") String regex,
            @RequestParam(required = false) Integer limit) {
        int effectiveLimit = getEffectiveLimit(limit);
        log.info("Exporting key search results for regex '{}' from region '{}' as CSV with limit {}", regex, region, effectiveLimit);
        
        StringBuilder csv = new StringBuilder();
        csv.append("key,value,type,ttl\n");
        
        List<Map<String, Object>> results = cacheService.searchKeysByRegex(region, regex, effectiveLimit);
        
        for (Map<String, Object> entry : results) {
            csv.append(escapeCsvField(String.valueOf(entry.get("key")))).append(",");
            csv.append(escapeCsvField(String.valueOf(entry.get("value")))).append(",");
            csv.append(entry.get("type")).append(",");
            csv.append(entry.get("ttl")).append("\n");
        }
        
        String filename = String.format("kuber_ksearch_%s_%s.csv", 
                region, LocalDateTime.now().format(FILENAME_FORMATTER));
        
        return buildCsvResponse(csv.toString(), filename);
    }
    
    /**
     * Export JSON search results as CSV.
     */
    @GetMapping("/jsearch")
    public ResponseEntity<byte[]> exportJsonSearch(
            @RequestParam(defaultValue = "default") String region,
            @RequestParam String query,
            @RequestParam(required = false) Integer limit) {
        int effectiveLimit = getEffectiveLimit(limit);
        log.info("Exporting JSON search results for query '{}' from region '{}' as CSV with limit {}", query, region, effectiveLimit);
        
        StringBuilder csv = new StringBuilder();
        csv.append("key,value,type,ttl\n");
        
        List<CacheEntry> results = cacheService.jsonSearch(region, query, effectiveLimit);
        
        for (CacheEntry entry : results) {
            csv.append(escapeCsvField(entry.getKey())).append(",");
            csv.append(escapeCsvField(entry.getStringValue())).append(",");
            csv.append("json").append(",");
            csv.append(entry.getTtlSeconds()).append("\n");
        }
        
        String filename = String.format("kuber_jsearch_%s_%s.csv", 
                region, LocalDateTime.now().format(FILENAME_FORMATTER));
        
        return buildCsvResponse(csv.toString(), filename);
    }
    
    /**
     * Export hash as CSV.
     */
    @GetMapping("/hash")
    public ResponseEntity<byte[]> exportHash(
            @RequestParam(defaultValue = "default") String region,
            @RequestParam String key) {
        log.info("Exporting hash '{}' from region '{}' as CSV", key, region);
        
        StringBuilder csv = new StringBuilder();
        csv.append("field,value\n");
        
        Map<String, String> hash = cacheService.hgetall(region, key);
        
        if (hash != null) {
            for (Map.Entry<String, String> entry : hash.entrySet()) {
                csv.append(escapeCsvField(entry.getKey())).append(",");
                csv.append(escapeCsvField(entry.getValue())).append("\n");
            }
        }
        
        String filename = String.format("kuber_hash_%s_%s_%s.csv", 
                region, key, LocalDateTime.now().format(FILENAME_FORMATTER));
        
        return buildCsvResponse(csv.toString(), filename);
    }
    
    private static final int DEFAULT_EXPORT_LIMIT = 10000;
    private static final int MAX_EXPORT_LIMIT = 100000;
    
    /**
     * Export query results based on query type.
     */
    @GetMapping("/query")
    public ResponseEntity<byte[]> exportQueryResults(
            @RequestParam(defaultValue = "default") String region,
            @RequestParam String queryType,
            @RequestParam(required = false) String key,
            @RequestParam(required = false) String jsonQuery,
            @RequestParam(required = false) Integer limit) {
        
        int effectiveLimit = getEffectiveLimit(limit);
        
        log.info("Exporting query results: type={}, region={}, key={}, jsonQuery={}, limit={}", 
                queryType, region, key, jsonQuery, effectiveLimit);
        
        StringBuilder csv = new StringBuilder();
        String filename;
        
        switch (queryType) {
            case "keys":
                csv.append("key\n");
                Set<String> keys = cacheService.keys(region, key != null ? key : "*", effectiveLimit);
                for (String k : keys) {
                    csv.append(escapeCsvField(k)).append("\n");
                }
                filename = String.format("kuber_keys_%s_%s.csv", region, 
                        LocalDateTime.now().format(FILENAME_FORMATTER));
                break;
                
            case "ksearch":
                csv.append("key,value,type,ttl\n");
                List<Map<String, Object>> ksearchResults = cacheService.searchKeysByRegex(
                        region, key != null ? key : ".*", effectiveLimit);
                for (Map<String, Object> entry : ksearchResults) {
                    csv.append(escapeCsvField(String.valueOf(entry.get("key")))).append(",");
                    csv.append(escapeCsvField(String.valueOf(entry.get("value")))).append(",");
                    csv.append(entry.get("type")).append(",");
                    csv.append(entry.get("ttl")).append("\n");
                }
                filename = String.format("kuber_ksearch_%s_%s.csv", region, 
                        LocalDateTime.now().format(FILENAME_FORMATTER));
                break;
                
            case "jsearch":
                csv.append("key,value\n");
                List<CacheEntry> jsearchResults = cacheService.jsonSearch(region, jsonQuery, effectiveLimit);
                for (CacheEntry entry : jsearchResults) {
                    csv.append(escapeCsvField(entry.getKey())).append(",");
                    csv.append(escapeCsvField(entry.getStringValue())).append("\n");
                }
                filename = String.format("kuber_jsearch_%s_%s.csv", region, 
                        LocalDateTime.now().format(FILENAME_FORMATTER));
                break;
                
            case "hgetall":
                csv.append("field,value\n");
                Map<String, String> hash = cacheService.hgetall(region, key);
                if (hash != null) {
                    for (Map.Entry<String, String> entry : hash.entrySet()) {
                        csv.append(escapeCsvField(entry.getKey())).append(",");
                        csv.append(escapeCsvField(entry.getValue())).append("\n");
                    }
                }
                filename = String.format("kuber_hash_%s_%s_%s.csv", region, key, 
                        LocalDateTime.now().format(FILENAME_FORMATTER));
                break;
                
            case "get":
            case "jget":
                csv.append("key,value\n");
                String value = cacheService.get(region, key);
                if (value != null) {
                    csv.append(escapeCsvField(key)).append(",");
                    csv.append(escapeCsvField(value)).append("\n");
                }
                filename = String.format("kuber_entry_%s_%s_%s.csv", region, key, 
                        LocalDateTime.now().format(FILENAME_FORMATTER));
                break;
                
            default:
                return ResponseEntity.badRequest().body("Invalid query type".getBytes());
        }
        
        return buildCsvResponse(csv.toString(), filename);
    }
    
    /**
     * Get effective limit, respecting default and max bounds.
     */
    private int getEffectiveLimit(Integer userLimit) {
        if (userLimit == null || userLimit <= 0) {
            return DEFAULT_EXPORT_LIMIT;
        }
        return Math.min(userLimit, MAX_EXPORT_LIMIT);
    }
    
    /**
     * Escape a field for CSV format.
     */
    private String escapeCsvField(String field) {
        if (field == null) {
            return "";
        }
        // If field contains comma, newline, or quote, wrap in quotes and escape quotes
        if (field.contains(",") || field.contains("\n") || field.contains("\"") || field.contains("\r")) {
            return "\"" + field.replace("\"", "\"\"") + "\"";
        }
        return field;
    }
    
    /**
     * Build HTTP response with CSV content.
     */
    private ResponseEntity<byte[]> buildCsvResponse(String csvContent, String filename) {
        byte[] bytes = csvContent.getBytes(StandardCharsets.UTF_8);
        
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType("text/csv"));
        headers.setContentDispositionFormData("attachment", filename);
        headers.setContentLength(bytes.length);
        
        return ResponseEntity.ok()
                .headers(headers)
                .body(bytes);
    }
}
