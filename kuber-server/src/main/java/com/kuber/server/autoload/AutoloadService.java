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
package com.kuber.server.autoload;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service for automatically loading data from CSV and JSON files into the cache.
 * 
 * <p>Watches the configured autoload/inbox directory for data files (.csv, .json)
 * that have corresponding metadata files (.csv.metadata, .json.metadata).
 * 
 * <p>Metadata file format:
 * <pre>
 * region:myregion
 * ttl:60
 * key_field:id
 * delimiter:,
 * </pre>
 * 
 * <p>For CSV files, each row is converted to JSON using the header fields,
 * and the value from key_field column is used as the cache key.
 * 
 * <p>For JSON files, each line should be a complete JSON object,
 * and the value from key_field is used as the cache key.
 * 
 * <p>After processing, both data and metadata files are moved to outbox.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class AutoloadService {
    
    private final CacheService cacheService;
    private final KuberProperties properties;
    private final ObjectMapper objectMapper;
    
    private ScheduledExecutorService scheduler;
    private Path inboxPath;
    private Path outboxPath;
    
    // Statistics
    private final AtomicLong totalFilesProcessed = new AtomicLong(0);
    private final AtomicLong totalRecordsLoaded = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    
    private static final DateTimeFormatter TIMESTAMP_FORMAT = 
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    
    @PostConstruct
    public void init() {
        if (!properties.getAutoload().isEnabled()) {
            log.info("Autoload service is disabled");
            return;
        }
        
        try {
            initializeDirectories();
            startWatcher();
            log.info("Autoload service started - watching: {}", inboxPath);
        } catch (Exception e) {
            log.error("Failed to initialize autoload service", e);
        }
    }
    
    @PreDestroy
    public void shutdown() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            log.info("Autoload service stopped");
        }
    }
    
    /**
     * Initialize inbox and outbox directories.
     */
    private void initializeDirectories() throws IOException {
        Path basePath = Paths.get(properties.getAutoload().getDirectory());
        inboxPath = basePath.resolve("inbox");
        outboxPath = basePath.resolve("outbox");
        
        if (properties.getAutoload().isCreateDirectories()) {
            Files.createDirectories(inboxPath);
            Files.createDirectories(outboxPath);
            log.info("Created autoload directories: inbox={}, outbox={}", inboxPath, outboxPath);
        } else {
            if (!Files.exists(inboxPath)) {
                throw new IOException("Inbox directory does not exist: " + inboxPath);
            }
            if (!Files.exists(outboxPath)) {
                throw new IOException("Outbox directory does not exist: " + outboxPath);
            }
        }
    }
    
    /**
     * Start the scheduled watcher.
     */
    private void startWatcher() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kuber-autoload");
            t.setDaemon(true);
            return t;
        });
        
        int intervalSeconds = properties.getAutoload().getScanIntervalSeconds();
        scheduler.scheduleWithFixedDelay(
                this::scanAndProcess,
                intervalSeconds,  // initial delay
                intervalSeconds,  // period
                TimeUnit.SECONDS
        );
        
        log.info("Autoload watcher scheduled with interval: {} seconds", intervalSeconds);
    }
    
    /**
     * Scan inbox and process files.
     */
    public void scanAndProcess() {
        try {
            if (!Files.exists(inboxPath)) {
                log.warn("Inbox directory does not exist: {}", inboxPath);
                return;
            }
            
            // Find all data files with metadata
            List<Path> dataFiles = new ArrayList<>();
            
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(inboxPath)) {
                for (Path file : stream) {
                    String fileName = file.getFileName().toString().toLowerCase();
                    if ((fileName.endsWith(".csv") || fileName.endsWith(".json")) 
                            && !fileName.endsWith(".metadata")) {
                        // Check if metadata file exists
                        Path metadataPath = file.resolveSibling(file.getFileName() + ".metadata");
                        if (Files.exists(metadataPath)) {
                            dataFiles.add(file);
                        } else {
                            log.debug("Skipping {} - no metadata file found", file.getFileName());
                        }
                    }
                }
            }
            
            if (dataFiles.isEmpty()) {
                log.debug("No files to process in inbox");
                return;
            }
            
            log.info("Found {} file(s) to process", dataFiles.size());
            
            for (Path dataFile : dataFiles) {
                processFile(dataFile);
            }
            
        } catch (Exception e) {
            log.error("Error scanning inbox", e);
            totalErrors.incrementAndGet();
        }
    }
    
    /**
     * Process a single data file.
     */
    private void processFile(Path dataFile) {
        Path metadataPath = dataFile.resolveSibling(dataFile.getFileName() + ".metadata");
        String fileName = dataFile.getFileName().toString();
        
        log.info("Processing file: {}", fileName);
        
        try {
            // Parse metadata
            AutoloadMetadata metadata = parseMetadata(metadataPath);
            
            // Validate metadata
            if (metadata.getKeyField() == null || metadata.getKeyField().isBlank()) {
                log.error("Missing key_field in metadata for file: {}", fileName);
                moveToOutbox(dataFile, metadataPath, "ERROR_NO_KEY_FIELD");
                totalErrors.incrementAndGet();
                return;
            }
            
            // Process based on file type
            ProcessingResult result;
            if (fileName.toLowerCase().endsWith(".csv")) {
                result = processCsvFile(dataFile, metadata);
            } else {
                result = processJsonFile(dataFile, metadata);
            }
            
            log.info("Processed {} - loaded: {}, skipped: {}, errors: {}", 
                    fileName, result.getLoaded(), result.getSkipped(), result.getErrors());
            
            // Move to outbox
            moveToOutbox(dataFile, metadataPath, "SUCCESS");
            
            totalFilesProcessed.incrementAndGet();
            totalRecordsLoaded.addAndGet(result.getLoaded());
            totalErrors.addAndGet(result.getErrors());
            
        } catch (Exception e) {
            log.error("Failed to process file: {}", fileName, e);
            try {
                moveToOutbox(dataFile, metadataPath, "ERROR_" + e.getClass().getSimpleName());
            } catch (IOException ioe) {
                log.error("Failed to move file to outbox", ioe);
            }
            totalErrors.incrementAndGet();
        }
    }
    
    /**
     * Parse metadata file.
     */
    private AutoloadMetadata parseMetadata(Path metadataPath) throws IOException {
        AutoloadMetadata metadata = new AutoloadMetadata();
        
        Charset charset = Charset.forName(properties.getAutoload().getFileEncoding());
        List<String> lines = Files.readAllLines(metadataPath, charset);
        
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            
            int colonIndex = line.indexOf(':');
            if (colonIndex > 0) {
                String key = line.substring(0, colonIndex).trim().toLowerCase();
                String value = line.substring(colonIndex + 1).trim();
                
                switch (key) {
                    case "region":
                        metadata.setRegion(value);
                        break;
                    case "ttl":
                        try {
                            metadata.setTtl(Long.parseLong(value));
                        } catch (NumberFormatException e) {
                            log.warn("Invalid TTL value: {}", value);
                        }
                        break;
                    case "key_field":
                        metadata.setKeyField(value);
                        break;
                    case "delimiter":
                        if (!value.isEmpty()) {
                            metadata.setDelimiter(value.charAt(0));
                        }
                        break;
                    default:
                        log.debug("Unknown metadata field: {}", key);
                }
            }
        }
        
        return metadata;
    }
    
    /**
     * Process a CSV file.
     */
    private ProcessingResult processCsvFile(Path csvFile, AutoloadMetadata metadata) throws IOException {
        ProcessingResult result = new ProcessingResult();
        
        Charset charset = Charset.forName(properties.getAutoload().getFileEncoding());
        
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(metadata.getDelimiter())
                .setHeader()
                .setSkipHeaderRecord(true)
                .setIgnoreEmptyLines(true)
                .setTrim(true)
                .build();
        
        try (Reader reader = Files.newBufferedReader(csvFile, charset);
             CSVParser parser = new CSVParser(reader, format)) {
            
            // Get headers
            List<String> headers = parser.getHeaderNames();
            if (headers.isEmpty()) {
                log.error("CSV file has no headers: {}", csvFile.getFileName());
                result.setErrors(1);
                return result;
            }
            
            // Check if key field exists in headers
            if (!headers.contains(metadata.getKeyField())) {
                log.error("Key field '{}' not found in CSV headers: {}", 
                        metadata.getKeyField(), headers);
                result.setErrors(1);
                return result;
            }
            
            int maxRecords = properties.getAutoload().getMaxRecordsPerFile();
            int recordCount = 0;
            
            for (CSVRecord record : parser) {
                if (maxRecords > 0 && recordCount >= maxRecords) {
                    log.info("Reached max records limit: {}", maxRecords);
                    break;
                }
                
                try {
                    // Convert CSV record to JSON
                    ObjectNode jsonNode = objectMapper.createObjectNode();
                    for (String header : headers) {
                        String value = record.get(header);
                        if (value != null) {
                            // Try to parse as number or boolean and put appropriately
                            putParsedValue(jsonNode, header, value);
                        }
                    }
                    
                    // Get key value
                    String keyValue = record.get(metadata.getKeyField());
                    if (keyValue == null || keyValue.isBlank()) {
                        log.debug("Skipping record {} - empty key field", record.getRecordNumber());
                        result.incrementSkipped();
                        continue;
                    }
                    
                    // Save to cache (this will trigger replication if enabled)
                    cacheService.jsonSet(
                            metadata.getRegion(),
                            keyValue,
                            "$",
                            jsonNode,
                            metadata.getTtl()
                    );
                    
                    result.incrementLoaded();
                    recordCount++;
                    
                } catch (Exception e) {
                    log.debug("Error processing record {}: {}", record.getRecordNumber(), e.getMessage());
                    result.incrementErrors();
                }
            }
        }
        
        return result;
    }
    
    /**
     * Process a JSON file (one JSON object per line - JSONL format).
     */
    private ProcessingResult processJsonFile(Path jsonFile, AutoloadMetadata metadata) throws IOException {
        ProcessingResult result = new ProcessingResult();
        
        Charset charset = Charset.forName(properties.getAutoload().getFileEncoding());
        
        int maxRecords = properties.getAutoload().getMaxRecordsPerFile();
        int recordCount = 0;
        int lineNumber = 0;
        
        try (BufferedReader reader = Files.newBufferedReader(jsonFile, charset)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();
                
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                
                if (maxRecords > 0 && recordCount >= maxRecords) {
                    log.info("Reached max records limit: {}", maxRecords);
                    break;
                }
                
                try {
                    // Parse JSON
                    JsonNode jsonNode = objectMapper.readTree(line);
                    
                    if (!jsonNode.isObject()) {
                        log.debug("Skipping line {} - not a JSON object", lineNumber);
                        result.incrementSkipped();
                        continue;
                    }
                    
                    // Get key value
                    JsonNode keyNode = jsonNode.get(metadata.getKeyField());
                    if (keyNode == null || keyNode.isNull()) {
                        log.debug("Skipping line {} - missing key field '{}'", 
                                lineNumber, metadata.getKeyField());
                        result.incrementSkipped();
                        continue;
                    }
                    
                    String keyValue = keyNode.asText();
                    if (keyValue.isBlank()) {
                        log.debug("Skipping line {} - empty key field", lineNumber);
                        result.incrementSkipped();
                        continue;
                    }
                    
                    // Save to cache (this will trigger replication if enabled)
                    cacheService.jsonSet(
                            metadata.getRegion(),
                            keyValue,
                            "$",
                            jsonNode,
                            metadata.getTtl()
                    );
                    
                    result.incrementLoaded();
                    recordCount++;
                    
                } catch (Exception e) {
                    log.debug("Error processing line {}: {}", lineNumber, e.getMessage());
                    result.incrementErrors();
                }
            }
        }
        
        return result;
    }
    
    /**
     * Parse a string value and put it into ObjectNode with appropriate type.
     */
    private void putParsedValue(ObjectNode node, String fieldName, String value) {
        if (value == null || value.isEmpty()) {
            node.put(fieldName, value);
            return;
        }
        
        // Try boolean
        if ("true".equalsIgnoreCase(value)) {
            node.put(fieldName, true);
            return;
        }
        if ("false".equalsIgnoreCase(value)) {
            node.put(fieldName, false);
            return;
        }
        
        // Try integer/long
        try {
            long longVal = Long.parseLong(value);
            node.put(fieldName, longVal);
            return;
        } catch (NumberFormatException ignored) {}
        
        // Try double
        try {
            double doubleVal = Double.parseDouble(value);
            node.put(fieldName, doubleVal);
            return;
        } catch (NumberFormatException ignored) {}
        
        // Default to string
        node.put(fieldName, value);
    }
    
    /**
     * Move processed files to outbox.
     */
    private void moveToOutbox(Path dataFile, Path metadataPath, String status) throws IOException {
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);
        String baseName = dataFile.getFileName().toString();
        
        // Create new names with timestamp and status
        String newDataName = timestamp + "_" + status + "_" + baseName;
        String newMetaName = timestamp + "_" + status + "_" + baseName + ".metadata";
        
        Path targetData = outboxPath.resolve(newDataName);
        Path targetMeta = outboxPath.resolve(newMetaName);
        
        Files.move(dataFile, targetData, StandardCopyOption.REPLACE_EXISTING);
        Files.move(metadataPath, targetMeta, StandardCopyOption.REPLACE_EXISTING);
        
        log.debug("Moved files to outbox: {}, {}", newDataName, newMetaName);
    }
    
    /**
     * Get autoload statistics.
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("enabled", properties.getAutoload().isEnabled());
        stats.put("inboxPath", inboxPath != null ? inboxPath.toString() : null);
        stats.put("outboxPath", outboxPath != null ? outboxPath.toString() : null);
        stats.put("scanIntervalSeconds", properties.getAutoload().getScanIntervalSeconds());
        stats.put("totalFilesProcessed", totalFilesProcessed.get());
        stats.put("totalRecordsLoaded", totalRecordsLoaded.get());
        stats.put("totalErrors", totalErrors.get());
        return stats;
    }
    
    /**
     * Trigger an immediate scan (for testing or manual invocation).
     */
    public void triggerScan() {
        if (properties.getAutoload().isEnabled()) {
            scheduler.execute(this::scanAndProcess);
        }
    }
    
    /**
     * Metadata parsed from .metadata file.
     */
    @Data
    public static class AutoloadMetadata {
        private String region = "default";
        private long ttl = -1;
        private String keyField;
        private char delimiter = ',';
    }
    
    /**
     * Result of processing a file.
     */
    @Data
    public static class ProcessingResult {
        private int loaded = 0;
        private int skipped = 0;
        private int errors = 0;
        
        public void incrementLoaded() { loaded++; }
        public void incrementSkipped() { skipped++; }
        public void incrementErrors() { errors++; }
    }
}
