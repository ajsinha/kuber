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
import com.kuber.core.model.CacheEntry;
import com.kuber.core.util.JsonUtils;
import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.persistence.PersistenceOperationLock;
import com.kuber.server.persistence.PersistenceOperationLock.OperationType;
import jakarta.annotation.PreDestroy;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service for automatically loading data from CSV, TXT, and JSON files into the cache.
 * 
 * <p>Watches the configured autoload/inbox directory for data files (.csv, .txt, .json)
 * that have corresponding metadata files (.csv.metadata, .txt.metadata, .json.metadata).
 * 
 * <p>Supported file formats:
 * <ul>
 *   <li><b>CSV/TXT</b>: Delimited text files with header row. TXT files are processed identically to CSV.</li>
 *   <li><b>JSON</b>: JSONL format (one JSON object per line).</li>
 * </ul>
 * 
 * <p>Metadata file format:
 * <pre>
 * region:myregion
 * ttl:60
 * key_field:id
 * delimiter:,
 * key_delimiter:/
 * </pre>
 * 
 * <p>Composite Key Support:
 * <p>The key_field supports composite keys using "/" as separator:
 * <pre>
 * key_field:country/state/city
 * </pre>
 * <p>This will extract values from "country", "state", and "city" fields
 * and join them with "/" to form keys like "US/CA/Los Angeles".
 * 
 * <p>The key_delimiter can be customized if needed (default is "/"):
 * <pre>
 * key_field:country/state/city
 * key_delimiter:-
 * </pre>
 * <p>This would produce keys like "US-CA-Los Angeles".
 * 
 * <p>For CSV/TXT files, each row is converted to JSON using the header fields,
 * and the value from key_field column(s) is used as the cache key.
 * 
 * <p>For JSON files, each line should be a complete JSON object,
 * and the value from key_field is used as the cache key.
 * 
 * <p>After processing, both data and metadata files are moved to outbox.
 * 
 * <p>Files are processed sequentially (one at a time) for data consistency.
 * 
 * <p>CONCURRENCY SAFETY (v1.3.2):
 * <p>Uses PersistenceOperationLock to ensure autoload does not run concurrently
 * with compaction operations. Acquires region locks during file processing.
 * 
 * @version 1.4.2
 */
@Service
@Slf4j
public class AutoloadService {
    
    private final CacheService cacheService;
    private final KuberProperties properties;
    private final ObjectMapper objectMapper;
    private final PersistenceOperationLock operationLock;
    
    public AutoloadService(CacheService cacheService, 
                           KuberProperties properties, 
                           ObjectMapper objectMapper,
                           PersistenceOperationLock operationLock) {
        this.cacheService = cacheService;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.operationLock = operationLock;
    }
    
    private ScheduledExecutorService scheduler;
    private Path inboxPath;
    private Path outboxPath;
    
    // Statistics
    private final AtomicLong totalFilesProcessed = new AtomicLong(0);
    private final AtomicLong totalRecordsLoaded = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong totalScans = new AtomicLong(0);
    
    // Activity tracking
    private volatile Instant lastScanTime = null;
    private volatile Instant lastFileProcessedTime = null;
    private volatile String lastFileProcessed = null;
    private volatile int lastFileRecordsLoaded = 0;
    private volatile int lastFileErrors = 0;
    private volatile String lastActivityMessage = "Waiting for first scan...";
    private volatile int filesInInbox = 0;
    
    // Recent activity log (keep last 10 entries)
    private final Deque<ActivityLogEntry> recentActivity = new ConcurrentLinkedDeque<>();
    private static final int MAX_RECENT_ACTIVITY = 10;
    
    private static final DateTimeFormatter TIMESTAMP_FORMAT = 
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    
    // Track if autoload has been started (to prevent double-start)
    private volatile boolean started = false;
    
    /**
     * Start autoload service after cache recovery is complete.
     * Called by StartupOrchestrator after CacheService is initialized.
     * This ensures autoload does not process files before persistence recovery.
     */
    public synchronized void startAfterRecovery() {
        if (started) {
            log.warn("AutoloadService already started, skipping...");
            return;
        }
        
        if (!properties.getAutoload().isEnabled()) {
            log.info("Autoload service is disabled");
            started = true;
            return;
        }
        
        // Verify cache service is initialized
        if (!cacheService.isInitialized()) {
            log.error("Cannot start AutoloadService - CacheService not initialized!");
            return;
        }
        
        try {
            initializeDirectories();
            startWatcher();
            started = true;
            log.info("Autoload service started - watching: {}", inboxPath);
        } catch (Exception e) {
            log.error("Failed to initialize autoload service", e);
        }
    }
    
    /**
     * Check if autoload service has been started.
     */
    public boolean isStarted() {
        return started;
    }
    
    @PreDestroy
    public void shutdown() {
        // Shutdown scheduler
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
        
        log.info("Autoload watcher scheduled with interval: {} seconds (sequential processing)", 
                intervalSeconds);
    }
    
    /**
     * Scan inbox and process files.
     */
    public void scanAndProcess() {
        totalScans.incrementAndGet();
        lastScanTime = Instant.now();
        
        try {
            if (!Files.exists(inboxPath)) {
                lastActivityMessage = "Inbox directory does not exist";
                log.warn("Inbox directory does not exist: {}", inboxPath);
                return;
            }
            
            // Find all data files with metadata
            List<Path> dataFiles = new ArrayList<>();
            int pendingFiles = 0;
            
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(inboxPath)) {
                for (Path file : stream) {
                    String fileName = file.getFileName().toString().toLowerCase();
                    if ((fileName.endsWith(".csv") || fileName.endsWith(".txt") || 
                         fileName.endsWith(".json")) 
                            && !fileName.endsWith(".metadata")) {
                        pendingFiles++;
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
            
            filesInInbox = pendingFiles;
            
            if (dataFiles.isEmpty()) {
                lastActivityMessage = "Scan complete - no files to process";
                log.debug("No files to process in inbox");
                return;
            }
            
            lastActivityMessage = String.format("Processing %d file(s) sequentially...", dataFiles.size());
            log.info("Found {} file(s) to process (sequential)", dataFiles.size());
            
            // Process files sequentially (one at a time)
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
        Path attributeMappingPath = dataFile.resolveSibling(dataFile.getFileName() + ".metadata.attributemapping.json");
        String fileName = dataFile.getFileName().toString();
        Instant startTime = Instant.now();
        
        log.info("Processing file: {}", fileName);
        lastActivityMessage = "Processing: " + fileName;
        
        // Check if system is shutting down
        if (operationLock.isShuttingDown()) {
            log.warn("System is shutting down - skipping file: {}", fileName);
            return;
        }
        
        String regionName = "default";
        boolean lockAcquired = false;
        
        try {
            // Parse metadata first to get region name
            AutoloadMetadata metadata = parseMetadata(metadataPath);
            regionName = metadata.getRegion();
            
            // Acquire region lock - this ensures no compaction or other operations on this region
            if (!operationLock.acquireRegionLock(regionName, OperationType.AUTOLOAD, 
                    "Processing " + fileName, 2, java.util.concurrent.TimeUnit.MINUTES)) {
                log.warn("Could not acquire lock for region '{}' - skipping file: {}", regionName, fileName);
                return;
            }
            lockAcquired = true;
            
            // Parse attribute mapping if present
            Map<String, String> attributeMapping = null;
            if (Files.exists(attributeMappingPath)) {
                attributeMapping = parseAttributeMapping(attributeMappingPath);
                log.info("Found attribute mapping file with {} mappings", 
                        attributeMapping != null ? attributeMapping.size() : 0);
            }
            metadata.setAttributeMapping(attributeMapping);
            
            // Validate metadata
            if (metadata.getKeyField() == null || metadata.getKeyField().isBlank()) {
                log.error("Missing key_field in metadata for file: {}", fileName);
                moveToOutbox(dataFile, metadataPath, attributeMappingPath, "ERROR_NO_KEY_FIELD");
                totalErrors.incrementAndGet();
                addActivityLogEntry(fileName, metadata.getRegion(), 0, 1, startTime, "ERROR: Missing key_field");
                return;
            }
            
            // Process based on file type
            ProcessingResult result;
            String fileNameLower = fileName.toLowerCase();
            if (fileNameLower.endsWith(".csv") || fileNameLower.endsWith(".txt")) {
                result = processCsvFile(dataFile, metadata);
            } else {
                result = processJsonFile(dataFile, metadata);
            }
            
            log.info("Processed {} - loaded: {}, skipped: {}, errors: {}", 
                    fileName, result.getLoaded(), result.getSkipped(), result.getErrors());
            
            // Move to outbox
            moveToOutbox(dataFile, metadataPath, attributeMappingPath, "SUCCESS");
            
            totalFilesProcessed.incrementAndGet();
            totalRecordsLoaded.addAndGet(result.getLoaded());
            totalErrors.addAndGet(result.getErrors());
            
            // Update last file tracking
            lastFileProcessedTime = Instant.now();
            lastFileProcessed = fileName;
            lastFileRecordsLoaded = result.getLoaded();
            lastFileErrors = result.getErrors();
            lastActivityMessage = String.format("Loaded %d records from %s", result.getLoaded(), fileName);
            
            // Add to activity log
            addActivityLogEntry(fileName, metadata.getRegion(), result.getLoaded(), result.getErrors(), startTime, "SUCCESS");
            
        } catch (Exception e) {
            log.error("Failed to process file: {}", fileName, e);
            try {
                moveToOutbox(dataFile, metadataPath, attributeMappingPath, "ERROR_" + e.getClass().getSimpleName());
            } catch (IOException ioe) {
                log.error("Failed to move file to outbox", ioe);
            }
            totalErrors.incrementAndGet();
            lastActivityMessage = "Error processing: " + fileName + " - " + e.getMessage();
            addActivityLogEntry(fileName, "unknown", 0, 1, startTime, "ERROR: " + e.getMessage());
        } finally {
            // Release region lock
            if (lockAcquired) {
                operationLock.releaseRegionLock(regionName);
            }
        }
    }
    
    /**
     * Add an entry to the recent activity log.
     */
    private void addActivityLogEntry(String fileName, String region, int recordsLoaded, int errors, 
                                      Instant startTime, String status) {
        long durationMs = java.time.Duration.between(startTime, Instant.now()).toMillis();
        ActivityLogEntry entry = new ActivityLogEntry(
                Instant.now(), fileName, region, recordsLoaded, errors, durationMs, status);
        
        recentActivity.addFirst(entry);
        
        // Trim to max size
        while (recentActivity.size() > MAX_RECENT_ACTIVITY) {
            recentActivity.removeLast();
        }
    }
    
    /**
     * Parse attribute mapping file.
     * Expected format: JSON object mapping source attribute names to target names.
     * Example: {"firstName": "first_name", "lastName": "last_name"}
     */
    private Map<String, String> parseAttributeMapping(Path mappingPath) throws IOException {
        Charset charset = Charset.forName(properties.getAutoload().getFileEncoding());
        String content = Files.readString(mappingPath, charset);
        return objectMapper.readValue(content, 
                new com.fasterxml.jackson.core.type.TypeReference<Map<String, String>>() {});
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
                    case "key_delimiter":
                        if (!value.isEmpty()) {
                            metadata.setKeyDelimiter(value);
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
     * Process a CSV file with batch writes for better performance.
     */
    private ProcessingResult processCsvFile(Path csvFile, AutoloadMetadata metadata) throws IOException {
        ProcessingResult result = new ProcessingResult();
        
        Charset charset = Charset.forName(properties.getAutoload().getFileEncoding());
        int batchSize = properties.getAutoload().getBatchSize();
        String fileName = csvFile.getFileName().toString();
        
        log.info("Processing CSV file '{}' with batch size: {}", fileName, batchSize);
        
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
                log.error("CSV file has no headers: {}", fileName);
                result.setErrors(1);
                return result;
            }
            
            // Get key fields (supports composite keys with "/" separator)
            List<String> keyFields = metadata.getKeyFields();
            if (keyFields.isEmpty()) {
                log.error("No key fields specified in metadata for file: {}", fileName);
                result.setErrors(1);
                return result;
            }
            
            // Validate all key fields exist in headers
            for (String keyField : keyFields) {
                if (!headers.contains(keyField)) {
                    log.error("Key field '{}' not found in CSV headers: {}", keyField, headers);
                    result.setErrors(1);
                    return result;
                }
            }
            
            if (metadata.isCompositeKey()) {
                log.info("Using composite key from fields: {} (joined with '{}')", 
                        keyFields, metadata.getKeyDelimiter());
            }
            
            int maxRecords = properties.getAutoload().getMaxRecordsPerFile();
            int recordCount = 0;
            int batchNumber = 0;
            
            // Get attribute mapping if present
            Map<String, String> attrMapping = metadata.getAttributeMapping();
            
            // Batch accumulator
            List<CacheEntry> batch = new ArrayList<>(batchSize);
            String region = metadata.getRegion();
            long ttl = metadata.getTtl();
            
            // Ensure region exists before processing
            cacheService.ensureRegionExistsPublic(region);
            
            long startTime = System.currentTimeMillis();
            
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
                            // Apply attribute mapping if present
                            String targetField = (attrMapping != null) 
                                    ? attrMapping.getOrDefault(header, header) 
                                    : header;
                            // Try to parse as number or boolean and put appropriately
                            putParsedValue(jsonNode, targetField, value);
                        }
                    }
                    
                    // Build key value (single or composite)
                    String keyValue = buildCompositeKeyFromCsvRecord(record, keyFields, metadata.getKeyDelimiter());
                    if (keyValue == null || keyValue.isBlank()) {
                        log.debug("Skipping record {} - empty key field(s)", record.getRecordNumber());
                        result.incrementSkipped();
                        continue;
                    }
                    
                    // Create CacheEntry for batch
                    CacheEntry entry = createCacheEntry(region, keyValue, jsonNode, ttl);
                    batch.add(entry);
                    result.incrementLoaded();
                    recordCount++;
                    
                    // Flush batch when full
                    if (batch.size() >= batchSize) {
                        batchNumber++;
                        int saved = cacheService.putEntriesBatch(batch);
                        log.info("[{}] Batch #{}: Processed {} records so far ({} in this batch) -> region '{}'", 
                                fileName, batchNumber, recordCount, saved, region);
                        batch.clear();
                    }
                    
                } catch (Exception e) {
                    log.debug("Error processing record {}: {}", record.getRecordNumber(), e.getMessage());
                    result.incrementErrors();
                }
            }
            
            // Flush remaining entries
            if (!batch.isEmpty()) {
                batchNumber++;
                int saved = cacheService.putEntriesBatch(batch);
                log.info("[{}] Batch #{} (final): Processed {} records so far ({} in this batch) -> region '{}'", 
                        fileName, batchNumber, recordCount, saved, region);
            }
            
            long elapsedMs = System.currentTimeMillis() - startTime;
            log.info("[{}] COMPLETED: Total {} records processed in {} batches ({} ms) -> region '{}'", 
                    fileName, recordCount, batchNumber, elapsedMs, region);
        }
        
        return result;
    }
    
    /**
     * Build composite key from CSV record by extracting values for each key field
     * and joining them with the specified delimiter.
     * 
     * @param record the CSV record
     * @param keyFields list of field names (e.g., ["country", "state", "city"])
     * @param delimiter the delimiter to join values (e.g., "/")
     * @return composite key (e.g., "US/CA/Los Angeles") or null if any field is empty
     */
    private String buildCompositeKeyFromCsvRecord(CSVRecord record, List<String> keyFields, String delimiter) {
        List<String> values = new ArrayList<>();
        for (String field : keyFields) {
            String value = record.get(field);
            if (value == null || value.isBlank()) {
                return null; // All parts of composite key must have values
            }
            values.add(value.trim());
        }
        return String.join(delimiter, values);
    }
    
    /**
     * Process a JSON file (one JSON object per line - JSONL format) with batch writes.
     */
    private ProcessingResult processJsonFile(Path jsonFile, AutoloadMetadata metadata) throws IOException {
        ProcessingResult result = new ProcessingResult();
        
        Charset charset = Charset.forName(properties.getAutoload().getFileEncoding());
        int batchSize = properties.getAutoload().getBatchSize();
        String fileName = jsonFile.getFileName().toString();
        
        log.info("Processing JSON file '{}' with batch size: {}", fileName, batchSize);
        
        int maxRecords = properties.getAutoload().getMaxRecordsPerFile();
        int recordCount = 0;
        int lineNumber = 0;
        int batchNumber = 0;
        
        // Get key fields (supports composite keys with "/" separator)
        List<String> keyFields = metadata.getKeyFields();
        if (keyFields.isEmpty()) {
            log.error("No key fields specified in metadata for file: {}", fileName);
            result.setErrors(1);
            return result;
        }
        
        if (metadata.isCompositeKey()) {
            log.info("Using composite key from fields: {} (joined with '{}')", 
                    keyFields, metadata.getKeyDelimiter());
        }
        
        // Get attribute mapping if present
        Map<String, String> attrMapping = metadata.getAttributeMapping();
        
        // Batch accumulator
        List<CacheEntry> batch = new ArrayList<>(batchSize);
        String region = metadata.getRegion();
        long ttl = metadata.getTtl();
        
        // Ensure region exists before processing
        cacheService.ensureRegionExistsPublic(region);
        
        long startTime = System.currentTimeMillis();
        
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
                    
                    // Build key value (single or composite)
                    String keyValue = buildCompositeKeyFromJsonNode(jsonNode, keyFields, metadata.getKeyDelimiter());
                    if (keyValue == null || keyValue.isBlank()) {
                        log.debug("Skipping line {} - missing or empty key field(s)", lineNumber);
                        result.incrementSkipped();
                        continue;
                    }
                    
                    // Apply attribute mapping if present
                    JsonNode finalNode = jsonNode;
                    if (attrMapping != null && !attrMapping.isEmpty()) {
                        finalNode = cacheService.applyAttributeMapping(jsonNode, attrMapping);
                    }
                    
                    // Create CacheEntry for batch
                    CacheEntry entry = createCacheEntry(region, keyValue, finalNode, ttl);
                    batch.add(entry);
                    result.incrementLoaded();
                    recordCount++;
                    
                    // Flush batch when full
                    if (batch.size() >= batchSize) {
                        batchNumber++;
                        int saved = cacheService.putEntriesBatch(batch);
                        log.info("[{}] Batch #{}: Processed {} records so far ({} in this batch) -> region '{}'", 
                                fileName, batchNumber, recordCount, saved, region);
                        batch.clear();
                    }
                    
                } catch (Exception e) {
                    log.debug("Error processing line {}: {}", lineNumber, e.getMessage());
                    result.incrementErrors();
                }
            }
        }
        
        // Flush remaining entries
        if (!batch.isEmpty()) {
            batchNumber++;
            int saved = cacheService.putEntriesBatch(batch);
            log.info("[{}] Batch #{} (final): Processed {} records so far ({} in this batch) -> region '{}'", 
                    fileName, batchNumber, recordCount, saved, region);
        }
        
        long elapsedMs = System.currentTimeMillis() - startTime;
        log.info("[{}] COMPLETED: Total {} records processed in {} batches ({} ms) -> region '{}'", 
                fileName, recordCount, batchNumber, elapsedMs, region);
        
        return result;
    }
    
    /**
     * Build composite key from JSON node by extracting values for each key field
     * and joining them with the specified delimiter.
     * 
     * @param jsonNode the JSON object
     * @param keyFields list of field names (e.g., ["country", "state", "city"])
     * @param delimiter the delimiter to join values (e.g., "/")
     * @return composite key (e.g., "US/CA/Los Angeles") or null if any field is missing/empty
     */
    private String buildCompositeKeyFromJsonNode(JsonNode jsonNode, List<String> keyFields, String delimiter) {
        List<String> values = new ArrayList<>();
        for (String field : keyFields) {
            JsonNode fieldNode = jsonNode.get(field);
            if (fieldNode == null || fieldNode.isNull()) {
                return null; // All parts of composite key must have values
            }
            String value = fieldNode.asText();
            if (value == null || value.isBlank()) {
                return null;
            }
            values.add(value.trim());
        }
        return String.join(delimiter, values);
    }
    
    /**
     * Create a CacheEntry from JSON data for batch processing.
     * 
     * @param region the cache region
     * @param key the cache key
     * @param jsonValue the JSON value
     * @param ttlSeconds TTL in seconds (-1 for no expiry)
     * @return CacheEntry ready for batch save
     */
    private CacheEntry createCacheEntry(String region, String key, JsonNode jsonValue, long ttlSeconds) {
        Instant now = Instant.now();
        Instant expiresAt = ttlSeconds > 0 ? now.plusSeconds(ttlSeconds) : null;
        
        return CacheEntry.builder()
                .id(UUID.randomUUID().toString())
                .key(key)
                .region(region)
                .valueType(CacheEntry.ValueType.JSON)
                .jsonValue(jsonValue)
                .stringValue(JsonUtils.toJson(jsonValue))
                .ttlSeconds(ttlSeconds)
                .createdAt(now)
                .updatedAt(now)
                .expiresAt(expiresAt)
                .build();
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
        moveToOutbox(dataFile, metadataPath, null, status);
    }
    
    /**
     * Move processed files to outbox, including optional attribute mapping file.
     */
    private void moveToOutbox(Path dataFile, Path metadataPath, Path attributeMappingPath, String status) throws IOException {
        String timestamp = LocalDateTime.now().format(TIMESTAMP_FORMAT);
        String baseName = dataFile.getFileName().toString();
        
        // Create new names with timestamp and status
        String newDataName = timestamp + "_" + status + "_" + baseName;
        String newMetaName = timestamp + "_" + status + "_" + baseName + ".metadata";
        
        Path targetData = outboxPath.resolve(newDataName);
        Path targetMeta = outboxPath.resolve(newMetaName);
        
        Files.move(dataFile, targetData, StandardCopyOption.REPLACE_EXISTING);
        Files.move(metadataPath, targetMeta, StandardCopyOption.REPLACE_EXISTING);
        
        // Move attribute mapping file if it exists
        if (attributeMappingPath != null && Files.exists(attributeMappingPath)) {
            String newMappingName = timestamp + "_" + status + "_" + baseName + ".metadata.attributemapping.json";
            Path targetMapping = outboxPath.resolve(newMappingName);
            Files.move(attributeMappingPath, targetMapping, StandardCopyOption.REPLACE_EXISTING);
            log.debug("Moved files to outbox: {}, {}, {}", newDataName, newMetaName, newMappingName);
        } else {
            log.debug("Moved files to outbox: {}, {}", newDataName, newMetaName);
        }
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
        stats.put("mode", "sequential");
        stats.put("batchSize", properties.getAutoload().getBatchSize());
        stats.put("filesInInbox", filesInInbox);
        
        // Totals
        Map<String, Object> totals = new LinkedHashMap<>();
        totals.put("scans", totalScans.get());
        totals.put("filesProcessed", totalFilesProcessed.get());
        totals.put("recordsLoaded", totalRecordsLoaded.get());
        totals.put("errors", totalErrors.get());
        stats.put("totals", totals);
        
        // Last scan
        Map<String, Object> lastScan = new LinkedHashMap<>();
        lastScan.put("time", lastScanTime != null ? lastScanTime.toString() : null);
        stats.put("lastScan", lastScan);
        
        // Last file processed
        Map<String, Object> lastFile = new LinkedHashMap<>();
        lastFile.put("name", lastFileProcessed);
        lastFile.put("time", lastFileProcessedTime != null ? lastFileProcessedTime.toString() : null);
        lastFile.put("recordsLoaded", lastFileRecordsLoaded);
        lastFile.put("errors", lastFileErrors);
        stats.put("lastFile", lastFile);
        
        // Last activity message
        stats.put("lastActivityMessage", lastActivityMessage);
        
        // Recent activity
        List<Map<String, Object>> activityList = new ArrayList<>();
        for (ActivityLogEntry entry : recentActivity) {
            Map<String, Object> entryMap = new LinkedHashMap<>();
            entryMap.put("time", entry.time().toString());
            entryMap.put("fileName", entry.fileName());
            entryMap.put("region", entry.region());
            entryMap.put("recordsLoaded", entry.recordsLoaded());
            entryMap.put("errors", entry.errors());
            entryMap.put("durationMs", entry.durationMs());
            entryMap.put("status", entry.status());
            activityList.add(entryMap);
        }
        stats.put("recentActivity", activityList);
        
        return stats;
    }
    
    /**
     * Get recent activity log entries.
     */
    public List<ActivityLogEntry> getRecentActivity() {
        return new ArrayList<>(recentActivity);
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
     * Activity log entry record.
     */
    public record ActivityLogEntry(
            Instant time,
            String fileName,
            String region,
            int recordsLoaded,
            int errors,
            long durationMs,
            String status
    ) {}
    
    /**
     * Metadata parsed from .metadata file.
     */
    @Data
    public static class AutoloadMetadata {
        private String region = "default";
        private long ttl = -1;
        private String keyField;
        private char delimiter = ',';
        /**
         * Delimiter used to join composite key parts.
         * Default is "/" (e.g., "US/CA/12345" for country/state/zipcode).
         */
        private String keyDelimiter = "/";
        /**
         * Optional attribute mapping for JSON transformation.
         * Loaded from .metadata.attributemapping.json file if present.
         */
        private Map<String, String> attributeMapping;
        
        /**
         * Parse key_field into list of field names.
         * Supports "/" separated composite keys (e.g., "country/state/city").
         * @return list of key field names, never null
         */
        public List<String> getKeyFields() {
            if (keyField == null || keyField.isBlank()) {
                return Collections.emptyList();
            }
            return Arrays.stream(keyField.split("/"))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
        }
        
        /**
         * Check if this is a composite key (multiple fields separated by "/").
         */
        public boolean isCompositeKey() {
            return getKeyFields().size() > 1;
        }
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
