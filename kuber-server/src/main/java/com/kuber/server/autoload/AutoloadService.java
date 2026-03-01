/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
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
import com.kuber.server.publishing.RegionEventPublishingService;
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
 * encoding:UTF-8
 * </pre>
 * 
 * <p><b>FILE ENCODING SUPPORT (v1.7.9):</b>
 * <p>Autoload supports automatic encoding detection and explicit encoding specification:
 * <ul>
 *   <li><b>Auto-detection</b>: If encoding is not specified (default), files are analyzed for:
 *     <ul>
 *       <li>BOM (Byte Order Mark): UTF-8, UTF-16 LE/BE, UTF-32 LE/BE</li>
 *       <li>Character validation: Tests common encodings (UTF-8, ISO-8859-1, windows-1252, ASCII)</li>
 *     </ul>
 *   </li>
 *   <li><b>Explicit encoding</b>: Specify in metadata file: <code>encoding:UTF-16</code></li>
 *   <li><b>Force auto-detect</b>: <code>encoding:auto</code></li>
 * </ul>
 * 
 * <p>Supported encodings include: UTF-8, UTF-16, UTF-16LE, UTF-16BE, UTF-32, UTF-32LE, UTF-32BE,
 * US-ASCII, ISO-8859-1 (Latin-1), windows-1252, Shift_JIS, EUC-JP, GB2312, GBK, Big5, EUC-KR.
 * 
 * <p><b>ASCII NORMALIZATION (v1.7.9):</b>
 * <p>Autoload can normalize all text values to US-ASCII before storing in cache:
 * <ul>
 *   <li><b>Accented characters</b>: é → e, ü → u, ñ → n, ç → c</li>
 *   <li><b>Special characters</b>: ß → ss, æ → ae, œ → oe, ø → o</li>
 *   <li><b>Ligatures</b>: ﬁ → fi, ﬂ → fl</li>
 *   <li><b>Currency</b>: € → EUR, £ → GBP, ¥ → YEN</li>
 *   <li><b>Typography</b>: "smart quotes" → "straight quotes", em-dash → hyphen</li>
 *   <li><b>Greek letters</b>: α → alpha, β → beta, π → pi</li>
 * </ul>
 * 
 * <p>Configuration:
 * <pre>
 * kuber.autoload.ascii-normalize=true      # Global default (enabled)
 * kuber.autoload.ascii-normalize-keys=true # Also normalize cache keys
 * </pre>
 * 
 * <p>Per-file override in metadata:
 * <pre>
 * ascii_normalize:false   # Disable for this file
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
 * <p><b>Empty Key Components (v1.5.0):</b>
 * <p>One or more components of a composite key can be empty/null. The record will still
 * be processed with the empty component represented in the key. For example:
 * <ul>
 *   <li>country="US", state="", city="LA" → key="US//LA" (record processed)</li>
 *   <li>country="", state="CA", city="" → key="/CA/" (record processed)</li>
 *   <li>country="", state="", city="" → key="//" (record SKIPPED - all components empty)</li>
 * </ul>
 * <p>Records are only skipped when ALL key components are empty.
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
 * <p><b>MEMORY MANAGEMENT (v1.5.0):</b>
 * <p>Autoload uses "persistence-only" mode for efficient bulk loading:
 * <ul>
 *   <li>Data is written to KeyIndex and persistence store</li>
 *   <li>Value cache is NOT populated during autoload (avoids eviction pressure)</li>
 *   <li>Memory stays within configured limits during bulk loads</li>
 *   <li>Data is loaded on-demand when accessed via GET (lazy loading)</li>
 *   <li>This is the recommended approach for large datasets</li>
 * </ul>
 * 
 * <p><b>CACHE WARMING:</b>
 * <p>The value cache warms naturally through GET operations. Each GET:
 * <ol>
 *   <li>Checks KeyIndex - if key exists, continues</li>
 *   <li>Checks value cache - if present, returns immediately</li>
 *   <li>If not in cache, loads from persistence and puts in cache</li>
 * </ol>
 * <p>This lazy loading approach is memory-efficient and works well for large datasets.
 * After autoload completes, 25% of the cache capacity is proactively warmed in a
 * background thread to provide better initial performance without blocking operations.
 * Manual warming via /api/regions/{name}/warm is also available.
 * 
 * <p>CONCURRENCY SAFETY (v1.3.2):
 * <p>Uses PersistenceOperationLock to ensure autoload does not run concurrently
 * with compaction operations. Acquires region locks during file processing.
 * Write operations wait for autoload to complete before proceeding.
 * 
 * @version 2.6.3
 */
@Service
@Slf4j
public class AutoloadService {
    
    private final CacheService cacheService;
    private final KuberProperties properties;
    private final ObjectMapper objectMapper;
    private final PersistenceOperationLock operationLock;
    private final RegionEventPublishingService publishingService;
    
    public AutoloadService(CacheService cacheService, 
                           KuberProperties properties, 
                           ObjectMapper objectMapper,
                           PersistenceOperationLock operationLock,
                           @org.springframework.beans.factory.annotation.Autowired(required = false)
                           RegionEventPublishingService publishingService) {
        this.cacheService = cacheService;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.operationLock = operationLock;
        this.publishingService = publishingService;
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
                // SAFETY CHECK (v1.6.3): Stop processing if shutdown in progress
                if (cacheService.isShuttingDown()) {
                    log.info("Autoload stopping: shutdown in progress ({} files remaining)", 
                            dataFiles.size() - dataFiles.indexOf(dataFile));
                    lastActivityMessage = "Stopped - shutdown in progress";
                    break;
                }
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
        
        // SAFETY CHECK (v1.6.3): Check both operationLock and cacheService for shutdown
        if (operationLock.isShuttingDown() || cacheService.isShuttingDown()) {
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
            
            // Publish autoload_start event if event publishing is configured for this region
            if (publishingService != null) {
                publishingService.publishAutoloadStart(regionName, fileName, properties.getNodeId());
            }
            
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
                if (publishingService != null) {
                    long durationMs = java.time.Duration.between(startTime, Instant.now()).toMillis();
                    publishingService.publishAutoloadEnd(regionName, fileName, 
                            0, 1, durationMs, "ERROR: Missing key_field", properties.getNodeId());
                }
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
            
            // Start background warming using configured percentage (default: 10%)
            // Set warm-percentage to 0 to disable automatic warming (use lazy loading only)
            // This proactively loads some data into memory without blocking operations
            // Remaining data will load on-demand via GET operations (lazy loading)
            int warmPercentage = properties.getAutoload().getWarmPercentage();
            if (result.getLoaded() > 0 && warmPercentage > 0) {
                log.info("Autoload complete for region '{}': {} records committed to persistence. " +
                        "Starting background warming ({}% of capacity)...", regionName, result.getLoaded(), warmPercentage);
                cacheService.warmRegionCacheInBackground(regionName, warmPercentage);
            } else if (result.getLoaded() > 0) {
                log.info("Autoload complete for region '{}': {} records committed to persistence. " +
                        "Background warming disabled (warm-percentage=0).", regionName, result.getLoaded());
            } else {
                log.info("Autoload complete for region '{}': no records loaded.", regionName);
            }
            
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
            
            // Publish autoload_end event if event publishing is configured for this region
            if (publishingService != null) {
                long durationMs = java.time.Duration.between(startTime, Instant.now()).toMillis();
                publishingService.publishAutoloadEnd(regionName, fileName, 
                        result.getLoaded(), result.getErrors(), durationMs, "SUCCESS", properties.getNodeId());
            }
            
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
            
            // Publish autoload_end event with error status
            if (publishingService != null) {
                long durationMs = java.time.Duration.between(startTime, Instant.now()).toMillis();
                publishingService.publishAutoloadEnd(regionName, fileName, 
                        0, 1, durationMs, "ERROR: " + e.getMessage(), properties.getNodeId());
            }
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
     * 
     * <p>Uses auto-detection for file encoding with fallback to configured default.
     */
    private Map<String, String> parseAttributeMapping(Path mappingPath) throws IOException {
        Charset defaultCharset = Charset.forName(properties.getAutoload().getFileEncoding());
        String content = EncodingDetector.readString(mappingPath, defaultCharset);
        return objectMapper.readValue(content, 
                new com.fasterxml.jackson.core.type.TypeReference<Map<String, String>>() {});
    }
    
    /**
     * Parse metadata file.
     * 
     * <p>Supports the following fields:
     * <ul>
     *   <li>region: target region name (default: "default")</li>
     *   <li>ttl: time-to-live in seconds (default: -1, no expiration)</li>
     *   <li>key_field: field(s) to use as cache key, supports composite with "/"</li>
     *   <li>delimiter: CSV field delimiter (default: ",")</li>
     *   <li>key_delimiter: delimiter for composite key values (default: "/")</li>
     *   <li>encoding: file encoding (default: auto-detect, fallback to configured encoding)</li>
     * </ul>
     * 
     * @param metadataPath path to the metadata file
     * @return parsed AutoloadMetadata
     * @throws IOException if the file cannot be read
     */
    private AutoloadMetadata parseMetadata(Path metadataPath) throws IOException {
        AutoloadMetadata metadata = new AutoloadMetadata();
        
        // Use default encoding for metadata file, with auto-detection
        Charset defaultCharset = Charset.forName(properties.getAutoload().getFileEncoding());
        List<String> lines = EncodingDetector.readAllLines(metadataPath, defaultCharset);
        
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
                    case "encoding":
                        // Allow explicit encoding specification
                        // "auto" means use auto-detection (default behavior)
                        metadata.setEncoding(value);
                        if (!"auto".equalsIgnoreCase(value)) {
                            log.info("Using explicit encoding from metadata: {}", value);
                        }
                        break;
                    case "ascii_normalize":
                    case "asciinormalize":
                    case "ascii-normalize":
                        // Override ASCII normalization for this file
                        // "true"/"yes"/"1" = normalize, "false"/"no"/"0" = don't normalize
                        metadata.setAsciiNormalize(value);
                        log.info("ASCII normalization override from metadata: {}", value);
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
     * 
     * <p>Supports automatic encoding detection or explicit encoding from metadata.
     */
    private ProcessingResult processCsvFile(Path csvFile, AutoloadMetadata metadata) throws IOException {
        ProcessingResult result = new ProcessingResult();
        
        int batchSize = properties.getAutoload().getBatchSize();
        String fileName = csvFile.getFileName().toString();
        
        // Determine encoding: use metadata encoding if specified, otherwise auto-detect
        Charset defaultCharset = Charset.forName(properties.getAutoload().getFileEncoding());
        Charset charset;
        String encodingSource;
        
        if (metadata.isAutoDetectEncoding()) {
            // Auto-detect encoding
            EncodingDetector.EncodingResult encodingResult = EncodingDetector.detectEncoding(csvFile, defaultCharset);
            charset = encodingResult.charset();
            encodingSource = "auto-detected (" + encodingResult.detectionMethod() + ")";
        } else {
            // Use explicit encoding from metadata
            charset = EncodingDetector.parseCharset(metadata.getEncoding(), defaultCharset);
            encodingSource = "metadata";
        }
        
        log.info("Processing CSV file '{}' with encoding: {} [{}], batch size: {}", 
                fileName, charset, encodingSource, batchSize);
        
        CSVFormat format = CSVFormat.DEFAULT.builder()
                .setDelimiter(metadata.getDelimiter())
                .setHeader()
                .setSkipHeaderRecord(true)
                .setIgnoreEmptyLines(true)
                .setTrim(true)
                .build();
        
        // Use EncodingDetector to create reader (handles BOM stripping)
        try (Reader reader = EncodingDetector.createReader(csvFile, charset, true);
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
            
            // Determine if ASCII normalization should be applied
            boolean asciiNormalize = metadata.shouldNormalizeAscii(properties.getAutoload().isAsciiNormalize());
            boolean asciiNormalizeKeys = asciiNormalize && properties.getAutoload().isAsciiNormalizeKeys();
            if (asciiNormalize) {
                log.info("[{}] ASCII normalization enabled (keys: {})", fileName, asciiNormalizeKeys);
            }
            
            // Batch accumulator
            List<CacheEntry> batch = new ArrayList<>(batchSize);
            String region = metadata.getRegion();
            long ttl = metadata.getTtl();
            
            // Ensure region exists before processing
            cacheService.ensureRegionExistsPublic(region);
            
            long startTime = System.currentTimeMillis();
            
            log.debug("[{}] Starting CSV record iteration for region '{}'", fileName, region);
            int iterationCount = 0;
            
            for (CSVRecord record : parser) {
                iterationCount++;
                if (iterationCount == 1) {
                    log.info("[{}] First CSV record found, processing...", fileName);
                }
                // SAFETY CHECK (v1.6.3): Stop if shutdown in progress
                // Note: We don't abort mid-batch - we let the current batch finish writing to persistence
                if (cacheService.isShuttingDown()) {
                    log.info("[{}] Autoload stopped at record {}: shutdown in progress", fileName, recordCount);
                    // Flush any accumulated entries before stopping
                    if (!batch.isEmpty()) {
                        batchNumber++;
                        int saved = cacheService.putEntriesBatch(batch, true);
                        log.info("[{}] Batch #{} (shutdown flush): saved {} entries before shutdown", 
                                fileName, batchNumber, saved);
                        batch.clear();
                    }
                    break;
                }
                
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
                    // Skip only if ALL key components are empty (key is just delimiters or blank)
                    if (keyValue == null || isKeyEffectivelyEmpty(keyValue, metadata.getKeyDelimiter())) {
                        log.debug("Skipping record {} - all key field(s) are empty", record.getRecordNumber());
                        result.incrementSkipped();
                        continue;
                    }
                    
                    // Apply ASCII normalization if enabled (v1.7.9)
                    if (asciiNormalize) {
                        // Normalize all string values in the JSON
                        AsciiNormalizer.normalizeJsonInPlace(jsonNode, objectMapper, false);
                        
                        // Normalize the key if key normalization is enabled
                        if (asciiNormalizeKeys) {
                            keyValue = AsciiNormalizer.normalizeKey(keyValue);
                        }
                    }
                    
                    // Create CacheEntry for batch
                    CacheEntry entry = createCacheEntry(region, keyValue, jsonNode, ttl);
                    batch.add(entry);
                    result.incrementLoaded();
                    recordCount++;
                    
                    // Flush batch when full
                    if (batch.size() >= batchSize) {
                        batchNumber++;
                        // Skip value cache to avoid eviction during autoload
                        int saved = cacheService.putEntriesBatch(batch, true);
                        log.info("[{}] Batch #{}: Processed {} records so far ({} in this batch) -> region '{}'", 
                                fileName, batchNumber, recordCount, saved, region);
                        batch.clear();
                    }
                    
                } catch (Exception e) {
                    log.debug("Error processing record {}: {}", record.getRecordNumber(), e.getMessage());
                    result.incrementErrors();
                }
            }
            
            log.debug("[{}] CSV iteration complete: {} total iterations, {} records processed, batch size: {}", 
                    fileName, iterationCount, recordCount, batch.size());
            
            // Flush remaining entries
            if (!batch.isEmpty()) {
                batchNumber++;
                // Skip value cache to avoid eviction during autoload
                int saved = cacheService.putEntriesBatch(batch, true);
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
            // Allow empty/null values in composite key - use empty string
            // Record will only be skipped if the ENTIRE key is blank (checked by caller)
            if (value == null) {
                values.add("");
            } else {
                values.add(value.trim());
            }
        }
        return String.join(delimiter, values);
    }
    
    /**
     * Process a JSON file (one JSON object per line - JSONL format) with batch writes.
     * 
     * <p>Supports automatic encoding detection or explicit encoding from metadata.
     */
    private ProcessingResult processJsonFile(Path jsonFile, AutoloadMetadata metadata) throws IOException {
        ProcessingResult result = new ProcessingResult();
        
        int batchSize = properties.getAutoload().getBatchSize();
        String fileName = jsonFile.getFileName().toString();
        
        // Determine encoding: use metadata encoding if specified, otherwise auto-detect
        Charset defaultCharset = Charset.forName(properties.getAutoload().getFileEncoding());
        Charset charset;
        String encodingSource;
        
        if (metadata.isAutoDetectEncoding()) {
            // Auto-detect encoding
            EncodingDetector.EncodingResult encodingResult = EncodingDetector.detectEncoding(jsonFile, defaultCharset);
            charset = encodingResult.charset();
            encodingSource = "auto-detected (" + encodingResult.detectionMethod() + ")";
        } else {
            // Use explicit encoding from metadata
            charset = EncodingDetector.parseCharset(metadata.getEncoding(), defaultCharset);
            encodingSource = "metadata";
        }
        
        log.info("Processing JSON file '{}' with encoding: {} [{}], batch size: {}", 
                fileName, charset, encodingSource, batchSize);
        
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
        
        // Determine if ASCII normalization should be applied
        boolean asciiNormalize = metadata.shouldNormalizeAscii(properties.getAutoload().isAsciiNormalize());
        boolean asciiNormalizeKeys = asciiNormalize && properties.getAutoload().isAsciiNormalizeKeys();
        if (asciiNormalize) {
            log.info("[{}] ASCII normalization enabled (keys: {})", fileName, asciiNormalizeKeys);
        }
        
        // Batch accumulator
        List<CacheEntry> batch = new ArrayList<>(batchSize);
        String region = metadata.getRegion();
        long ttl = metadata.getTtl();
        
        // Ensure region exists before processing
        cacheService.ensureRegionExistsPublic(region);
        
        long startTime = System.currentTimeMillis();
        
        // Use EncodingDetector to create reader (handles BOM stripping)
        try (BufferedReader reader = EncodingDetector.createReader(jsonFile, charset, true)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();
                
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                
                // SAFETY CHECK (v1.6.3): Stop if shutdown in progress
                // Note: We let current batch finish writing to persistence
                if (cacheService.isShuttingDown()) {
                    log.info("[{}] Autoload stopped at line {}: shutdown in progress", fileName, lineNumber);
                    // Flush any accumulated entries before stopping
                    if (!batch.isEmpty()) {
                        batchNumber++;
                        int saved = cacheService.putEntriesBatch(batch, true);
                        log.info("[{}] Batch #{} (shutdown flush): saved {} entries before shutdown", 
                                fileName, batchNumber, saved);
                        batch.clear();
                    }
                    break;
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
                    // Skip only if ALL key components are empty (key is just delimiters or blank)
                    if (keyValue == null || isKeyEffectivelyEmpty(keyValue, metadata.getKeyDelimiter())) {
                        log.debug("Skipping line {} - all key field(s) are empty", lineNumber);
                        result.incrementSkipped();
                        continue;
                    }
                    
                    // Apply attribute mapping if present
                    JsonNode finalNode = jsonNode;
                    if (attrMapping != null && !attrMapping.isEmpty()) {
                        finalNode = cacheService.applyAttributeMapping(jsonNode, attrMapping);
                    }
                    
                    // Apply ASCII normalization if enabled (v1.7.9)
                    if (asciiNormalize) {
                        // Normalize all string values in the JSON
                        finalNode = AsciiNormalizer.normalizeJson(finalNode, objectMapper, false, true);
                        
                        // Normalize the key if key normalization is enabled
                        if (asciiNormalizeKeys) {
                            keyValue = AsciiNormalizer.normalizeKey(keyValue);
                        }
                    }
                    
                    // Create CacheEntry for batch
                    CacheEntry entry = createCacheEntry(region, keyValue, finalNode, ttl);
                    batch.add(entry);
                    result.incrementLoaded();
                    recordCount++;
                    
                    // Flush batch when full
                    if (batch.size() >= batchSize) {
                        batchNumber++;
                        // Skip value cache to avoid eviction during autoload
                        int saved = cacheService.putEntriesBatch(batch, true);
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
            // Skip value cache to avoid eviction during autoload
            int saved = cacheService.putEntriesBatch(batch, true);
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
     * <p>Empty or null field values are allowed - they will be included as empty strings
     * in the composite key. The record will only be skipped if the ENTIRE key is blank
     * (checked by the caller).
     * 
     * @param jsonNode the JSON object
     * @param keyFields list of field names (e.g., ["country", "state", "city"])
     * @param delimiter the delimiter to join values (e.g., "/")
     * @return composite key (e.g., "US/CA/Los Angeles" or "US//Los Angeles" if state is empty)
     */
    private String buildCompositeKeyFromJsonNode(JsonNode jsonNode, List<String> keyFields, String delimiter) {
        List<String> values = new ArrayList<>();
        for (String field : keyFields) {
            JsonNode fieldNode = jsonNode.get(field);
            // Allow empty/null values in composite key - use empty string
            // Record will only be skipped if the ENTIRE key is blank (checked by caller)
            if (fieldNode == null || fieldNode.isNull()) {
                values.add("");
            } else {
                String value = fieldNode.asText();
                values.add(value != null ? value.trim() : "");
            }
        }
        return String.join(delimiter, values);
    }
    
    /**
     * Check if a key is effectively empty (all components are empty).
     * 
     * <p>A key is effectively empty if after removing all delimiter characters,
     * the remaining string is blank. This handles cases like "//" where all
     * key components were empty.
     * 
     * <p>Examples (with "/" delimiter):
     * <ul>
     *   <li>"US/CA/LA" → NOT empty (has content)</li>
     *   <li>"US//LA" → NOT empty (has content, one component empty)</li>
     *   <li>"//" → EMPTY (all components empty)</li>
     *   <li>"" → EMPTY (blank)</li>
     *   <li>"  " → EMPTY (blank after trim)</li>
     * </ul>
     * 
     * @param key the composite key to check
     * @param delimiter the delimiter used in the key
     * @return true if the key is effectively empty
     */
    private boolean isKeyEffectivelyEmpty(String key, String delimiter) {
        if (key == null || key.isBlank()) {
            return true;
        }
        // Remove all delimiter characters and check if anything remains
        String withoutDelimiters = key.replace(delimiter, "");
        return withoutDelimiters.isBlank();
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
         * Optional encoding override for the data file.
         * If not specified, encoding is auto-detected.
         * Supported values: UTF-8, UTF-16, ISO-8859-1, windows-1252, US-ASCII, etc.
         * Can also be set to "auto" to force auto-detection (default behavior).
         * @since 1.7.9
         */
        private String encoding;
        
        /**
         * Optional override for ASCII normalization.
         * If null, uses the global kuber.autoload.ascii-normalize setting.
         * Set to "true" or "false" to override per-file.
         * @since 1.7.9
         */
        private String asciiNormalize;
        
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
        
        /**
         * Check if encoding should be auto-detected.
         * @return true if encoding is null, empty, or "auto"
         */
        public boolean isAutoDetectEncoding() {
            return encoding == null || encoding.isBlank() || "auto".equalsIgnoreCase(encoding.trim());
        }
        
        /**
         * Check if ASCII normalization should be applied.
         * @param globalDefault the global default from configuration
         * @return true if ASCII normalization should be applied
         */
        public boolean shouldNormalizeAscii(boolean globalDefault) {
            if (asciiNormalize == null || asciiNormalize.isBlank()) {
                return globalDefault;
            }
            return "true".equalsIgnoreCase(asciiNormalize.trim()) || 
                   "yes".equalsIgnoreCase(asciiNormalize.trim()) ||
                   "1".equals(asciiNormalize.trim());
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
