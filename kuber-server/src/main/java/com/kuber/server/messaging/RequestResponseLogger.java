/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Request/Response Logger Service (v1.7.9)
 * 
 * Asynchronously logs request/response pairs for each broker to rolling files.
 * Maintains up to 10 versions of log files per broker/topic.
 */
package com.kuber.server.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kuber.server.config.KuberProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Asynchronous request/response logger for message broker interactions.
 * 
 * <p>Features:
 * <ul>
 *   <li>Asynchronous file writing to avoid blocking message processing</li>
 *   <li>Rolling files with configurable message count (default: 1000)</li>
 *   <li>Up to 10 file versions maintained per broker/topic</li>
 *   <li>JSON format for easy parsing and viewing</li>
 * </ul>
 * 
 * @version 1.8.3
 */
@Slf4j
@Service
public class RequestResponseLogger {
    
    private static final String LOG_FOLDER = "request_response";
    private static final int MAX_FILE_VERSIONS = 10;
    private static final DateTimeFormatter FILE_TIMESTAMP_FORMAT = 
            DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    
    private final KuberProperties properties;
    private final ObjectMapper objectMapper;
    
    // Configuration
    private int maxMessagesPerFile = 1000;
    private boolean loggingEnabled = true;
    
    // Base folder for logs
    private Path baseLogFolder;
    
    // Async write queue per broker/topic
    private final Map<String, LogWriter> writers = new ConcurrentHashMap<>();
    
    // Write queue for async processing
    private final BlockingQueue<LogEntry> writeQueue = new LinkedBlockingQueue<>(10000);
    private ExecutorService writerExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    @Autowired
    public RequestResponseLogger(KuberProperties properties) {
        this.properties = properties;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    }
    
    @PostConstruct
    public void init() {
        // Create base log folder
        String secureFolder = properties.getSecure().getFolder();
        baseLogFolder = Paths.get(secureFolder, LOG_FOLDER);
        
        try {
            Files.createDirectories(baseLogFolder);
            log.info("Request/Response log folder initialized: {}", baseLogFolder);
        } catch (IOException e) {
            log.error("Failed to create log folder: {}", e.getMessage());
        }
        
        // Start async writer thread
        running.set(true);
        writerExecutor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "ReqRespLogger");
            t.setDaemon(true);
            return t;
        });
        
        writerExecutor.submit(this::processWriteQueue);
        log.info("Request/Response logger started");
    }
    
    @PreDestroy
    public void shutdown() {
        running.set(false);
        
        // Close all writers
        for (LogWriter writer : writers.values()) {
            writer.close();
        }
        writers.clear();
        
        // Shutdown executor
        if (writerExecutor != null) {
            writerExecutor.shutdown();
            try {
                writerExecutor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        log.info("Request/Response logger stopped");
    }
    
    /**
     * Configure the logger settings.
     */
    public void configure(int maxMessages, boolean enabled) {
        this.maxMessagesPerFile = maxMessages > 0 ? maxMessages : 1000;
        this.loggingEnabled = enabled;
        log.info("Request/Response logger configured: maxMessages={}, enabled={}", 
                maxMessagesPerFile, loggingEnabled);
    }
    
    /**
     * Log a request/response pair asynchronously.
     */
    public void logRequestResponse(String brokerName, String topic, 
                                   CacheRequest request, CacheResponse response) {
        if (!loggingEnabled) {
            return;
        }
        
        try {
            LogEntry entry = new LogEntry(
                    brokerName, topic, request, response, 
                    Instant.now(), System.currentTimeMillis()
            );
            
            // Non-blocking offer
            if (!writeQueue.offer(entry)) {
                log.warn("Request/Response log queue full, dropping entry for {}/{}", brokerName, topic);
            }
        } catch (Exception e) {
            log.error("Error queuing log entry: {}", e.getMessage());
        }
    }
    
    /**
     * Process the write queue asynchronously.
     */
    private void processWriteQueue() {
        while (running.get()) {
            try {
                LogEntry entry = writeQueue.poll(1, TimeUnit.SECONDS);
                if (entry != null) {
                    writeEntry(entry);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error processing log entry: {}", e.getMessage());
            }
        }
        
        // Drain remaining entries on shutdown
        List<LogEntry> remaining = new ArrayList<>();
        writeQueue.drainTo(remaining);
        for (LogEntry entry : remaining) {
            try {
                writeEntry(entry);
            } catch (Exception e) {
                log.error("Error writing remaining entry: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Write a log entry to the appropriate file.
     */
    private void writeEntry(LogEntry entry) {
        String key = entry.getBrokerName() + "/" + sanitizeTopic(entry.getTopic());
        
        LogWriter writer = writers.computeIfAbsent(key, k -> {
            Path brokerFolder = baseLogFolder.resolve(entry.getBrokerName());
            try {
                Files.createDirectories(brokerFolder);
            } catch (IOException e) {
                log.error("Failed to create broker folder: {}", e.getMessage());
            }
            return new LogWriter(brokerFolder, sanitizeTopic(entry.getTopic()), maxMessagesPerFile);
        });
        
        writer.write(entry, objectMapper);
    }
    
    /**
     * Sanitize topic name for use in file names.
     */
    private String sanitizeTopic(String topic) {
        if (topic == null) return "unknown";
        return topic.replaceAll("[^a-zA-Z0-9_-]", "_");
    }
    
    /**
     * Get available brokers with logs.
     */
    public List<String> getAvailableBrokers() {
        List<String> brokers = new ArrayList<>();
        try {
            if (Files.exists(baseLogFolder)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(baseLogFolder)) {
                    for (Path path : stream) {
                        if (Files.isDirectory(path)) {
                            brokers.add(path.getFileName().toString());
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.error("Error listing brokers: {}", e.getMessage());
        }
        return brokers;
    }
    
    /**
     * Get available topics for a broker.
     */
    public List<String> getAvailableTopics(String brokerName) {
        List<String> topics = new ArrayList<>();
        Path brokerFolder = baseLogFolder.resolve(brokerName);
        
        try {
            if (Files.exists(brokerFolder)) {
                Set<String> topicSet = new HashSet<>();
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(brokerFolder, "*.json")) {
                    for (Path path : stream) {
                        String fileName = path.getFileName().toString();
                        // Extract topic from filename: topic_YYYYMMDD_HHMMSS.json or topic_YYYYMMDD_HHMMSS_N.json
                        String topic = extractTopicFromFilename(fileName);
                        if (topic != null) {
                            topicSet.add(topic);
                        }
                    }
                }
                topics.addAll(topicSet);
                Collections.sort(topics);
            }
        } catch (IOException e) {
            log.error("Error listing topics for broker {}: {}", brokerName, e.getMessage());
        }
        return topics;
    }
    
    /**
     * Extract topic name from log filename.
     */
    private String extractTopicFromFilename(String fileName) {
        // Format: topic_YYYYMMDD_HHMMSS.json or topic_YYYYMMDD_HHMMSS_N.json
        if (!fileName.endsWith(".json")) return null;
        String name = fileName.substring(0, fileName.length() - 5); // Remove .json
        
        // Find the timestamp pattern
        int lastUnderscore = name.lastIndexOf('_');
        if (lastUnderscore > 0) {
            String afterUnderscore = name.substring(lastUnderscore + 1);
            // Check if it's a number (version) or timestamp part
            if (afterUnderscore.matches("\\d+") && afterUnderscore.length() <= 2) {
                // It's a version number, look for previous underscore
                name = name.substring(0, lastUnderscore);
                lastUnderscore = name.lastIndexOf('_');
            }
        }
        
        // Now find YYYYMMDD_HHMMSS pattern
        if (lastUnderscore > 8) {
            // Check for timestamp at end
            String potentialTimestamp = name.substring(lastUnderscore - 8);
            if (potentialTimestamp.matches("\\d{8}_\\d{6}")) {
                return name.substring(0, lastUnderscore - 9);
            }
        }
        
        // Fallback: return everything before first date-like pattern
        int dateStart = name.indexOf("_20");
        if (dateStart > 0) {
            return name.substring(0, dateStart);
        }
        
        return name;
    }
    
    /**
     * Get log files for a broker/topic combination.
     */
    public List<LogFileInfo> getLogFiles(String brokerName, String topic) {
        List<LogFileInfo> files = new ArrayList<>();
        Path brokerFolder = baseLogFolder.resolve(brokerName);
        String topicSanitized = sanitizeTopic(topic);
        
        try {
            if (Files.exists(brokerFolder)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(brokerFolder, 
                        topicSanitized + "*.json")) {
                    for (Path path : stream) {
                        LogFileInfo info = new LogFileInfo();
                        info.setFileName(path.getFileName().toString());
                        info.setFilePath(path.toString());
                        info.setSize(Files.size(path));
                        info.setLastModified(Files.getLastModifiedTime(path).toInstant());
                        info.setMessageCount(countMessagesInFile(path));
                        files.add(info);
                    }
                }
                // Sort by last modified descending
                files.sort((a, b) -> b.getLastModified().compareTo(a.getLastModified()));
            }
        } catch (IOException e) {
            log.error("Error listing files for {}/{}: {}", brokerName, topic, e.getMessage());
        }
        return files;
    }
    
    /**
     * Count messages in a log file.
     */
    private int countMessagesInFile(Path path) {
        try {
            List<LoggedMessage> messages = readLogFile(path);
            return messages != null ? messages.size() : 0;
        } catch (Exception e) {
            return 0;
        }
    }
    
    /**
     * Read messages from a specific log file.
     */
    public List<LoggedMessage> readLogFile(String brokerName, String fileName) {
        Path filePath = baseLogFolder.resolve(brokerName).resolve(fileName);
        return readLogFile(filePath);
    }
    
    /**
     * Read messages from a log file path.
     */
    private List<LoggedMessage> readLogFile(Path path) {
        if (!Files.exists(path)) {
            return Collections.emptyList();
        }
        
        try {
            String content = Files.readString(path, StandardCharsets.UTF_8);
            LogFileContent fileContent = objectMapper.readValue(content, LogFileContent.class);
            return fileContent.getMessages() != null ? fileContent.getMessages() : Collections.emptyList();
        } catch (Exception e) {
            log.error("Error reading log file {}: {}", path, e.getMessage());
            return Collections.emptyList();
        }
    }
    
    /**
     * Get recent messages for a broker/topic with pagination.
     */
    public List<LoggedMessage> getRecentMessages(String brokerName, String topic, 
                                                  int limit, int offset) {
        List<LogFileInfo> files = getLogFiles(brokerName, topic);
        if (files.isEmpty()) {
            return Collections.emptyList();
        }
        
        List<LoggedMessage> allMessages = new ArrayList<>();
        
        // Collect messages from files, newest first
        for (LogFileInfo file : files) {
            List<LoggedMessage> fileMessages = readLogFile(brokerName, file.getFileName());
            // Reverse to get newest first
            Collections.reverse(fileMessages);
            allMessages.addAll(fileMessages);
            
            if (allMessages.size() >= offset + limit) {
                break;
            }
        }
        
        // Apply pagination
        int start = Math.min(offset, allMessages.size());
        int end = Math.min(offset + limit, allMessages.size());
        
        return allMessages.subList(start, end);
    }
    
    /**
     * Get total message count for broker/topic.
     */
    public int getTotalMessageCount(String brokerName, String topic) {
        List<LogFileInfo> files = getLogFiles(brokerName, topic);
        return files.stream().mapToInt(LogFileInfo::getMessageCount).sum();
    }
    
    /**
     * Get logging statistics.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("enabled", loggingEnabled);
        stats.put("maxMessagesPerFile", maxMessagesPerFile);
        stats.put("queueSize", writeQueue.size());
        stats.put("activeWriters", writers.size());
        stats.put("logFolder", baseLogFolder.toString());
        
        List<Map<String, Object>> brokerStats = new ArrayList<>();
        for (String broker : getAvailableBrokers()) {
            Map<String, Object> bs = new LinkedHashMap<>();
            bs.put("broker", broker);
            List<String> topics = getAvailableTopics(broker);
            bs.put("topicCount", topics.size());
            bs.put("topics", topics);
            brokerStats.add(bs);
        }
        stats.put("brokers", brokerStats);
        
        return stats;
    }
    
    // =========================================================================
    // Inner Classes
    // =========================================================================
    
    /**
     * Log entry for async writing.
     */
    @Data
    @AllArgsConstructor
    private static class LogEntry {
        private String brokerName;
        private String topic;
        private CacheRequest request;
        private CacheResponse response;
        private Instant timestamp;
        private long sequenceNumber;
    }
    
    /**
     * Log writer for a specific broker/topic.
     */
    private static class LogWriter {
        private final Path brokerFolder;
        private final String topic;
        private final int maxMessages;
        private final AtomicInteger messageCount = new AtomicInteger(0);
        
        private Path currentFile;
        private List<LoggedMessage> currentMessages = new ArrayList<>();
        private final Object lock = new Object();
        
        LogWriter(Path brokerFolder, String topic, int maxMessages) {
            this.brokerFolder = brokerFolder;
            this.topic = topic;
            this.maxMessages = maxMessages;
            createNewFile();
        }
        
        void createNewFile() {
            String timestamp = LocalDateTime.now().format(FILE_TIMESTAMP_FORMAT);
            currentFile = brokerFolder.resolve(topic + "_" + timestamp + ".json");
            currentMessages = new ArrayList<>();
            messageCount.set(0);
        }
        
        void write(LogEntry entry, ObjectMapper mapper) {
            synchronized (lock) {
                try {
                    LoggedMessage msg = new LoggedMessage();
                    msg.setTimestamp(entry.getTimestamp());
                    msg.setTopic(entry.getTopic());
                    msg.setMessageId(entry.getRequest() != null ? 
                            entry.getRequest().getMessageId() : null);
                    msg.setOperation(entry.getRequest() != null ? 
                            entry.getRequest().getOperation() : null);
                    msg.setRequest(entry.getRequest());
                    msg.setResponse(entry.getResponse());
                    msg.setProcessingTimeMs(entry.getResponse() != null ? 
                            entry.getResponse().getProcessingTimeMs() : 0);
                    msg.setSuccess(entry.getResponse() != null && 
                            entry.getResponse().getResponse() != null &&
                            entry.getResponse().getResponse().isSuccess());
                    
                    currentMessages.add(msg);
                    
                    // Write to file
                    LogFileContent content = new LogFileContent();
                    content.setBroker(entry.getBrokerName());
                    content.setTopic(entry.getTopic());
                    content.setCreated(currentMessages.isEmpty() ? Instant.now() : 
                            currentMessages.get(0).getTimestamp());
                    content.setUpdated(Instant.now());
                    content.setMessages(currentMessages);
                    
                    String json = mapper.writeValueAsString(content);
                    Files.writeString(currentFile, json, StandardCharsets.UTF_8);
                    
                    int count = messageCount.incrementAndGet();
                    
                    // Roll to new file if needed
                    if (count >= maxMessages) {
                        rollFiles();
                        createNewFile();
                    }
                } catch (Exception e) {
                    log.error("Error writing log entry: {}", e.getMessage());
                }
            }
        }
        
        void rollFiles() {
            // Get all files for this topic
            try {
                List<Path> existingFiles = new ArrayList<>();
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(brokerFolder, 
                        topic + "*.json")) {
                    for (Path p : stream) {
                        existingFiles.add(p);
                    }
                }
                
                // Sort by modification time (oldest first)
                existingFiles.sort((a, b) -> {
                    try {
                        return Files.getLastModifiedTime(a).compareTo(
                                Files.getLastModifiedTime(b));
                    } catch (IOException e) {
                        return 0;
                    }
                });
                
                // Delete oldest files if we have too many
                while (existingFiles.size() >= MAX_FILE_VERSIONS) {
                    Path oldest = existingFiles.remove(0);
                    Files.deleteIfExists(oldest);
                    log.debug("Deleted old log file: {}", oldest);
                }
            } catch (IOException e) {
                log.error("Error rolling log files: {}", e.getMessage());
            }
        }
        
        void close() {
            // Final flush is handled by write
        }
    }
    
    /**
     * Log file content structure.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LogFileContent {
        private String broker;
        private String topic;
        private Instant created;
        private Instant updated;
        private List<LoggedMessage> messages = new ArrayList<>();
    }
    
    /**
     * Logged message structure.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LoggedMessage {
        private Instant timestamp;
        private String topic;
        private String messageId;
        private String operation;
        private CacheRequest request;
        private CacheResponse response;
        private long processingTimeMs;
        private boolean success;
    }
    
    /**
     * Log file info for listing.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LogFileInfo {
        private String fileName;
        private String filePath;
        private long size;
        private Instant lastModified;
        private int messageCount;
    }
}
