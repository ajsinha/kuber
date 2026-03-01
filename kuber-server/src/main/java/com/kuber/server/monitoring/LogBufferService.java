/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * In-memory ring buffer for recent log entries.
 * Used by the monitoring UI to display live logs.
 *
 * @version 2.6.4
 */
package com.kuber.server.monitoring;

import lombok.Data;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Service that maintains a ring buffer of recent log entries.
 * Entries are added by the LogBufferAppender (Logback appender).
 * The monitoring UI polls this service for live log display.
 *
 * @version 2.6.4
 */
@Service
public class LogBufferService {

    private static final int MAX_BUFFER_SIZE = 2000;
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private final ConcurrentLinkedDeque<LogEntry> buffer = new ConcurrentLinkedDeque<>();
    private final AtomicLong sequenceCounter = new AtomicLong(0);

    // Global singleton for Logback appender access (set on init)
    private static volatile LogBufferService instance;

    public LogBufferService() {
        instance = this;
    }

    public static LogBufferService getInstance() {
        return instance;
    }

    /**
     * Add a log entry to the buffer. Called by LogBufferAppender.
     */
    public void addEntry(String level, String loggerName, String message,
                         String threadName, long timestamp, String stackTrace) {
        LogEntry entry = new LogEntry();
        entry.setId(sequenceCounter.incrementAndGet());
        entry.setLevel(level);
        entry.setLogger(loggerName);
        entry.setMessage(message);
        entry.setThread(threadName);
        entry.setTimestamp(timestamp);
        entry.setFormattedTime(FORMATTER.format(Instant.ofEpochMilli(timestamp)));
        entry.setStackTrace(stackTrace);

        buffer.addLast(entry);

        // Trim if over max
        while (buffer.size() > MAX_BUFFER_SIZE) {
            buffer.pollFirst();
        }
    }

    /**
     * Get recent log entries, optionally filtered.
     *
     * @param maxLines  Maximum number of lines to return
     * @param level     Minimum log level filter (null = all)
     * @param search    Text search filter (null = all)
     * @param afterId   Only return entries after this sequence ID (for incremental polling)
     * @return List of matching log entries (newest last)
     */
    public List<LogEntry> getEntries(int maxLines, String level, String search, Long afterId) {
        List<LogEntry> entries = new ArrayList<>(buffer);

        // Filter by afterId (for incremental polling)
        if (afterId != null && afterId > 0) {
            entries = entries.stream()
                    .filter(e -> e.getId() > afterId)
                    .collect(Collectors.toList());
        }

        // Filter by level
        if (level != null && !level.isEmpty() && !"ALL".equalsIgnoreCase(level)) {
            int minLevel = levelToInt(level);
            entries = entries.stream()
                    .filter(e -> levelToInt(e.getLevel()) >= minLevel)
                    .collect(Collectors.toList());
        }

        // Filter by search text
        if (search != null && !search.isEmpty()) {
            String lowerSearch = search.toLowerCase();
            entries = entries.stream()
                    .filter(e -> e.getMessage().toLowerCase().contains(lowerSearch)
                            || e.getLogger().toLowerCase().contains(lowerSearch)
                            || e.getThread().toLowerCase().contains(lowerSearch))
                    .collect(Collectors.toList());
        }

        // Return last maxLines entries
        if (entries.size() > maxLines) {
            entries = entries.subList(entries.size() - maxLines, entries.size());
        }

        return entries;
    }

    /**
     * Get the current latest sequence ID (for polling baseline).
     */
    public long getLatestId() {
        return sequenceCounter.get();
    }

    /**
     * Get buffer statistics.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("bufferSize", buffer.size());
        stats.put("maxBufferSize", MAX_BUFFER_SIZE);
        stats.put("totalProcessed", sequenceCounter.get());
        stats.put("latestId", sequenceCounter.get());
        return stats;
    }

    /**
     * Clear the log buffer.
     */
    public void clear() {
        buffer.clear();
    }

    private static int levelToInt(String level) {
        if (level == null) return 0;
        return switch (level.toUpperCase()) {
            case "TRACE" -> 0;
            case "DEBUG" -> 1;
            case "INFO" -> 2;
            case "WARN", "WARNING" -> 3;
            case "ERROR" -> 4;
            default -> 0;
        };
    }

    @Data
    public static class LogEntry {
        private long id;
        private String level;
        private String logger;
        private String message;
        private String thread;
        private long timestamp;
        private String formattedTime;
        private String stackTrace;
    }
}
