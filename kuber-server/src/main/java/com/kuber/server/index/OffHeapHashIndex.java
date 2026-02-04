/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Patent Pending: Off-Heap Secondary Index Architecture
 */
package com.kuber.server.index;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Off-Heap Hash-based secondary index for equality queries.
 * 
 * <p>Stores index data in direct memory (DRAM) outside the Java heap, providing:
 * <ul>
 *   <li>Zero GC pressure for index storage</li>
 *   <li>O(1) lookup for equality queries</li>
 *   <li>Scalability to hundreds of millions of entries</li>
 *   <li>Predictable latency without GC pauses</li>
 * </ul>
 * 
 * <h3>Trade-offs vs On-Heap:</h3>
 * <ul>
 *   <li>~5-20x slower individual lookups</li>
 *   <li>Still 100-1000x faster than full table scans</li>
 *   <li>Zero GC pressure (no heap usage for data)</li>
 *   <li>5-10x more scalable (limited by RAM, not heap)</li>
 * </ul>
 * 
 * <h3>Architecture:</h3>
 * <ul>
 *   <li>On-heap: fieldValue -> offset mapping (small footprint)</li>
 *   <li>Off-heap: Document keys stored in ByteBuffer segments</li>
 *   <li>Segmented buffers support >2GB total storage</li>
 * </ul>
 * 
 * @version 2.0.0
 * @since 1.9.0
 */
@Slf4j
public class OffHeapHashIndex implements SecondaryIndex {
    
    private static final int SEGMENT_SIZE = 256 * 1024 * 1024; // 256MB per segment
    private static final long DEFAULT_INITIAL_SIZE = 8L * 1024L * 1024L; // 8MB initial
    private static final long DEFAULT_MAX_SIZE = 1024L * 1024L * 1024L; // 1GB max
    
    private final IndexDefinition definition;
    private final long maxTotalSize;
    
    // On-heap: fieldValue -> list of offsets in off-heap buffer
    // Each offset points to a document key stored off-heap
    private final ConcurrentHashMap<String, List<Long>> valueToOffsets;
    
    // Reverse mapping: documentKey -> (fieldValue, offset) for efficient removal
    private final ConcurrentHashMap<String, KeyLocation> keyToLocation;
    
    // Off-heap storage
    private final List<ByteBuffer> segments;
    private final AtomicLong totalAllocatedSize;
    private final AtomicLong writePosition;
    private final AtomicLong offHeapBytesUsed;
    private final AtomicLong deletedBytes;
    
    // Statistics
    private final AtomicLong totalEntries;
    private final AtomicLong lookupCount;
    
    // Thread safety
    private final ReentrantReadWriteLock bufferLock;
    
    // Location tracking for removal
    private static class KeyLocation {
        final String fieldValue;
        final long offset;
        
        KeyLocation(String fieldValue, long offset) {
            this.fieldValue = fieldValue;
            this.offset = offset;
        }
    }
    
    public OffHeapHashIndex(IndexDefinition definition) {
        this(definition, DEFAULT_INITIAL_SIZE, DEFAULT_MAX_SIZE);
    }
    
    public OffHeapHashIndex(IndexDefinition definition, long initialSize, long maxSize) {
        this.definition = definition;
        this.maxTotalSize = maxSize;
        
        this.valueToOffsets = new ConcurrentHashMap<>();
        this.keyToLocation = new ConcurrentHashMap<>();
        this.segments = Collections.synchronizedList(new ArrayList<>());
        this.totalAllocatedSize = new AtomicLong(0);
        this.writePosition = new AtomicLong(0);
        this.offHeapBytesUsed = new AtomicLong(0);
        this.deletedBytes = new AtomicLong(0);
        this.totalEntries = new AtomicLong(0);
        this.lookupCount = new AtomicLong(0);
        this.bufferLock = new ReentrantReadWriteLock();
        
        // Allocate initial segment
        int initialSegmentSize = (int) Math.min(initialSize, SEGMENT_SIZE);
        ByteBuffer initialSegment = ByteBuffer.allocateDirect(initialSegmentSize);
        segments.add(initialSegment);
        totalAllocatedSize.set(initialSegmentSize);
        
        log.debug("Created OffHeapHashIndex for field '{}' with {} initial, {} max",
            definition.getField(), formatBytes(initialSize), formatBytes(maxSize));
    }
    
    @Override
    public IndexDefinition getDefinition() {
        return definition;
    }
    
    @Override
    public void add(Object fieldValue, String documentKey) {
        if (fieldValue == null || documentKey == null) {
            return;
        }
        
        String normalizedValue = normalizeValue(fieldValue);
        byte[] keyBytes = documentKey.getBytes(StandardCharsets.UTF_8);
        
        bufferLock.writeLock().lock();
        try {
            // Check if already exists
            if (keyToLocation.containsKey(documentKey)) {
                // Update instead
                KeyLocation existing = keyToLocation.get(documentKey);
                if (!existing.fieldValue.equals(normalizedValue)) {
                    remove(existing.fieldValue, documentKey);
                } else {
                    return; // Already indexed with same value
                }
            }
            
            // Write key to off-heap
            long offset = writeKey(keyBytes);
            if (offset < 0) {
                log.warn("Failed to write key to off-heap index: buffer full");
                return;
            }
            
            // Update indexes
            valueToOffsets.computeIfAbsent(normalizedValue, k -> Collections.synchronizedList(new ArrayList<>()))
                         .add(offset);
            keyToLocation.put(documentKey, new KeyLocation(normalizedValue, offset));
            totalEntries.incrementAndGet();
            
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
    
    @Override
    public void remove(Object fieldValue, String documentKey) {
        if (fieldValue == null || documentKey == null) {
            return;
        }
        
        bufferLock.writeLock().lock();
        try {
            KeyLocation location = keyToLocation.remove(documentKey);
            if (location != null) {
                List<Long> offsets = valueToOffsets.get(location.fieldValue);
                if (offsets != null) {
                    offsets.remove(location.offset);
                    if (offsets.isEmpty()) {
                        valueToOffsets.remove(location.fieldValue);
                    }
                }
                // Mark space as deleted (for compaction tracking)
                byte[] keyBytes = documentKey.getBytes(StandardCharsets.UTF_8);
                deletedBytes.addAndGet(2 + keyBytes.length);
                totalEntries.decrementAndGet();
            }
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
    
    @Override
    public void update(Object oldValue, Object newValue, String documentKey) {
        if (documentKey == null) {
            return;
        }
        
        String oldNorm = oldValue != null ? normalizeValue(oldValue) : null;
        String newNorm = newValue != null ? normalizeValue(newValue) : null;
        
        if (Objects.equals(oldNorm, newNorm)) {
            return;
        }
        
        remove(oldValue, documentKey);
        add(newValue, documentKey);
    }
    
    @Override
    public Set<String> findEquals(Object fieldValue) {
        if (fieldValue == null) {
            return Collections.emptySet();
        }
        
        lookupCount.incrementAndGet();
        String normalizedValue = normalizeValue(fieldValue);
        
        List<Long> offsets = valueToOffsets.get(normalizedValue);
        if (offsets == null || offsets.isEmpty()) {
            return Collections.emptySet();
        }
        
        Set<String> result = new HashSet<>();
        bufferLock.readLock().lock();
        try {
            for (Long offset : offsets) {
                String key = readKey(offset);
                if (key != null) {
                    result.add(key);
                }
            }
        } finally {
            bufferLock.readLock().unlock();
        }
        
        return result;
    }
    
    @Override
    public Set<String> findIn(Set<Object> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptySet();
        }
        
        Set<String> result = new HashSet<>();
        for (Object value : values) {
            result.addAll(findEquals(value));
        }
        return result;
    }
    
    @Override
    public Set<String> findGreaterThan(Object value, boolean inclusive) {
        log.warn("OffHeapHashIndex does not support range queries. Use BTREE index for field: {}", 
            definition.getField());
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> findLessThan(Object value, boolean inclusive) {
        log.warn("OffHeapHashIndex does not support range queries. Use BTREE index for field: {}", 
            definition.getField());
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> findRange(Object from, Object to, boolean fromInclusive, boolean toInclusive) {
        log.warn("OffHeapHashIndex does not support range queries. Use BTREE index for field: {}", 
            definition.getField());
        return Collections.emptySet();
    }
    
    @Override
    public Set<String> findRegex(java.util.regex.Pattern pattern) {
        return null; // Not supported
    }
    
    @Override
    public Set<String> findPrefix(String prefix) {
        return null; // Not supported
    }
    
    @Override
    public Set<String> findContains(String substring) {
        return null; // Not supported
    }
    
    @Override
    public void clear() {
        bufferLock.writeLock().lock();
        try {
            valueToOffsets.clear();
            keyToLocation.clear();
            writePosition.set(0);
            offHeapBytesUsed.set(0);
            deletedBytes.set(0);
            totalEntries.set(0);
            log.debug("Cleared OffHeapHashIndex for field: {}", definition.getField());
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
    
    @Override
    public long size() {
        return totalEntries.get();
    }
    
    @Override
    public int uniqueValues() {
        return valueToOffsets.size();
    }
    
    @Override
    public long memoryUsageBytes() {
        // On-heap overhead + off-heap used
        long onHeapEstimate = valueToOffsets.size() * 100L + keyToLocation.size() * 80L;
        return onHeapEstimate + offHeapBytesUsed.get();
    }
    
    @Override
    public boolean isOffHeap() {
        return true;
    }
    
    @Override
    public long getOffHeapBytesUsed() {
        return offHeapBytesUsed.get();
    }
    
    @Override
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("field", definition.getField());
        stats.put("type", "HASH");
        stats.put("storage", "OFFHEAP");
        stats.put("entries", totalEntries.get());
        stats.put("uniqueValues", valueToOffsets.size());
        stats.put("memoryBytes", memoryUsageBytes());
        stats.put("offHeapBytesUsed", offHeapBytesUsed.get());
        stats.put("offHeapBytesAllocated", totalAllocatedSize.get());
        stats.put("segmentCount", segments.size());
        stats.put("lookups", lookupCount.get());
        stats.put("supportsRangeQueries", false);
        
        // Value distribution (top 10)
        List<Map<String, Object>> distribution = new ArrayList<>();
        valueToOffsets.entrySet().stream()
            .sorted((a, b) -> Integer.compare(b.getValue().size(), a.getValue().size()))
            .limit(10)
            .forEach(entry -> {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("value", entry.getKey());
                item.put("count", entry.getValue().size());
                item.put("percentage", String.format("%.1f%%", 
                    (entry.getValue().size() * 100.0) / Math.max(1, totalEntries.get())));
                distribution.add(item);
            });
        stats.put("distribution", distribution);
        
        return stats;
    }
    
    @Override
    public boolean supportsRangeQueries() {
        return false;
    }
    
    @Override
    public void shutdown() {
        bufferLock.writeLock().lock();
        try {
            valueToOffsets.clear();
            keyToLocation.clear();
            
            for (ByteBuffer segment : segments) {
                cleanDirectBuffer(segment);
            }
            segments.clear();
            totalAllocatedSize.set(0);
            
            log.info("Shut down OffHeapHashIndex for field '{}'", definition.getField());
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
    
    // ==================== Private Methods ====================
    
    private String normalizeValue(Object value) {
        return value == null ? null : value.toString();
    }
    
    /**
     * Write a key to off-heap storage.
     * Format: [keyLength:2][keyBytes:variable]
     * @return offset where key was written, or -1 if failed
     */
    private long writeKey(byte[] keyBytes) {
        int entrySize = 2 + keyBytes.length;
        long currentPos = writePosition.get();
        
        // Ensure capacity
        while (currentPos + entrySize > totalAllocatedSize.get()) {
            if (!growBuffer()) {
                return -1; // Can't grow further
            }
        }
        
        // Write entry
        long offset = currentPos;
        writeShort(offset, (short) keyBytes.length);
        for (int i = 0; i < keyBytes.length; i++) {
            writeByte(offset + 2 + i, keyBytes[i]);
        }
        
        writePosition.addAndGet(entrySize);
        offHeapBytesUsed.addAndGet(entrySize);
        
        return offset;
    }
    
    /**
     * Read a key from off-heap storage.
     */
    private String readKey(long offset) {
        try {
            int keyLen = readShort(offset) & 0xFFFF;
            if (keyLen <= 0 || keyLen > 10000) {
                return null;
            }
            
            byte[] keyBytes = new byte[keyLen];
            for (int i = 0; i < keyLen; i++) {
                keyBytes[i] = readByte(offset + 2 + i);
            }
            
            return new String(keyBytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.trace("Error reading key at offset {}: {}", offset, e.getMessage());
            return null;
        }
    }
    
    private boolean growBuffer() {
        if (totalAllocatedSize.get() >= maxTotalSize) {
            return false;
        }
        
        long remaining = maxTotalSize - totalAllocatedSize.get();
        int newSegmentSize = (int) Math.min(SEGMENT_SIZE, remaining);
        
        try {
            ByteBuffer newSegment = ByteBuffer.allocateDirect(newSegmentSize);
            segments.add(newSegment);
            totalAllocatedSize.addAndGet(newSegmentSize);
            log.debug("Grew OffHeapHashIndex buffer for '{}': {} total", 
                definition.getField(), formatBytes(totalAllocatedSize.get()));
            return true;
        } catch (OutOfMemoryError e) {
            log.warn("Failed to allocate off-heap buffer for index '{}': {}", 
                definition.getField(), e.getMessage());
            return false;
        }
    }
    
    private void writeShort(long offset, short value) {
        int segIdx = (int) (offset / SEGMENT_SIZE);
        int pos = (int) (offset % SEGMENT_SIZE);
        ByteBuffer seg = segments.get(segIdx);
        seg.putShort(pos, value);
    }
    
    private void writeByte(long offset, byte value) {
        int segIdx = (int) (offset / SEGMENT_SIZE);
        int pos = (int) (offset % SEGMENT_SIZE);
        ByteBuffer seg = segments.get(segIdx);
        seg.put(pos, value);
    }
    
    private short readShort(long offset) {
        int segIdx = (int) (offset / SEGMENT_SIZE);
        int pos = (int) (offset % SEGMENT_SIZE);
        ByteBuffer seg = segments.get(segIdx);
        return seg.getShort(pos);
    }
    
    private byte readByte(long offset) {
        int segIdx = (int) (offset / SEGMENT_SIZE);
        int pos = (int) (offset % SEGMENT_SIZE);
        ByteBuffer seg = segments.get(segIdx);
        return seg.get(pos);
    }
    
    private void cleanDirectBuffer(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) return;
        
        try {
            Field cleanerField = buffer.getClass().getDeclaredField("cleaner");
            cleanerField.setAccessible(true);
            Object cleaner = cleanerField.get(buffer);
            if (cleaner != null) {
                java.lang.reflect.Method cleanMethod = cleaner.getClass().getMethod("clean");
                cleanMethod.setAccessible(true);
                cleanMethod.invoke(cleaner);
            }
        } catch (Exception e) {
            log.trace("Could not manually clean direct buffer: {}", e.getMessage());
        }
    }
    
    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + "B";
        if (bytes < 1024 * 1024) return String.format("%.1fKB", bytes / 1024.0);
        if (bytes < 1024L * 1024 * 1024) return String.format("%.1fMB", bytes / (1024.0 * 1024.0));
        return String.format("%.2fGB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
}
