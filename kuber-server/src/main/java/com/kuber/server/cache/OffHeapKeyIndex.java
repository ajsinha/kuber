/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.server.cache;

import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.KeyIndexEntry;
import com.kuber.core.model.KeyIndexEntry.ValueLocation;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Off-Heap Key Index for a single region.
 * 
 * <p>Stores keys in direct memory (DRAM) outside the Java heap, providing:
 * <ul>
 *   <li>Zero GC pressure for key storage</li>
 *   <li>O(1) key existence checks</li>
 *   <li>O(n) key pattern matching</li>
 *   <li>Immediate negative lookups</li>
 *   <li>Exact entry counts</li>
 * </ul>
 * 
 * <h3>SEGMENTED BUFFER ARCHITECTURE (v1.3.2)</h3>
 * <ul>
 *   <li>Supports buffer sizes exceeding 2GB using multiple ByteBuffer segments</li>
 *   <li>Each segment is up to 1GB (SEGMENT_SIZE)</li>
 *   <li>Offsets are stored as long values spanning across segments</li>
 *   <li>Automatic segment allocation as data grows</li>
 * </ul>
 * 
 * <h3>Architecture</h3>
 * <ul>
 *   <li>Keys stored in off-heap ByteBuffer segments (direct memory)</li>
 *   <li>Metadata kept in compact off-heap structures</li>
 *   <li>Small on-heap hash index for key → offset mapping</li>
 * </ul>
 * 
 * <h3>Memory Layout per entry in keyBuffer</h3>
 * <pre>
 * [keyLength:2][keyBytes:variable][expiresAt:8][valueLocation:1][valueSize:4][lastAccess:8][accessCount:4]
 * </pre>
 * 
 * <p>Thread-safe using ReadWriteLock for buffer operations.
 * 
 * @version 2.6.3
 */
@Slf4j
public class OffHeapKeyIndex implements KeyIndexInterface {
    
    // Entry metadata size: expiresAt(8) + valueLocation(1) + valueSize(4) + lastAccess(8) + accessCount(4) = 25 bytes
    private static final int METADATA_SIZE = 25;
    private static final int KEY_LENGTH_SIZE = 2; // short for key length
    
    // Segment size: 1GB per segment (safe, well under 2GB ByteBuffer limit)
    private static final int SEGMENT_SIZE = 1024 * 1024 * 1024; // 1GB
    private static final long DEFAULT_INITIAL_SIZE = 16L * 1024L * 1024L; // 16MB initial
    private static final long DEFAULT_MAX_SIZE = 8L * 1024L * 1024L * 1024L; // 8GB max
    private static final float GROW_FACTOR = 1.5f;
    
    private final String region;
    private final long maxTotalSize;  // Maximum total size across all segments
    
    // On-heap hash index: key hash → offset across all segments
    // This is small: 8 bytes per entry (long offset)
    private final ConcurrentHashMap<String, Long> keyOffsets;
    
    // Off-heap storage: multiple ByteBuffer segments for >2GB support
    private final CopyOnWriteArrayList<ByteBuffer> segments;
    private final AtomicLong totalAllocatedSize;  // Total bytes allocated across segments
    private final AtomicLong writePosition;
    
    // Statistics (small, kept on heap)
    private final AtomicLong totalValueSize;
    private final AtomicLong memoryValueSize;
    private final AtomicLong indexHits;
    private final AtomicLong indexMisses;
    private final AtomicLong offHeapBytesUsed;
    
    // Thread safety
    private final ReentrantReadWriteLock bufferLock;
    
    // Deleted entry tracking for compaction
    private final AtomicLong deletedBytes;
    private static final float COMPACTION_THRESHOLD = 0.3f; // Compact when 30% is deleted
    
    public OffHeapKeyIndex(String region) {
        this(region, DEFAULT_INITIAL_SIZE, DEFAULT_MAX_SIZE);
    }
    
    /**
     * Create off-heap key index with specified sizes.
     * 
     * @param region Region name
     * @param initialSize Initial buffer size in bytes
     * @param maxSize Maximum total size in bytes (can exceed 2GB with segmented buffers)
     */
    public OffHeapKeyIndex(String region, long initialSize, long maxSize) {
        this.region = region;
        
        // Validate sizes
        if (initialSize < 1024 * 1024) {
            log.warn("Off-heap initial size {} too small for region '{}', using 16MB", 
                    formatBytes(initialSize), region);
            initialSize = 16L * 1024L * 1024L; // 16MB minimum
        }
        if (maxSize < initialSize) {
            log.warn("Off-heap max size {} less than initial {} for region '{}', using initial * 4", 
                    formatBytes(maxSize), formatBytes(initialSize), region);
            maxSize = initialSize * 4;
        }
        
        this.maxTotalSize = maxSize;
        this.keyOffsets = new ConcurrentHashMap<>();
        this.segments = new CopyOnWriteArrayList<>();
        this.totalAllocatedSize = new AtomicLong(0);
        this.writePosition = new AtomicLong(0);
        this.totalValueSize = new AtomicLong(0);
        this.memoryValueSize = new AtomicLong(0);
        this.indexHits = new AtomicLong(0);
        this.indexMisses = new AtomicLong(0);
        this.offHeapBytesUsed = new AtomicLong(0);
        this.deletedBytes = new AtomicLong(0);
        this.bufferLock = new ReentrantReadWriteLock();
        
        // Allocate initial segment
        int initialSegmentSize = (int) Math.min(initialSize, SEGMENT_SIZE);
        ByteBuffer initialSegment = ByteBuffer.allocateDirect(initialSegmentSize);
        segments.add(initialSegment);
        totalAllocatedSize.set(initialSegmentSize);
        
        log.info("Created off-heap key index for region '{}' with {} initial, {} max (segmented)", 
                region, formatBytes(initialSize), formatBytes(maxSize));
    }
    
    /**
     * Legacy constructor for backward compatibility.
     * Converts int parameters to long.
     */
    public OffHeapKeyIndex(String region, int initialBufferSize, int maxBufferSize) {
        this(region, (long) initialBufferSize, (long) maxBufferSize);
    }
    
    /**
     * Get the region name.
     */
    @Override
    public String getRegion() {
        return region;
    }
    
    /**
     * Get exact count of keys in this region - O(1).
     */
    @Override
    public int size() {
        return keyOffsets.size();
    }
    
    /**
     * Check if index is empty.
     */
    @Override
    public boolean isEmpty() {
        return keyOffsets.isEmpty();
    }
    
    /**
     * Check if key exists - O(1).
     */
    @Override
    public boolean containsKey(String key) {
        boolean exists = keyOffsets.containsKey(key);
        if (exists) {
            indexHits.incrementAndGet();
        } else {
            indexMisses.incrementAndGet();
        }
        return exists;
    }
    
    /**
     * Get key entry with statistics tracking.
     */
    @Override
    public KeyIndexEntry get(String key) {
        Long offset = keyOffsets.get(key);
        if (offset == null) {
            indexMisses.incrementAndGet();
            return null;
        }
        
        indexHits.incrementAndGet();
        return readEntry(key, offset);
    }
    
    /**
     * Get key entry without updating statistics.
     */
    @Override
    public KeyIndexEntry getWithoutTracking(String key) {
        Long offset = keyOffsets.get(key);
        if (offset == null) {
            return null;
        }
        return readEntry(key, offset);
    }
    
    /**
     * Get segment index for a given offset.
     */
    private int getSegmentIndex(long offset) {
        return (int) (offset / SEGMENT_SIZE);
    }
    
    /**
     * Get position within segment for a given offset.
     */
    private int getPositionInSegment(long offset) {
        return (int) (offset % SEGMENT_SIZE);
    }
    
    /**
     * Read a byte from the given absolute offset.
     */
    private byte readByte(long offset) {
        int segIdx = getSegmentIndex(offset);
        int pos = getPositionInSegment(offset);
        return segments.get(segIdx).get(pos);
    }
    
    /**
     * Read a short from the given absolute offset.
     */
    private short readShort(long offset) {
        int segIdx = getSegmentIndex(offset);
        int pos = getPositionInSegment(offset);
        return segments.get(segIdx).getShort(pos);
    }
    
    /**
     * Read an int from the given absolute offset.
     */
    private int readInt(long offset) {
        int segIdx = getSegmentIndex(offset);
        int pos = getPositionInSegment(offset);
        return segments.get(segIdx).getInt(pos);
    }
    
    /**
     * Read a long from the given absolute offset.
     */
    private long readLong(long offset) {
        int segIdx = getSegmentIndex(offset);
        int pos = getPositionInSegment(offset);
        return segments.get(segIdx).getLong(pos);
    }
    
    /**
     * Write a byte at the given absolute offset.
     */
    private void writeByte(long offset, byte value) {
        int segIdx = getSegmentIndex(offset);
        int pos = getPositionInSegment(offset);
        segments.get(segIdx).put(pos, value);
    }
    
    /**
     * Write a short at the given absolute offset.
     */
    private void writeShort(long offset, short value) {
        int segIdx = getSegmentIndex(offset);
        int pos = getPositionInSegment(offset);
        segments.get(segIdx).putShort(pos, value);
    }
    
    /**
     * Write an int at the given absolute offset.
     */
    private void writeInt(long offset, int value) {
        int segIdx = getSegmentIndex(offset);
        int pos = getPositionInSegment(offset);
        segments.get(segIdx).putInt(pos, value);
    }
    
    /**
     * Write a long at the given absolute offset.
     */
    private void writeLong(long offset, long value) {
        int segIdx = getSegmentIndex(offset);
        int pos = getPositionInSegment(offset);
        segments.get(segIdx).putLong(pos, value);
    }
    
    /**
     * Read entry from off-heap buffer.
     */
    private KeyIndexEntry readEntry(String key, long offset) {
        bufferLock.readLock().lock();
        try {
            // Read key length
            int keyLen = readShort(offset) & 0xFFFF;
            long metadataOffset = offset + KEY_LENGTH_SIZE + keyLen;
            
            // Read metadata
            long expiresAt = readLong(metadataOffset);
            byte valueLoc = readByte(metadataOffset + 8);
            int valueSize = readInt(metadataOffset + 9);
            long lastAccess = readLong(metadataOffset + 13);
            int accessCount = readInt(metadataOffset + 21);
            
            ValueLocation location = ValueLocation.values()[valueLoc];
            Instant expiresAtInstant = expiresAt == -1 ? null : Instant.ofEpochMilli(expiresAt);
            Instant lastAccessInstant = lastAccess == 0 ? null : Instant.ofEpochMilli(lastAccess);
            
            KeyIndexEntry entry = KeyIndexEntry.builder()
                    .key(key)
                    .valueLocation(location)
                    .valueSize(valueSize)
                    .expiresAt(expiresAtInstant)
                    .lastAccessedAt(lastAccessInstant)
                    .accessCount(accessCount)
                    .build();
            
            return entry;
        } finally {
            bufferLock.readLock().unlock();
        }
    }
    
    /**
     * Put a new key entry.
     */
    @Override
    public void put(String key, KeyIndexEntry entry) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int entrySize = KEY_LENGTH_SIZE + keyBytes.length + METADATA_SIZE;
        
        bufferLock.writeLock().lock();
        try {
            // Check if key already exists
            Long existingOffset = keyOffsets.get(key);
            if (existingOffset != null) {
                // Update existing entry in place if same key length
                int existingKeyLen = readShort(existingOffset) & 0xFFFF;
                if (existingKeyLen == keyBytes.length) {
                    // Same size - update in place
                    writeMetadata(existingOffset + KEY_LENGTH_SIZE + keyBytes.length, entry);
                    return;
                } else {
                    // Different size - mark old as deleted
                    deletedBytes.addAndGet(KEY_LENGTH_SIZE + existingKeyLen + METADATA_SIZE);
                }
            }
            
            // Ensure buffer has space
            ensureCapacity(entrySize);
            
            long offset = writePosition.get();
            
            // Write key length
            writeShort(offset, (short) keyBytes.length);
            
            // Write key bytes
            for (int i = 0; i < keyBytes.length; i++) {
                writeByte(offset + KEY_LENGTH_SIZE + i, keyBytes[i]);
            }
            
            // Write metadata
            writeMetadata(offset + KEY_LENGTH_SIZE + keyBytes.length, entry);
            
            // Update position and index
            writePosition.addAndGet(entrySize);
            offHeapBytesUsed.addAndGet(entrySize);
            keyOffsets.put(key, offset);
            
            // Update size tracking - only count as memory if in MEMORY or BOTH (not DISK or PERSISTENCE_ONLY)
            ValueLocation loc = entry.getValueLocation();
            if (loc == ValueLocation.MEMORY || loc == ValueLocation.BOTH) {
                memoryValueSize.addAndGet(entry.getValueSize());
            }
            totalValueSize.addAndGet(entry.getValueSize());
            
        } finally {
            bufferLock.writeLock().unlock();
        }
        
        // Check if compaction needed
        maybeCompact();
    }
    
    private void writeMetadata(long offset, KeyIndexEntry entry) {
        long expiresAt = entry.getExpiresAt() != null ? entry.getExpiresAt().toEpochMilli() : -1;
        byte valueLoc = (byte) entry.getValueLocation().ordinal();
        int valueSize = (int) entry.getValueSize();
        long lastAccess = entry.getLastAccessedAt() != null ? entry.getLastAccessedAt().toEpochMilli() : 0;
        int accessCount = (int) entry.getAccessCount();
        
        writeLong(offset, expiresAt);
        writeByte(offset + 8, valueLoc);
        writeInt(offset + 9, valueSize);
        writeLong(offset + 13, lastAccess);
        writeInt(offset + 21, accessCount);
    }
    
    /**
     * Put from CacheEntry.
     */
    @Override
    public void putFromCacheEntry(CacheEntry cacheEntry, ValueLocation location) {
        KeyIndexEntry entry = KeyIndexEntry.fromCacheEntry(cacheEntry, location);
        put(cacheEntry.getKey(), entry);
    }
    
    /**
     * Update value location for a key.
     */
    @Override
    public void updateLocation(String key, ValueLocation newLocation) {
        Long offset = keyOffsets.get(key);
        if (offset == null) return;
        
        bufferLock.writeLock().lock();
        try {
            int keyLen = readShort(offset) & 0xFFFF;
            long metadataOffset = offset + KEY_LENGTH_SIZE + keyLen;
            
            byte oldLoc = readByte(metadataOffset + 8);
            int valueSize = readInt(metadataOffset + 9);
            
            ValueLocation oldLocation = ValueLocation.values()[oldLoc];
            
            // Update memory value size tracking
            // PERSISTENCE_ONLY is treated like DISK (value not in memory)
            boolean wasInMemory = (oldLocation == ValueLocation.MEMORY || oldLocation == ValueLocation.BOTH);
            boolean isNowInMemory = (newLocation == ValueLocation.MEMORY || newLocation == ValueLocation.BOTH);
            
            if (wasInMemory && !isNowInMemory) {
                // Moving from memory to disk/persistence-only
                memoryValueSize.addAndGet(-valueSize);
            } else if (!wasInMemory && isNowInMemory) {
                // Moving from disk/persistence-only to memory
                memoryValueSize.addAndGet(valueSize);
            }
            
            // Write new location
            writeByte(metadataOffset + 8, (byte) newLocation.ordinal());
            
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
    
    /**
     * Update access statistics for a key.
     */
    public void recordAccess(String key) {
        Long offset = keyOffsets.get(key);
        if (offset == null) return;
        
        bufferLock.writeLock().lock();
        try {
            int keyLen = readShort(offset) & 0xFFFF;
            long metadataOffset = offset + KEY_LENGTH_SIZE + keyLen;
            
            // Update last access time
            writeLong(metadataOffset + 13, System.currentTimeMillis());
            
            // Increment access count
            int currentCount = readInt(metadataOffset + 21);
            writeInt(metadataOffset + 21, currentCount + 1);
            
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
    
    /**
     * Remove a key from the index.
     */
    @Override
    public KeyIndexEntry remove(String key) {
        Long offset = keyOffsets.remove(key);
        if (offset == null) {
            return null;
        }
        
        bufferLock.writeLock().lock();
        try {
            KeyIndexEntry entry = readEntry(key, offset);
            
            // Mark space as deleted
            int keyLen = readShort(offset) & 0xFFFF;
            int entrySize = KEY_LENGTH_SIZE + keyLen + METADATA_SIZE;
            deletedBytes.addAndGet(entrySize);
            
            // Update size tracking
            if (entry != null) {
                totalValueSize.addAndGet(-entry.getValueSize());
                // Only decrement memory size if value was actually in memory (MEMORY or BOTH)
                ValueLocation loc = entry.getValueLocation();
                if (loc == ValueLocation.MEMORY || loc == ValueLocation.BOTH) {
                    memoryValueSize.addAndGet(-entry.getValueSize());
                }
            }
            
            return entry;
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
    
    /**
     * Find all keys matching a glob pattern - O(n) memory scan.
     */
    @Override
    public List<String> findKeysByPattern(String pattern) {
        if (pattern == null || pattern.equals("*")) {
            // Return all non-expired keys
            return keyOffsets.keySet().stream()
                    .filter(key -> {
                        KeyIndexEntry entry = getWithoutTracking(key);
                        return entry != null && !entry.isExpired();
                    })
                    .collect(Collectors.toList());
        }
        
        // Convert glob pattern to regex
        String regex = globToRegex(pattern);
        Pattern compiledPattern = Pattern.compile(regex);
        
        return keyOffsets.keySet().stream()
                .filter(key -> compiledPattern.matcher(key).matches())
                .filter(key -> {
                    KeyIndexEntry entry = getWithoutTracking(key);
                    return entry != null && !entry.isExpired();
                })
                .collect(Collectors.toList());
    }
    
    /**
     * Get all keys in this region.
     */
    @Override
    public Set<String> getAllKeys() {
        return keyOffsets.keySet().stream()
                .filter(key -> {
                    KeyIndexEntry entry = getWithoutTracking(key);
                    return entry != null && !entry.isExpired();
                })
                .collect(Collectors.toSet());
    }
    
    /**
     * Get keys with values in memory.
     */
    @Override
    public List<String> getKeysInMemory() {
        return keyOffsets.keySet().stream()
                .filter(key -> {
                    KeyIndexEntry entry = getWithoutTracking(key);
                    return entry != null && !entry.isExpired() && entry.isValueInMemory();
                })
                .collect(Collectors.toList());
    }
    
    /**
     * Get keys with values only on disk (not in memory cache).
     * Includes both DISK and PERSISTENCE_ONLY locations.
     */
    @Override
    public List<String> getKeysOnDiskOnly() {
        return keyOffsets.keySet().stream()
                .filter(key -> {
                    KeyIndexEntry entry = getWithoutTracking(key);
                    if (entry == null || entry.isExpired()) return false;
                    ValueLocation loc = entry.getValueLocation();
                    return loc == ValueLocation.DISK || loc == ValueLocation.PERSISTENCE_ONLY;
                })
                .collect(Collectors.toList());
    }
    
    /**
     * Find expired keys for cleanup.
     */
    @Override
    public List<String> findExpiredKeys() {
        return keyOffsets.keySet().stream()
                .filter(key -> {
                    KeyIndexEntry entry = getWithoutTracking(key);
                    return entry != null && entry.isExpired();
                })
                .collect(Collectors.toList());
    }
    
    /**
     * Clear all entries.
     */
    @Override
    public void clear() {
        bufferLock.writeLock().lock();
        try {
            keyOffsets.clear();
            writePosition.set(0);
            offHeapBytesUsed.set(0);
            deletedBytes.set(0);
            totalValueSize.set(0);
            memoryValueSize.set(0);
            
            // Keep first segment, release others
            while (segments.size() > 1) {
                ByteBuffer removed = segments.remove(segments.size() - 1);
                cleanDirectBuffer(removed);
            }
            totalAllocatedSize.set(segments.get(0).capacity());
            
            log.debug("Cleared off-heap key index for region '{}'", region);
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
    
    /**
     * Ensure capacity for additional bytes, allocating new segments if needed.
     */
    private void ensureCapacity(int additionalBytes) {
        long requiredPosition = writePosition.get() + additionalBytes;
        long currentAllocated = totalAllocatedSize.get();
        
        if (requiredPosition <= currentAllocated) {
            return;
        }
        
        // Need to grow
        if (currentAllocated >= maxTotalSize) {
            throw new OutOfMemoryError("Off-heap key index exceeded maximum size: current=" + 
                    formatBytes(currentAllocated) + ", max=" + formatBytes(maxTotalSize) + 
                    " for region: " + region);
        }
        
        // Calculate how much to grow
        long neededBytes = requiredPosition - currentAllocated;
        long growSize = Math.max(neededBytes, (long) (currentAllocated * (GROW_FACTOR - 1)));
        growSize = Math.min(growSize, maxTotalSize - currentAllocated);
        
        // Allocate new segment(s)
        while (growSize > 0) {
            int segmentSize = (int) Math.min(growSize, SEGMENT_SIZE);
            
            log.info("Allocating new off-heap segment for region '{}': {} (total: {})", 
                    region, formatBytes(segmentSize), formatBytes(currentAllocated + segmentSize));
            
            ByteBuffer newSegment = ByteBuffer.allocateDirect(segmentSize);
            segments.add(newSegment);
            totalAllocatedSize.addAndGet(segmentSize);
            currentAllocated += segmentSize;
            growSize -= segmentSize;
        }
    }
    
    /**
     * Compact buffer if fragmentation is high.
     */
    private void maybeCompact() {
        long deleted = deletedBytes.get();
        long used = offHeapBytesUsed.get();
        
        if (used > 0 && (float) deleted / used > COMPACTION_THRESHOLD) {
            compact();
        }
    }
    
    /**
     * Compact the off-heap buffer to reclaim deleted space.
     */
    public void compact() {
        bufferLock.writeLock().lock();
        try {
            if (keyOffsets.isEmpty()) {
                writePosition.set(0);
                deletedBytes.set(0);
                offHeapBytesUsed.set(0);
                return;
            }
            
            long startSize = writePosition.get();
            
            // Create temporary storage for compacted data
            long dataSize = offHeapBytesUsed.get() - deletedBytes.get();
            int tempSegmentSize = (int) Math.min(dataSize + 1024 * 1024, SEGMENT_SIZE);
            List<ByteBuffer> tempSegments = new ArrayList<>();
            tempSegments.add(ByteBuffer.allocateDirect(tempSegmentSize));
            long tempAllocated = tempSegmentSize;
            
            long newPosition = 0;
            Map<String, Long> newOffsets = new HashMap<>();
            
            // Copy live entries
            for (Map.Entry<String, Long> entry : keyOffsets.entrySet()) {
                String key = entry.getKey();
                long oldOffset = entry.getValue();
                
                int keyLen = readShort(oldOffset) & 0xFFFF;
                int entrySize = KEY_LENGTH_SIZE + keyLen + METADATA_SIZE;
                
                // Ensure temp buffer has space
                while (newPosition + entrySize > tempAllocated) {
                    int newSegSize = (int) Math.min(SEGMENT_SIZE, maxTotalSize - tempAllocated);
                    tempSegments.add(ByteBuffer.allocateDirect(newSegSize));
                    tempAllocated += newSegSize;
                }
                
                // Copy entry to temp buffer
                for (int i = 0; i < entrySize; i++) {
                    byte b = readByte(oldOffset + i);
                    int segIdx = (int) ((newPosition + i) / SEGMENT_SIZE);
                    int pos = (int) ((newPosition + i) % SEGMENT_SIZE);
                    tempSegments.get(segIdx).put(pos, b);
                }
                
                newOffsets.put(key, newPosition);
                newPosition += entrySize;
            }
            
            // Clean up old segments
            for (ByteBuffer segment : segments) {
                cleanDirectBuffer(segment);
            }
            segments.clear();
            
            // Use new segments
            segments.addAll(tempSegments);
            totalAllocatedSize.set(tempAllocated);
            
            keyOffsets.clear();
            keyOffsets.putAll(newOffsets);
            writePosition.set(newPosition);
            offHeapBytesUsed.set(newPosition);
            deletedBytes.set(0);
            
            long reclaimed = startSize - newPosition;
            log.info("Compacted off-heap index for region '{}': reclaimed {} ({})",
                    region, formatBytes(reclaimed), 
                    String.format("%.1f%%", (reclaimed * 100.0) / startSize));
            
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
    
    /**
     * Clean direct buffer to release off-heap memory immediately.
     */
    private void cleanDirectBuffer(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) return;
        
        try {
            // Use reflection to access the cleaner (works on most JVMs)
            Field cleanerField = buffer.getClass().getDeclaredField("cleaner");
            cleanerField.setAccessible(true);
            Object cleaner = cleanerField.get(buffer);
            if (cleaner != null) {
                java.lang.reflect.Method cleanMethod = cleaner.getClass().getMethod("clean");
                cleanMethod.setAccessible(true);
                cleanMethod.invoke(cleaner);
            }
        } catch (Exception e) {
            // Fallback: let GC handle it
            log.debug("Could not manually clean direct buffer: {}", e.getMessage());
        }
    }
    
    /**
     * Format bytes as human-readable string.
     */
    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + "B";
        if (bytes < 1024 * 1024) return String.format("%.1fKB", bytes / 1024.0);
        if (bytes < 1024L * 1024 * 1024) return String.format("%.1fMB", bytes / (1024.0 * 1024.0));
        return String.format("%.2fGB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
    
    /**
     * Convert glob pattern to regex.
     */
    private String globToRegex(String glob) {
        StringBuilder regex = new StringBuilder("^");
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            switch (c) {
                case '*' -> regex.append(".*");
                case '?' -> regex.append(".");
                case '.', '(', ')', '+', '|', '^', '$', '@', '%' -> regex.append("\\").append(c);
                case '\\' -> regex.append("\\\\");
                case '[' -> regex.append("\\[");
                case ']' -> regex.append("\\]");
                case '{' -> regex.append("\\{");
                case '}' -> regex.append("\\}");
                default -> regex.append(c);
            }
        }
        regex.append("$");
        return regex.toString();
    }
    
    /**
     * Get statistics about this index.
     */
    @Override
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("region", region);
        stats.put("storageType", "OFF_HEAP");
        stats.put("totalKeys", size());
        stats.put("segmentCount", segments.size());
        stats.put("offHeapBytesUsed", offHeapBytesUsed.get());
        stats.put("offHeapBytesAllocated", totalAllocatedSize.get());
        stats.put("offHeapMaxBytes", maxTotalSize);
        stats.put("deletedBytes", deletedBytes.get());
        stats.put("fragmentationRatio", String.format("%.2f%%", 
                (offHeapBytesUsed.get() > 0 ? (deletedBytes.get() * 100.0 / offHeapBytesUsed.get()) : 0)));
        stats.put("totalValueSize", totalValueSize.get());
        stats.put("memoryValueSize", memoryValueSize.get());
        stats.put("indexHits", indexHits.get());
        stats.put("indexMisses", indexMisses.get());
        
        long total = indexHits.get() + indexMisses.get();
        if (total > 0) {
            stats.put("indexHitRate", String.format("%.2f%%", (indexHits.get() * 100.0) / total));
        } else {
            stats.put("indexHitRate", "N/A");
        }
        
        return stats;
    }
    
    /**
     * Get hit rate as decimal (0.0 to 1.0).
     */
    @Override
    public double getHitRate() {
        long total = indexHits.get() + indexMisses.get();
        if (total > 0) {
            return (double) indexHits.get() / total;
        }
        return 0.0;
    }
    
    /**
     * Get total off-heap bytes used.
     */
    @Override
    public long getOffHeapBytesUsed() {
        return offHeapBytesUsed.get();
    }
    
    /**
     * Get total off-heap bytes allocated.
     */
    public long getOffHeapBytesAllocated() {
        return totalAllocatedSize.get();
    }
    
    /**
     * Check if this is an off-heap implementation.
     */
    @Override
    public boolean isOffHeap() {
        return true;
    }
    
    /**
     * Get memory entry count (values in memory).
     */
    @Override
    public long getMemoryEntryCount() {
        return keyOffsets.keySet().stream()
                .filter(key -> {
                    KeyIndexEntry entry = getWithoutTracking(key);
                    return entry != null && !entry.isExpired() && entry.isValueInMemory();
                })
                .count();
    }
    
    /**
     * Get disk-only entry count (entries not in memory cache).
     * Includes both DISK and PERSISTENCE_ONLY locations.
     */
    @Override
    public long getDiskOnlyEntryCount() {
        return keyOffsets.keySet().stream()
                .filter(key -> {
                    KeyIndexEntry entry = getWithoutTracking(key);
                    if (entry == null || entry.isExpired()) return false;
                    ValueLocation loc = entry.getValueLocation();
                    return loc == ValueLocation.DISK || loc == ValueLocation.PERSISTENCE_ONLY;
                })
                .count();
    }
    
    /**
     * Shutdown and release all off-heap memory.
     */
    @Override
    public void shutdown() {
        bufferLock.writeLock().lock();
        try {
            int segmentCount = segments.size();
            keyOffsets.clear();
            for (ByteBuffer segment : segments) {
                cleanDirectBuffer(segment);
            }
            segments.clear();
            totalAllocatedSize.set(0);
            log.info("Shut down off-heap key index for region '{}' (released {} segments)", region, segmentCount);
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
}
