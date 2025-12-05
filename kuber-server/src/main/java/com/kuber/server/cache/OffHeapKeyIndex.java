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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Off-Heap Key Index for a single region.
 * 
 * Stores keys in direct memory (DRAM) outside the Java heap, providing:
 * - Zero GC pressure for key storage
 * - O(1) key existence checks
 * - O(n) key pattern matching
 * - Immediate negative lookups
 * - Exact entry counts
 * 
 * Architecture:
 * - Keys stored in off-heap ByteBuffer (direct memory)
 * - Metadata kept in compact off-heap structures
 * - Small on-heap hash index for key → offset mapping
 * 
 * Memory Layout per entry in keyBuffer:
 * [keyLength:2][keyBytes:variable][expiresAt:8][valueLocation:1][valueSize:4][lastAccess:8][accessCount:4]
 * 
 * Thread-safe using ReadWriteLock for buffer operations.
 * 
 * @version 1.2.2
 */
@Slf4j
public class OffHeapKeyIndex implements KeyIndexInterface {
    
    // Entry metadata size: expiresAt(8) + valueLocation(1) + valueSize(4) + lastAccess(8) + accessCount(4) = 25 bytes
    private static final int METADATA_SIZE = 25;
    private static final int KEY_LENGTH_SIZE = 2; // short for key length
    private static final int DEFAULT_INITIAL_BUFFER_SIZE = 16 * 1024 * 1024; // 16MB initial
    private static final int DEFAULT_MAX_BUFFER_SIZE = 1024 * 1024 * 1024; // 1GB max
    private static final float GROW_FACTOR = 1.5f;
    
    private final String region;
    private final int maxBufferSize;
    
    // On-heap hash index: key hash → offset in keyBuffer
    // This is small: 8 bytes per entry (long offset)
    private final ConcurrentHashMap<String, Long> keyOffsets;
    
    // Off-heap storage for keys and metadata
    private ByteBuffer keyBuffer;
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
        this(region, DEFAULT_INITIAL_BUFFER_SIZE, DEFAULT_MAX_BUFFER_SIZE);
    }
    
    public OffHeapKeyIndex(String region, int initialBufferSize, int maxBufferSize) {
        this.region = region;
        this.maxBufferSize = maxBufferSize;
        this.keyOffsets = new ConcurrentHashMap<>();
        this.keyBuffer = ByteBuffer.allocateDirect(initialBufferSize);
        this.writePosition = new AtomicLong(0);
        this.totalValueSize = new AtomicLong(0);
        this.memoryValueSize = new AtomicLong(0);
        this.indexHits = new AtomicLong(0);
        this.indexMisses = new AtomicLong(0);
        this.offHeapBytesUsed = new AtomicLong(0);
        this.deletedBytes = new AtomicLong(0);
        this.bufferLock = new ReentrantReadWriteLock();
        
        log.info("Created off-heap key index for region '{}' with {}MB initial, {}MB max direct memory", 
                region, initialBufferSize / (1024 * 1024), maxBufferSize / (1024 * 1024));
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
     * Read entry from off-heap buffer.
     */
    private KeyIndexEntry readEntry(String key, long offset) {
        bufferLock.readLock().lock();
        try {
            // Read key length
            int keyLen = keyBuffer.getShort((int) offset) & 0xFFFF;
            int metadataOffset = (int) offset + KEY_LENGTH_SIZE + keyLen;
            
            // Read metadata
            long expiresAt = keyBuffer.getLong(metadataOffset);
            byte valueLoc = keyBuffer.get(metadataOffset + 8);
            int valueSize = keyBuffer.getInt(metadataOffset + 9);
            long lastAccess = keyBuffer.getLong(metadataOffset + 13);
            int accessCount = keyBuffer.getInt(metadataOffset + 21);
            
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
                int existingKeyLen = keyBuffer.getShort(existingOffset.intValue()) & 0xFFFF;
                if (existingKeyLen == keyBytes.length) {
                    // Same size - update in place
                    writeMetadata(existingOffset.intValue() + KEY_LENGTH_SIZE + keyBytes.length, entry);
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
            keyBuffer.putShort((int) offset, (short) keyBytes.length);
            
            // Write key bytes
            for (int i = 0; i < keyBytes.length; i++) {
                keyBuffer.put((int) offset + KEY_LENGTH_SIZE + i, keyBytes[i]);
            }
            
            // Write metadata
            writeMetadata((int) offset + KEY_LENGTH_SIZE + keyBytes.length, entry);
            
            // Update position and index
            writePosition.addAndGet(entrySize);
            offHeapBytesUsed.addAndGet(entrySize);
            keyOffsets.put(key, offset);
            
            // Update size tracking
            if (entry.getValueLocation() != ValueLocation.DISK) {
                memoryValueSize.addAndGet(entry.getValueSize());
            }
            totalValueSize.addAndGet(entry.getValueSize());
            
        } finally {
            bufferLock.writeLock().unlock();
        }
        
        // Check if compaction needed
        maybeCompact();
    }
    
    private void writeMetadata(int offset, KeyIndexEntry entry) {
        long expiresAt = entry.getExpiresAt() != null ? entry.getExpiresAt().toEpochMilli() : -1;
        byte valueLoc = (byte) entry.getValueLocation().ordinal();
        int valueSize = (int) entry.getValueSize();
        long lastAccess = entry.getLastAccessedAt() != null ? entry.getLastAccessedAt().toEpochMilli() : 0;
        int accessCount = (int) entry.getAccessCount();
        
        keyBuffer.putLong(offset, expiresAt);
        keyBuffer.put(offset + 8, valueLoc);
        keyBuffer.putInt(offset + 9, valueSize);
        keyBuffer.putLong(offset + 13, lastAccess);
        keyBuffer.putInt(offset + 21, accessCount);
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
            int keyLen = keyBuffer.getShort(offset.intValue()) & 0xFFFF;
            int metadataOffset = offset.intValue() + KEY_LENGTH_SIZE + keyLen;
            
            byte oldLoc = keyBuffer.get(metadataOffset + 8);
            int valueSize = keyBuffer.getInt(metadataOffset + 9);
            
            ValueLocation oldLocation = ValueLocation.values()[oldLoc];
            
            // Update memory size tracking
            if (oldLocation != ValueLocation.DISK && newLocation == ValueLocation.DISK) {
                memoryValueSize.addAndGet(-valueSize);
            } else if (oldLocation == ValueLocation.DISK && newLocation != ValueLocation.DISK) {
                memoryValueSize.addAndGet(valueSize);
            }
            
            // Write new location
            keyBuffer.put(metadataOffset + 8, (byte) newLocation.ordinal());
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
    
    /**
     * Record access to a key (updates last access time and count).
     */
    public void recordAccess(String key) {
        Long offset = keyOffsets.get(key);
        if (offset == null) return;
        
        bufferLock.writeLock().lock();
        try {
            int keyLen = keyBuffer.getShort(offset.intValue()) & 0xFFFF;
            int metadataOffset = offset.intValue() + KEY_LENGTH_SIZE + keyLen;
            
            // Update last access time
            keyBuffer.putLong(metadataOffset + 13, System.currentTimeMillis());
            
            // Increment access count
            int count = keyBuffer.getInt(metadataOffset + 21);
            keyBuffer.putInt(metadataOffset + 21, count + 1);
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
        if (offset == null) return null;
        
        KeyIndexEntry entry = readEntry(key, offset);
        
        bufferLock.writeLock().lock();
        try {
            int keyLen = keyBuffer.getShort(offset.intValue()) & 0xFFFF;
            int entrySize = KEY_LENGTH_SIZE + keyLen + METADATA_SIZE;
            deletedBytes.addAndGet(entrySize);
            
            // Update size tracking
            if (entry.getValueLocation() != ValueLocation.DISK) {
                memoryValueSize.addAndGet(-entry.getValueSize());
            }
            totalValueSize.addAndGet(-entry.getValueSize());
        } finally {
            bufferLock.writeLock().unlock();
        }
        
        maybeCompact();
        return entry;
    }
    
    /**
     * Get all keys - O(n).
     */
    @Override
    public Set<String> getAllKeys() {
        return new HashSet<>(keyOffsets.keySet());
    }
    
    /**
     * Find keys matching a glob pattern.
     */
    @Override
    public List<String> findKeysByPattern(String pattern) {
        if (pattern == null || pattern.equals("*")) {
            return new ArrayList<>(keyOffsets.keySet());
        }
        
        String regex = globToRegex(pattern);
        Pattern compiledPattern = Pattern.compile(regex);
        
        return keyOffsets.keySet().stream()
                .filter(k -> compiledPattern.matcher(k).matches())
                .collect(Collectors.toList());
    }
    
    /**
     * Get keys with values in memory.
     */
    @Override
    public List<String> getKeysInMemory() {
        return keyOffsets.entrySet().stream()
                .filter(e -> {
                    KeyIndexEntry entry = readEntry(e.getKey(), e.getValue());
                    return entry != null && !entry.isExpired() && entry.isValueInMemory();
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
    
    /**
     * Get keys with values only on disk.
     */
    @Override
    public List<String> getKeysOnDiskOnly() {
        return keyOffsets.entrySet().stream()
                .filter(e -> {
                    KeyIndexEntry entry = readEntry(e.getKey(), e.getValue());
                    return entry != null && !entry.isExpired() 
                            && entry.getValueLocation() == ValueLocation.DISK;
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
    
    /**
     * Find expired keys.
     */
    @Override
    public List<String> findExpiredKeys() {
        return keyOffsets.entrySet().stream()
                .filter(e -> {
                    KeyIndexEntry entry = readEntry(e.getKey(), e.getValue());
                    return entry != null && entry.isExpired();
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
    
    /**
     * Clear all entries and release off-heap memory.
     */
    @Override
    public void clear() {
        bufferLock.writeLock().lock();
        try {
            keyOffsets.clear();
            writePosition.set(0);
            totalValueSize.set(0);
            memoryValueSize.set(0);
            deletedBytes.set(0);
            offHeapBytesUsed.set(0);
            
            // Clear buffer content
            keyBuffer.clear();
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
    
    /**
     * Ensure buffer has enough capacity.
     */
    private void ensureCapacity(int additionalBytes) {
        long requiredPosition = writePosition.get() + additionalBytes;
        if (requiredPosition <= keyBuffer.capacity()) {
            return;
        }
        
        // Need to grow
        int newCapacity = (int) Math.min(
                keyBuffer.capacity() * GROW_FACTOR,
                maxBufferSize
        );
        
        if (newCapacity < requiredPosition) {
            newCapacity = (int) Math.min(requiredPosition * GROW_FACTOR, maxBufferSize);
        }
        
        if (newCapacity > maxBufferSize) {
            throw new OutOfMemoryError("Off-heap key index exceeded maximum size of " + 
                    maxBufferSize / (1024 * 1024) + "MB for region: " + region);
        }
        
        log.info("Growing off-heap buffer for region '{}' from {}MB to {}MB",
                region, keyBuffer.capacity() / (1024 * 1024), newCapacity / (1024 * 1024));
        
        ByteBuffer newBuffer = ByteBuffer.allocateDirect(newCapacity);
        
        // Copy existing data
        keyBuffer.position(0);
        keyBuffer.limit((int) writePosition.get());
        newBuffer.put(keyBuffer);
        
        // Clean up old buffer
        cleanDirectBuffer(keyBuffer);
        
        keyBuffer = newBuffer;
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
            
            // Create new compacted buffer
            ByteBuffer newBuffer = ByteBuffer.allocateDirect(keyBuffer.capacity());
            long newPosition = 0;
            Map<String, Long> newOffsets = new HashMap<>();
            
            // Copy live entries
            for (Map.Entry<String, Long> entry : keyOffsets.entrySet()) {
                String key = entry.getKey();
                long oldOffset = entry.getValue();
                
                int keyLen = keyBuffer.getShort((int) oldOffset) & 0xFFFF;
                int entrySize = KEY_LENGTH_SIZE + keyLen + METADATA_SIZE;
                
                // Copy entry to new buffer
                for (int i = 0; i < entrySize; i++) {
                    newBuffer.put((int) newPosition + i, keyBuffer.get((int) oldOffset + i));
                }
                
                newOffsets.put(key, newPosition);
                newPosition += entrySize;
            }
            
            // Update state
            cleanDirectBuffer(keyBuffer);
            keyBuffer = newBuffer;
            keyOffsets.clear();
            keyOffsets.putAll(newOffsets);
            writePosition.set(newPosition);
            offHeapBytesUsed.set(newPosition);
            deletedBytes.set(0);
            
            long reclaimed = startSize - newPosition;
            log.info("Compacted off-heap index for region '{}': reclaimed {} bytes ({}KB)",
                    region, reclaimed, reclaimed / 1024);
            
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
     * Get statistics about this index.
     */
    @Override
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("region", region);
        stats.put("storageType", "OFF_HEAP");
        stats.put("totalKeys", size());
        stats.put("offHeapBytesUsed", offHeapBytesUsed.get());
        stats.put("offHeapBytesAllocated", keyBuffer.capacity());
        stats.put("offHeapMaxBytes", maxBufferSize);
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
        return keyBuffer.capacity();
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
        return keyOffsets.entrySet().stream()
                .filter(e -> {
                    KeyIndexEntry entry = readEntry(e.getKey(), e.getValue());
                    return entry != null && !entry.isExpired() && entry.isValueInMemory();
                })
                .count();
    }
    
    /**
     * Get disk-only entry count.
     */
    @Override
    public long getDiskOnlyEntryCount() {
        return keyOffsets.entrySet().stream()
                .filter(e -> {
                    KeyIndexEntry entry = readEntry(e.getKey(), e.getValue());
                    return entry != null && !entry.isExpired() 
                            && entry.getValueLocation() == ValueLocation.DISK;
                })
                .count();
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
     * Shutdown and release all off-heap memory.
     */
    @Override
    public void shutdown() {
        bufferLock.writeLock().lock();
        try {
            keyOffsets.clear();
            cleanDirectBuffer(keyBuffer);
            keyBuffer = null;
            log.info("Shut down off-heap key index for region '{}'", region);
        } finally {
            bufferLock.writeLock().unlock();
        }
    }
}
