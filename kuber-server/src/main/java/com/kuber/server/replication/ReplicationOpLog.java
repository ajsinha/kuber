/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 */
package com.kuber.server.replication;

import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Bounded, lock-free circular buffer that journals every write mutation on the PRIMARY.
 * 
 * <h3>Architecture</h3>
 * <pre>
 *   ┌─────────────────────────────────────────────────────────────┐
 *   │  Ring Buffer (AtomicReferenceArray)                         │
 *   │  ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐        │
 *   │  │ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │ 8 │ 9 │...│ N │        │
 *   │  └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘        │
 *   │        ▲ oldest                 ▲ current                   │
 *   │                                                             │
 *   │  Write index = sequenceId % capacity                        │
 *   │  Oldest available = max(1, currentSequence - capacity + 1)  │
 *   └─────────────────────────────────────────────────────────────┘
 * </pre>
 * 
 * <h3>Thread Safety</h3>
 * <ul>
 *   <li>{@link AtomicLong} for the sequence counter → atomic increment</li>
 *   <li>{@link AtomicReferenceArray} for the buffer → volatile read/write per slot</li>
 *   <li>Multiple concurrent writers are safe (each gets a unique sequence)</li>
 *   <li>Readers verify slot occupancy by checking sequenceId matches</li>
 * </ul>
 * 
 * <h3>Overflow Behavior</h3>
 * When the buffer is full, the oldest entries are silently overwritten.
 * If a SECONDARY falls too far behind (its last sequence is no longer in the buffer),
 * it must perform a full resync.
 * 
 * @since 1.9.0
 */
@Slf4j
@Component
public class ReplicationOpLog {
    
    private final AtomicReferenceArray<ReplicationOpLogEntry> buffer;
    private final int capacity;
    private final AtomicLong currentSequence = new AtomicLong(0);
    
    public ReplicationOpLog(KuberProperties properties) {
        this.capacity = properties.getReplication().getOplogCapacity();
        this.buffer = new AtomicReferenceArray<>(capacity);
        log.info("Replication OpLog initialized with capacity: {} entries", capacity);
    }
    
    /**
     * Append a mutation entry to the oplog.
     * Called by CacheService after every successful write operation.
     * 
     * @param entry the operation to journal
     * @return the assigned sequence number
     */
    public long append(ReplicationOpLogEntry entry) {
        long seq = currentSequence.incrementAndGet();
        entry.setSequenceId(seq);
        if (entry.getTimestamp() == null) {
            entry.setTimestamp(Instant.now());
        }
        buffer.set((int) (seq % capacity), entry);
        return seq;
    }
    
    /**
     * @return the most recently assigned sequence number (0 if no entries)
     */
    public long getCurrentSequence() {
        return currentSequence.get();
    }
    
    /**
     * @return the oldest sequence number still available in the buffer
     */
    public long getOldestAvailableSequence() {
        long current = currentSequence.get();
        if (current == 0) return 0;
        return Math.max(1, current - capacity + 1);
    }
    
    /**
     * Check if a given sequence number is still available in the buffer.
     * If false, the SECONDARY needs a full resync.
     * 
     * @param sequence the sequence to check
     * @return true if entries after this sequence can be served
     */
    public boolean isSequenceAvailable(long sequence) {
        if (sequence <= 0) return currentSequence.get() <= capacity;
        return sequence >= getOldestAvailableSequence() - 1;
    }
    
    /**
     * Retrieve oplog entries after a given sequence number.
     * 
     * @param afterSequence return entries with sequenceId > afterSequence
     * @param limit maximum number of entries to return
     * @return ordered list of oplog entries (may be empty)
     */
    public List<ReplicationOpLogEntry> getEntriesAfter(long afterSequence, int limit) {
        long current = currentSequence.get();
        if (current == 0 || afterSequence >= current) {
            return Collections.emptyList();
        }
        
        long oldest = getOldestAvailableSequence();
        long start = Math.max(afterSequence + 1, oldest);
        long end = Math.min(start + limit, current + 1);
        
        List<ReplicationOpLogEntry> entries = new ArrayList<>((int) (end - start));
        for (long seq = start; seq < end; seq++) {
            ReplicationOpLogEntry entry = buffer.get((int) (seq % capacity));
            // Verify this slot holds the expected sequence (not overwritten by a newer entry)
            if (entry != null && entry.getSequenceId() == seq) {
                entries.add(entry);
            }
        }
        return entries;
    }
    
    /**
     * Reset the oplog (used when this node transitions from SECONDARY to PRIMARY).
     * New PRIMARY starts with a fresh oplog.
     */
    public void reset() {
        log.info("Resetting replication oplog (previous sequence: {})", currentSequence.get());
        currentSequence.set(0);
        for (int i = 0; i < capacity; i++) {
            buffer.set(i, null);
        }
    }
    
    /**
     * @return number of entries currently in the buffer
     */
    public long size() {
        long current = currentSequence.get();
        if (current == 0) return 0;
        return Math.min(current, capacity);
    }
    
    /**
     * @return total capacity of the ring buffer
     */
    public int getCapacity() {
        return capacity;
    }
}
