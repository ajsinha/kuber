/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 */
package com.kuber.server.replication;

import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheRegion;
import com.kuber.core.util.JsonUtils;
import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.client.SimpleClientHttpRequestFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background sync service that runs on SECONDARY nodes.
 * 
 * <h3>Sync Protocol</h3>
 * <pre>
 *  SECONDARY                                 PRIMARY
 *     │                                         │
 *     │──── GET /api/replication/info ──────────►│  Phase 0: Discovery
 *     │◄─── { currentSequence: N } ─────────────│
 *     │                                         │
 *     │──── GET /api/replication/regions ───────►│  Phase 1: Region Sync
 *     │◄─── [{ name, description, ... }] ───────│
 *     │                                         │
 *     │──── GET /snapshot/{region}?offset=0 ───►│  Phase 2: Full Snapshot
 *     │◄─── { entries: [...], hasMore: true } ──│
 *     │──── GET /snapshot/{region}?offset=5000 ►│  (paginate until done)
 *     │◄─── { entries: [...], hasMore: false } ─│
 *     │     ... repeat for each region ...       │
 *     │                                         │
 *     │──── GET /oplog?afterSequence=N ────────►│  Phase 3: Catch-up
 *     │◄─── { entries: [...], status: OK } ─────│
 *     │                                         │
 *     │──── GET /oplog?afterSequence=M ────────►│  Phase 4: Continuous tail
 *     │◄─── { entries: [...] } ─────────────────│  (poll every syncIntervalMs)
 *     │     ... forever until role change ...    │
 * </pre>
 * 
 * <h3>Failure Handling</h3>
 * <ul>
 *   <li>Network errors: retry with exponential backoff (up to 30s)</li>
 *   <li>SEQUENCE_TOO_OLD: oplog wrapped → restart full resync</li>
 *   <li>PRIMARY role change: stop sync, await new election</li>
 * </ul>
 * 
 * @since 1.9.0
 */
@Slf4j
@Service
public class ReplicationSyncService {
    
    private final CacheService cacheService;
    private final KuberProperties properties;
    
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean synced = new AtomicBoolean(false);
    private final AtomicLong lastAppliedSequence = new AtomicLong(0);
    private final AtomicLong entriesReplicated = new AtomicLong(0);
    
    private Thread syncThread;
    private RestTemplate restTemplate;
    private String primaryBaseUrl;
    
    public ReplicationSyncService(CacheService cacheService, KuberProperties properties) {
        this.cacheService = cacheService;
        this.properties = properties;
    }
    
    // ==================== Lifecycle ====================
    
    /**
     * Start syncing from the PRIMARY node.
     * Called by ReplicationManager when this node becomes SECONDARY.
     * 
     * @param primaryHost PRIMARY's advertised hostname or IP
     * @param primaryHttpPort PRIMARY's HTTP port
     */
    public void startSync(String primaryHost, int primaryHttpPort) {
        if (running.get()) {
            log.warn("Sync is already running, stopping first...");
            stopSync();
        }
        
        this.primaryBaseUrl = "http://" + primaryHost + ":" + primaryHttpPort;
        
        // Create RestTemplate with configured timeouts
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(properties.getReplication().getConnectTimeoutMs());
        factory.setReadTimeout(properties.getReplication().getReadTimeoutMs());
        this.restTemplate = new RestTemplate(factory);
        
        running.set(true);
        synced.set(false);
        lastAppliedSequence.set(0);
        entriesReplicated.set(0);
        
        syncThread = new Thread(this::syncLoop, "kuber-replication-sync");
        syncThread.setDaemon(true);
        syncThread.start();
        
        log.info("Replication sync started → PRIMARY at {}", primaryBaseUrl);
    }
    
    /**
     * Stop the sync background thread.
     * Called when this node becomes PRIMARY or during shutdown.
     */
    public void stopSync() {
        running.set(false);
        if (syncThread != null) {
            syncThread.interrupt();
            try {
                syncThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            syncThread = null;
        }
        log.info("Replication sync stopped (last applied sequence: {})", lastAppliedSequence.get());
    }
    
    /**
     * @return true if this service is actively syncing
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * @return true if initial full sync completed and continuous tailing is active
     */
    public boolean isSynced() {
        return synced.get();
    }
    
    /**
     * @return the last oplog sequence applied from PRIMARY
     */
    public long getLastAppliedSequence() {
        return lastAppliedSequence.get();
    }
    
    /**
     * @return total number of entries replicated since sync started
     */
    public long getEntriesReplicated() {
        return entriesReplicated.get();
    }
    
    // ==================== Sync Loop ====================
    
    private void syncLoop() {
        int retryDelay = 1000; // Start with 1s, exponential backoff to 30s
        
        while (running.get()) {
            try {
                // Phase 1+2: Full Sync (regions + snapshot)
                if (!synced.get()) {
                    log.info("Starting full sync from PRIMARY at {}", primaryBaseUrl);
                    performFullSync();
                    synced.set(true);
                    retryDelay = 1000; // Reset backoff after success
                    log.info("Full sync completed. Transitioning to continuous replication...");
                }
                
                // Phase 3+4: Continuous oplog tail
                while (running.get() && synced.get()) {
                    int applied = pollOpLog();
                    retryDelay = 1000; // Reset backoff on success
                    
                    if (applied == 0) {
                        // No new entries — sleep before next poll
                        Thread.sleep(properties.getReplication().getSyncIntervalMs());
                    }
                    // If we got entries, poll again immediately (might have more)
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (SequenceTooOldException e) {
                log.warn("OpLog sequence too old — PRIMARY's oplog wrapped. Restarting full sync...");
                synced.set(false);
                lastAppliedSequence.set(0);
                retryDelay = 1000;
            } catch (Exception e) {
                if (!running.get()) break; // Clean shutdown
                
                log.error("Replication sync error (retry in {}ms): {}", retryDelay, e.getMessage());
                try {
                    Thread.sleep(retryDelay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
                retryDelay = Math.min(retryDelay * 2, 30000); // Exponential backoff, max 30s
            }
        }
        
        log.info("Replication sync loop exited");
    }
    
    // ==================== Full Sync ====================
    
    @SuppressWarnings("unchecked")
    private void performFullSync() throws Exception {
        // Step 1: Get PRIMARY info (note current sequence for later catch-up)
        Map<String, Object> info = callPrimary("/api/replication/info", Map.class);
        long snapshotSequence = ((Number) info.get("currentSequence")).longValue();
        log.info("Full sync: PRIMARY at sequence {}, {} regions", snapshotSequence, info.get("regionCount"));
        
        // Step 2: Sync regions
        List<Map<String, Object>> regions = callPrimary("/api/replication/regions", List.class);
        for (Map<String, Object> regionMap : regions) {
            syncRegion(regionMap);
        }
        
        // Step 3: Snapshot all entries per region
        for (Map<String, Object> regionMap : regions) {
            String regionName = (String) regionMap.get("name");
            snapshotRegionEntries(regionName);
        }
        
        // Step 4: Catch up with oplog from snapshot point
        log.info("Full sync: catching up oplog from sequence {} to current...", snapshotSequence);
        lastAppliedSequence.set(snapshotSequence);
        
        int totalCatchUp = 0;
        boolean hasMore = true;
        while (hasMore && running.get()) {
            int applied = pollOpLog();
            totalCatchUp += applied;
            hasMore = applied > 0;
        }
        log.info("Full sync: caught up with {} oplog entries", totalCatchUp);
    }
    
    /**
     * Create or update a region definition on this SECONDARY.
     */
    private void syncRegion(Map<String, Object> regionMap) {
        String name = (String) regionMap.get("name");
        String description = (String) regionMap.get("description");
        
        try {
            if (!cacheService.getRegionNames().contains(name)) {
                cacheService.applyReplicatedRegionCreate(name, description);
                log.info("Full sync: created region '{}'", name);
            }
        } catch (Exception e) {
            log.warn("Full sync: error syncing region '{}': {}", name, e.getMessage());
        }
    }
    
    /**
     * Paginate through all entries in a region and apply them locally.
     */
    @SuppressWarnings("unchecked")
    private void snapshotRegionEntries(String regionName) throws Exception {
        int offset = 0;
        int batchSize = properties.getReplication().getSyncBatchSize();
        int totalEntries = 0;
        boolean hasMore = true;
        
        while (hasMore && running.get()) {
            String url = String.format("/api/replication/snapshot/%s?offset=%d&limit=%d",
                    regionName, offset, batchSize);
            
            Map<String, Object> page = callPrimary(url, Map.class);
            List<Map<String, Object>> entries = (List<Map<String, Object>>) page.get("entries");
            
            if (entries != null) {
                for (Map<String, Object> entryMap : entries) {
                    applyEntryFromMap(regionName, entryMap);
                    totalEntries++;
                    entriesReplicated.incrementAndGet();
                }
            }
            
            hasMore = Boolean.TRUE.equals(page.get("hasMore"));
            offset += batchSize;
        }
        
        log.info("Full sync: snapshot region '{}' → {} entries", regionName, totalEntries);
    }
    
    // ==================== OpLog Tail ====================
    
    /**
     * Poll the PRIMARY's oplog for new entries and apply them.
     * 
     * @return number of entries applied
     * @throws SequenceTooOldException if oplog has wrapped past our last sequence
     */
    @SuppressWarnings("unchecked")
    private int pollOpLog() throws Exception {
        String url = String.format("/api/replication/oplog?afterSequence=%d&limit=%d",
                lastAppliedSequence.get(), properties.getReplication().getSyncBatchSize());
        
        Map<String, Object> result = callPrimary(url, Map.class);
        
        String status = (String) result.get("status");
        if ("SEQUENCE_TOO_OLD".equals(status)) {
            throw new SequenceTooOldException();
        }
        
        List<Map<String, Object>> entries = (List<Map<String, Object>>) result.get("entries");
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        
        int applied = 0;
        for (Map<String, Object> opEntry : entries) {
            applyOpLogEntry(opEntry);
            long seq = ((Number) opEntry.get("sequenceId")).longValue();
            lastAppliedSequence.set(seq);
            applied++;
            entriesReplicated.incrementAndGet();
        }
        
        if (applied > 0) {
            log.debug("Replication: applied {} oplog entries (sequence now at {})", 
                    applied, lastAppliedSequence.get());
        }
        
        return applied;
    }
    
    // ==================== Entry Application ====================
    
    /**
     * Apply a single oplog entry to the local cache.
     */
    @SuppressWarnings("unchecked")
    private void applyOpLogEntry(Map<String, Object> opEntry) {
        String opType = (String) opEntry.get("opType");
        String region = (String) opEntry.get("region");
        String key = (String) opEntry.get("key");
        
        try {
            switch (opType) {
                case "SET":
                    Map<String, Object> entryMap = (Map<String, Object>) opEntry.get("entry");
                    if (entryMap != null) {
                        applyEntryFromMap(region, entryMap);
                    }
                    break;
                    
                case "DELETE":
                    cacheService.applyReplicatedDelete(region, key);
                    break;
                    
                case "REGION_CREATE":
                    Map<String, Object> regionDef = (Map<String, Object>) opEntry.get("regionDef");
                    String desc = regionDef != null ? (String) regionDef.get("description") : null;
                    if (!cacheService.getRegionNames().contains(region)) {
                        cacheService.applyReplicatedRegionCreate(region, desc);
                    }
                    break;
                    
                case "REGION_DELETE":
                    cacheService.applyReplicatedRegionDelete(region);
                    break;
                    
                case "REGION_PURGE":
                    cacheService.applyReplicatedRegionPurge(region);
                    break;
                    
                default:
                    log.warn("Unknown oplog entry type: {}", opType);
            }
        } catch (Exception e) {
            log.warn("Failed to apply oplog entry (opType={}, region={}, key={}): {}", 
                    opType, region, key, e.getMessage());
        }
    }
    
    /**
     * Deserialize an entry from a Map and apply it to the local cache.
     */
    private void applyEntryFromMap(String region, Map<String, Object> entryMap) {
        String key = (String) entryMap.get("key");
        String valueType = (String) entryMap.get("valueType");
        String stringValue = (String) entryMap.get("stringValue");
        String jsonValueStr = (String) entryMap.get("jsonValue");
        
        Number ttlNum = (Number) entryMap.get("ttlSeconds");
        long ttlSeconds = ttlNum != null ? ttlNum.longValue() : -1;
        
        Number versionNum = (Number) entryMap.get("version");
        long version = versionNum != null ? versionNum.longValue() : 1;
        
        // Reconstruct CacheEntry
        CacheEntry.CacheEntryBuilder builder = CacheEntry.builder()
                .region(region)
                .key(key)
                .valueType(CacheEntry.ValueType.valueOf(valueType != null ? valueType : "STRING"))
                .stringValue(stringValue)
                .ttlSeconds(ttlSeconds)
                .version(version)
                .createdAt(parseInstant(entryMap.get("createdAt")))
                .updatedAt(parseInstant(entryMap.get("updatedAt")))
                .expiresAt(parseInstant(entryMap.get("expiresAt")));
        
        if (jsonValueStr != null && !jsonValueStr.isBlank()) {
            try {
                builder.jsonValue(JsonUtils.parse(jsonValueStr));
            } catch (Exception e) {
                // Fall back to string value
            }
        }
        
        String metadata = (String) entryMap.get("metadata");
        if (metadata != null) {
            builder.metadata(metadata);
        }
        
        CacheEntry entry = builder.build();
        
        // Skip if already expired
        if (entry.isExpired()) return;
        
        // Ensure region exists
        if (!cacheService.getRegionNames().contains(region)) {
            try {
                cacheService.applyReplicatedRegionCreate(region, null);
            } catch (Exception e) {
                // Region might already exist by now
            }
        }
        
        cacheService.applyReplicatedSet(region, key, entry);
    }
    
    // ==================== HTTP Client ====================
    
    @SuppressWarnings("unchecked")
    private <T> T callPrimary(String path, Class<T> responseType) {
        String url = primaryBaseUrl + path;
        
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        
        // Add auth token if configured
        String authToken = properties.getReplication().getAuthToken();
        if (authToken != null && !authToken.isBlank()) {
            headers.set("X-Replication-Token", authToken);
        }
        
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.GET, entity, responseType);
        
        if (!response.getStatusCode().is2xxSuccessful()) {
            throw new RuntimeException("PRIMARY returned " + response.getStatusCode() + " for " + path);
        }
        
        return response.getBody();
    }
    
    // ==================== Utilities ====================
    
    private Instant parseInstant(Object value) {
        if (value == null) return null;
        if (value instanceof String) {
            try {
                return Instant.parse((String) value);
            } catch (Exception e) {
                return null;
            }
        }
        if (value instanceof Number) {
            return Instant.ofEpochMilli(((Number) value).longValue());
        }
        return null;
    }
    
    /**
     * Get sync status for monitoring.
     */
    public Map<String, Object> getSyncStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("running", running.get());
        status.put("synced", synced.get());
        status.put("lastAppliedSequence", lastAppliedSequence.get());
        status.put("entriesReplicated", entriesReplicated.get());
        status.put("primaryUrl", primaryBaseUrl);
        return status;
    }
    
    /**
     * Sentinel exception indicating the oplog has wrapped and a full resync is needed.
     */
    private static class SequenceTooOldException extends Exception {
        SequenceTooOldException() {
            super("OpLog sequence too old - full resync required");
        }
    }
}
