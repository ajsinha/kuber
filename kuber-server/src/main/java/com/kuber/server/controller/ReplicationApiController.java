/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Patent Pending
 */
package com.kuber.server.controller;

import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheRegion;
import com.kuber.core.util.JsonUtils;
import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.persistence.PersistenceStore;
import com.kuber.server.replication.ReplicationManager;
import com.kuber.server.replication.ReplicationOpLog;
import com.kuber.server.replication.ReplicationOpLogEntry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import jakarta.servlet.http.HttpServletRequest;
import java.util.*;
import java.util.stream.Collectors;

/**
 * REST API endpoints for replication protocol.
 * 
 * <p>These endpoints are called by SECONDARY nodes to:
 * <ol>
 *   <li>Discover PRIMARY state ({@code /api/replication/info})</li>
 *   <li>Fetch all region definitions ({@code /api/replication/regions})</li>
 *   <li>Stream entry snapshots per region ({@code /api/replication/snapshot/{region}})</li>
 *   <li>Tail the oplog for incremental changes ({@code /api/replication/oplog})</li>
 * </ol>
 * 
 * <p>All endpoints are protected by a shared replication auth token.
 * Only PRIMARY nodes should serve these endpoints; SECONDARY nodes return 503.
 * 
 * @since 1.8.3
 */
@Slf4j
@RestController
@RequestMapping("/api/replication")
public class ReplicationApiController {
    
    @Autowired
    private CacheService cacheService;
    
    @Autowired
    private PersistenceStore persistenceStore;
    
    @Autowired
    private ReplicationOpLog opLog;
    
    @Autowired
    private KuberProperties properties;
    
    @Autowired(required = false)
    private ReplicationManager replicationManager;
    
    // ==================== Info ====================
    
    /**
     * Returns PRIMARY node metadata and oplog state.
     * SECONDARY uses this to discover the PRIMARY's current sequence before syncing.
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> getInfo(HttpServletRequest request) {
        validateToken(request);
        ensurePrimary();
        
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("nodeId", properties.getNodeId());
        info.put("isPrimary", true);
        info.put("currentSequence", opLog.getCurrentSequence());
        info.put("oldestSequence", opLog.getOldestAvailableSequence());
        info.put("oplogSize", opLog.size());
        info.put("oplogCapacity", opLog.getCapacity());
        info.put("regionCount", cacheService.getRegionNames().size());
        info.put("version", properties.getVersion());
        
        return ResponseEntity.ok(info);
    }
    
    // ==================== Regions ====================
    
    /**
     * Returns all region definitions.
     * SECONDARY creates any missing regions during full sync.
     */
    @GetMapping("/regions")
    public ResponseEntity<List<Map<String, Object>>> getRegions(HttpServletRequest request) {
        validateToken(request);
        ensurePrimary();
        
        List<Map<String, Object>> regions = new ArrayList<>();
        for (String name : cacheService.getRegionNames()) {
            CacheRegion region = cacheService.getRegion(name);
            if (region != null) {
                Map<String, Object> r = new LinkedHashMap<>();
                r.put("name", region.getName());
                r.put("description", region.getDescription());
                r.put("captive", region.isCaptive());
                r.put("maxEntries", region.getMaxEntries());
                r.put("defaultTtlSeconds", region.getDefaultTtlSeconds());
                r.put("entryCount", region.getEntryCount());
                r.put("createdAt", region.getCreatedAt());
                r.put("updatedAt", region.getUpdatedAt());
                r.put("createdBy", region.getCreatedBy());
                r.put("enabled", region.isEnabled());
                r.put("collectionName", region.getCollectionName());
                regions.add(r);
            }
        }
        
        log.debug("Replication: serving {} regions to secondary", regions.size());
        return ResponseEntity.ok(regions);
    }
    
    // ==================== Snapshot (Full Sync) ====================
    
    /**
     * Paginated snapshot of all non-expired entries in a region.
     * SECONDARY iterates through pages until hasMore=false.
     * 
     * @param region region name
     * @param offset starting offset (0-based)
     * @param limit max entries per page (capped at 10000)
     */
    @GetMapping("/snapshot/{region}")
    public ResponseEntity<Map<String, Object>> getSnapshot(
            @PathVariable String region,
            @RequestParam(defaultValue = "0") int offset,
            @RequestParam(defaultValue = "5000") int limit,
            HttpServletRequest request) {
        
        validateToken(request);
        ensurePrimary();
        
        limit = Math.min(limit, 10000); // Safety cap
        
        // Use persistence store to get entries (streaming-friendly, not limited by memory cache)
        List<CacheEntry> allEntries = persistenceStore.loadEntries(region, offset + limit + 1);
        
        // Apply offset and limit
        int start = Math.min(offset, allEntries.size());
        int end = Math.min(offset + limit, allEntries.size());
        List<CacheEntry> page = allEntries.subList(start, end);
        
        // Filter out expired entries
        List<Map<String, Object>> serialized = page.stream()
                .filter(e -> !e.isExpired())
                .map(this::serializeEntry)
                .collect(Collectors.toList());
        
        boolean hasMore = (offset + limit) < allEntries.size();
        
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("region", region);
        result.put("entries", serialized);
        result.put("offset", offset);
        result.put("count", serialized.size());
        result.put("hasMore", hasMore);
        
        log.debug("Replication: snapshot region '{}' offset={} count={} hasMore={}", 
                region, offset, serialized.size(), hasMore);
        
        return ResponseEntity.ok(result);
    }
    
    // ==================== OpLog (Delta Sync) ====================
    
    /**
     * Returns oplog entries after a given sequence number.
     * 
     * <p>If the requested sequence has fallen off the circular buffer,
     * returns status=SEQUENCE_TOO_OLD, signaling the SECONDARY to perform a full resync.
     * 
     * @param afterSequence return entries with sequenceId > afterSequence
     * @param limit max entries to return (capped at 10000)
     */
    @GetMapping("/oplog")
    public ResponseEntity<Map<String, Object>> getOpLog(
            @RequestParam(defaultValue = "0") long afterSequence,
            @RequestParam(defaultValue = "5000") int limit,
            HttpServletRequest request) {
        
        validateToken(request);
        ensurePrimary();
        
        limit = Math.min(limit, 10000);
        
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("currentSequence", opLog.getCurrentSequence());
        result.put("oldestSequence", opLog.getOldestAvailableSequence());
        
        // Check if the requested sequence is still in the buffer
        if (afterSequence > 0 && !opLog.isSequenceAvailable(afterSequence)) {
            result.put("status", "SEQUENCE_TOO_OLD");
            result.put("entries", Collections.emptyList());
            result.put("count", 0);
            result.put("hasMore", false);
            
            log.warn("Replication: secondary requested sequence {} but oldest is {} - full resync needed",
                    afterSequence, opLog.getOldestAvailableSequence());
            
            return ResponseEntity.ok(result);
        }
        
        List<ReplicationOpLogEntry> entries = opLog.getEntriesAfter(afterSequence, limit);
        
        List<Map<String, Object>> serialized = entries.stream()
                .map(this::serializeOpLogEntry)
                .collect(Collectors.toList());
        
        // Check if there are more entries beyond what we returned
        boolean hasMore = !entries.isEmpty() && 
                entries.get(entries.size() - 1).getSequenceId() < opLog.getCurrentSequence();
        
        result.put("status", "OK");
        result.put("entries", serialized);
        result.put("count", serialized.size());
        result.put("hasMore", hasMore);
        
        if (!entries.isEmpty()) {
            log.debug("Replication: serving {} oplog entries (seq {}-{}) to secondary",
                    entries.size(), entries.get(0).getSequenceId(), 
                    entries.get(entries.size() - 1).getSequenceId());
        }
        
        return ResponseEntity.ok(result);
    }
    
    // ==================== Helpers ====================
    
    /**
     * Validate the shared replication auth token.
     */
    private void validateToken(HttpServletRequest request) {
        String expected = properties.getReplication().getAuthToken();
        if (expected != null && !expected.isBlank()) {
            String provided = request.getHeader("X-Replication-Token");
            if (!expected.equals(provided)) {
                throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid replication token");
            }
        }
    }
    
    /**
     * Ensure this node is the PRIMARY. SECONDARY nodes should not serve replication data.
     */
    private void ensurePrimary() {
        if (replicationManager != null && !replicationManager.isPrimary()) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, 
                    "This node is not the PRIMARY - replication endpoints are only available on PRIMARY");
        }
    }
    
    /**
     * Serialize a CacheEntry to a Map for JSON transport.
     */
    private Map<String, Object> serializeEntry(CacheEntry entry) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("region", entry.getRegion());
        map.put("key", entry.getKey());
        map.put("valueType", entry.getValueType() != null ? entry.getValueType().name() : "STRING");
        map.put("stringValue", entry.getStringValue());
        map.put("ttlSeconds", entry.getTtlSeconds());
        map.put("createdAt", entry.getCreatedAt());
        map.put("updatedAt", entry.getUpdatedAt());
        map.put("expiresAt", entry.getExpiresAt());
        map.put("version", entry.getVersion());
        map.put("metadata", entry.getMetadata());
        
        if (entry.getJsonValue() != null) {
            try {
                map.put("jsonValue", JsonUtils.toJson(entry.getJsonValue()));
            } catch (Exception e) {
                map.put("jsonValue", null);
            }
        }
        
        return map;
    }
    
    /**
     * Serialize an OpLogEntry to a Map for JSON transport.
     */
    private Map<String, Object> serializeOpLogEntry(ReplicationOpLogEntry entry) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("sequenceId", entry.getSequenceId());
        map.put("timestamp", entry.getTimestamp());
        map.put("opType", entry.getOpType().name());
        map.put("region", entry.getRegion());
        map.put("key", entry.getKey());
        
        if (entry.getEntry() != null) {
            map.put("entry", serializeEntry(entry.getEntry()));
        }
        
        if (entry.getRegionDef() != null) {
            CacheRegion r = entry.getRegionDef();
            Map<String, Object> regionMap = new LinkedHashMap<>();
            regionMap.put("name", r.getName());
            regionMap.put("description", r.getDescription());
            regionMap.put("captive", r.isCaptive());
            regionMap.put("maxEntries", r.getMaxEntries());
            regionMap.put("defaultTtlSeconds", r.getDefaultTtlSeconds());
            regionMap.put("createdAt", r.getCreatedAt());
            regionMap.put("updatedAt", r.getUpdatedAt());
            regionMap.put("createdBy", r.getCreatedBy());
            regionMap.put("enabled", r.isEnabled());
            regionMap.put("collectionName", r.getCollectionName());
            map.put("regionDef", regionMap);
        }
        
        return map;
    }
}
