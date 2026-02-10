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
package com.kuber.server.persistence;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.*;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheRegion;
import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Aerospike-based persistence store for Kuber cache data.
 * 
 * <p>Aerospike is a high-performance, distributed NoSQL database that provides:
 * <ul>
 *   <li>Sub-millisecond read/write latency</li>
 *   <li>Native TTL support per record (perfect for cache expiration)</li>
 *   <li>Horizontal scalability with automatic data distribution</li>
 *   <li>Strong consistency with immediate persistence</li>
 *   <li>Excellent batch operation performance</li>
 * </ul>
 * 
 * <h2>Data Model Mapping:</h2>
 * <pre>
 * Kuber Concept     → Aerospike Concept
 * ─────────────────────────────────────
 * Region            → Set (within configured Namespace)
 * Cache Key         → Record Key (Primary Key)
 * JSON Value        → "v" Bin (String - serialized JSON)
 * TTL               → Record TTL (native Aerospike feature)
 * Metadata          → "_regions" Set for region metadata
 * </pre>
 * 
 * <h2>Bin Structure:</h2>
 * <pre>
 * Record Bins:
 *   ├── "v"    : String (JSON value)
 *   ├── "ct"   : Long (created timestamp, epoch millis)
 *   └── TTL    : Native Aerospike record expiration
 * </pre>
 * 
 * @version 2.1.0
 * @since 1.9.0
 */
@Slf4j
public class AerospikePersistenceStore implements PersistenceStore {
    
    // Bin names
    private static final String BIN_VALUE = "v";
    private static final String BIN_CREATED = "ct";
    private static final String BIN_REGION_DESC = "desc";
    private static final String BIN_REGION_TTL = "ttl";
    
    // Special set for region metadata
    private static final String REGIONS_SET = "_kuber_regions";
    
    private final KuberProperties properties;
    private final KuberProperties.Aerospike config;
    
    private AerospikeClient client;
    private String namespace;
    private WritePolicy writePolicy;
    private WritePolicy writePolicyNoTtl;
    private Policy readPolicy;
    private BatchPolicy batchPolicy;
    private ScanPolicy scanPolicy;
    
    // Statistics
    private final AtomicLong readCount = new AtomicLong(0);
    private final AtomicLong writeCount = new AtomicLong(0);
    private final AtomicLong deleteCount = new AtomicLong(0);
    
    // Cache region info locally to avoid repeated lookups
    private final ConcurrentHashMap<String, CacheRegion> regionCache = new ConcurrentHashMap<>();
    
    public AerospikePersistenceStore(KuberProperties properties) {
        this.properties = properties;
        this.config = properties.getPersistence().getAerospike();
    }
    
    @Override
    public PersistenceType getType() {
        return PersistenceType.AEROSPIKE;
    }
    
    @Override
    public void initialize() {
        try {
            // Parse hosts
            Host[] hosts = parseHosts(config.getHosts());
            
            // Configure client policy
            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.timeout = config.getConnectionTimeoutMs();
            clientPolicy.maxConnsPerNode = config.getMaxConnsPerNode();
            clientPolicy.connPoolsPerNode = config.getConnPoolsPerNode();
            clientPolicy.failIfNotConnected = true;
            
            // Authentication (if configured)
            if (config.getUsername() != null && !config.getUsername().isEmpty()) {
                clientPolicy.user = config.getUsername();
                clientPolicy.password = config.getPassword();
            }
            
            // Create client
            client = new AerospikeClient(clientPolicy, hosts);
            namespace = config.getNamespace();
            
            // Configure write policy
            writePolicy = new WritePolicy();
            writePolicy.sendKey = config.isSendKey();
            writePolicy.socketTimeout = config.getSocketTimeoutMs();
            writePolicy.totalTimeout = config.getSocketTimeoutMs() * 2;
            writePolicy.commitLevel = CommitLevel.valueOf(config.getCommitLevel());
            writePolicy.recordExistsAction = RecordExistsAction.REPLACE;
            
            // Write policy without TTL changes (for updates that preserve TTL)
            writePolicyNoTtl = new WritePolicy(writePolicy);
            writePolicyNoTtl.expiration = -2; // Don't change TTL
            
            // Configure read policy
            readPolicy = new Policy();
            readPolicy.socketTimeout = config.getSocketTimeoutMs();
            readPolicy.totalTimeout = config.getSocketTimeoutMs() * 2;
            readPolicy.replica = Replica.valueOf(config.getReplica());
            
            // Configure batch policy
            batchPolicy = new BatchPolicy();
            batchPolicy.socketTimeout = config.getSocketTimeoutMs();
            batchPolicy.totalTimeout = config.getSocketTimeoutMs() * 3;
            batchPolicy.maxConcurrentThreads = 0; // Use default
            
            // Configure scan policy
            scanPolicy = new ScanPolicy();
            scanPolicy.concurrentNodes = true;
            scanPolicy.includeBinData = true;
            
            log.info("╔══════════════════════════════════════════════════════════════╗");
            log.info("║         AEROSPIKE PERSISTENCE STORE INITIALIZED               ║");
            log.info("╠══════════════════════════════════════════════════════════════╣");
            log.info("║  Hosts:      {}", String.format("%-48s║", config.getHosts()));
            log.info("║  Namespace:  {}", String.format("%-48s║", namespace));
            log.info("║  Send Key:   {}", String.format("%-48s║", config.isSendKey()));
            log.info("║  Commit:     {}", String.format("%-48s║", config.getCommitLevel()));
            log.info("║  Connected:  {}", String.format("%-48s║", client.isConnected()));
            log.info("╚══════════════════════════════════════════════════════════════╝");
            
        } catch (AerospikeException e) {
            log.error("Failed to initialize Aerospike persistence store: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize Aerospike persistence store", e);
        }
    }
    
    @Override
    public void shutdown() {
        if (client != null) {
            client.close();
            client = null;
            log.info("Aerospike persistence store shutdown (reads: {}, writes: {}, deletes: {})",
                    readCount.get(), writeCount.get(), deleteCount.get());
        }
    }
    
    @Override
    public void sync() {
        // Aerospike writes are synchronous by default when commitLevel is COMMIT_ALL
        // No explicit sync needed
    }
    
    @Override
    public boolean isAvailable() {
        return client != null && client.isConnected();
    }
    
    // ==================== Region Operations ====================
    
    @Override
    public void saveRegion(CacheRegion region) {
        if (!isAvailable()) return;
        
        Key key = new Key(namespace, REGIONS_SET, region.getName());
        
        Bin nameBin = new Bin("name", region.getName());
        Bin descBin = new Bin(BIN_REGION_DESC, region.getDescription() != null ? region.getDescription() : "");
        Bin ttlBin = new Bin(BIN_REGION_TTL, region.getDefaultTtlSeconds());
        Bin createdBin = new Bin(BIN_CREATED, region.getCreatedAt() != null ? 
                region.getCreatedAt().toEpochMilli() : System.currentTimeMillis());
        
        WritePolicy wp = new WritePolicy(writePolicy);
        wp.expiration = -1; // Never expire region metadata
        
        client.put(wp, key, nameBin, descBin, ttlBin, createdBin);
        regionCache.put(region.getName(), region);
        
        writeCount.incrementAndGet();
        log.debug("Saved region: {}", region.getName());
    }
    
    @Override
    public List<CacheRegion> loadAllRegions() {
        if (!isAvailable()) return Collections.emptyList();
        
        List<CacheRegion> regions = new ArrayList<>();
        
        try {
            client.scanAll(scanPolicy, namespace, REGIONS_SET, (key, record) -> {
                CacheRegion region = recordToRegion(record);
                if (region != null) {
                    regions.add(region);
                    regionCache.put(region.getName(), region);
                }
            });
        } catch (AerospikeException e) {
            log.warn("Error scanning regions: {}", e.getMessage());
        }
        
        readCount.incrementAndGet();
        log.debug("Loaded {} regions from Aerospike", regions.size());
        return regions;
    }
    
    @Override
    public CacheRegion loadRegion(String name) {
        if (!isAvailable()) return null;
        
        // Check cache first
        CacheRegion cached = regionCache.get(name);
        if (cached != null) return cached;
        
        Key key = new Key(namespace, REGIONS_SET, name);
        Record record = client.get(readPolicy, key);
        
        readCount.incrementAndGet();
        
        if (record == null) return null;
        
        CacheRegion region = recordToRegion(record);
        if (region != null) {
            regionCache.put(name, region);
        }
        return region;
    }
    
    @Override
    public void deleteRegion(String name) {
        if (!isAvailable()) return;
        
        // Delete region metadata
        Key key = new Key(namespace, REGIONS_SET, name);
        client.delete(writePolicy, key);
        regionCache.remove(name);
        
        // Truncate the set (delete all entries in region)
        try {
            client.truncate(null, namespace, name, null);
            log.info("Deleted region and truncated set: {}", name);
        } catch (AerospikeException e) {
            // Truncate may fail on community edition, fall back to scan+delete
            log.warn("Truncate not available, using scan+delete for region: {}", name);
            scanAndDelete(name);
        }
        
        deleteCount.incrementAndGet();
    }
    
    @Override
    public void purgeRegion(String name) {
        if (!isAvailable()) return;
        
        try {
            client.truncate(null, namespace, name, null);
            log.info("Purged region (truncated set): {}", name);
        } catch (AerospikeException e) {
            log.warn("Truncate not available, using scan+delete for region: {}", name);
            scanAndDelete(name);
        }
        
        deleteCount.incrementAndGet();
    }
    
    private void scanAndDelete(String setName) {
        List<Key> keysToDelete = new ArrayList<>();
        
        client.scanAll(scanPolicy, namespace, setName, (key, record) -> {
            keysToDelete.add(key);
        });
        
        for (Key key : keysToDelete) {
            client.delete(writePolicy, key);
        }
        
        log.debug("Deleted {} entries from set: {}", keysToDelete.size(), setName);
    }
    
    // ==================== Entry Operations ====================
    
    @Override
    public void saveEntry(CacheEntry entry) {
        if (!isAvailable() || entry == null) return;
        
        Key key = new Key(namespace, entry.getRegion(), entry.getKey());
        
        Bin valueBin = new Bin(BIN_VALUE, entry.getStringValue());
        Bin createdBin = new Bin(BIN_CREATED, entry.getCreatedAt() != null ? 
                entry.getCreatedAt().toEpochMilli() : System.currentTimeMillis());
        
        WritePolicy wp = new WritePolicy(writePolicy);
        
        // Set TTL if specified (> 0 means TTL is set, -1 means no expiration)
        if (entry.getTtlSeconds() > 0) {
            wp.expiration = (int) entry.getTtlSeconds();
        } else {
            // Check region default TTL
            CacheRegion region = regionCache.get(entry.getRegion());
            if (region != null && region.getDefaultTtlSeconds() > 0) {
                wp.expiration = (int) region.getDefaultTtlSeconds();
            } else {
                wp.expiration = config.getDefaultTtl();
            }
        }
        
        client.put(wp, key, valueBin, createdBin);
        writeCount.incrementAndGet();
    }
    
    @Override
    public CompletableFuture<Void> saveEntryAsync(CacheEntry entry) {
        return CompletableFuture.runAsync(() -> saveEntry(entry));
    }
    
    @Override
    public void saveEntries(List<CacheEntry> entries) {
        if (!isAvailable() || entries == null || entries.isEmpty()) return;
        
        // Group entries by region for efficient batch writes
        Map<String, List<CacheEntry>> byRegion = new HashMap<>();
        for (CacheEntry entry : entries) {
            byRegion.computeIfAbsent(entry.getRegion(), k -> new ArrayList<>()).add(entry);
        }
        
        for (Map.Entry<String, List<CacheEntry>> regionEntries : byRegion.entrySet()) {
            String region = regionEntries.getKey();
            List<CacheEntry> regionList = regionEntries.getValue();
            
            // Get region default TTL
            CacheRegion cachedRegion = regionCache.get(region);
            int defaultTtl = (cachedRegion != null && cachedRegion.getDefaultTtlSeconds() > 0) ?
                    (int) cachedRegion.getDefaultTtlSeconds() : config.getDefaultTtl();
            
            // Batch write
            for (CacheEntry entry : regionList) {
                Key key = new Key(namespace, region, entry.getKey());
                
                Bin valueBin = new Bin(BIN_VALUE, entry.getStringValue());
                Bin createdBin = new Bin(BIN_CREATED, entry.getCreatedAt() != null ?
                        entry.getCreatedAt().toEpochMilli() : System.currentTimeMillis());
                
                WritePolicy wp = new WritePolicy(writePolicy);
                if (entry.getTtlSeconds() > 0) {
                    wp.expiration = (int) entry.getTtlSeconds();
                } else {
                    wp.expiration = defaultTtl;
                }
                
                client.put(wp, key, valueBin, createdBin);
            }
            
            writeCount.addAndGet(regionList.size());
        }
        
        log.debug("Batch saved {} entries", entries.size());
    }
    
    @Override
    public CacheEntry loadEntry(String region, String key) {
        return get(region, key);
    }
    
    @Override
    public Map<String, CacheEntry> loadEntriesByKeys(String region, List<String> keys) {
        if (!isAvailable() || keys == null || keys.isEmpty()) return Collections.emptyMap();
        
        Key[] asKeys = keys.stream()
                .map(k -> new Key(namespace, region, k))
                .toArray(Key[]::new);
        
        Record[] records = client.get(batchPolicy, asKeys);
        
        Map<String, CacheEntry> result = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            if (records[i] != null) {
                CacheEntry entry = recordToEntry(region, keys.get(i), records[i]);
                if (entry != null) {
                    result.put(keys.get(i), entry);
                }
            }
        }
        
        readCount.incrementAndGet();
        return result;
    }
    
    @Override
    public List<CacheEntry> loadEntries(String region, int limit) {
        if (!isAvailable()) return Collections.emptyList();
        
        List<CacheEntry> entries = new ArrayList<>();
        AtomicLong count = new AtomicLong(0);
        
        ScanPolicy sp = new ScanPolicy(scanPolicy);
        sp.maxRecords = limit > 0 ? limit : 0;
        
        try {
            client.scanAll(sp, namespace, region, (key, record) -> {
                if (limit <= 0 || count.get() < limit) {
                    CacheEntry entry = recordToEntry(region, key.userKey.toString(), record);
                    if (entry != null) {
                        entries.add(entry);
                        count.incrementAndGet();
                    }
                }
            });
        } catch (AerospikeException e) {
            log.warn("Error scanning entries in region {}: {}", region, e.getMessage());
        }
        
        readCount.incrementAndGet();
        return entries;
    }
    
    @Override
    public void deleteEntry(String region, String key) {
        if (!isAvailable()) return;
        
        Key asKey = new Key(namespace, region, key);
        client.delete(writePolicy, asKey);
        deleteCount.incrementAndGet();
    }
    
    @Override
    public void deleteEntries(String region, List<String> keys) {
        if (!isAvailable() || keys == null || keys.isEmpty()) return;
        
        for (String key : keys) {
            Key asKey = new Key(namespace, region, key);
            client.delete(writePolicy, asKey);
        }
        
        deleteCount.addAndGet(keys.size());
    }
    
    @Override
    public long countEntries(String region) {
        if (!isAvailable()) return 0;
        
        AtomicLong count = new AtomicLong(0);
        
        ScanPolicy sp = new ScanPolicy(scanPolicy);
        sp.includeBinData = false; // Don't need data, just count
        
        try {
            client.scanAll(sp, namespace, region, (key, record) -> count.incrementAndGet());
        } catch (AerospikeException e) {
            log.warn("Error counting entries in region {}: {}", region, e.getMessage());
        }
        
        readCount.incrementAndGet();
        return count.get();
    }
    
    @Override
    public long forEachEntry(String region, Consumer<CacheEntry> consumer) {
        if (!isAvailable()) return 0;
        
        AtomicLong count = new AtomicLong(0);
        
        try {
            client.scanAll(scanPolicy, namespace, region, (key, record) -> {
                CacheEntry entry = recordToEntry(region, key.userKey.toString(), record);
                if (entry != null) {
                    consumer.accept(entry);
                    count.incrementAndGet();
                }
            });
        } catch (AerospikeException e) {
            log.warn("Error iterating entries in region {}: {}", region, e.getMessage());
        }
        
        readCount.incrementAndGet();
        return count.get();
    }
    
    @Override
    public List<String> getKeys(String region, String pattern, int limit) {
        if (!isAvailable()) return Collections.emptyList();
        
        List<String> keys = new ArrayList<>();
        AtomicLong count = new AtomicLong(0);
        
        ScanPolicy sp = new ScanPolicy(scanPolicy);
        sp.includeBinData = false;
        sp.maxRecords = limit > 0 ? limit * 2 : 0; // Get extra in case of pattern filtering
        
        try {
            client.scanAll(sp, namespace, region, (key, record) -> {
                String keyStr = key.userKey.toString();
                
                if (pattern == null || pattern.equals("*") || matchesPattern(keyStr, pattern)) {
                    if (limit <= 0 || count.get() < limit) {
                        keys.add(keyStr);
                        count.incrementAndGet();
                    }
                }
            });
        } catch (AerospikeException e) {
            log.warn("Error getting keys from region {}: {}", region, e.getMessage());
        }
        
        readCount.incrementAndGet();
        return keys;
    }
    
    @Override
    public CacheEntry get(String region, String key) {
        if (!isAvailable()) return null;
        
        Key asKey = new Key(namespace, region, key);
        Record record = client.get(readPolicy, asKey);
        
        readCount.incrementAndGet();
        
        if (record == null) return null;
        
        return recordToEntry(region, key, record);
    }
    
    @Override
    public long deleteExpiredEntries(String region) {
        // Aerospike handles TTL automatically - expired records are not returned
        // This is a no-op for Aerospike
        return 0;
    }
    
    @Override
    public long deleteAllExpiredEntries() {
        // Aerospike handles TTL automatically - expired records are not returned
        // This is a no-op for Aerospike
        return 0;
    }
    
    @Override
    public long countNonExpiredEntries(String region) {
        // In Aerospike, expired entries are automatically filtered
        return countEntries(region);
    }
    
    @Override
    public List<String> getNonExpiredKeys(String region, String pattern, int limit) {
        // In Aerospike, expired entries are automatically filtered
        return getKeys(region, pattern, limit);
    }
    
    @Override
    public boolean supportsNativeJsonQuery() {
        // String bin storage doesn't support native JSON queries
        // Would need Map bin storage + Expressions for this
        return false;
    }
    
    // ==================== Helper Methods ====================
    
    private Host[] parseHosts(String hostsStr) {
        String[] hostParts = hostsStr.split(",");
        Host[] hosts = new Host[hostParts.length];
        
        for (int i = 0; i < hostParts.length; i++) {
            String[] parts = hostParts[i].trim().split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 3000;
            hosts[i] = new Host(host, port);
        }
        
        return hosts;
    }
    
    private CacheRegion recordToRegion(Record record) {
        if (record == null) return null;
        
        try {
            String name = record.getString("name");
            if (name == null) return null;
            
            CacheRegion region = new CacheRegion();
            region.setName(name);
            region.setDescription(record.getString(BIN_REGION_DESC));
            
            Long ttl = record.getLong(BIN_REGION_TTL);
            region.setDefaultTtlSeconds(ttl != null ? ttl : -1);
            
            Long created = record.getLong(BIN_CREATED);
            region.setCreatedAt(created != null ? Instant.ofEpochMilli(created) : Instant.now());
            
            return region;
        } catch (Exception e) {
            log.warn("Error converting record to region: {}", e.getMessage());
            return null;
        }
    }
    
    private CacheEntry recordToEntry(String region, String key, Record record) {
        if (record == null) return null;
        
        try {
            String value = record.getString(BIN_VALUE);
            if (value == null) return null;
            
            CacheEntry entry = new CacheEntry();
            entry.setRegion(region);
            entry.setKey(key);
            entry.setStringValue(value);
            
            Long created = record.getLong(BIN_CREATED);
            entry.setCreatedAt(created != null ? Instant.ofEpochMilli(created) : Instant.now());
            
            // Get TTL from record metadata
            int ttl = record.getTimeToLive();
            if (ttl > 0) {
                entry.setTtlSeconds((long) ttl);
                entry.setExpiresAt(Instant.now().plusSeconds(ttl));
            }
            
            return entry;
        } catch (Exception e) {
            log.warn("Error converting record to entry: {}", e.getMessage());
            return null;
        }
    }
    
    private boolean matchesPattern(String str, String pattern) {
        if (pattern == null || pattern.equals("*")) return true;
        
        // Convert glob pattern to regex
        String regex = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        
        return str.matches(regex);
    }
}
