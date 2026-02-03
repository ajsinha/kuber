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

import com.kuber.core.constants.KuberConstants;
import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheRegion;
import com.kuber.core.util.JsonUtils;
import com.kuber.server.config.KuberProperties;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * MongoDB implementation of PersistenceStore.
 */
@Slf4j
public class MongoPersistenceStore extends AbstractPersistenceStore {
    
    private final MongoDatabase database;
    private final KuberProperties properties;
    
    // Guard against double shutdown
    private volatile boolean alreadyShutdown = false;
    
    public MongoPersistenceStore(MongoDatabase database, KuberProperties properties) {
        this.database = database;
        this.properties = properties;
    }
    
    @Override
    public PersistenceType getType() {
        return PersistenceType.MONGODB;
    }
    
    @Override
    public void initialize() {
        log.info("Initializing MongoDB persistence store...");
        
        try {
            // Create indexes for system collections
            createIndexes(KuberConstants.MONGO_REGIONS_COLLECTION,
                    Indexes.ascending("name"));
            
            // Configure batched async persistence (v1.6.2)
            int batchSize = properties.getCache().getPersistenceBatchSize();
            int flushIntervalMs = properties.getCache().getPersistenceIntervalMs();
            configureBatching(batchSize, flushIntervalMs);
            
            available = true;
            log.info("MongoDB persistence store initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize MongoDB persistence store: {}", e.getMessage(), e);
            available = false;
        }
    }
    
    @Override
    public void shutdown() {
        // Guard against double shutdown
        if (alreadyShutdown) {
            log.debug("MongoDB shutdown already completed - skipping duplicate shutdown call");
            return;
        }
        alreadyShutdown = true;
        
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  MONGODB GRACEFUL SHUTDOWN INITIATED                                ║");
        log.info("╚════════════════════════════════════════════════════════════════════╝");
        
        // Mark as unavailable to prevent new operations
        available = false;
        
        // Step 1: Shutdown async save executor FIRST and wait for all pending saves
        log.info("Step 1: Shutting down async save executor...");
        shutdownAsyncExecutor();
        
        // Step 2: Give any remaining in-flight operations time to complete
        try {
            log.info("Step 2: Waiting for in-flight operations to complete...");
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        // MongoDB client lifecycle is managed by Spring/MongoConfig
        // No explicit close needed here as MongoConfig handles it via @PreDestroy
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  MONGODB SHUTDOWN COMPLETE                                          ║");
        log.info("║  Note: MongoClient is managed by Spring and will close separately   ║");
        log.info("╚════════════════════════════════════════════════════════════════════╝");
    }
    
    /**
     * Sync is a no-op for MongoDB as it handles durability based on write concern.
     * With WriteConcern.ACKNOWLEDGED (default), writes are durable once acknowledged.
     */
    @Override
    public void sync() {
        log.debug("MongoDB sync called - no action needed (write concern handles durability)");
        // MongoDB handles durability via write concern settings
    }
    
    private void createIndexes(String collectionName, Bson... indexes) {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        for (Bson index : indexes) {
            try {
                collection.createIndex(index);
            } catch (Exception e) {
                log.debug("Index may already exist: {}", e.getMessage());
            }
        }
    }
    
    // ==================== Region Operations ====================
    
    @Override
    public void saveRegion(CacheRegion region) {
        log.info("Saving region '{}' to MongoDB collection '{}'", region.getName(), KuberConstants.MONGO_REGIONS_COLLECTION);
        
        MongoCollection<Document> collection = database.getCollection(KuberConstants.MONGO_REGIONS_COLLECTION);
        
        Document doc = new Document()
                .append("name", region.getName())
                .append("description", region.getDescription())
                .append("captive", region.isCaptive())
                .append("maxEntries", region.getMaxEntries())
                .append("defaultTtlSeconds", region.getDefaultTtlSeconds())
                .append("entryCount", region.getEntryCount())
                .append("createdAt", region.getCreatedAt())
                .append("updatedAt", region.getUpdatedAt())
                .append("createdBy", region.getCreatedBy())
                .append("enabled", region.isEnabled())
                .append("collectionName", region.getCollectionName());
        
        collection.replaceOne(
                Filters.eq("name", region.getName()),
                doc,
                new ReplaceOptions().upsert(true)
        );
        
        log.info("Successfully saved region '{}' to MongoDB", region.getName());
        
        // Create collection for region if it doesn't exist
        ensureRegionCollection(region.getCollectionName());
    }
    
    private void ensureRegionCollection(String collectionName) {
        try {
            database.createCollection(collectionName);
        } catch (Exception e) {
            // Collection may already exist
        }
        
        MongoCollection<Document> collection = database.getCollection(collectionName);
        try {
            collection.createIndex(Indexes.ascending("key"));
            collection.createIndex(Indexes.ascending("expiresAt"));
        } catch (Exception e) {
            // Indexes may already exist
        }
    }
    
    @Override
    public List<CacheRegion> loadAllRegions() {
        log.info("Loading all regions from MongoDB collection '{}'", KuberConstants.MONGO_REGIONS_COLLECTION);
        
        MongoCollection<Document> collection = database.getCollection(KuberConstants.MONGO_REGIONS_COLLECTION);
        List<CacheRegion> regions = new ArrayList<>();
        
        for (Document doc : collection.find()) {
            CacheRegion region = documentToRegion(doc);
            log.info("Loaded region '{}' from MongoDB", region.getName());
            regions.add(region);
        }
        
        log.info("Loaded {} regions from MongoDB", regions.size());
        return regions;
    }
    
    @Override
    public CacheRegion loadRegion(String name) {
        MongoCollection<Document> collection = database.getCollection(KuberConstants.MONGO_REGIONS_COLLECTION);
        Document doc = collection.find(Filters.eq("name", name)).first();
        return doc != null ? documentToRegion(doc) : null;
    }
    
    @Override
    public void deleteRegion(String name) {
        // Delete region metadata
        MongoCollection<Document> collection = database.getCollection(KuberConstants.MONGO_REGIONS_COLLECTION);
        Document regionDoc = collection.findOneAndDelete(Filters.eq("name", name));
        
        // Drop the region's collection
        if (regionDoc != null) {
            String collectionName = regionDoc.getString("collectionName");
            if (collectionName == null) {
                collectionName = getCollectionName(name);
            }
            try {
                database.getCollection(collectionName).drop();
            } catch (Exception e) {
                log.warn("Failed to drop collection {}: {}", collectionName, e.getMessage());
            }
        }
    }
    
    @Override
    public void purgeRegion(String name) {
        CacheRegion region = loadRegion(name);
        if (region != null) {
            MongoCollection<Document> collection = database.getCollection(region.getCollectionName());
            collection.deleteMany(new Document());
        }
    }
    
    private CacheRegion documentToRegion(Document doc) {
        return CacheRegion.builder()
                .name(doc.getString("name"))
                .description(doc.getString("description"))
                .captive(doc.getBoolean("captive", false))
                .maxEntries(doc.getLong("maxEntries") != null ? doc.getLong("maxEntries") : -1)
                .defaultTtlSeconds(doc.getLong("defaultTtlSeconds") != null ? doc.getLong("defaultTtlSeconds") : -1)
                .entryCount(doc.getLong("entryCount") != null ? doc.getLong("entryCount") : 0)
                .createdAt(toInstant(doc.get("createdAt")))
                .updatedAt(toInstant(doc.get("updatedAt")))
                .createdBy(doc.getString("createdBy"))
                .enabled(doc.getBoolean("enabled", true))
                .collectionName(doc.getString("collectionName"))
                .build();
    }
    
    // ==================== Entry Operations ====================
    
    @Override
    public void saveEntry(CacheEntry entry) {
        String collectionName = getCollectionName(entry.getRegion());
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        Document doc = entryToDocument(entry);
        
        collection.replaceOne(
                Filters.eq("key", entry.getKey()),
                doc,
                new ReplaceOptions().upsert(true)
        );
    }
    
    @Override
    public void saveEntries(List<CacheEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        
        // Group by region
        entries.stream()
                .collect(Collectors.groupingBy(CacheEntry::getRegion))
                .forEach((region, regionEntries) -> {
                    String collectionName = getCollectionName(region);
                    MongoCollection<Document> collection = database.getCollection(collectionName);
                    
                    List<WriteModel<Document>> writes = new ArrayList<>();
                    for (CacheEntry entry : regionEntries) {
                        writes.add(new ReplaceOneModel<>(
                                Filters.eq("key", entry.getKey()),
                                entryToDocument(entry),
                                new ReplaceOptions().upsert(true)
                        ));
                    }
                    
                    if (!writes.isEmpty()) {
                        collection.bulkWrite(writes, new BulkWriteOptions().ordered(false));
                    }
                });
    }
    
    @Override
    public CacheEntry loadEntry(String region, String key) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        Document doc = collection.find(Filters.eq("key", key)).first();
        return doc != null ? documentToEntry(doc, region) : null;
    }
    
    @Override
    public java.util.Map<String, CacheEntry> loadEntriesByKeys(String region, List<String> keys) {
        java.util.Map<String, CacheEntry> result = new java.util.HashMap<>();
        if (keys == null || keys.isEmpty()) return result;
        
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        // Use MongoDB's $in operator for batch retrieval
        collection.find(Filters.in("key", keys)).forEach(doc -> {
            CacheEntry entry = documentToEntry(doc, region);
            if (entry != null) {
                result.put(entry.getKey(), entry);
            }
        });
        
        return result;
    }
    
    @Override
    public List<CacheEntry> loadEntries(String region, int limit) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        List<CacheEntry> entries = new ArrayList<>();
        // Sort by lastAccessedAt first (if exists), then by updatedAt - most recently accessed entries first
        // Using compound sort: lastAccessedAt DESC, then updatedAt DESC as fallback
        collection.find()
                .sort(Sorts.orderBy(Sorts.descending("lastAccessedAt"), Sorts.descending("updatedAt")))
                .limit(limit)
                .forEach(doc -> {
                    CacheEntry entry = documentToEntry(doc, region);
                    if (!entry.isExpired()) {
                        entries.add(entry);
                    }
                });
        
        return entries;
    }
    
    @Override
    public void deleteEntry(String region, String key) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.deleteOne(Filters.eq("key", key));
    }
    
    @Override
    public void deleteEntries(String region, List<String> keys) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.deleteMany(Filters.in("key", keys));
    }
    
    @Override
    public long countEntries(String region) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        return collection.countDocuments();
    }
    
    /**
     * Fast estimate of entry count using MongoDB's estimatedDocumentCount().
     * This uses collection metadata and is O(1) - suitable for dashboard display.
     * Unlike countDocuments(), this does not scan the collection.
     */
    @Override
    public long estimateEntryCount(String region) {
        try {
            String collectionName = getCollectionName(region);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            return collection.estimatedDocumentCount();
        } catch (Exception e) {
            log.debug("Could not get estimated count for region '{}': {}", region, e.getMessage());
            return countEntries(region);
        }
    }
    
    @Override
    public List<String> getKeys(String region, String pattern, int limit) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        List<String> keys = new ArrayList<>();
        
        // Use server-side regex filtering if pattern is specified
        if (pattern != null && !pattern.isEmpty() && !"*".equals(pattern)) {
            String regex = globToMongoRegex(pattern);
            collection.find(Filters.regex("key", regex))
                    .projection(Projections.include("key"))
                    .limit(limit > 0 ? limit : 0)
                    .forEach(doc -> keys.add(doc.getString("key")));
        } else {
            // No pattern - just apply limit
            var cursor = collection.find()
                    .projection(Projections.include("key"));
            if (limit > 0) {
                cursor = cursor.limit(limit);
            }
            cursor.forEach(doc -> keys.add(doc.getString("key")));
        }
        
        return keys;
    }
    
    /**
     * Iterate through all entries in a region without loading all into memory.
     * Uses MongoDB cursor for memory-efficient streaming - critical for startup
     * data loading and backup operations on large regions.
     * 
     * @param region Region name
     * @param consumer Consumer to process each entry
     * @return Number of entries processed
     * @since 1.8.3
     */
    @Override
    public long forEachEntry(String region, java.util.function.Consumer<CacheEntry> consumer) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        long count = 0;
        try (var cursor = collection.find().cursor()) {
            while (cursor.hasNext()) {
                try {
                    Document doc = cursor.next();
                    CacheEntry entry = documentToEntry(doc, region);
                    if (!entry.isExpired()) {
                        consumer.accept(entry);
                        count++;
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse entry during MongoDB iteration: {}", e.getMessage());
                }
            }
        }
        
        return count;
    }
    
    @Override
    public long deleteExpiredEntries(String region) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        // Delete where expiresAt is not null and is before now
        Bson filter = Filters.and(
                Filters.ne("expiresAt", null),
                Filters.lt("expiresAt", Instant.now())
        );
        
        long deleted = collection.deleteMany(filter).getDeletedCount();
        
        if (deleted > 0) {
            log.info("Deleted {} expired entries from region '{}' in MongoDB", deleted, region);
        }
        
        return deleted;
    }
    
    @Override
    public long deleteAllExpiredEntries() {
        long total = 0;
        for (CacheRegion region : loadAllRegions()) {
            total += deleteExpiredEntries(region.getName());
        }
        return total;
    }
    
    @Override
    public long countNonExpiredEntries(String region) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        // Count where expiresAt is null OR expiresAt >= now
        Bson filter = Filters.or(
                Filters.eq("expiresAt", null),
                Filters.gte("expiresAt", Instant.now())
        );
        
        return collection.countDocuments(filter);
    }
    
    @Override
    public List<String> getNonExpiredKeys(String region, String pattern, int limit) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        // Build compound filter: non-expired AND optional key pattern
        List<Bson> filters = new ArrayList<>();
        
        // Non-expired filter
        filters.add(Filters.or(
                Filters.eq("expiresAt", null),
                Filters.gte("expiresAt", Instant.now())
        ));
        
        // Key pattern filter (server-side)
        if (pattern != null && !pattern.isEmpty() && !"*".equals(pattern)) {
            filters.add(Filters.regex("key", globToMongoRegex(pattern)));
        }
        
        List<String> keys = new ArrayList<>();
        var cursor = collection.find(Filters.and(filters))
                .projection(Projections.include("key"));
        if (limit > 0) {
            cursor = cursor.limit(limit);
        }
        cursor.forEach(doc -> keys.add(doc.getString("key")));
        
        return keys;
    }
    
    // ==================== Native JSON Query Support (v1.8.3) ====================
    
    @Override
    public boolean supportsNativeJsonQuery() {
        return true;
    }
    
    @Override
    public List<CacheEntry> searchByJsonCriteria(String region, java.util.Map<String, Object> criteria, int limit) {
        if (criteria == null || criteria.isEmpty()) {
            return loadEntries(region, limit);
        }
        
        List<CacheEntry> results = new ArrayList<>();
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        // Build MongoDB query
        List<Bson> filters = new ArrayList<>();
        
        // Filter non-expired entries
        filters.add(Filters.or(
            Filters.eq("expiresAt", null),
            Filters.gt("expiresAt", java.time.Instant.now())
        ));
        
        // Add criteria filters
        for (java.util.Map.Entry<String, Object> entry : criteria.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();
            String jsonField = "jsonValue." + field;
            
            if (value instanceof List) {
                // IN clause: field in [a, b, c]
                @SuppressWarnings("unchecked")
                List<?> values = (List<?>) value;
                filters.add(Filters.in(jsonField, values));
                
            } else if (value instanceof String) {
                String strValue = (String) value;
                
                // Check for comparison operators
                if (strValue.startsWith(">=")) {
                    filters.add(Filters.gte(jsonField, parseValue(strValue.substring(2))));
                } else if (strValue.startsWith("<=")) {
                    filters.add(Filters.lte(jsonField, parseValue(strValue.substring(2))));
                } else if (strValue.startsWith("!=")) {
                    filters.add(Filters.ne(jsonField, parseValue(strValue.substring(2))));
                } else if (strValue.startsWith(">")) {
                    filters.add(Filters.gt(jsonField, parseValue(strValue.substring(1))));
                } else if (strValue.startsWith("<")) {
                    filters.add(Filters.lt(jsonField, parseValue(strValue.substring(1))));
                } else if (strValue.startsWith("=")) {
                    filters.add(Filters.eq(jsonField, parseValue(strValue.substring(1))));
                } else {
                    // Equality
                    filters.add(Filters.eq(jsonField, parseValue(strValue)));
                }
            } else {
                // Direct value match
                filters.add(Filters.eq(jsonField, value));
            }
        }
        
        // Execute query
        FindIterable<Document> cursor = collection.find(Filters.and(filters))
                .limit(limit);
        
        for (Document doc : cursor) {
            results.add(documentToEntry(doc, region));
        }
        
        log.debug("MongoDB native JSON query: {} results for region '{}' with {} criteria", 
                results.size(), region, criteria.size());
        
        return results;
    }
    
    /**
     * Parse a string value to appropriate type for MongoDB queries.
     */
    private Object parseValue(String value) {
        if (value == null) return null;
        
        // Try numeric
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            } else {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException e) {
            // Not numeric
        }
        
        // Try boolean
        if ("true".equalsIgnoreCase(value)) return true;
        if ("false".equalsIgnoreCase(value)) return false;
        
        // Return as string
        return value;
    }
    
    /**
     * Convert a glob pattern to a MongoDB-compatible regex string.
     * Handles *, ? wildcards and escapes special regex characters.
     */
    private String globToMongoRegex(String glob) {
        if (glob == null || glob.isEmpty() || glob.equals("*")) {
            return ".*";
        }
        
        StringBuilder regex = new StringBuilder("^");
        for (char c : glob.toCharArray()) {
            switch (c) {
                case '*':
                    regex.append(".*");
                    break;
                case '?':
                    regex.append(".");
                    break;
                case '.':
                case '(':
                case ')':
                case '+':
                case '|':
                case '^':
                case '$':
                case '@':
                case '%':
                case '\\':
                case '{':
                case '}':
                case '[':
                case ']':
                    regex.append("\\").append(c);
                    break;
                default:
                    regex.append(c);
            }
        }
        regex.append("$");
        return regex.toString();
    }
    
    private Document entryToDocument(CacheEntry entry) {
        Document doc = new Document()
                .append("key", entry.getKey())
                .append("valueType", entry.getValueType().name())
                .append("stringValue", entry.getStringValue())
                .append("ttlSeconds", entry.getTtlSeconds())
                .append("createdAt", entry.getCreatedAt())
                .append("updatedAt", entry.getUpdatedAt())
                .append("expiresAt", entry.getExpiresAt())
                .append("version", entry.getVersion())
                .append("accessCount", entry.getAccessCount())
                .append("lastAccessedAt", entry.getLastAccessedAt())
                .append("metadata", entry.getMetadata());
        
        if (entry.getJsonValue() != null) {
            doc.append("jsonValue", Document.parse(JsonUtils.toJson(entry.getJsonValue())));
        }
        
        return doc;
    }
    
    private CacheEntry documentToEntry(Document doc, String region) {
        CacheEntry.CacheEntryBuilder builder = CacheEntry.builder()
                .key(doc.getString("key"))
                .region(region)
                .valueType(CacheEntry.ValueType.valueOf(doc.getString("valueType")))
                .stringValue(doc.getString("stringValue"))
                .ttlSeconds(doc.getLong("ttlSeconds") != null ? doc.getLong("ttlSeconds") : -1)
                .createdAt(toInstant(doc.get("createdAt")))
                .updatedAt(toInstant(doc.get("updatedAt")))
                .expiresAt(toInstant(doc.get("expiresAt")))
                .version(doc.getLong("version") != null ? doc.getLong("version") : 1)
                .accessCount(doc.getLong("accessCount") != null ? doc.getLong("accessCount") : 0)
                .lastAccessedAt(toInstant(doc.get("lastAccessedAt")))
                .metadata(doc.getString("metadata"));
        
        Document jsonDoc = doc.get("jsonValue", Document.class);
        if (jsonDoc != null) {
            builder.jsonValue(JsonUtils.parse(jsonDoc.toJson()));
        }
        
        return builder.build();
    }
}
