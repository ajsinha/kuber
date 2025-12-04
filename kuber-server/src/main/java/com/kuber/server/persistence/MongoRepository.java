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

import com.fasterxml.jackson.databind.JsonNode;
import com.kuber.core.constants.KuberConstants;
import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheRegion;
import com.kuber.core.util.JsonUtils;
import com.kuber.server.config.KuberProperties;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Repository;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * MongoDB repository for persisting cache data.
 * Each region maps to a separate collection.
 * Only activated when kuber.persistence.type=mongodb is explicitly set.
 */
@Slf4j
@Repository
@RequiredArgsConstructor
@ConditionalOnProperty(name = "kuber.persistence.type", havingValue = "mongodb")
public class MongoRepository {
    
    private final MongoDatabase database;
    private final KuberProperties properties;
    
    @PostConstruct
    public void initialize() {
        log.info("Initializing MongoDB repository...");
        
        // Create indexes for system collections
        createIndexes(KuberConstants.MONGO_REGIONS_COLLECTION,
                Indexes.ascending("name"));
        
        log.info("MongoDB repository initialized");
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
    
    public CacheRegion loadRegion(String name) {
        MongoCollection<Document> collection = database.getCollection(KuberConstants.MONGO_REGIONS_COLLECTION);
        Document doc = collection.find(Filters.eq("name", name)).first();
        return doc != null ? documentToRegion(doc) : null;
    }
    
    public void deleteRegion(String name) {
        // Delete region metadata
        MongoCollection<Document> collection = database.getCollection(KuberConstants.MONGO_REGIONS_COLLECTION);
        Document regionDoc = collection.findOneAndDelete(Filters.eq("name", name));
        
        // Drop the region's collection
        if (regionDoc != null) {
            String collectionName = regionDoc.getString("collectionName");
            if (collectionName == null) {
                collectionName = "kuber_" + name.toLowerCase().replaceAll("[^a-z0-9_]", "_");
            }
            try {
                database.getCollection(collectionName).drop();
            } catch (Exception e) {
                log.warn("Failed to drop collection {}: {}", collectionName, e.getMessage());
            }
        }
    }
    
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
    
    /**
     * Convert MongoDB date field to Instant.
     * MongoDB stores dates as java.util.Date, need to convert to Instant.
     */
    private Instant toInstant(Object dateObj) {
        if (dateObj == null) {
            return null;
        }
        if (dateObj instanceof Instant) {
            return (Instant) dateObj;
        }
        if (dateObj instanceof java.util.Date) {
            return ((java.util.Date) dateObj).toInstant();
        }
        return null;
    }
    
    // ==================== Entry Operations ====================
    
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
    
    @Async
    public CompletableFuture<Void> saveEntryAsync(CacheEntry entry) {
        return CompletableFuture.runAsync(() -> saveEntry(entry));
    }
    
    public void saveEntries(List<CacheEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        
        // Group by region
        entries.stream()
                .collect(java.util.stream.Collectors.groupingBy(CacheEntry::getRegion))
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
    
    public CacheEntry loadEntry(String region, String key) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        Document doc = collection.find(Filters.eq("key", key)).first();
        return doc != null ? documentToEntry(doc, region) : null;
    }
    
    public List<CacheEntry> loadEntries(String region, int limit) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        List<CacheEntry> entries = new ArrayList<>();
        collection.find()
                .sort(Sorts.descending("updatedAt"))
                .limit(limit)
                .forEach(doc -> entries.add(documentToEntry(doc, region)));
        
        return entries;
    }
    
    public void deleteEntry(String region, String key) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.deleteOne(Filters.eq("key", key));
    }
    
    public void deleteEntries(String region, List<String> keys) {
        String collectionName = getCollectionName(region);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        collection.deleteMany(Filters.in("key", keys));
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
    
    private String getCollectionName(String region) {
        return KuberConstants.MONGO_COLLECTION_PREFIX + 
               region.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }
}
