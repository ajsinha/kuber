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
import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheRegion;
import com.kuber.core.util.JsonUtils;
import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * SQLite implementation of PersistenceStore.
 * Uses a single SQLite database file for all regions.
 */
@Slf4j
public class SqlitePersistenceStore extends AbstractPersistenceStore {
    
    private final KuberProperties properties;
    private Connection connection;
    private final String dbPath;
    
    public SqlitePersistenceStore(KuberProperties properties) {
        this.properties = properties;
        this.dbPath = properties.getPersistence().getSqlite().getPath();
    }
    
    @Override
    public PersistenceType getType() {
        return PersistenceType.SQLITE;
    }
    
    @Override
    public void initialize() {
        log.info("Initializing SQLite persistence store at: {}", dbPath);
        
        try {
            // Ensure parent directory exists
            File dbFile = new File(dbPath);
            File parentDir = dbFile.getParentFile();
            if (parentDir != null && !parentDir.exists()) {
                parentDir.mkdirs();
            }
            
            // Load SQLite JDBC driver
            Class.forName("org.sqlite.JDBC");
            
            // Connect to database
            connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
            connection.setAutoCommit(true);
            
            // Enable WAL mode for better concurrency
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("PRAGMA journal_mode=WAL");
                stmt.execute("PRAGMA synchronous=NORMAL");
                stmt.execute("PRAGMA cache_size=10000");
            }
            
            // Create tables
            createTables();
            
            available = true;
            log.info("SQLite persistence store initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize SQLite persistence store: {}", e.getMessage(), e);
            available = false;
        }
    }
    
    @Override
    public void shutdown() {
        log.info("Shutting down SQLite persistence store...");
        available = false;
        
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn("Error closing SQLite connection: {}", e.getMessage());
            }
        }
    }
    
    private void createTables() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Regions table
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS kuber_regions (
                    name TEXT PRIMARY KEY,
                    description TEXT,
                    captive INTEGER DEFAULT 0,
                    max_entries INTEGER DEFAULT -1,
                    default_ttl_seconds INTEGER DEFAULT -1,
                    entry_count INTEGER DEFAULT 0,
                    created_at INTEGER,
                    updated_at INTEGER,
                    created_by TEXT,
                    enabled INTEGER DEFAULT 1,
                    collection_name TEXT
                )
            """);
            
            // Entries table
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS kuber_entries (
                    region TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value_type TEXT NOT NULL,
                    string_value TEXT,
                    json_value TEXT,
                    ttl_seconds INTEGER DEFAULT -1,
                    created_at INTEGER,
                    updated_at INTEGER,
                    expires_at INTEGER,
                    version INTEGER DEFAULT 1,
                    access_count INTEGER DEFAULT 0,
                    last_accessed_at INTEGER,
                    metadata TEXT,
                    PRIMARY KEY (region, key)
                )
            """);
            
            // Create indexes
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_entries_region ON kuber_entries(region)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_entries_expires ON kuber_entries(expires_at)");
        }
    }
    
    // ==================== Region Operations ====================
    
    @Override
    public void saveRegion(CacheRegion region) {
        String sql = """
            INSERT OR REPLACE INTO kuber_regions 
            (name, description, captive, max_entries, default_ttl_seconds, entry_count, 
             created_at, updated_at, created_by, enabled, collection_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, region.getName());
            stmt.setString(2, region.getDescription());
            stmt.setInt(3, region.isCaptive() ? 1 : 0);
            stmt.setLong(4, region.getMaxEntries());
            stmt.setLong(5, region.getDefaultTtlSeconds());
            stmt.setLong(6, region.getEntryCount());
            stmt.setLong(7, region.getCreatedAt() != null ? region.getCreatedAt().toEpochMilli() : System.currentTimeMillis());
            stmt.setLong(8, region.getUpdatedAt() != null ? region.getUpdatedAt().toEpochMilli() : System.currentTimeMillis());
            stmt.setString(9, region.getCreatedBy());
            stmt.setInt(10, region.isEnabled() ? 1 : 0);
            stmt.setString(11, region.getCollectionName());
            stmt.executeUpdate();
            
            log.info("Saved region '{}' to SQLite", region.getName());
        } catch (SQLException e) {
            log.error("Failed to save region '{}': {}", region.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to save region", e);
        }
    }
    
    @Override
    public List<CacheRegion> loadAllRegions() {
        List<CacheRegion> regions = new ArrayList<>();
        String sql = "SELECT * FROM kuber_regions";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                regions.add(resultSetToRegion(rs));
            }
            
            log.info("Loaded {} regions from SQLite", regions.size());
        } catch (SQLException e) {
            log.error("Failed to load regions: {}", e.getMessage(), e);
        }
        
        return regions;
    }
    
    @Override
    public CacheRegion loadRegion(String name) {
        String sql = "SELECT * FROM kuber_regions WHERE name = ?";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, name);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return resultSetToRegion(rs);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to load region '{}': {}", name, e.getMessage(), e);
        }
        
        return null;
    }
    
    @Override
    public void deleteRegion(String name) {
        try {
            // Delete all entries in the region
            try (PreparedStatement stmt = connection.prepareStatement("DELETE FROM kuber_entries WHERE region = ?")) {
                stmt.setString(1, name);
                stmt.executeUpdate();
            }
            
            // Delete the region
            try (PreparedStatement stmt = connection.prepareStatement("DELETE FROM kuber_regions WHERE name = ?")) {
                stmt.setString(1, name);
                stmt.executeUpdate();
            }
            
            log.info("Deleted region '{}' from SQLite", name);
        } catch (SQLException e) {
            log.error("Failed to delete region '{}': {}", name, e.getMessage(), e);
            throw new RuntimeException("Failed to delete region", e);
        }
    }
    
    @Override
    public void purgeRegion(String name) {
        try (PreparedStatement stmt = connection.prepareStatement("DELETE FROM kuber_entries WHERE region = ?")) {
            stmt.setString(1, name);
            int deleted = stmt.executeUpdate();
            log.info("Purged {} entries from region '{}' in SQLite", deleted, name);
        } catch (SQLException e) {
            log.error("Failed to purge region '{}': {}", name, e.getMessage(), e);
            throw new RuntimeException("Failed to purge region", e);
        }
    }
    
    private CacheRegion resultSetToRegion(ResultSet rs) throws SQLException {
        return CacheRegion.builder()
                .name(rs.getString("name"))
                .description(rs.getString("description"))
                .captive(rs.getInt("captive") == 1)
                .maxEntries(rs.getLong("max_entries"))
                .defaultTtlSeconds(rs.getLong("default_ttl_seconds"))
                .entryCount(rs.getLong("entry_count"))
                .createdAt(Instant.ofEpochMilli(rs.getLong("created_at")))
                .updatedAt(Instant.ofEpochMilli(rs.getLong("updated_at")))
                .createdBy(rs.getString("created_by"))
                .enabled(rs.getInt("enabled") == 1)
                .collectionName(rs.getString("collection_name"))
                .build();
    }
    
    // ==================== Entry Operations ====================
    
    @Override
    public void saveEntry(CacheEntry entry) {
        String sql = """
            INSERT OR REPLACE INTO kuber_entries 
            (region, key, value_type, string_value, json_value, ttl_seconds, 
             created_at, updated_at, expires_at, version, access_count, last_accessed_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setEntryParameters(stmt, entry);
            stmt.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to save entry '{}' in region '{}': {}", entry.getKey(), entry.getRegion(), e.getMessage(), e);
            throw new RuntimeException("Failed to save entry", e);
        }
    }
    
    @Override
    public void saveEntries(List<CacheEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }
        
        String sql = """
            INSERT OR REPLACE INTO kuber_entries 
            (region, key, value_type, string_value, json_value, ttl_seconds, 
             created_at, updated_at, expires_at, version, access_count, last_accessed_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (CacheEntry entry : entries) {
                setEntryParameters(stmt, entry);
                stmt.addBatch();
            }
            stmt.executeBatch();
            log.debug("Saved {} entries to SQLite", entries.size());
        } catch (SQLException e) {
            log.error("Failed to save entries: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to save entries", e);
        }
    }
    
    private void setEntryParameters(PreparedStatement stmt, CacheEntry entry) throws SQLException {
        stmt.setString(1, entry.getRegion());
        stmt.setString(2, entry.getKey());
        stmt.setString(3, entry.getValueType().name());
        stmt.setString(4, entry.getStringValue());
        stmt.setString(5, entry.getJsonValue() != null ? JsonUtils.toJson(entry.getJsonValue()) : null);
        stmt.setLong(6, entry.getTtlSeconds());
        stmt.setLong(7, entry.getCreatedAt() != null ? entry.getCreatedAt().toEpochMilli() : System.currentTimeMillis());
        stmt.setLong(8, entry.getUpdatedAt() != null ? entry.getUpdatedAt().toEpochMilli() : System.currentTimeMillis());
        stmt.setObject(9, entry.getExpiresAt() != null ? entry.getExpiresAt().toEpochMilli() : null);
        stmt.setLong(10, entry.getVersion());
        stmt.setLong(11, entry.getAccessCount());
        stmt.setObject(12, entry.getLastAccessedAt() != null ? entry.getLastAccessedAt().toEpochMilli() : null);
        stmt.setString(13, entry.getMetadata());
    }
    
    @Override
    public CacheEntry loadEntry(String region, String key) {
        String sql = "SELECT * FROM kuber_entries WHERE region = ? AND key = ?";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, region);
            stmt.setString(2, key);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return resultSetToEntry(rs);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to load entry '{}' from region '{}': {}", key, region, e.getMessage(), e);
        }
        
        return null;
    }
    
    @Override
    public List<CacheEntry> loadEntries(String region, int limit) {
        List<CacheEntry> entries = new ArrayList<>();
        String sql = "SELECT * FROM kuber_entries WHERE region = ? ORDER BY updated_at DESC LIMIT ?";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, region);
            stmt.setInt(2, limit);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    entries.add(resultSetToEntry(rs));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to load entries from region '{}': {}", region, e.getMessage(), e);
        }
        
        return entries;
    }
    
    @Override
    public void deleteEntry(String region, String key) {
        try (PreparedStatement stmt = connection.prepareStatement("DELETE FROM kuber_entries WHERE region = ? AND key = ?")) {
            stmt.setString(1, region);
            stmt.setString(2, key);
            stmt.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to delete entry '{}' from region '{}': {}", key, region, e.getMessage(), e);
            throw new RuntimeException("Failed to delete entry", e);
        }
    }
    
    @Override
    public void deleteEntries(String region, List<String> keys) {
        if (keys.isEmpty()) {
            return;
        }
        
        String placeholders = String.join(",", keys.stream().map(k -> "?").toList());
        String sql = "DELETE FROM kuber_entries WHERE region = ? AND key IN (" + placeholders + ")";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, region);
            for (int i = 0; i < keys.size(); i++) {
                stmt.setString(i + 2, keys.get(i));
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to delete entries from region '{}': {}", region, e.getMessage(), e);
            throw new RuntimeException("Failed to delete entries", e);
        }
    }
    
    @Override
    public long countEntries(String region) {
        String sql = "SELECT COUNT(*) FROM kuber_entries WHERE region = ?";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, region);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to count entries in region '{}': {}", region, e.getMessage(), e);
        }
        
        return 0;
    }
    
    @Override
    public List<String> getKeys(String region, String pattern, int limit) {
        List<String> keys = new ArrayList<>();
        String sql = "SELECT key FROM kuber_entries WHERE region = ?";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, region);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    keys.add(rs.getString("key"));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get keys from region '{}': {}", region, e.getMessage(), e);
        }
        
        return filterKeys(keys, pattern, limit);
    }
    
    @Override
    public long deleteExpiredEntries(String region) {
        String sql = "DELETE FROM kuber_entries WHERE region = ? AND expires_at IS NOT NULL AND expires_at < ?";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, region);
            stmt.setLong(2, System.currentTimeMillis());
            
            int deleted = stmt.executeUpdate();
            
            if (deleted > 0) {
                log.info("Deleted {} expired entries from region '{}' in SQLite", deleted, region);
            }
            
            return deleted;
        } catch (SQLException e) {
            log.error("Failed to delete expired entries from region '{}': {}", region, e.getMessage(), e);
            return 0;
        }
    }
    
    @Override
    public long deleteAllExpiredEntries() {
        String sql = "DELETE FROM kuber_entries WHERE expires_at IS NOT NULL AND expires_at < ?";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, System.currentTimeMillis());
            
            int deleted = stmt.executeUpdate();
            
            if (deleted > 0) {
                log.info("Deleted {} expired entries from all regions in SQLite", deleted);
            }
            
            return deleted;
        } catch (SQLException e) {
            log.error("Failed to delete expired entries: {}", e.getMessage(), e);
            return 0;
        }
    }
    
    @Override
    public long countNonExpiredEntries(String region) {
        String sql = "SELECT COUNT(*) FROM kuber_entries WHERE region = ? AND (expires_at IS NULL OR expires_at >= ?)";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, region);
            stmt.setLong(2, System.currentTimeMillis());
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to count non-expired entries in region '{}': {}", region, e.getMessage(), e);
        }
        
        return 0;
    }
    
    @Override
    public List<String> getNonExpiredKeys(String region, String pattern, int limit) {
        List<String> keys = new ArrayList<>();
        String sql = "SELECT key FROM kuber_entries WHERE region = ? AND (expires_at IS NULL OR expires_at >= ?)";
        
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, region);
            stmt.setLong(2, System.currentTimeMillis());
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    keys.add(rs.getString("key"));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get non-expired keys from region '{}': {}", region, e.getMessage(), e);
        }
        
        return filterKeys(keys, pattern, limit);
    }
    
    private CacheEntry resultSetToEntry(ResultSet rs) throws SQLException {
        CacheEntry.CacheEntryBuilder builder = CacheEntry.builder()
                .region(rs.getString("region"))
                .key(rs.getString("key"))
                .valueType(CacheEntry.ValueType.valueOf(rs.getString("value_type")))
                .stringValue(rs.getString("string_value"))
                .ttlSeconds(rs.getLong("ttl_seconds"))
                .createdAt(Instant.ofEpochMilli(rs.getLong("created_at")))
                .updatedAt(Instant.ofEpochMilli(rs.getLong("updated_at")))
                .version(rs.getLong("version"))
                .accessCount(rs.getLong("access_count"))
                .metadata(rs.getString("metadata"));
        
        Long expiresAt = rs.getObject("expires_at", Long.class);
        if (expiresAt != null) {
            builder.expiresAt(Instant.ofEpochMilli(expiresAt));
        }
        
        Long lastAccessedAt = rs.getObject("last_accessed_at", Long.class);
        if (lastAccessedAt != null) {
            builder.lastAccessedAt(Instant.ofEpochMilli(lastAccessedAt));
        }
        
        String jsonValue = rs.getString("json_value");
        if (jsonValue != null) {
            builder.jsonValue(JsonUtils.parse(jsonValue));
        }
        
        return builder.build();
    }
}
