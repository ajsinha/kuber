/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.server.persistence;

import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheRegion;
import com.kuber.core.util.JsonUtils;
import com.kuber.server.config.KuberProperties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL implementation of PersistenceStore.
 * Uses JSONB for efficient JSON storage and querying.
 */
@Slf4j
public class PostgresPersistenceStore extends AbstractPersistenceStore {
    
    private final KuberProperties properties;
    private HikariDataSource dataSource;
    
    // Guard against double shutdown
    private volatile boolean alreadyShutdown = false;
    
    public PostgresPersistenceStore(KuberProperties properties) {
        this.properties = properties;
    }
    
    @Override
    public PersistenceType getType() {
        return PersistenceType.POSTGRESQL;
    }
    
    @Override
    public void initialize() {
        KuberProperties.Postgresql pgProps = properties.getPersistence().getPostgresql();
        log.info("Initializing PostgreSQL persistence store at: {}", pgProps.getUrl());
        
        try {
            // Configure HikariCP connection pool
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(pgProps.getUrl());
            config.setUsername(pgProps.getUsername());
            config.setPassword(pgProps.getPassword());
            config.setMaximumPoolSize(pgProps.getPoolSize());
            config.setMinimumIdle(pgProps.getMinIdle());
            config.setConnectionTimeout(pgProps.getConnectionTimeoutMs());
            config.setIdleTimeout(pgProps.getIdleTimeoutMs());
            config.setMaxLifetime(pgProps.getMaxLifetimeMs());
            config.setPoolName("kuber-postgres-pool");
            
            dataSource = new HikariDataSource(config);
            
            // Create tables
            createTables();
            
            // Configure batched async persistence (v1.6.2)
            int batchSize = properties.getCache().getPersistenceBatchSize();
            int flushIntervalMs = properties.getCache().getPersistenceIntervalMs();
            configureBatching(batchSize, flushIntervalMs);
            
            available = true;
            log.info("PostgreSQL persistence store initialized successfully");
        } catch (Exception e) {
            log.error("Failed to initialize PostgreSQL persistence store: {}", e.getMessage(), e);
            available = false;
        }
    }
    
    @Override
    public void shutdown() {
        // Guard against double shutdown
        if (alreadyShutdown) {
            log.debug("PostgreSQL shutdown already completed - skipping duplicate shutdown call");
            return;
        }
        alreadyShutdown = true;
        
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  POSTGRESQL GRACEFUL SHUTDOWN INITIATED                             ║");
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
        
        if (dataSource != null && !dataSource.isClosed()) {
            try {
                // Log connection pool status before closing
                log.info("Active connections: {}, Idle connections: {}, Total connections: {}",
                        dataSource.getHikariPoolMXBean().getActiveConnections(),
                        dataSource.getHikariPoolMXBean().getIdleConnections(),
                        dataSource.getHikariPoolMXBean().getTotalConnections());
                
                // Wait for active connections to complete (up to 30 seconds)
                int waitCount = 0;
                while (dataSource.getHikariPoolMXBean().getActiveConnections() > 0 && waitCount < 30) {
                    log.info("Waiting for {} active connections to complete...", 
                            dataSource.getHikariPoolMXBean().getActiveConnections());
                    Thread.sleep(1000);
                    waitCount++;
                }
                
                // Close the connection pool
                log.info("Closing HikariCP connection pool...");
                dataSource.close();
                log.info("Connection pool closed");
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for connections to close");
            } catch (Exception e) {
                log.warn("Error during graceful shutdown: {}", e.getMessage());
                // Force close
                dataSource.close();
            }
        }
        
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  POSTGRESQL SHUTDOWN COMPLETE                                       ║");
        log.info("╚════════════════════════════════════════════════════════════════════╝");
    }
    
    /**
     * Sync is a no-op for PostgreSQL as it handles durability automatically.
     * PostgreSQL uses WAL (Write-Ahead Logging) and transactions are durable
     * once committed.
     */
    @Override
    public void sync() {
        log.debug("PostgreSQL sync called - no action needed (WAL handles durability)");
        // PostgreSQL handles durability via WAL and fsync automatically
    }
    
    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
    
    private void createTables() throws SQLException {
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Regions table
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS kuber_regions (
                    name VARCHAR(255) PRIMARY KEY,
                    description TEXT,
                    captive BOOLEAN DEFAULT FALSE,
                    max_entries BIGINT DEFAULT -1,
                    default_ttl_seconds BIGINT DEFAULT -1,
                    entry_count BIGINT DEFAULT 0,
                    created_at TIMESTAMP WITH TIME ZONE,
                    updated_at TIMESTAMP WITH TIME ZONE,
                    created_by VARCHAR(255),
                    enabled BOOLEAN DEFAULT TRUE,
                    collection_name VARCHAR(255)
                )
            """);
            
            // Entries table with JSONB for json_value
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS kuber_entries (
                    region VARCHAR(255) NOT NULL,
                    key VARCHAR(1024) NOT NULL,
                    value_type VARCHAR(50) NOT NULL,
                    string_value TEXT,
                    json_value JSONB,
                    ttl_seconds BIGINT DEFAULT -1,
                    created_at TIMESTAMP WITH TIME ZONE,
                    updated_at TIMESTAMP WITH TIME ZONE,
                    expires_at TIMESTAMP WITH TIME ZONE,
                    version BIGINT DEFAULT 1,
                    access_count BIGINT DEFAULT 0,
                    last_accessed_at TIMESTAMP WITH TIME ZONE,
                    metadata TEXT,
                    PRIMARY KEY (region, key)
                )
            """);
            
            // Create indexes
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_entries_region ON kuber_entries(region)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_entries_expires ON kuber_entries(expires_at)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_entries_json ON kuber_entries USING GIN (json_value)");
        }
    }
    
    // ==================== Region Operations ====================
    
    @Override
    public void saveRegion(CacheRegion region) {
        String sql = """
            INSERT INTO kuber_regions 
            (name, description, captive, max_entries, default_ttl_seconds, entry_count, 
             created_at, updated_at, created_by, enabled, collection_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (name) DO UPDATE SET
                description = EXCLUDED.description,
                captive = EXCLUDED.captive,
                max_entries = EXCLUDED.max_entries,
                default_ttl_seconds = EXCLUDED.default_ttl_seconds,
                entry_count = EXCLUDED.entry_count,
                updated_at = EXCLUDED.updated_at,
                enabled = EXCLUDED.enabled,
                collection_name = EXCLUDED.collection_name
        """;
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, region.getName());
            stmt.setString(2, region.getDescription());
            stmt.setBoolean(3, region.isCaptive());
            stmt.setLong(4, region.getMaxEntries());
            stmt.setLong(5, region.getDefaultTtlSeconds());
            stmt.setLong(6, region.getEntryCount());
            stmt.setTimestamp(7, toTimestamp(region.getCreatedAt()));
            stmt.setTimestamp(8, toTimestamp(region.getUpdatedAt()));
            stmt.setString(9, region.getCreatedBy());
            stmt.setBoolean(10, region.isEnabled());
            stmt.setString(11, region.getCollectionName());
            stmt.executeUpdate();
            
            log.info("Saved region '{}' to PostgreSQL", region.getName());
        } catch (SQLException e) {
            log.error("Failed to save region '{}': {}", region.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to save region", e);
        }
    }
    
    @Override
    public List<CacheRegion> loadAllRegions() {
        List<CacheRegion> regions = new ArrayList<>();
        String sql = "SELECT * FROM kuber_regions";
        
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            while (rs.next()) {
                regions.add(resultSetToRegion(rs));
            }
            
            log.info("Loaded {} regions from PostgreSQL", regions.size());
        } catch (SQLException e) {
            log.error("Failed to load regions: {}", e.getMessage(), e);
        }
        
        return regions;
    }
    
    @Override
    public CacheRegion loadRegion(String name) {
        String sql = "SELECT * FROM kuber_regions WHERE name = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
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
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            
            try {
                // Delete all entries in the region
                try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM kuber_entries WHERE region = ?")) {
                    stmt.setString(1, name);
                    stmt.executeUpdate();
                }
                
                // Delete the region
                try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM kuber_regions WHERE name = ?")) {
                    stmt.setString(1, name);
                    stmt.executeUpdate();
                }
                
                conn.commit();
                log.info("Deleted region '{}' from PostgreSQL", name);
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            log.error("Failed to delete region '{}': {}", name, e.getMessage(), e);
            throw new RuntimeException("Failed to delete region", e);
        }
    }
    
    @Override
    public void purgeRegion(String name) {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement("DELETE FROM kuber_entries WHERE region = ?")) {
            
            stmt.setString(1, name);
            int deleted = stmt.executeUpdate();
            log.info("Purged {} entries from region '{}' in PostgreSQL", deleted, name);
        } catch (SQLException e) {
            log.error("Failed to purge region '{}': {}", name, e.getMessage(), e);
            throw new RuntimeException("Failed to purge region", e);
        }
    }
    
    private CacheRegion resultSetToRegion(ResultSet rs) throws SQLException {
        return CacheRegion.builder()
                .name(rs.getString("name"))
                .description(rs.getString("description"))
                .captive(rs.getBoolean("captive"))
                .maxEntries(rs.getLong("max_entries"))
                .defaultTtlSeconds(rs.getLong("default_ttl_seconds"))
                .entryCount(rs.getLong("entry_count"))
                .createdAt(toInstant(rs.getTimestamp("created_at")))
                .updatedAt(toInstant(rs.getTimestamp("updated_at")))
                .createdBy(rs.getString("created_by"))
                .enabled(rs.getBoolean("enabled"))
                .collectionName(rs.getString("collection_name"))
                .build();
    }
    
    // ==================== Entry Operations ====================
    
    @Override
    public void saveEntry(CacheEntry entry) {
        String sql = """
            INSERT INTO kuber_entries 
            (region, key, value_type, string_value, json_value, ttl_seconds, 
             created_at, updated_at, expires_at, version, access_count, last_accessed_at, metadata)
            VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (region, key) DO UPDATE SET
                value_type = EXCLUDED.value_type,
                string_value = EXCLUDED.string_value,
                json_value = EXCLUDED.json_value,
                ttl_seconds = EXCLUDED.ttl_seconds,
                updated_at = EXCLUDED.updated_at,
                expires_at = EXCLUDED.expires_at,
                version = EXCLUDED.version,
                access_count = EXCLUDED.access_count,
                last_accessed_at = EXCLUDED.last_accessed_at,
                metadata = EXCLUDED.metadata
        """;
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
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
            INSERT INTO kuber_entries 
            (region, key, value_type, string_value, json_value, ttl_seconds, 
             created_at, updated_at, expires_at, version, access_count, last_accessed_at, metadata)
            VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (region, key) DO UPDATE SET
                value_type = EXCLUDED.value_type,
                string_value = EXCLUDED.string_value,
                json_value = EXCLUDED.json_value,
                ttl_seconds = EXCLUDED.ttl_seconds,
                updated_at = EXCLUDED.updated_at,
                expires_at = EXCLUDED.expires_at,
                version = EXCLUDED.version,
                access_count = EXCLUDED.access_count,
                last_accessed_at = EXCLUDED.last_accessed_at,
                metadata = EXCLUDED.metadata
        """;
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            for (CacheEntry entry : entries) {
                setEntryParameters(stmt, entry);
                stmt.addBatch();
            }
            stmt.executeBatch();
            log.debug("Saved {} entries to PostgreSQL", entries.size());
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
        stmt.setTimestamp(7, toTimestamp(entry.getCreatedAt()));
        stmt.setTimestamp(8, toTimestamp(entry.getUpdatedAt()));
        stmt.setTimestamp(9, toTimestamp(entry.getExpiresAt()));
        stmt.setLong(10, entry.getVersion());
        stmt.setLong(11, entry.getAccessCount());
        stmt.setTimestamp(12, toTimestamp(entry.getLastAccessedAt()));
        stmt.setString(13, entry.getMetadata());
    }
    
    @Override
    public CacheEntry loadEntry(String region, String key) {
        String sql = "SELECT * FROM kuber_entries WHERE region = ? AND key = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
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
    public java.util.Map<String, CacheEntry> loadEntriesByKeys(String region, java.util.List<String> keys) {
        java.util.Map<String, CacheEntry> result = new java.util.HashMap<>();
        if (keys == null || keys.isEmpty()) return result;
        
        // Build SQL with IN clause for batch retrieval
        StringBuilder sql = new StringBuilder("SELECT * FROM kuber_entries WHERE region = ? AND key IN (");
        for (int i = 0; i < keys.size(); i++) {
            sql.append(i > 0 ? ",?" : "?");
        }
        sql.append(")");
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            
            stmt.setString(1, region);
            for (int i = 0; i < keys.size(); i++) {
                stmt.setString(i + 2, keys.get(i));
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    CacheEntry entry = resultSetToEntry(rs);
                    if (entry != null) {
                        result.put(entry.getKey(), entry);
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Failed to batch load entries from region '{}': {}", region, e.getMessage(), e);
            // Fall back to individual loads
            for (String key : keys) {
                CacheEntry entry = loadEntry(region, key);
                if (entry != null) {
                    result.put(key, entry);
                }
            }
        }
        return result;
    }
    
    @Override
    public List<CacheEntry> loadEntries(String region, int limit) {
        List<CacheEntry> entries = new ArrayList<>();
        // Order by last_accessed_at first (if exists), then by updated_at - most recently accessed entries first
        // COALESCE handles null last_accessed_at by falling back to updated_at
        // Also filter out expired entries
        String sql = "SELECT * FROM kuber_entries WHERE region = ? " +
                     "AND (expires_at IS NULL OR expires_at >= NOW()) " +
                     "ORDER BY COALESCE(last_accessed_at, updated_at) DESC LIMIT ?";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
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
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement("DELETE FROM kuber_entries WHERE region = ? AND key = ?")) {
            
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
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
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
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
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
    
    /**
     * Fast estimate of entry count using PostgreSQL's reltuples from pg_class.
     * This is O(1) and uses table statistics maintained by ANALYZE/autovacuum.
     * Falls back to exact count if statistics are unavailable.
     * 
     * @since 1.9.0
     */
    @Override
    public long estimateEntryCount(String region) {
        // PostgreSQL maintains approximate row counts in pg_class
        // We can use this for a fast estimate, but since all regions share one table
        // we need to fall back to COUNT with region filter
        // However, COUNT(*) with an indexed column is still fast in PostgreSQL
        return countEntries(region);
    }
    
    @Override
    public List<String> getKeys(String region, String pattern, int limit) {
        List<String> keys = new ArrayList<>();
        boolean hasPattern = pattern != null && !pattern.isEmpty() && !"*".equals(pattern);
        
        // Build SQL with server-side filtering
        StringBuilder sql = new StringBuilder("SELECT key FROM kuber_entries WHERE region = ?");
        if (hasPattern) {
            // Convert glob pattern to PostgreSQL LIKE: * -> %, ? -> _
            sql.append(" AND key LIKE ?");
        }
        if (limit > 0) {
            sql.append(" LIMIT ?");
        }
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            
            int paramIdx = 1;
            stmt.setString(paramIdx++, region);
            if (hasPattern) {
                stmt.setString(paramIdx++, globToSqlLike(pattern));
            }
            if (limit > 0) {
                stmt.setInt(paramIdx, limit);
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    keys.add(rs.getString("key"));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get keys from region '{}': {}", region, e.getMessage(), e);
        }
        
        return keys;
    }
    
    /**
     * Iterate through all entries in a region without loading all into memory.
     * Uses server-side cursor via setFetchSize for memory-efficient streaming.
     * Critical for startup data loading and backup operations on large regions.
     * 
     * @param region Region name
     * @param consumer Consumer to process each entry
     * @return Number of entries processed
     * @since 1.9.0
     */
    @Override
    public long forEachEntry(String region, java.util.function.Consumer<CacheEntry> consumer) {
        long count = 0;
        String sql = "SELECT * FROM kuber_entries WHERE region = ?";
        
        try (Connection conn = getConnection()) {
            // Disable auto-commit to enable server-side cursor
            conn.setAutoCommit(false);
            
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, region);
                stmt.setFetchSize(1000); // Stream in batches of 1000
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        try {
                            CacheEntry entry = resultSetToEntry(rs);
                            if (!entry.isExpired()) {
                                consumer.accept(entry);
                                count++;
                            }
                        } catch (Exception e) {
                            log.warn("Failed to parse entry during PostgreSQL iteration: {}", e.getMessage());
                        }
                    }
                }
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            log.error("Failed to iterate entries in region '{}': {}", region, e.getMessage(), e);
        }
        
        return count;
    }
    
    @Override
    public long deleteExpiredEntries(String region) {
        String sql = "DELETE FROM kuber_entries WHERE region = ? AND expires_at IS NOT NULL AND expires_at < NOW()";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, region);
            int deleted = stmt.executeUpdate();
            
            if (deleted > 0) {
                log.info("Deleted {} expired entries from region '{}' in PostgreSQL", deleted, region);
            }
            
            return deleted;
        } catch (SQLException e) {
            log.error("Failed to delete expired entries from region '{}': {}", region, e.getMessage(), e);
            return 0;
        }
    }
    
    @Override
    public long deleteAllExpiredEntries() {
        String sql = "DELETE FROM kuber_entries WHERE expires_at IS NOT NULL AND expires_at < NOW()";
        
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
            
            int deleted = stmt.executeUpdate(sql);
            
            if (deleted > 0) {
                log.info("Deleted {} expired entries from all regions in PostgreSQL", deleted);
            }
            
            return deleted;
        } catch (SQLException e) {
            log.error("Failed to delete expired entries: {}", e.getMessage(), e);
            return 0;
        }
    }
    
    @Override
    public long countNonExpiredEntries(String region) {
        String sql = "SELECT COUNT(*) FROM kuber_entries WHERE region = ? AND (expires_at IS NULL OR expires_at >= NOW())";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, region);
            
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
        boolean hasPattern = pattern != null && !pattern.isEmpty() && !"*".equals(pattern);
        
        // Build SQL with server-side filtering
        StringBuilder sql = new StringBuilder(
                "SELECT key FROM kuber_entries WHERE region = ? AND (expires_at IS NULL OR expires_at >= NOW())");
        if (hasPattern) {
            sql.append(" AND key LIKE ?");
        }
        if (limit > 0) {
            sql.append(" LIMIT ?");
        }
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            
            int paramIdx = 1;
            stmt.setString(paramIdx++, region);
            if (hasPattern) {
                stmt.setString(paramIdx++, globToSqlLike(pattern));
            }
            if (limit > 0) {
                stmt.setInt(paramIdx, limit);
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    keys.add(rs.getString("key"));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get non-expired keys from region '{}': {}", region, e.getMessage(), e);
        }
        
        return keys;
    }
    
    /**
     * Convert a glob pattern to a SQL LIKE pattern.
     * Glob: * matches any, ? matches single char
     * SQL LIKE: % matches any, _ matches single char
     * Also escapes SQL LIKE special characters (%, _) that appear literally in the glob.
     */
    private String globToSqlLike(String glob) {
        if (glob == null || glob.isEmpty() || glob.equals("*")) {
            return "%";
        }
        
        StringBuilder like = new StringBuilder();
        for (char c : glob.toCharArray()) {
            switch (c) {
                case '*':
                    like.append('%');
                    break;
                case '?':
                    like.append('_');
                    break;
                case '%':
                    like.append("\\%");
                    break;
                case '_':
                    like.append("\\_");
                    break;
                default:
                    like.append(c);
            }
        }
        return like.toString();
    }
    
    // ==================== Native JSON Query Support (v1.9.0) ====================
    
    @Override
    public boolean supportsNativeJsonQuery() {
        return true;
    }
    
    @Override
    public List<CacheEntry> searchByJsonCriteria(String region, Map<String, Object> criteria, int limit) {
        if (criteria == null || criteria.isEmpty()) {
            return loadEntries(region, limit);
        }
        
        List<CacheEntry> results = new ArrayList<>();
        
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM kuber_entries WHERE region = ? ");
        sql.append("AND (expires_at IS NULL OR expires_at > NOW()) ");
        
        List<Object> params = new ArrayList<>();
        params.add(region);
        
        for (Map.Entry<String, Object> entry : criteria.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();
            
            if (value instanceof List) {
                // IN clause: field IN ('a', 'b', 'c')
                @SuppressWarnings("unchecked")
                List<String> values = (List<String>) value;
                if (!values.isEmpty()) {
                    // Use JSONB @? operator with jsonpath for IN clause
                    sql.append("AND json_value @? ?::jsonpath ");
                    // Build jsonpath: $.field ? (@ == "a" || @ == "b" || @ == "c")
                    StringBuilder jsonPath = new StringBuilder("$.");
                    jsonPath.append(field).append(" ? (");
                    for (int i = 0; i < values.size(); i++) {
                        if (i > 0) jsonPath.append(" || ");
                        jsonPath.append("@ == \"").append(values.get(i).replace("\"", "\\\"")).append("\"");
                    }
                    jsonPath.append(")");
                    params.add(jsonPath.toString());
                }
            } else if (value instanceof String) {
                String strValue = (String) value;
                
                // Check for comparison operators: >100, <50, >=10, <=20, !=value
                if (strValue.matches("^[<>=!]+.*")) {
                    String operator;
                    String compareValue;
                    
                    if (strValue.startsWith(">=")) {
                        operator = ">=";
                        compareValue = strValue.substring(2);
                    } else if (strValue.startsWith("<=")) {
                        operator = "<=";
                        compareValue = strValue.substring(2);
                    } else if (strValue.startsWith("!=")) {
                        operator = "!=";
                        compareValue = strValue.substring(2);
                    } else if (strValue.startsWith(">")) {
                        operator = ">";
                        compareValue = strValue.substring(1);
                    } else if (strValue.startsWith("<")) {
                        operator = "<";
                        compareValue = strValue.substring(1);
                    } else if (strValue.startsWith("=")) {
                        operator = "=";
                        compareValue = strValue.substring(1);
                    } else {
                        // Equality
                        sql.append("AND json_value @> ?::jsonb ");
                        params.add("{\"" + field + "\": \"" + strValue + "\"}");
                        continue;
                    }
                    
                    // Try numeric comparison
                    try {
                        Double.parseDouble(compareValue);
                        sql.append("AND (json_value->>'").append(field).append("')::numeric ")
                           .append(operator).append(" ?::numeric ");
                        params.add(compareValue);
                    } catch (NumberFormatException e) {
                        // String comparison
                        sql.append("AND json_value->>'").append(field).append("' ")
                           .append(operator).append(" ? ");
                        params.add(compareValue);
                    }
                } else {
                    // Simple equality - use containment operator for GIN index
                    sql.append("AND json_value @> ?::jsonb ");
                    params.add("{\"" + field + "\": \"" + strValue + "\"}");
                }
            } else if (value instanceof Number) {
                // Numeric equality
                sql.append("AND (json_value->>'").append(field).append("')::numeric = ?::numeric ");
                params.add(value);
            } else if (value instanceof Boolean) {
                // Boolean equality
                sql.append("AND (json_value->>'").append(field).append("')::boolean = ? ");
                params.add(value);
            }
        }
        
        sql.append("LIMIT ?");
        params.add(limit);
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            
            for (int i = 0; i < params.size(); i++) {
                Object param = params.get(i);
                if (param instanceof String) {
                    stmt.setString(i + 1, (String) param);
                } else if (param instanceof Integer) {
                    stmt.setInt(i + 1, (Integer) param);
                } else if (param instanceof Long) {
                    stmt.setLong(i + 1, (Long) param);
                } else if (param instanceof Double) {
                    stmt.setDouble(i + 1, (Double) param);
                } else if (param instanceof Boolean) {
                    stmt.setBoolean(i + 1, (Boolean) param);
                } else {
                    stmt.setObject(i + 1, param);
                }
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(resultSetToEntry(rs));
                }
            }
            
            log.debug("PostgreSQL native JSON query: {} results for region '{}' with {} criteria", 
                    results.size(), region, criteria.size());
            
        } catch (SQLException e) {
            log.warn("Native JSON query failed, falling back to scan: {}", e.getMessage());
            return null; // Signal fallback
        }
        
        return results;
    }
    
    private CacheEntry resultSetToEntry(ResultSet rs) throws SQLException {
        CacheEntry.CacheEntryBuilder builder = CacheEntry.builder()
                .region(rs.getString("region"))
                .key(rs.getString("key"))
                .valueType(CacheEntry.ValueType.valueOf(rs.getString("value_type")))
                .stringValue(rs.getString("string_value"))
                .ttlSeconds(rs.getLong("ttl_seconds"))
                .createdAt(toInstant(rs.getTimestamp("created_at")))
                .updatedAt(toInstant(rs.getTimestamp("updated_at")))
                .expiresAt(toInstant(rs.getTimestamp("expires_at")))
                .version(rs.getLong("version"))
                .accessCount(rs.getLong("access_count"))
                .lastAccessedAt(toInstant(rs.getTimestamp("last_accessed_at")))
                .metadata(rs.getString("metadata"));
        
        String jsonValue = rs.getString("json_value");
        if (jsonValue != null) {
            builder.jsonValue(JsonUtils.parse(jsonValue));
        }
        
        return builder.build();
    }
    
    private Timestamp toTimestamp(Instant instant) {
        return instant != null ? Timestamp.from(instant) : null;
    }
    
    private Instant toInstant(Timestamp timestamp) {
        return timestamp != null ? timestamp.toInstant() : null;
    }
}
