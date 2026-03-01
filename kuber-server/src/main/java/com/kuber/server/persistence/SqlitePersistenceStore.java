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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SQLite implementation of PersistenceStore.
 * Each region gets its own dedicated SQLite database for better concurrency and isolation.
 * 
 * File structure:
 * - {basePath}/_metadata.db - SQLite database for region metadata
 * - {basePath}/{regionName}.db - Separate SQLite database for each region's entries
 * 
 * @version 2.6.4
 */
@Slf4j
public class SqlitePersistenceStore extends AbstractPersistenceStore {
    
    private static final String METADATA_DB = "_metadata.db";
    
    private final KuberProperties properties;
    private final String basePath;
    
    // Metadata database for storing region information
    private Connection metadataConnection;
    
    // Separate SQLite connection per region for entries
    private final Map<String, Connection> regionConnections = new ConcurrentHashMap<>();
    
    // Guard against double shutdown
    private volatile boolean alreadyShutdown = false;
    
    public SqlitePersistenceStore(KuberProperties properties) {
        this.properties = properties;
        // Extract directory from configured path (e.g., ./data/kuber.db -> ./data/sqlite)
        String configPath = properties.getPersistence().getSqlite().getPath();
        File configFile = new File(configPath);
        this.basePath = configFile.getParent() != null 
                ? configFile.getParent() + File.separator + "sqlite"
                : "./data/sqlite";
    }
    
    @Override
    public PersistenceType getType() {
        return PersistenceType.SQLITE;
    }
    
    @Override
    public void initialize() {
        log.info("Initializing SQLite persistence store at: {}", basePath);
        log.info("Using separate SQLite database per region for improved concurrency");
        
        try {
            // Ensure base directory exists
            File baseDir = new File(basePath);
            if (!baseDir.exists()) {
                baseDir.mkdirs();
            }
            
            // Load SQLite JDBC driver
            Class.forName("org.sqlite.JDBC");
            
            // Initialize metadata database
            initializeMetadataDb();
            
            // Discover and open existing region databases
            discoverExistingRegions();
            
            // Configure batched async persistence (v1.6.2)
            int batchSize = properties.getCache().getPersistenceBatchSize();
            int flushIntervalMs = properties.getCache().getPersistenceIntervalMs();
            configureBatching(batchSize, flushIntervalMs);
            
            // Note: Startup vacuum is now handled by PreStartupCompaction
            // which runs BEFORE Spring context initialization
            
            available = true;
            log.info("SQLite persistence store initialized successfully with {} region databases", 
                    regionConnections.size());
        } catch (Exception e) {
            log.error("Failed to initialize SQLite persistence store: {}", e.getMessage(), e);
            available = false;
        }
    }
    
    private void initializeMetadataDb() throws SQLException {
        String metadataPath = basePath + File.separator + METADATA_DB;
        metadataConnection = DriverManager.getConnection("jdbc:sqlite:" + metadataPath);
        metadataConnection.setAutoCommit(true);
        
        // Enable WAL mode
        try (Statement stmt = metadataConnection.createStatement()) {
            stmt.execute("PRAGMA journal_mode=WAL");
            stmt.execute("PRAGMA synchronous=NORMAL");
        }
        
        // Create regions table
        try (Statement stmt = metadataConnection.createStatement()) {
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
        }
        
        log.info("Opened metadata database at: {}", metadataPath);
    }
    
    private void discoverExistingRegions() {
        // Discover and open existing region databases from .db files
        // File names (without .db extension) ARE the region names
        File baseDir = new File(basePath);
        File[] dbFiles = baseDir.listFiles((dir, name) -> 
                name.endsWith(".db") && !name.equals(METADATA_DB));
        
        if (dbFiles != null) {
            for (File dbFile : dbFiles) {
                // Extract region name from filename (remove .db extension)
                String regionName = dbFile.getName().replace(".db", "");
                
                if (isValidRegionName(regionName)) {
                    try {
                        // Open the database for this region and add to map
                        Connection conn = getOrCreateRegionConnection(regionName);
                        if (conn != null) {
                            log.info("Discovered and opened region database: {}", regionName);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to open discovered region database '{}': {}", regionName, e.getMessage());
                    }
                }
            }
            if (!regionConnections.isEmpty()) {
                log.info("Discovered and opened {} existing region databases", regionConnections.size());
            }
        }
    }
    
    /**
     * Validate region name: alphanumeric and underscore only, at least one alphabetic character.
     */
    public static boolean isValidRegionName(String name) {
        if (name == null || name.isEmpty()) {
            return false;
        }
        // Must contain only alphanumeric and underscore
        if (!name.matches("^[a-zA-Z0-9_]+$")) {
            return false;
        }
        // Must contain at least one alphabetic character
        if (!name.matches(".*[a-zA-Z].*")) {
            return false;
        }
        return true;
    }
    
    // Lock for connection creation to prevent race conditions
    private final Object connCreationLock = new Object();
    
    private Connection getOrCreateRegionConnection(String region) {
        // Fast path: check if already exists
        Connection existingConn = regionConnections.get(region);
        if (existingConn != null) {
            return existingConn;
        }
        
        // Slow path: synchronized creation
        synchronized (connCreationLock) {
            // Double-check after acquiring lock
            existingConn = regionConnections.get(region);
            if (existingConn != null) {
                return existingConn;
            }
            
            // Create new connection
            Connection newConn = openRegionDatabase(region);
            regionConnections.put(region, newConn);
            return newConn;
        }
    }
    
    private Connection openRegionDatabase(String region) {
        String regionPath = basePath + File.separator + sanitizeRegionName(region) + ".db";
        
        try {
            Connection conn = DriverManager.getConnection("jdbc:sqlite:" + regionPath);
            conn.setAutoCommit(true);
            
            // Enable WAL mode for better concurrency
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("PRAGMA journal_mode=WAL");
                stmt.execute("PRAGMA synchronous=NORMAL");
                stmt.execute("PRAGMA cache_size=10000");
            }
            
            // Create entries table
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("""
                    CREATE TABLE IF NOT EXISTS kuber_entries (
                        key TEXT PRIMARY KEY,
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
                        metadata TEXT
                    )
                """);
                stmt.execute("CREATE INDEX IF NOT EXISTS idx_entries_expires ON kuber_entries(expires_at)");
            }
            
            log.info("Opened SQLite database for region '{}' at: {}", region, regionPath);
            return conn;
            
        } catch (SQLException e) {
            log.error("Failed to open SQLite for region '{}': {}", region, e.getMessage(), e);
            throw new RuntimeException("Failed to open region database", e);
        }
    }
    
    private String sanitizeRegionName(String region) {
        return region.replaceAll("[^a-zA-Z0-9_-]", "_");
    }
    
    @Override
    public void shutdown() {
        // Guard against double shutdown
        if (alreadyShutdown) {
            log.debug("SQLite shutdown already completed - skipping duplicate shutdown call");
            return;
        }
        alreadyShutdown = true;
        
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  SQLITE GRACEFUL SHUTDOWN INITIATED                                 ║");
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
        
        int totalRegions = regionConnections.size();
        int successCount = 0;
        int failCount = 0;
        
        log.info("Step 3: Closing {} region database(s)...", totalRegions);
        
        // Close all region connections with checkpoint
        for (Map.Entry<String, Connection> entry : regionConnections.entrySet()) {
            String regionName = entry.getKey();
            try {
                gracefulCloseConnection(regionName, entry.getValue());
                successCount++;
            } catch (Exception e) {
                log.error("Failed to gracefully close region '{}': {}", regionName, e.getMessage(), e);
                failCount++;
                // Still try to close
                try {
                    entry.getValue().close();
                } catch (SQLException closeEx) {
                    log.warn("Force close also failed for region '{}': {}", regionName, closeEx.getMessage());
                }
            }
        }
        regionConnections.clear();
        
        // Close metadata connection with checkpoint
        if (metadataConnection != null) {
            try {
                gracefulCloseConnection("_metadata", metadataConnection);
            } catch (Exception e) {
                log.warn("Error closing metadata connection: {}", e.getMessage());
                try {
                    metadataConnection.close();
                } catch (SQLException closeEx) {
                    log.warn("Force close also failed for metadata: {}", closeEx.getMessage());
                }
            }
        }
        
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  SQLITE SHUTDOWN COMPLETE                                           ║");
        log.info("║  Regions closed: {} success, {} failed                              ║", 
                String.format("%-3d", successCount), String.format("%-3d", failCount));
        log.info("╚════════════════════════════════════════════════════════════════════╝");
    }
    
    /**
     * Gracefully close a SQLite connection with proper checkpoint.
     * 
     * The sequence is:
     * 1. Run PRAGMA wal_checkpoint(TRUNCATE) to flush WAL to database
     * 2. Wait for any pending I/O
     * 3. Close the connection
     * 
     * @param name Name for logging
     * @param conn The connection to close
     */
    private void gracefulCloseConnection(String name, Connection conn) throws SQLException {
        log.info("Gracefully closing database '{}'...", name);
        long startTime = System.currentTimeMillis();
        
        try {
            // Step 1: Checkpoint the WAL file
            log.debug("  [{}] Running WAL checkpoint...", name);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("PRAGMA wal_checkpoint(TRUNCATE)");
            }
            log.debug("  [{}] WAL checkpoint complete", name);
            
            // Step 2: Wait for OS buffers to flush
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Step 3: Close the connection
            log.debug("  [{}] Closing connection...", name);
            conn.close();
            
            long elapsed = System.currentTimeMillis() - startTime;
            log.info("  [{}] Closed successfully in {}ms", name, elapsed);
            
        } catch (SQLException e) {
            throw new SQLException("Failed to gracefully close database: " + name, e);
        }
    }
    
    /**
     * Sync all SQLite databases to disk.
     * Runs WAL checkpoint on all connections to ensure durability.
     */
    @Override
    public void sync() {
        log.info("Syncing all SQLite databases to disk...");
        
        // Checkpoint all region databases
        for (Map.Entry<String, Connection> entry : regionConnections.entrySet()) {
            try (Statement stmt = entry.getValue().createStatement()) {
                stmt.execute("PRAGMA wal_checkpoint(PASSIVE)");
                log.debug("Synced region '{}' to disk", entry.getKey());
            } catch (SQLException e) {
                log.warn("Failed to sync region '{}': {}", entry.getKey(), e.getMessage());
            }
        }
        
        // Checkpoint metadata database
        if (metadataConnection != null) {
            try (Statement stmt = metadataConnection.createStatement()) {
                stmt.execute("PRAGMA wal_checkpoint(PASSIVE)");
                log.debug("Synced metadata database to disk");
            } catch (SQLException e) {
                log.warn("Failed to sync metadata database: {}", e.getMessage());
            }
        }
        
        log.info("All SQLite databases synced to disk");
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
        
        try (PreparedStatement stmt = metadataConnection.prepareStatement(sql)) {
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
            
            // Ensure region database exists
            getOrCreateRegionConnection(region.getName());
            
            log.info("Saved region '{}' to SQLite", region.getName());
        } catch (SQLException e) {
            log.error("Failed to save region '{}': {}", region.getName(), e.getMessage(), e);
            throw new RuntimeException("Failed to save region", e);
        }
    }
    
    @Override
    public List<CacheRegion> loadAllRegions() {
        List<CacheRegion> regions = new ArrayList<>();
        
        // Load regions based on discovered .db files (connections are already opened in discoverExistingRegions)
        for (String regionName : regionConnections.keySet()) {
            CacheRegion region = null;
            
            // Try to load metadata for this region
            String sql = "SELECT * FROM kuber_regions WHERE name = ?";
            try (PreparedStatement stmt = metadataConnection.prepareStatement(sql)) {
                stmt.setString(1, regionName);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        region = resultSetToRegion(rs);
                        log.debug("Loaded region metadata for '{}'", regionName);
                    }
                }
            } catch (SQLException e) {
                log.warn("Failed to load metadata for region '{}': {}", regionName, e.getMessage());
            }
            
            // If no metadata, create a default CacheRegion from file name
            if (region == null) {
                region = CacheRegion.builder()
                        .name(regionName)
                        .description("Discovered region")
                        .collectionName("kuber_" + regionName.toLowerCase())
                        .createdAt(Instant.now())
                        .updatedAt(Instant.now())
                        .build();
                log.info("Created default region metadata for discovered region '{}'", regionName);
                
                // Save the metadata for next time
                try {
                    saveRegion(region);
                } catch (Exception e) {
                    log.warn("Failed to save metadata for discovered region '{}': {}", regionName, e.getMessage());
                }
            }
            
            regions.add(region);
        }
        
        log.info("Loaded {} regions from SQLite (file discovery)", regions.size());
        return regions;
    }
    
    @Override
    public CacheRegion loadRegion(String name) {
        String sql = "SELECT * FROM kuber_regions WHERE name = ?";
        
        try (PreparedStatement stmt = metadataConnection.prepareStatement(sql)) {
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
            // Delete from metadata
            try (PreparedStatement stmt = metadataConnection.prepareStatement(
                    "DELETE FROM kuber_regions WHERE name = ?")) {
                stmt.setString(1, name);
                stmt.executeUpdate();
            }
            
            // Close and remove region connection
            Connection conn = regionConnections.remove(name);
            if (conn != null) {
                conn.close();
            }
            
            // Delete the region database file
            String regionPath = basePath + File.separator + sanitizeRegionName(name) + ".db";
            File dbFile = new File(regionPath);
            if (dbFile.exists()) {
                dbFile.delete();
            }
            // Also delete WAL and SHM files
            new File(regionPath + "-wal").delete();
            new File(regionPath + "-shm").delete();
            
            log.info("Deleted region '{}' and its database from SQLite", name);
        } catch (SQLException e) {
            log.error("Failed to delete region '{}': {}", name, e.getMessage(), e);
            throw new RuntimeException("Failed to delete region", e);
        }
    }
    
    @Override
    public void purgeRegion(String name) {
        Connection conn = regionConnections.get(name);
        if (conn != null) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM kuber_entries");
            } catch (SQLException e) {
                log.error("Failed to purge region '{}': {}", name, e.getMessage(), e);
                throw new RuntimeException("Failed to purge region", e);
            }
        }
        log.info("Purged region '{}' in SQLite", name);
    }
    
    // ==================== Entry Operations ====================
    
    @Override
    public void saveEntry(CacheEntry entry) {
        Connection conn = getOrCreateRegionConnection(entry.getRegion());
        
        String sql = """
            INSERT OR REPLACE INTO kuber_entries 
            (key, value_type, string_value, json_value, ttl_seconds, 
             created_at, updated_at, expires_at, version, access_count, last_accessed_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            setEntryParameters(stmt, entry);
            stmt.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to save entry '{}' to region '{}': {}", 
                    entry.getKey(), entry.getRegion(), e.getMessage(), e);
            throw new RuntimeException("Failed to save entry", e);
        }
    }
    
    @Override
    public void saveEntries(List<CacheEntry> entries) {
        if (entries.isEmpty()) return;
        
        // Group entries by region for batch inserts
        Map<String, List<CacheEntry>> entriesByRegion = new ConcurrentHashMap<>();
        for (CacheEntry entry : entries) {
            entriesByRegion.computeIfAbsent(entry.getRegion(), k -> new ArrayList<>()).add(entry);
        }
        
        String sql = """
            INSERT OR REPLACE INTO kuber_entries 
            (key, value_type, string_value, json_value, ttl_seconds, 
             created_at, updated_at, expires_at, version, access_count, last_accessed_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;
        
        for (Map.Entry<String, List<CacheEntry>> regionEntries : entriesByRegion.entrySet()) {
            String region = regionEntries.getKey();
            Connection conn = getOrCreateRegionConnection(region);
            
            try {
                conn.setAutoCommit(false);
                
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    for (CacheEntry entry : regionEntries.getValue()) {
                        setEntryParameters(stmt, entry);
                        stmt.addBatch();
                    }
                    stmt.executeBatch();
                }
                
                conn.commit();
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    log.warn("Rollback failed: {}", ex.getMessage());
                }
                log.error("Failed to batch save entries to region '{}': {}", region, e.getMessage(), e);
                throw new RuntimeException("Failed to batch save entries", e);
            } finally {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                    log.warn("Failed to reset auto-commit: {}", e.getMessage());
                }
            }
        }
    }
    
    private void setEntryParameters(PreparedStatement stmt, CacheEntry entry) throws SQLException {
        stmt.setString(1, entry.getKey());
        stmt.setString(2, entry.getValueType().name());
        stmt.setString(3, entry.getStringValue());
        stmt.setString(4, entry.getJsonValue() != null ? JsonUtils.toJson(entry.getJsonValue()) : null);
        stmt.setLong(5, entry.getTtlSeconds());
        stmt.setLong(6, entry.getCreatedAt() != null ? entry.getCreatedAt().toEpochMilli() : System.currentTimeMillis());
        stmt.setLong(7, entry.getUpdatedAt() != null ? entry.getUpdatedAt().toEpochMilli() : System.currentTimeMillis());
        
        if (entry.getExpiresAt() != null) {
            stmt.setLong(8, entry.getExpiresAt().toEpochMilli());
        } else {
            stmt.setNull(8, Types.INTEGER);
        }
        
        stmt.setLong(9, entry.getVersion());
        stmt.setLong(10, entry.getAccessCount());
        
        if (entry.getLastAccessedAt() != null) {
            stmt.setLong(11, entry.getLastAccessedAt().toEpochMilli());
        } else {
            stmt.setNull(11, Types.INTEGER);
        }
        
        stmt.setString(12, entry.getMetadata());
    }
    
    @Override
    public CacheEntry loadEntry(String region, String key) {
        Connection conn = regionConnections.get(region);
        if (conn == null) return null;
        
        String sql = "SELECT * FROM kuber_entries WHERE key = ?";
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, key);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return resultSetToEntry(rs, region);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to load entry '{}' from region '{}': {}", key, region, e.getMessage(), e);
        }
        
        return null;
    }
    
    @Override
    public java.util.Map<String, CacheEntry> loadEntriesByKeys(String region, List<String> keys) {
        java.util.Map<String, CacheEntry> result = new java.util.HashMap<>();
        Connection conn = regionConnections.get(region);
        if (conn == null || keys.isEmpty()) return result;
        
        // Build SQL with IN clause for batch retrieval
        StringBuilder sql = new StringBuilder("SELECT * FROM kuber_entries WHERE key IN (");
        for (int i = 0; i < keys.size(); i++) {
            sql.append(i > 0 ? ",?" : "?");
        }
        sql.append(")");
        
        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < keys.size(); i++) {
                stmt.setString(i + 1, keys.get(i));
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    CacheEntry entry = resultSetToEntry(rs, region);
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
        Connection conn = regionConnections.get(region);
        if (conn == null) return entries;
        
        // Order by last_accessed_at first (most recently accessed), then by updated_at
        // COALESCE handles null last_accessed_at by falling back to updated_at
        String sql = "SELECT * FROM kuber_entries WHERE (expires_at IS NULL OR expires_at >= ?) " +
                     "ORDER BY COALESCE(last_accessed_at, updated_at) DESC LIMIT ?";
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, System.currentTimeMillis());
            stmt.setInt(2, limit);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    entries.add(resultSetToEntry(rs, region));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to load entries from region '{}': {}", region, e.getMessage(), e);
        }
        
        return entries;
    }
    
    @Override
    public void deleteEntry(String region, String key) {
        Connection conn = regionConnections.get(region);
        if (conn == null) return;
        
        try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM kuber_entries WHERE key = ?")) {
            stmt.setString(1, key);
            stmt.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to delete entry '{}' from region '{}': {}", key, region, e.getMessage(), e);
            throw new RuntimeException("Failed to delete entry", e);
        }
    }
    
    @Override
    public void deleteEntries(String region, List<String> keys) {
        if (keys.isEmpty()) return;
        
        Connection conn = regionConnections.get(region);
        if (conn == null) return;
        
        String placeholders = String.join(",", keys.stream().map(k -> "?").toList());
        String sql = "DELETE FROM kuber_entries WHERE key IN (" + placeholders + ")";
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < keys.size(); i++) {
                stmt.setString(i + 1, keys.get(i));
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            log.error("Failed to delete entries from region '{}': {}", region, e.getMessage(), e);
            throw new RuntimeException("Failed to delete entries", e);
        }
    }
    
    @Override
    public long countEntries(String region) {
        Connection conn = regionConnections.get(region);
        if (conn == null) return 0;
        
        String sql = "SELECT COUNT(*) FROM kuber_entries";
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            log.error("Failed to count entries in region '{}': {}", region, e.getMessage(), e);
        }
        
        return 0;
    }
    
    @Override
    public List<String> getKeys(String region, String pattern, int limit) {
        List<String> keys = new ArrayList<>();
        Connection conn = regionConnections.get(region);
        if (conn == null) return keys;
        
        // Use server-side GLOB filtering and LIMIT
        String sql;
        boolean hasPattern = pattern != null && !pattern.isEmpty() && !"*".equals(pattern);
        
        if (hasPattern && limit > 0) {
            sql = "SELECT key FROM kuber_entries WHERE key GLOB ? LIMIT ?";
        } else if (hasPattern) {
            sql = "SELECT key FROM kuber_entries WHERE key GLOB ?";
        } else if (limit > 0) {
            sql = "SELECT key FROM kuber_entries LIMIT ?";
        } else {
            sql = "SELECT key FROM kuber_entries";
        }
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int paramIdx = 1;
            if (hasPattern) {
                stmt.setString(paramIdx++, pattern);
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
     * Fast estimate of entry count for dashboard display.
     * Uses max(rowid) as a fast O(1) approximation for SQLite.
     * Falls back to exact count if the approximation fails.
     */
    @Override
    public long estimateEntryCount(String region) {
        Connection conn = regionConnections.get(region);
        if (conn == null) return 0;
        
        // For SQLite, COUNT(*) is already fast with the B-tree, 
        // but we can use max(rowid) as a faster approximation
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM kuber_entries")) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            log.debug("Could not get estimated count for region '{}': {}", region, e.getMessage());
        }
        return 0;
    }
    
    /**
     * Iterate through all entries in a region without loading all into memory.
     * Uses streaming ResultSet for memory-efficient iteration - critical for startup
     * data loading and backup operations on large regions.
     * 
     * @param region Region name
     * @param consumer Consumer to process each entry
     * @return Number of entries processed
     * @since 1.9.0
     */
    @Override
    public long forEachEntry(String region, java.util.function.Consumer<CacheEntry> consumer) {
        Connection conn = regionConnections.get(region);
        if (conn == null) {
            log.warn("forEachEntry: Region '{}' connection not found", region);
            return 0;
        }
        
        long count = 0;
        String sql = "SELECT * FROM kuber_entries";
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                try {
                    CacheEntry entry = resultSetToEntry(rs, region);
                    if (!entry.isExpired()) {
                        consumer.accept(entry);
                        count++;
                    }
                } catch (Exception e) {
                    log.warn("Failed to parse entry during SQLite iteration: {}", e.getMessage());
                }
            }
        } catch (SQLException e) {
            log.error("Failed to iterate entries in region '{}': {}", region, e.getMessage(), e);
        }
        
        return count;
    }
    
    @Override
    public long deleteExpiredEntries(String region) {
        Connection conn = regionConnections.get(region);
        if (conn == null) return 0;
        
        String sql = "DELETE FROM kuber_entries WHERE expires_at IS NOT NULL AND expires_at < ?";
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, System.currentTimeMillis());
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
        long totalDeleted = 0;
        for (String region : regionConnections.keySet()) {
            totalDeleted += deleteExpiredEntries(region);
        }
        return totalDeleted;
    }
    
    @Override
    public long countNonExpiredEntries(String region) {
        Connection conn = regionConnections.get(region);
        if (conn == null) return 0;
        
        String sql = "SELECT COUNT(*) FROM kuber_entries WHERE expires_at IS NULL OR expires_at >= ?";
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, System.currentTimeMillis());
            
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
        Connection conn = regionConnections.get(region);
        if (conn == null) return keys;
        
        boolean hasPattern = pattern != null && !pattern.isEmpty() && !"*".equals(pattern);
        
        // Build SQL with server-side filtering
        StringBuilder sql = new StringBuilder("SELECT key FROM kuber_entries WHERE (expires_at IS NULL OR expires_at >= ?)");
        if (hasPattern) {
            sql.append(" AND key GLOB ?");
        }
        if (limit > 0) {
            sql.append(" LIMIT ?");
        }
        
        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            int paramIdx = 1;
            stmt.setLong(paramIdx++, System.currentTimeMillis());
            if (hasPattern) {
                stmt.setString(paramIdx++, pattern);
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
    
    // ==================== Native JSON Query Support (v1.9.0) ====================
    
    @Override
    public boolean supportsNativeJsonQuery() {
        return true;
    }
    
    @Override
    public List<CacheEntry> searchByJsonCriteria(String region, java.util.Map<String, Object> criteria, int limit) {
        if (criteria == null || criteria.isEmpty()) {
            return loadEntries(region, limit);
        }
        
        Connection conn = getOrCreateRegionConnection(region);
        if (conn == null) {
            return null;
        }
        
        List<CacheEntry> results = new ArrayList<>();
        
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM kuber_entries WHERE ");
        sql.append("(expires_at IS NULL OR expires_at > ?) ");
        
        List<Object> params = new ArrayList<>();
        params.add(System.currentTimeMillis());
        
        // Add criteria using json_extract
        for (java.util.Map.Entry<String, Object> entry : criteria.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();
            
            if (value instanceof List) {
                // IN clause: json_extract(json_value, '$.field') IN (?, ?, ?)
                @SuppressWarnings("unchecked")
                List<?> values = (List<?>) value;
                if (!values.isEmpty()) {
                    sql.append("AND json_extract(json_value, '$.").append(field).append("') IN (");
                    for (int i = 0; i < values.size(); i++) {
                        if (i > 0) sql.append(", ");
                        sql.append("?");
                        params.add(values.get(i).toString());
                    }
                    sql.append(") ");
                }
                
            } else if (value instanceof String) {
                String strValue = (String) value;
                
                // Check for comparison operators
                if (strValue.startsWith(">=")) {
                    sql.append("AND CAST(json_extract(json_value, '$.").append(field).append("') AS REAL) >= ? ");
                    params.add(parseNumeric(strValue.substring(2)));
                } else if (strValue.startsWith("<=")) {
                    sql.append("AND CAST(json_extract(json_value, '$.").append(field).append("') AS REAL) <= ? ");
                    params.add(parseNumeric(strValue.substring(2)));
                } else if (strValue.startsWith("!=")) {
                    sql.append("AND json_extract(json_value, '$.").append(field).append("') != ? ");
                    params.add(strValue.substring(2));
                } else if (strValue.startsWith(">")) {
                    sql.append("AND CAST(json_extract(json_value, '$.").append(field).append("') AS REAL) > ? ");
                    params.add(parseNumeric(strValue.substring(1)));
                } else if (strValue.startsWith("<")) {
                    sql.append("AND CAST(json_extract(json_value, '$.").append(field).append("') AS REAL) < ? ");
                    params.add(parseNumeric(strValue.substring(1)));
                } else if (strValue.startsWith("=")) {
                    sql.append("AND json_extract(json_value, '$.").append(field).append("') = ? ");
                    params.add(strValue.substring(1));
                } else {
                    // Equality
                    sql.append("AND json_extract(json_value, '$.").append(field).append("') = ? ");
                    params.add(strValue);
                }
            } else if (value instanceof Number) {
                sql.append("AND CAST(json_extract(json_value, '$.").append(field).append("') AS REAL) = ? ");
                params.add(((Number) value).doubleValue());
            } else if (value instanceof Boolean) {
                // SQLite stores JSON booleans as 1/0 or true/false
                sql.append("AND (json_extract(json_value, '$.").append(field).append("') = ? ");
                sql.append("OR json_extract(json_value, '$.").append(field).append("') = ?) ");
                boolean boolValue = (Boolean) value;
                params.add(boolValue ? 1 : 0);
                params.add(boolValue ? "true" : "false");
            }
        }
        
        sql.append("LIMIT ?");
        params.add(limit);
        
        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
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
                } else {
                    stmt.setObject(i + 1, param);
                }
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(resultSetToEntry(rs, region));
                }
            }
            
            log.debug("SQLite native JSON query: {} results for region '{}' with {} criteria", 
                    results.size(), region, criteria.size());
            
        } catch (SQLException e) {
            log.warn("SQLite native JSON query failed, falling back to scan: {}", e.getMessage());
            return null; // Signal fallback
        }
        
        return results;
    }
    
    /**
     * Parse a string to numeric for comparison queries.
     */
    private Object parseNumeric(String value) {
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            } else {
                return Long.parseLong(value);
            }
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
    
    // ==================== Helper Methods ====================
    
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
    
    private CacheEntry resultSetToEntry(ResultSet rs, String region) throws SQLException {
        CacheEntry.CacheEntryBuilder builder = CacheEntry.builder()
                .region(region)
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
    
    // ==================== Statistics ====================
    
    /**
     * Get SQLite statistics and storage info.
     */
    public SqliteStats getStats() {
        long sizeBytes = getDatabaseSizeBytes();
        int regionCount = regionConnections.size();
        
        long totalEntries = 0;
        for (String region : regionConnections.keySet()) {
            totalEntries += countNonExpiredEntries(region);
        }
        
        return new SqliteStats(basePath, sizeBytes, sizeBytes / (1024.0 * 1024.0), 
                regionCount, totalEntries, available);
    }
    
    /**
     * Get the total size of all SQLite databases in bytes.
     */
    public long getDatabaseSizeBytes() {
        File baseDir = new File(basePath);
        return calculateDirectorySize(baseDir);
    }
    
    /**
     * Get the size of a specific region's database in bytes.
     */
    public long getRegionSizeBytes(String region) {
        String regionPath = basePath + File.separator + sanitizeRegionName(region) + ".db";
        File dbFile = new File(regionPath);
        long size = dbFile.exists() ? dbFile.length() : 0;
        // Include WAL file size
        File walFile = new File(regionPath + "-wal");
        if (walFile.exists()) {
            size += walFile.length();
        }
        return size;
    }
    
    private long calculateDirectorySize(File dir) {
        if (!dir.exists()) return 0;
        
        long size = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                size += file.isFile() ? file.length() : calculateDirectorySize(file);
            }
        }
        return size;
    }
    
    /**
     * Get list of all region database names.
     */
    public List<String> getRegionNames() {
        return new ArrayList<>(regionConnections.keySet());
    }
    
    /**
     * Vacuum a specific region's database to reclaim space.
     */
    public void vacuumRegion(String region) {
        Connection conn = regionConnections.get(region);
        if (conn != null) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("VACUUM");
                log.info("Vacuumed region '{}' database", region);
            } catch (SQLException e) {
                log.error("Failed to vacuum region '{}': {}", region, e.getMessage(), e);
            }
        }
    }
    
    /**
     * Vacuum all region databases.
     */
    public void vacuumAll() {
        for (String region : regionConnections.keySet()) {
            vacuumRegion(region);
        }
    }
    
    /**
     * Vacuum the metadata database.
     */
    public void vacuumMetadata() {
        if (metadataConnection != null) {
            try (Statement stmt = metadataConnection.createStatement()) {
                stmt.execute("VACUUM");
                log.info("Vacuumed metadata database");
            } catch (SQLException e) {
                log.error("Failed to vacuum metadata database: {}", e.getMessage(), e);
            }
        }
    }
    
    /**
     * Run vacuum on all SQLite databases at startup.
     * This optimizes storage and reclaims space from deleted entries.
     */
    private void runStartupVacuum() {
        log.info("Running startup vacuum on all SQLite databases...");
        long startTime = System.currentTimeMillis();
        
        // Vacuum metadata database
        vacuumMetadata();
        
        // Vacuum all region databases
        int regionCount = regionConnections.size();
        vacuumAll();
        
        long duration = System.currentTimeMillis() - startTime;
        log.info("Startup vacuum completed: {} region database(s) + metadata in {}ms", 
                regionCount, duration);
    }
    
    /**
     * SQLite statistics.
     */
    public record SqliteStats(
            String path, long sizeBytes, double sizeMB, int regionCount, long totalEntries, boolean available
    ) {}
}
