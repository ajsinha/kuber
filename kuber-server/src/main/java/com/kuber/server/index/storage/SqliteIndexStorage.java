/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.index.storage;

import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.sql.*;
import java.util.*;

/**
 * SQLite-based index storage for disk-persistent secondary indexes.
 * 
 * <p>SQLite provides a portable, SQL-queryable index storage that works
 * anywhere without native library dependencies (uses pure-Java driver).
 * 
 * <p><b>Schema:</b>
 * <pre>
 * CREATE TABLE kuber_index (
 *     index_name TEXT NOT NULL,
 *     field_value TEXT NOT NULL,
 *     cache_key TEXT NOT NULL,
 *     PRIMARY KEY (index_name, field_value, cache_key)
 * );
 * CREATE INDEX idx_lookup ON kuber_index(index_name, field_value);
 * CREATE INDEX idx_prefix ON kuber_index(index_name, field_value COLLATE NOCASE);
 * </pre>
 * 
 * <p><b>Characteristics:</b>
 * <ul>
 *   <li>Most portable (pure Java JDBC driver)</li>
 *   <li>SQL queryable for debugging</li>
 *   <li>Data persists across restarts</li>
 *   <li>Supports range and prefix queries</li>
 *   <li>Single-writer (serialized writes)</li>
 * </ul>
 * 
 * @version 1.9.0
 * @since 1.9.0
 */
@Slf4j
public class SqliteIndexStorage implements IndexStorageProvider {
    
    private static final String INDEX_DB_FILE = "kuber_indexes.db";
    
    private final KuberProperties properties;
    private final String dataDirectory;
    
    private Connection connection;
    private final Object writeLock = new Object();
    
    // Prepared statements cache
    private PreparedStatement insertStmt;
    private PreparedStatement deleteStmt;
    private PreparedStatement selectExactStmt;
    private PreparedStatement selectPrefixStmt;
    private PreparedStatement clearIndexStmt;
    private PreparedStatement clearRegionStmt;
    private PreparedStatement countIndexStmt;
    private PreparedStatement countMappingsStmt;
    
    public SqliteIndexStorage(KuberProperties properties) {
        this.properties = properties;
        this.dataDirectory = properties.getIndexing().getDiskDirectory();
    }
    
    @Override
    public void initialize() {
        try {
            // Create base index directory if needed
            File baseDir = new File(dataDirectory);
            if (!baseDir.exists()) {
                if (baseDir.mkdirs()) {
                    log.info("Created index directory: {}", baseDir.getAbsolutePath());
                }
            }
            
            // Create sqlite subfolder inside index directory
            File sqliteDir = new File(baseDir, "sqlite");
            if (!sqliteDir.exists()) {
                if (sqliteDir.mkdirs()) {
                    log.info("Created SQLite index subfolder: {}", sqliteDir.getAbsolutePath());
                }
            }
            
            String dbPath = sqliteDir.getAbsolutePath() + File.separator + INDEX_DB_FILE;
            
            // Load SQLite JDBC driver
            Class.forName("org.sqlite.JDBC");
            
            // Connect with optimized settings
            String url = "jdbc:sqlite:" + dbPath;
            connection = DriverManager.getConnection(url);
            
            // Configure SQLite for performance
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("PRAGMA journal_mode=WAL");
                stmt.execute("PRAGMA synchronous=NORMAL");
                stmt.execute("PRAGMA cache_size=" + (properties.getIndexing().getDiskCacheSizeMb() * 1024));
                stmt.execute("PRAGMA temp_store=MEMORY");
                stmt.execute("PRAGMA mmap_size=268435456"); // 256MB memory-mapped I/O
            }
            
            // Create schema
            createSchema();
            
            // Prepare statements
            prepareStatements();
            
            log.info("╔══════════════════════════════════════════════════════════════╗");
            log.info("║         SQLITE INDEX STORAGE INITIALIZED                      ║");
            log.info("╠══════════════════════════════════════════════════════════════╣");
            log.info("║  Path:       {}",  String.format("%-46s║", dbPath));
            log.info("║  Cache:      {} MB", String.format("%-43d║", properties.getIndexing().getDiskCacheSizeMb()));
            log.info("║  Journal:    WAL", String.format("%-46s║", ""));
            log.info("╚══════════════════════════════════════════════════════════════╝");
            
        } catch (Exception e) {
            log.error("Failed to initialize SQLite index storage: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to initialize SQLite index storage", e);
        }
    }
    
    private void createSchema() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Main index table
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS kuber_index (
                    index_name TEXT NOT NULL,
                    field_value TEXT NOT NULL,
                    cache_key TEXT NOT NULL,
                    PRIMARY KEY (index_name, field_value, cache_key)
                ) WITHOUT ROWID
                """);
            
            // Index for exact lookups
            stmt.execute("""
                CREATE INDEX IF NOT EXISTS idx_lookup 
                ON kuber_index(index_name, field_value)
                """);
            
            // Index for range/prefix queries
            stmt.execute("""
                CREATE INDEX IF NOT EXISTS idx_range 
                ON kuber_index(index_name, field_value COLLATE BINARY)
                """);
        }
    }
    
    private void prepareStatements() throws SQLException {
        insertStmt = connection.prepareStatement(
            "INSERT OR IGNORE INTO kuber_index (index_name, field_value, cache_key) VALUES (?, ?, ?)");
        
        deleteStmt = connection.prepareStatement(
            "DELETE FROM kuber_index WHERE index_name = ? AND field_value = ? AND cache_key = ?");
        
        selectExactStmt = connection.prepareStatement(
            "SELECT cache_key FROM kuber_index WHERE index_name = ? AND field_value = ?");
        
        selectPrefixStmt = connection.prepareStatement(
            "SELECT cache_key FROM kuber_index WHERE index_name = ? AND field_value LIKE ? ESCAPE '\\'");
        
        clearIndexStmt = connection.prepareStatement(
            "DELETE FROM kuber_index WHERE index_name = ?");
        
        clearRegionStmt = connection.prepareStatement(
            "DELETE FROM kuber_index WHERE index_name LIKE ?");
        
        countIndexStmt = connection.prepareStatement(
            "SELECT COUNT(DISTINCT field_value) FROM kuber_index WHERE index_name = ?");
        
        countMappingsStmt = connection.prepareStatement(
            "SELECT COUNT(*) FROM kuber_index WHERE index_name = ?");
    }
    
    @Override
    public void addToIndex(String indexName, Object fieldValue, String cacheKey) {
        if (connection == null || fieldValue == null || cacheKey == null) return;
        
        synchronized (writeLock) {
            try {
                insertStmt.setString(1, indexName);
                insertStmt.setString(2, fieldValue.toString());
                insertStmt.setString(3, cacheKey);
                insertStmt.executeUpdate();
            } catch (SQLException e) {
                log.error("Failed to add to index {}: {}", indexName, e.getMessage());
            }
        }
    }
    
    @Override
    public void removeFromIndex(String indexName, Object fieldValue, String cacheKey) {
        if (connection == null || fieldValue == null || cacheKey == null) return;
        
        synchronized (writeLock) {
            try {
                deleteStmt.setString(1, indexName);
                deleteStmt.setString(2, fieldValue.toString());
                deleteStmt.setString(3, cacheKey);
                deleteStmt.executeUpdate();
            } catch (SQLException e) {
                log.error("Failed to remove from index {}: {}", indexName, e.getMessage());
            }
        }
    }
    
    @Override
    public Set<String> getExact(String indexName, Object fieldValue) {
        if (connection == null || fieldValue == null) return Collections.emptySet();
        
        Set<String> results = new HashSet<>();
        
        try {
            selectExactStmt.setString(1, indexName);
            selectExactStmt.setString(2, fieldValue.toString());
            
            try (ResultSet rs = selectExactStmt.executeQuery()) {
                while (rs.next()) {
                    results.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get from index {}: {}", indexName, e.getMessage());
        }
        
        return results;
    }
    
    @Override
    public Set<String> getRange(String indexName, Object fromValue, Object toValue) {
        if (connection == null) return Collections.emptySet();
        
        Set<String> results = new HashSet<>();
        
        StringBuilder sql = new StringBuilder(
            "SELECT cache_key FROM kuber_index WHERE index_name = ?");
        
        List<String> params = new ArrayList<>();
        params.add(indexName);
        
        if (fromValue != null) {
            sql.append(" AND field_value >= ?");
            params.add(fromValue.toString());
        }
        if (toValue != null) {
            sql.append(" AND field_value <= ?");
            params.add(toValue.toString());
        }
        
        try (PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                stmt.setString(i + 1, params.get(i));
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            log.error("Failed range query on index {}: {}", indexName, e.getMessage());
        }
        
        return results;
    }
    
    @Override
    public Set<String> getByPrefix(String indexName, String prefix) {
        if (connection == null || prefix == null) return Collections.emptySet();
        
        Set<String> results = new HashSet<>();
        
        try {
            // Escape special LIKE characters
            String escapedPrefix = prefix
                    .replace("\\", "\\\\")
                    .replace("%", "\\%")
                    .replace("_", "\\_") + "%";
            
            selectPrefixStmt.setString(1, indexName);
            selectPrefixStmt.setString(2, escapedPrefix);
            
            try (ResultSet rs = selectPrefixStmt.executeQuery()) {
                while (rs.next()) {
                    results.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            log.error("Failed prefix query on index {}: {}", indexName, e.getMessage());
        }
        
        return results;
    }
    
    @Override
    public void clearIndex(String indexName) {
        if (connection == null) return;
        
        synchronized (writeLock) {
            try {
                clearIndexStmt.setString(1, indexName);
                int deleted = clearIndexStmt.executeUpdate();
                log.debug("Cleared index {}: {} entries", indexName, deleted);
            } catch (SQLException e) {
                log.error("Failed to clear index {}: {}", indexName, e.getMessage());
            }
        }
    }
    
    @Override
    public void clearRegion(String region) {
        if (connection == null) return;
        
        synchronized (writeLock) {
            try {
                clearRegionStmt.setString(1, region + ":%");
                int deleted = clearRegionStmt.executeUpdate();
                log.debug("Cleared all indexes for region {}: {} entries", region, deleted);
            } catch (SQLException e) {
                log.error("Failed to clear region {}: {}", region, e.getMessage());
            }
        }
    }
    
    @Override
    public long getIndexSize(String indexName) {
        if (connection == null) return 0;
        
        try {
            countIndexStmt.setString(1, indexName);
            try (ResultSet rs = countIndexStmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to count index {}: {}", indexName, e.getMessage());
        }
        
        return 0;
    }
    
    @Override
    public long getTotalMappings(String indexName) {
        if (connection == null) return 0;
        
        try {
            countMappingsStmt.setString(1, indexName);
            try (ResultSet rs = countMappingsStmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to count mappings in index {}: {}", indexName, e.getMessage());
        }
        
        return 0;
    }
    
    @Override
    public boolean supportsRangeQueries() {
        return true;
    }
    
    @Override
    public boolean supportsPrefixQueries() {
        return true;
    }
    
    @Override
    public boolean isPersistent() {
        return true;
    }
    
    @Override
    public long getMemoryUsage() {
        // SQLite cache is configurable via PRAGMA
        return properties.getIndexing().getDiskCacheSizeMb() * 1024L * 1024L;
    }
    
    @Override
    public StorageType getStorageType() {
        return StorageType.SQLITE;
    }
    
    @Override
    public void flush() {
        if (connection == null) return;
        
        synchronized (writeLock) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("PRAGMA wal_checkpoint(PASSIVE)");
                log.debug("Flushed SQLite index storage");
            } catch (SQLException e) {
                log.error("Failed to flush SQLite: {}", e.getMessage());
            }
        }
    }
    
    @Override
    public void compact() {
        if (connection == null) return;
        
        synchronized (writeLock) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("VACUUM");
                log.info("Compacted SQLite index storage");
            } catch (SQLException e) {
                log.error("Failed to compact SQLite: {}", e.getMessage());
            }
        }
    }
    
    @Override
    public void shutdown() {
        try {
            // Close prepared statements
            if (insertStmt != null) insertStmt.close();
            if (deleteStmt != null) deleteStmt.close();
            if (selectExactStmt != null) selectExactStmt.close();
            if (selectPrefixStmt != null) selectPrefixStmt.close();
            if (clearIndexStmt != null) clearIndexStmt.close();
            if (clearRegionStmt != null) clearRegionStmt.close();
            if (countIndexStmt != null) countIndexStmt.close();
            if (countMappingsStmt != null) countMappingsStmt.close();
            
            // Final checkpoint and close
            if (connection != null) {
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("PRAGMA wal_checkpoint(TRUNCATE)");
                }
                connection.close();
            }
            
            log.info("Shutdown SQLite index storage");
            
        } catch (SQLException e) {
            log.error("Error during SQLite shutdown: {}", e.getMessage());
        }
    }
}
