-- ============================================================================
-- Kuber Distributed Cache - SQLite Schema
-- Version: 1.8.2
-- ============================================================================
--
-- SQLite uses a SEPARATE DATABASE FILE PER REGION for better concurrency
-- and isolation. The file layout is:
--
--   {basePath}/_metadata.db       -> Region metadata (kuber_regions table)
--   {basePath}/{regionName}.db    -> One SQLite DB per region (kuber_entries table)
--
-- Default basePath: ./data/sqlite/
--
-- Each database runs with:
--   PRAGMA journal_mode = WAL          (write-ahead logging for concurrency)
--   PRAGMA synchronous  = NORMAL       (balanced durability/performance)
--   PRAGMA cache_size   = 10000        (region DBs only)
--
-- ============================================================================


-- ============================================================================
-- METADATA DATABASE: _metadata.db
-- ============================================================================
-- Stores region definitions. One row per region.
-- ============================================================================

CREATE TABLE IF NOT EXISTS kuber_regions (
    name                TEXT    PRIMARY KEY,          -- Region name (unique identifier)
    description         TEXT,                         -- Human-readable description
    captive             INTEGER DEFAULT 0,            -- 1 = captive region (cannot be deleted via API)
    max_entries         INTEGER DEFAULT -1,           -- Max entries allowed (-1 = unlimited)
    default_ttl_seconds INTEGER DEFAULT -1,           -- Default TTL for entries (-1 = no expiry)
    entry_count         INTEGER DEFAULT 0,            -- Cached entry count (approximate)
    created_at          INTEGER,                      -- Epoch millis - when region was created
    updated_at          INTEGER,                      -- Epoch millis - last region metadata update
    created_by          TEXT,                         -- Username who created the region
    enabled             INTEGER DEFAULT 1,            -- 1 = enabled, 0 = disabled
    collection_name     TEXT                          -- Logical collection name (kuber_{region})
);


-- ============================================================================
-- REGION DATABASE: {regionName}.db  (one per region)
-- ============================================================================
-- Stores cache entries for a single region. Each region gets its own
-- SQLite database file for improved write concurrency and isolation.
-- ============================================================================

CREATE TABLE IF NOT EXISTS kuber_entries (
    key              TEXT    PRIMARY KEY,              -- Cache key (unique within region)
    value_type       TEXT    NOT NULL,                 -- Entry type: STRING, JSON, LIST, SET, HASH, ZSET, BINARY
    string_value     TEXT,                             -- String representation of the value
    json_value       TEXT,                             -- JSON document (stored as TEXT in SQLite)
    ttl_seconds      INTEGER DEFAULT -1,              -- Time-to-live in seconds (-1 = no expiry)
    created_at       INTEGER,                         -- Epoch millis - when entry was created
    updated_at       INTEGER,                         -- Epoch millis - last value update
    expires_at       INTEGER,                         -- Epoch millis - expiration time (NULL = never)
    version          INTEGER DEFAULT 1,               -- Optimistic locking version counter
    access_count     INTEGER DEFAULT 0,               -- Number of times entry has been read
    last_accessed_at INTEGER,                         -- Epoch millis - last read time
    metadata         TEXT                             -- Optional metadata string
);

-- Index for efficient expired entry cleanup
CREATE INDEX IF NOT EXISTS idx_entries_expires
    ON kuber_entries(expires_at);


-- ============================================================================
-- NOTES
-- ============================================================================
--
-- Timestamps:
--   All timestamps are stored as INTEGER (epoch milliseconds).
--   Use datetime(column/1000, 'unixepoch') to convert to human-readable.
--
-- Booleans:
--   SQLite has no native BOOLEAN. Uses INTEGER: 0 = false, 1 = true.
--
-- Pattern Matching:
--   Key pattern queries use SQLite's native GLOB operator (supports * and ?).
--
-- Maintenance:
--   Run VACUUM periodically on each region DB to reclaim space.
--   Run PRAGMA wal_checkpoint(TRUNCATE) before shutdown for clean close.
--
-- Example Queries:
--
--   -- Count non-expired entries
--   SELECT COUNT(*) FROM kuber_entries
--   WHERE expires_at IS NULL OR expires_at >= (strftime('%s','now') * 1000);
--
--   -- Find keys matching pattern
--   SELECT key FROM kuber_entries WHERE key GLOB 'user:*' LIMIT 100;
--
--   -- Delete expired entries
--   DELETE FROM kuber_entries
--   WHERE expires_at IS NOT NULL AND expires_at < (strftime('%s','now') * 1000);
--
-- ============================================================================
