-- ============================================================================
-- Kuber Distributed Cache - PostgreSQL Schema
-- Version: 1.8.2
-- ============================================================================
--
-- PostgreSQL uses a SINGLE DATABASE with all regions stored in one
-- kuber_entries table, partitioned by the 'region' column.
--
-- Features:
--   - JSONB column for efficient JSON document storage and querying
--   - GIN index on json_value for fast JSON path queries
--   - HikariCP connection pooling (configurable pool size)
--   - Native TIMESTAMP WITH TIME ZONE for all temporal fields
--   - WAL-based durability (no explicit sync needed)
--
-- ============================================================================


-- ============================================================================
-- REGIONS TABLE
-- ============================================================================
-- Stores region definitions. One row per region.
-- ============================================================================

CREATE TABLE IF NOT EXISTS kuber_regions (
    name                VARCHAR(255)  PRIMARY KEY,       -- Region name (unique identifier)
    description         TEXT,                            -- Human-readable description
    captive             BOOLEAN       DEFAULT FALSE,     -- TRUE = captive (cannot be deleted via API)
    max_entries         BIGINT        DEFAULT -1,        -- Max entries allowed (-1 = unlimited)
    default_ttl_seconds BIGINT        DEFAULT -1,        -- Default TTL for entries (-1 = no expiry)
    entry_count         BIGINT        DEFAULT 0,         -- Cached entry count (approximate)
    created_at          TIMESTAMP WITH TIME ZONE,        -- When region was created
    updated_at          TIMESTAMP WITH TIME ZONE,        -- Last region metadata update
    created_by          VARCHAR(255),                    -- Username who created the region
    enabled             BOOLEAN       DEFAULT TRUE,      -- TRUE = enabled, FALSE = disabled
    collection_name     VARCHAR(255)                     -- Logical collection name (kuber_{region})
);


-- ============================================================================
-- ENTRIES TABLE
-- ============================================================================
-- Stores all cache entries across all regions.
-- Composite primary key: (region, key).
-- ============================================================================

CREATE TABLE IF NOT EXISTS kuber_entries (
    region           VARCHAR(255)  NOT NULL,              -- Region this entry belongs to
    key              VARCHAR(1024) NOT NULL,              -- Cache key (unique within region)
    value_type       VARCHAR(50)   NOT NULL,              -- Entry type: STRING, JSON, LIST, SET, HASH, ZSET, BINARY
    string_value     TEXT,                                -- String representation of the value
    json_value       JSONB,                               -- JSON document (PostgreSQL JSONB for indexing)
    ttl_seconds      BIGINT        DEFAULT -1,            -- Time-to-live in seconds (-1 = no expiry)
    created_at       TIMESTAMP WITH TIME ZONE,            -- When entry was created
    updated_at       TIMESTAMP WITH TIME ZONE,            -- Last value update
    expires_at       TIMESTAMP WITH TIME ZONE,            -- Expiration time (NULL = never expires)
    version          BIGINT        DEFAULT 1,             -- Optimistic locking version counter
    access_count     BIGINT        DEFAULT 0,             -- Number of times entry has been read
    last_accessed_at TIMESTAMP WITH TIME ZONE,            -- Last read time
    metadata         TEXT,                                -- Optional metadata string

    PRIMARY KEY (region, key)
);


-- ============================================================================
-- INDEXES
-- ============================================================================

-- Fast region-scoped lookups
CREATE INDEX IF NOT EXISTS idx_entries_region
    ON kuber_entries(region);

-- Efficient expired entry cleanup
CREATE INDEX IF NOT EXISTS idx_entries_expires
    ON kuber_entries(expires_at);

-- GIN index for JSONB queries (supports @>, ?, ?|, ?& operators)
CREATE INDEX IF NOT EXISTS idx_entries_json
    ON kuber_entries USING GIN (json_value);


-- ============================================================================
-- RECOMMENDED ADDITIONAL INDEXES (optional, for high-volume deployments)
-- ============================================================================

-- Composite index for non-expired key listing per region
-- Useful if getNonExpiredKeys is called frequently
-- CREATE INDEX IF NOT EXISTS idx_entries_region_expires
--     ON kuber_entries(region, expires_at);

-- Partial index for active (non-expired) entries only
-- Reduces index size by excluding expired entries
-- CREATE INDEX IF NOT EXISTS idx_entries_active
--     ON kuber_entries(region, key)
--     WHERE expires_at IS NULL OR expires_at >= NOW();


-- ============================================================================
-- CONNECTION POOL CONFIGURATION (HikariCP)
-- ============================================================================
--
-- Configured via application.properties:
--
--   kuber.persistence.postgresql.url=jdbc:postgresql://localhost:5432/kuber
--   kuber.persistence.postgresql.username=kuber
--   kuber.persistence.postgresql.password=kuber
--   kuber.persistence.postgresql.pool-size=10
--   kuber.persistence.postgresql.min-idle=2
--   kuber.persistence.postgresql.connection-timeout-ms=30000
--   kuber.persistence.postgresql.idle-timeout-ms=600000
--   kuber.persistence.postgresql.max-lifetime-ms=1800000
--
-- ============================================================================


-- ============================================================================
-- NOTES
-- ============================================================================
--
-- JSONB Queries:
--   PostgreSQL JSONB supports powerful server-side queries:
--
--   -- Find entries where json_value contains a specific key
--   SELECT * FROM kuber_entries WHERE json_value ? 'status';
--
--   -- Find entries matching a JSON path
--   SELECT * FROM kuber_entries WHERE json_value @> '{"status": "active"}';
--
--   -- Extract a JSON field
--   SELECT json_value->>'name' FROM kuber_entries WHERE region = 'users';
--
-- Pattern Matching:
--   Key pattern queries use LIKE with glob-to-SQL conversion:
--     Glob *  -> SQL %
--     Glob ?  -> SQL _
--
-- Maintenance:
--   PostgreSQL autovacuum handles cleanup automatically.
--   For large deletes, consider running VACUUM ANALYZE manually.
--
-- Streaming:
--   forEachEntry uses setFetchSize(1000) with auto-commit disabled
--   to enable PostgreSQL server-side cursors for memory-efficient iteration.
--
-- Example Queries:
--
--   -- Count non-expired entries in a region
--   SELECT COUNT(*) FROM kuber_entries
--   WHERE region = 'my_region'
--     AND (expires_at IS NULL OR expires_at >= NOW());
--
--   -- Find keys matching pattern in a region
--   SELECT key FROM kuber_entries
--   WHERE region = 'my_region' AND key LIKE 'user:%' LIMIT 100;
--
--   -- Delete all expired entries
--   DELETE FROM kuber_entries
--   WHERE expires_at IS NOT NULL AND expires_at < NOW();
--
-- ============================================================================
