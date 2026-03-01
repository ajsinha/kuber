# Kuber Database Schemas

Version: 2.6.3

This directory contains the database schema definitions for all supported SQL/NoSQL persistence backends.

## Files

| File | Backend | Format | Notes |
|------|---------|--------|-------|
| `sqlite_schema.sql` | SQLite | SQL DDL | Separate DB per region (`{region}.db`) + metadata DB (`_metadata.db`) |
| `postgresql_schema.sql` | PostgreSQL | SQL DDL | Single database, all regions in one `kuber_entries` table |
| `mongodb_schema.js` | MongoDB | JavaScript (mongo shell) | Separate collection per region (`kuber_{region}`) |

## Architecture Differences

### SQLite
- **Isolation Model**: One `.db` file per region + one `_metadata.db`
- **Timestamps**: Stored as `INTEGER` (epoch milliseconds)
- **Booleans**: Stored as `INTEGER` (0/1)
- **JSON**: Stored as `TEXT`
- **Pattern Matching**: Uses native `GLOB` operator

### PostgreSQL
- **Isolation Model**: Single database, composite PK `(region, key)` on entries table
- **Timestamps**: Native `TIMESTAMP WITH TIME ZONE`
- **Booleans**: Native `BOOLEAN`
- **JSON**: Stored as `JSONB` with GIN index for server-side queries
- **Pattern Matching**: Uses `LIKE` operator (glob converted to SQL wildcards)

### MongoDB
- **Isolation Model**: Separate collection per region (`kuber_{region_name}`)
- **Timestamps**: Native `ISODate`
- **Booleans**: Native `Boolean`
- **JSON**: Native BSON embedded document (directly queryable)
- **Pattern Matching**: Uses `$regex` operator

## Schema is Auto-Created

The Kuber server **automatically creates** all required tables, collections, and indexes on startup. These schema files are provided for:

- Documentation and reference
- Manual database setup or pre-provisioning
- DBA review and capacity planning
- Custom migration scripts

## Related Configuration

See `docs/APPLICATION_PROPERTIES.md` for persistence backend configuration options, or configure via `application.properties`:

```properties
# Choose backend: mongodb, sqlite, postgresql, rocksdb, lmdb, memory
kuber.persistence.type=sqlite

# SQLite
kuber.persistence.sqlite.path=./data/kuber.db

# PostgreSQL
kuber.persistence.postgresql.url=jdbc:postgresql://localhost:5432/kuber
kuber.persistence.postgresql.username=kuber
kuber.persistence.postgresql.password=kuber

# MongoDB
kuber.persistence.mongodb.uri=mongodb://localhost:27017
kuber.persistence.mongodb.database=kuber
```
