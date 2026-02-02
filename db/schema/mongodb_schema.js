// ============================================================================
// Kuber Distributed Cache - MongoDB Schema
// Version: 1.8.2
// ============================================================================
//
// MongoDB uses SEPARATE COLLECTIONS PER REGION for better performance
// and isolation. The collection layout is:
//
//   kuber_regions                    -> Region metadata (one document per region)
//   kuber_{region_name}             -> One collection per region's cache entries
//
// Collection naming convention:
//   Region name is lowercased, non-alphanumeric chars (except _) replaced with _
//   Example: "Mock_Trades" -> "kuber_mock_trades"
//
// Database name is configured via:
//   kuber.persistence.mongodb.database=kuber
//
// ============================================================================


// ============================================================================
// DATABASE SETUP
// ============================================================================

// use kuber;   // or your configured database name


// ============================================================================
// COLLECTION: kuber_regions
// ============================================================================
// Stores region definitions. One document per region.
// ============================================================================

// Document Structure:
// {
//     "name":              String,     // Region name (unique identifier, indexed)
//     "description":       String,     // Human-readable description
//     "captive":           Boolean,    // true = captive region (cannot be deleted via API)
//     "maxEntries":        Long,       // Max entries allowed (-1 = unlimited)
//     "defaultTtlSeconds": Long,       // Default TTL for new entries (-1 = no expiry)
//     "entryCount":        Long,       // Cached entry count (approximate)
//     "createdAt":         ISODate,    // When region was created
//     "updatedAt":         ISODate,    // Last region metadata update
//     "createdBy":         String,     // Username who created the region
//     "enabled":           Boolean,    // true = enabled, false = disabled
//     "collectionName":    String      // Actual MongoDB collection name (kuber_{region})
// }

// Index: unique lookup by region name
db.kuber_regions.createIndex(
    { "name": 1 },
    { unique: true, name: "idx_regions_name" }
);


// ============================================================================
// COLLECTION: kuber_{region_name}  (one per region)
// ============================================================================
// Stores cache entries for a single region.
// Collection is created automatically when a region is first saved.
// ============================================================================

// Document Structure:
// {
//     "key":            String,       // Cache key (unique within collection, indexed)
//     "valueType":      String,       // Entry type: "STRING", "JSON", "LIST", "SET", "HASH", "ZSET", "BINARY"
//     "stringValue":    String,       // String representation of the value
//     "jsonValue":      Object,       // Embedded JSON document (native BSON, queryable)
//     "ttlSeconds":     Long,         // Time-to-live in seconds (-1 = no expiry)
//     "createdAt":      ISODate,      // When entry was created
//     "updatedAt":      ISODate,      // Last value update
//     "expiresAt":      ISODate,      // Expiration time (null = never expires, indexed)
//     "version":        Long,         // Optimistic locking version counter
//     "accessCount":    Long,         // Number of times entry has been read
//     "lastAccessedAt": ISODate,      // Last read time
//     "metadata":       String        // Optional metadata string
// }

// Example: Create indexes for a region collection named "kuber_mock_trades"
// These indexes are created automatically by the server for each new region.

// Index: unique lookup by cache key
db.kuber_mock_trades.createIndex(
    { "key": 1 },
    { unique: true, name: "idx_entries_key" }
);

// Index: efficient expired entry cleanup and non-expired queries
db.kuber_mock_trades.createIndex(
    { "expiresAt": 1 },
    { name: "idx_entries_expires" }
);


// ============================================================================
// HELPER FUNCTION: Create indexes for a new region collection
// ============================================================================
// Run this when manually creating a region collection.
// The Kuber server does this automatically via ensureRegionCollection().
// ============================================================================

function createRegionIndexes(regionCollectionName) {
    var collection = db.getCollection(regionCollectionName);
    collection.createIndex({ "key": 1 },       { unique: true, name: "idx_entries_key" });
    collection.createIndex({ "expiresAt": 1 },  { name: "idx_entries_expires" });
    print("Created indexes for collection: " + regionCollectionName);
}

// Usage: createRegionIndexes("kuber_my_new_region");


// ============================================================================
// RECOMMENDED ADDITIONAL INDEXES (optional, for high-volume deployments)
// ============================================================================

// Compound index for sorting by last access time (used by loadEntries)
// db.kuber_mock_trades.createIndex(
//     { "lastAccessedAt": -1, "updatedAt": -1 },
//     { name: "idx_entries_access_sort" }
// );

// Text index for full-text search on string values
// db.kuber_mock_trades.createIndex(
//     { "stringValue": "text" },
//     { name: "idx_entries_text" }
// );

// Wildcard index on jsonValue for flexible JSON queries
// db.kuber_mock_trades.createIndex(
//     { "jsonValue.$**": 1 },
//     { name: "idx_entries_json_wildcard" }
// );


// ============================================================================
// CONNECTION CONFIGURATION
// ============================================================================
//
// Configured via application.properties:
//
//   kuber.persistence.mongodb.uri=mongodb://localhost:27017
//   kuber.persistence.mongodb.database=kuber
//
// Write Concern:
//   Default is ACKNOWLEDGED - writes are durable once acknowledged.
//   No explicit sync/fsync needed.
//
// ============================================================================


// ============================================================================
// NOTES
// ============================================================================
//
// JSON Storage:
//   MongoDB stores jsonValue as native BSON, making it directly queryable:
//
//   // Find entries where jsonValue contains status = "active"
//   db.kuber_mock_trades.find({ "jsonValue.status": "active" });
//
//   // Find entries with amount > 1000
//   db.kuber_mock_trades.find({ "jsonValue.amount": { $gt: 1000 } });
//
//   // Find entries matching nested path
//   db.kuber_mock_trades.find({ "jsonValue.address.city": "New York" });
//
// Pattern Matching:
//   Key pattern queries use MongoDB $regex for server-side filtering:
//
//   // Find keys matching glob pattern "user:*"
//   db.kuber_mock_trades.find({ "key": { $regex: "^user:.*$" } }, { "key": 1 });
//
// Entry Count Estimation:
//   estimateEntryCount uses estimatedDocumentCount() which reads collection
//   metadata (O(1)) instead of scanning documents.
//
// Streaming:
//   forEachEntry uses MongoDB cursor iteration for memory-efficient
//   processing of large collections without loading all into memory.
//
// Example Queries:
//
//   // Count non-expired entries
//   db.kuber_mock_trades.countDocuments({
//       $or: [
//           { "expiresAt": null },
//           { "expiresAt": { $gte: new Date() } }
//       ]
//   });
//
//   // Delete all expired entries
//   db.kuber_mock_trades.deleteMany({
//       "expiresAt": { $ne: null, $lt: new Date() }
//   });
//
//   // Get keys matching pattern (non-expired, limited)
//   db.kuber_mock_trades.find(
//       {
//           $and: [
//               { $or: [{ "expiresAt": null }, { "expiresAt": { $gte: new Date() } }] },
//               { "key": { $regex: "^trade_" } }
//           ]
//       },
//       { "key": 1, "_id": 0 }
//   ).limit(100);
//
// ============================================================================
