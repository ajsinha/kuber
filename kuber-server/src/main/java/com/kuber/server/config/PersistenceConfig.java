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
package com.kuber.server.config;

import com.kuber.server.persistence.*;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Configuration for persistence store.
 * Creates the appropriate persistence store based on configuration.
 */
@Slf4j
@Configuration
public class PersistenceConfig {
    
    @Autowired
    private KuberProperties properties;
    
    @Autowired(required = false)
    private MongoDatabase mongoDatabase;
    
    @Bean
    @Primary
    public PersistenceStore persistenceStore() {
        String type = properties.getPersistence().getType().toLowerCase();
        log.info("Creating persistence store of type: {}", type);
        
        PersistenceStore store = switch (type) {
            case "mongodb", "mongo" -> createMongoPersistenceStore();
            case "sqlite" -> createSqlitePersistenceStore();
            case "postgresql", "postgres" -> createPostgresPersistenceStore();
            case "rocksdb", "rocks" -> createRocksDbPersistenceStore();
            case "lmdb" -> createLmdbPersistenceStore();
            case "memory", "mem" -> createMemoryPersistenceStore();
            default -> {
                log.warn("Unknown persistence type '{}', defaulting to RocksDB", type);
                yield createRocksDbPersistenceStore();
            }
        };
        
        // Initialize the store
        store.initialize();
        
        log.info("Persistence store initialized: {} (available: {})", 
                store.getType(), store.isAvailable());
        
        return store;
    }
    
    private PersistenceStore createMongoPersistenceStore() {
        if (mongoDatabase == null) {
            log.error("MongoDB is not configured but mongodb persistence type is selected");
            log.warn("Falling back to RocksDB persistence store");
            return createRocksDbPersistenceStore();
        }
        return new MongoPersistenceStore(mongoDatabase, properties);
    }
    
    private PersistenceStore createSqlitePersistenceStore() {
        log.info("Creating SQLite persistence store at: {}", 
                properties.getPersistence().getSqlite().getPath());
        return new SqlitePersistenceStore(properties);
    }
    
    private PersistenceStore createPostgresPersistenceStore() {
        log.info("Creating PostgreSQL persistence store at: {}", 
                properties.getPersistence().getPostgresql().getUrl());
        return new PostgresPersistenceStore(properties);
    }
    
    private PersistenceStore createRocksDbPersistenceStore() {
        log.info("Creating RocksDB persistence store at: {}", 
                properties.getPersistence().getRocksdb().getPath());
        return new RocksDbPersistenceStore(properties);
    }
    
    private PersistenceStore createLmdbPersistenceStore() {
        log.info("Creating LMDB persistence store at: {} (map size: {} MB)", 
                properties.getPersistence().getLmdb().getPath(),
                properties.getPersistence().getLmdb().getMapSize() / (1024 * 1024));
        return new LmdbPersistenceStore(properties);
    }
    
    private PersistenceStore createMemoryPersistenceStore() {
        log.info("Creating in-memory persistence store (data will not survive restarts)");
        return new MemoryPersistenceStore();
    }
}
