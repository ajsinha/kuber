/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.server.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

/**
 * MongoDB configuration for Kuber persistence.
 * Only activated when kuber.persistence.type=mongodb explicitly.
 * When using other persistence types (rocksdb, sqlite, postgresql, memory),
 * this configuration is completely disabled and no MongoDB connection is attempted.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnProperty(name = "kuber.persistence.type", havingValue = "mongodb")
public class MongoConfig {
    
    private final KuberProperties properties;
    private MongoClient mongoClient;
    
    @Bean
    public MongoClient mongoClient() {
        KuberProperties.Mongo mongoProps = properties.getMongo();
        
        ConnectionString connectionString = new ConnectionString(mongoProps.getUri());
        
        MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applyToConnectionPoolSettings(builder -> builder
                        .maxSize(mongoProps.getConnectionPoolSize())
                        .minSize(5)
                        .maxWaitTime(mongoProps.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS))
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(mongoProps.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS)
                        .readTimeout(mongoProps.getSocketTimeoutMs(), TimeUnit.MILLISECONDS));
        
        if (mongoProps.isWriteConcernAcknowledged()) {
            settingsBuilder.writeConcern(WriteConcern.ACKNOWLEDGED);
        } else {
            settingsBuilder.writeConcern(WriteConcern.UNACKNOWLEDGED);
        }
        
        mongoClient = MongoClients.create(settingsBuilder.build());
        
        log.info("MongoDB client initialized: {}", mongoProps.getUri());
        
        return mongoClient;
    }
    
    @Bean
    public MongoDatabase mongoDatabase(MongoClient mongoClient) {
        return mongoClient.getDatabase(properties.getMongo().getDatabase());
    }
    
    @PreDestroy
    public void cleanup() {
        if (mongoClient != null) {
            log.info("Closing MongoDB client");
            mongoClient.close();
        }
    }
}
