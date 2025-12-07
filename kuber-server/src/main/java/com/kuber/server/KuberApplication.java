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
package com.kuber.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main entry point for the Kuber distributed cache server.
 * 
 * Kuber is a high-performance distributed cache that supports:
 * - Redis protocol for key/value operations
 * - NoSQL-like searches on JSON values
 * - Region-based data organization
 * - Automatic replication with primary/secondary failover
 * - Event subscription for cache operations
 * 
 * Startup sequence is managed by StartupOrchestrator:
 * 1. Spring context initialization
 * 2. Wait for stabilization (10 seconds)
 * 3. Persistence maintenance (compaction/vacuum)
 * 4. Cache service initialization
 * 5. Redis protocol server start
 * 6. Autoload service start
 * 
 * MongoDB auto-configuration is excluded because we handle MongoDB initialization
 * conditionally in MongoConfig only when kuber.persistence.type=mongodb.
 * 
 * @author Ashutosh Sinha
 * @version 1.3.10
 */
@SpringBootApplication(exclude = {
    MongoAutoConfiguration.class,
    MongoDataAutoConfiguration.class
})
@EnableAsync
@EnableScheduling
public class KuberApplication {
    
    public static void main(String[] args) {
        // Print banner
        printBanner();
        
        // Start Spring context - startup orchestration handled by StartupOrchestrator
        SpringApplication.run(KuberApplication.class, args);
    }
    
    private static void printBanner() {
        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════════════════╗");
        System.out.println("║                                                                   ║");
        System.out.println("║   ██╗  ██╗██╗   ██╗██████╗ ███████╗██████╗                        ║");
        System.out.println("║   ██║ ██╔╝██║   ██║██╔══██╗██╔════╝██╔══██╗                       ║");
        System.out.println("║   █████╔╝ ██║   ██║██████╔╝█████╗  ██████╔╝                       ║");
        System.out.println("║   ██╔═██╗ ██║   ██║██╔══██╗██╔══╝  ██╔══██╗                       ║");
        System.out.println("║   ██║  ██╗╚██████╔╝██████╔╝███████╗██║  ██║                       ║");
        System.out.println("║   ╚═╝  ╚═╝ ╚═════╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝                       ║");
        System.out.println("║                                                                   ║");
        System.out.println("║   High-Performance Distributed Cache                              ║");
        System.out.println("║   Version 1.3.7                                                  ║");
        System.out.println("║                                                                   ║");
        System.out.println("║   Copyright © 2025-2030 Ashutosh Sinha                            ║");
        System.out.println("║   All Rights Reserved                                             ║");
        System.out.println("║                                                                   ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════════╝");
        System.out.println();
    }
}
