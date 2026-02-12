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

import java.io.InputStream;
import java.util.Properties;

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
 * @version 2.3.0
 */
@SpringBootApplication(exclude = {
    MongoAutoConfiguration.class,
    MongoDataAutoConfiguration.class
})
@EnableAsync
@EnableScheduling
public class KuberApplication {
    
    private static final String DEFAULT_VERSION = "2.3.0";
    
    public static void main(String[] args) {
        // Allow %2F in URLs - must be set before Tomcat initializes
        System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
        
        // Print banner
        printBanner();
        
        // Start Spring context - startup orchestration handled by StartupOrchestrator
        SpringApplication.run(KuberApplication.class, args);
    }
    
    /**
     * Read version from application.properties.
     * Falls back to DEFAULT_VERSION if not found.
     * 
     * @since 1.6.1
     */
    private static String getVersion() {
        try (InputStream input = KuberApplication.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input != null) {
                Properties props = new Properties();
                props.load(input);
                return props.getProperty("kuber.version", DEFAULT_VERSION);
            }
        } catch (Exception e) {
            // Ignore - use default
        }
        return DEFAULT_VERSION;
    }
    
    private static void printBanner() {
        String version = getVersion();
        // Pad version to fit in banner (need 57 chars total for the content area)
        String versionLine = String.format("║   Version %-48s║", version);
        
        System.out.println();
        System.out.println("╔═══════════════════════════════════════════════════════════════════╗");
        System.out.println("║                                                                   ║");
        System.out.println("║   ██╗  ██╗██╗   ██║██████╗ ███████╗██████╗                        ║");
        System.out.println("║   ██║ ██╔╝██║   ██║██╔══██╗██╔════╝██╔══██╗                       ║");
        System.out.println("║   █████╔╝ ██║   ██║██████╔╝█████╗  ██████╔╝                       ║");
        System.out.println("║   ██╔═██╗ ██║   ██║██╔══██╗██╔══╝  ██╔══██╗                       ║");
        System.out.println("║   ██║  ██╗╚██████╔╝██████╔╝███████╗██║  ██║                       ║");
        System.out.println("║   ╚═╝  ╚═╝ ╚═════╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝                       ║");
        System.out.println("║                                                                   ║");
        System.out.println("║   High-Performance Distributed Cache                              ║");
        System.out.println(versionLine);
        System.out.println("║                                                                   ║");
        System.out.println("║   Copyright © 2025-2030 Ashutosh Sinha                            ║");
        System.out.println("║   All Rights Reserved                                             ║");
        System.out.println("║                                                                   ║");
        System.out.println("╚═══════════════════════════════════════════════════════════════════╝");
        System.out.println();
    }
}
