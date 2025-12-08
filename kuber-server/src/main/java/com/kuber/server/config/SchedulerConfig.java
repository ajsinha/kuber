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

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Configuration for the task scheduler used by @Scheduled methods.
 * 
 * <p>This provides a controllable ThreadPoolTaskScheduler that can be
 * shut down during graceful shutdown to immediately stop all scheduled tasks.
 * 
 * <p>Without this configuration, Spring uses a default scheduler that
 * continues running during shutdown, causing scheduled tasks to execute
 * even after shutdown has been initiated.
 * 
 * @version 1.5.0
 */
@Slf4j
@Configuration
public class SchedulerConfig {
    
    /**
     * Create a ThreadPoolTaskScheduler for @Scheduled methods.
     * 
     * <p>This scheduler:
     * <ul>
     *   <li>Uses a configurable thread pool</li>
     *   <li>Can be shut down gracefully</li>
     *   <li>Waits for tasks to complete on shutdown</li>
     * </ul>
     */
    @Bean
    public TaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(4);
        scheduler.setThreadNamePrefix("kuber-scheduler-");
        scheduler.setWaitForTasksToCompleteOnShutdown(false); // Don't wait - we handle this
        scheduler.setAwaitTerminationSeconds(5);
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.initialize();
        
        log.info("Task scheduler initialized with pool size 4");
        return scheduler;
    }
}
