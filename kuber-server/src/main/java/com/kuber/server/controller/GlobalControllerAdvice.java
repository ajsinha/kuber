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
package com.kuber.server.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;

/**
 * Global controller advice that injects common model attributes into all templates.
 * This includes the configurable application name.
 */
@ControllerAdvice
public class GlobalControllerAdvice {
    
    @Value("${server.app.name:Kuber}")
    private String appName;
    
    /**
     * Inject the application name into all model attributes.
     * Templates can use ${appName} to display the configurable application name.
     */
    @ModelAttribute("appName")
    public String getAppName() {
        return appName;
    }
}
