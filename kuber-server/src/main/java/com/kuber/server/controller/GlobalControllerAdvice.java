/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 */
package com.kuber.server.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;

/**
 * Global controller advice that injects common model attributes into all templates.
 * This includes the configurable application name, author, copyright, and other branding.
 */
@ControllerAdvice
public class GlobalControllerAdvice {
    
    @Value("${server.app.name:Kuber}")
    private String appName;
    
    @Value("${server.app.author:Ashutosh Sinha}")
    private String appAuthor;
    
    @Value("${server.app.email:ajsinha@gmail.com}")
    private String appEmail;
    
    @Value("${server.app.copyright:2025-2030}")
    private String appCopyright;
    
    @Value("${server.app.github:https://github.com/ajsinha/kuber}")
    private String appGithub;
    
    @Value("${kuber.version:2.6.4}")
    private String appVersion;
    
    @ModelAttribute("appName")
    public String getAppName() {
        return appName;
    }
    
    @ModelAttribute("appAuthor")
    public String getAppAuthor() {
        return appAuthor;
    }
    
    @ModelAttribute("appEmail")
    public String getAppEmail() {
        return appEmail;
    }
    
    @ModelAttribute("appCopyright")
    public String getAppCopyright() {
        return appCopyright;
    }
    
    @ModelAttribute("appGithub")
    public String getAppGithub() {
        return appGithub;
    }
    
    @ModelAttribute("appVersion")
    public String getAppVersion() {
        return appVersion;
    }
}
