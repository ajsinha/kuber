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

import com.kuber.server.replication.ReplicationManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Set;

/**
 * Controller for help documentation pages.
 * All help pages are accessible without authentication.
 */
@Controller
@RequestMapping("/help")
public class HelpController {
    
    @Autowired(required = false)
    private ReplicationManager replicationManager;
    
    // Valid section names
    private static final Set<String> VALID_SECTIONS = Set.of(
        "overview", "quickstart", "web-ui", "config",
        "rest-api", "redis-protocol", "generic-search", "autoload",
        "python-client", "java-client",
        "string-ops", "json-ops", "hash-ops", "key-ops", "ttl-ops", "batch-ops",
        "regions", "search-ops", "replication", "glossary"
    );
    
    /**
     * Help index page with card navigation.
     */
    @GetMapping("")
    public String helpIndex(Model model) {
        model.addAttribute("currentPage", "help");
        model.addAttribute("isPrimary", replicationManager == null || replicationManager.isPrimary());
        return "help/index";
    }
    
    /**
     * Individual help section pages.
     */
    @GetMapping("/{section}")
    public String helpSection(@PathVariable String section, Model model) {
        model.addAttribute("currentPage", "help");
        model.addAttribute("isPrimary", replicationManager == null || replicationManager.isPrimary());
        model.addAttribute("section", section);
        
        // Validate section name to prevent directory traversal
        if (!VALID_SECTIONS.contains(section)) {
            return "redirect:/help";
        }
        
        // Return the section-specific template
        return "help/" + section;
    }
}
