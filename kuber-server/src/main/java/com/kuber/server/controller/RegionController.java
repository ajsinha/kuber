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

import com.kuber.core.model.CacheRegion;
import com.kuber.server.cache.CacheService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.Collection;
import java.util.Map;

/**
 * Controller for region management operations.
 */
@Controller
@RequestMapping("/regions")
@RequiredArgsConstructor
public class RegionController {
    
    private final CacheService cacheService;
    
    @ModelAttribute
    public void addCurrentPage(Model model) {
        model.addAttribute("currentPage", "regions");
    }
    
    @GetMapping
    public String listRegions(Model model) {
        Collection<CacheRegion> regions = cacheService.getAllRegions();
        model.addAttribute("regions", regions);
        return "regions";
    }
    
    @GetMapping("/{name}")
    public String viewRegion(@PathVariable String name, Model model) {
        CacheRegion region = cacheService.getRegion(name);
        if (region == null) {
            return "redirect:/regions";
        }
        
        model.addAttribute("region", region);
        model.addAttribute("stats", cacheService.getStatistics(name));
        model.addAttribute("entryCount", cacheService.dbSize(name));
        
        return "region-detail";
    }
    
    @GetMapping("/create")
    @PreAuthorize("hasAnyRole('ADMIN', 'OPERATOR')")
    public String createRegionForm(Model model) {
        model.addAttribute("regionForm", new RegionForm());
        return "region-create";
    }
    
    @PostMapping("/create")
    @PreAuthorize("hasAnyRole('ADMIN', 'OPERATOR')")
    public String createRegion(@ModelAttribute RegionForm form,
                              RedirectAttributes redirectAttributes) {
        try {
            cacheService.createRegion(form.getName(), form.getDescription());
            redirectAttributes.addFlashAttribute("success", 
                    "Region '" + form.getName() + "' created successfully");
            return "redirect:/regions";
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", e.getMessage());
            return "redirect:/regions/create";
        }
    }
    
    @PostMapping("/{name}/purge")
    @PreAuthorize("hasAnyRole('ADMIN', 'OPERATOR')")
    public String purgeRegion(@PathVariable String name,
                             RedirectAttributes redirectAttributes) {
        try {
            cacheService.purgeRegion(name);
            redirectAttributes.addFlashAttribute("success", 
                    "Region '" + name + "' purged successfully");
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", e.getMessage());
        }
        return "redirect:/regions/" + name;
    }
    
    @PostMapping("/{name}/delete")
    @PreAuthorize("hasRole('ADMIN')")
    public String deleteRegion(@PathVariable String name,
                              RedirectAttributes redirectAttributes) {
        try {
            cacheService.deleteRegion(name);
            redirectAttributes.addFlashAttribute("success", 
                    "Region '" + name + "' deleted successfully");
            return "redirect:/regions";
        } catch (Exception e) {
            redirectAttributes.addFlashAttribute("error", e.getMessage());
            return "redirect:/regions/" + name;
        }
    }
    
    @Data
    public static class RegionForm {
        private String name;
        private String description;
        private long maxEntries = -1;
        private long defaultTtlSeconds = -1;
    }
}
