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
import com.kuber.server.security.AuthorizationService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Controller for region management operations.
 * Enforces RBAC permissions for all operations.
 * 
 * @version 1.7.9
 */
@Slf4j
@Controller
@RequestMapping("/regions")
@RequiredArgsConstructor
public class RegionController {
    
    private final CacheService cacheService;
    private final AuthorizationService authorizationService;
    
    @ModelAttribute
    public void addCommonAttributes(Model model) {
        model.addAttribute("currentPage", "regions");
        model.addAttribute("isAdmin", authorizationService.isAdmin());
    }
    
    @GetMapping
    public String listRegions(Model model) {
        Collection<CacheRegion> allRegions = cacheService.getAllRegions();
        
        // Filter to accessible regions for non-admins
        Collection<CacheRegion> regions;
        if (authorizationService.isAdmin()) {
            regions = allRegions;
        } else {
            regions = allRegions.stream()
                    .filter(region -> authorizationService.canRead(region.getName()) ||
                                      authorizationService.canWrite(region.getName()) ||
                                      authorizationService.canDelete(region.getName()))
                    .collect(Collectors.toList());
        }
        
        log.debug("Listing {} accessible regions out of {} total", regions.size(), allRegions.size());
        model.addAttribute("regions", regions);
        return "regions";
    }
    
    @GetMapping("/{name}")
    public String viewRegion(@PathVariable String name, Model model,
                            RedirectAttributes redirectAttributes) {
        // Check if user has any access to this region
        if (!authorizationService.isAdmin() &&
            !authorizationService.canRead(name) && 
            !authorizationService.canWrite(name) && 
            !authorizationService.canDelete(name)) {
            redirectAttributes.addFlashAttribute("error", 
                    "You do not have permission to view region '" + name + "'");
            return "redirect:/regions";
        }
        
        CacheRegion region = cacheService.getRegion(name);
        if (region == null) {
            return "redirect:/regions";
        }
        
        model.addAttribute("region", region);
        model.addAttribute("stats", cacheService.getStatistics(name));
        model.addAttribute("entryCount", cacheService.dbSize(name));
        
        // Add permission flags for the view
        model.addAttribute("canRead", authorizationService.canRead(name));
        model.addAttribute("canWrite", authorizationService.canWrite(name));
        model.addAttribute("canDelete", authorizationService.canDelete(name));
        
        return "region-detail";
    }
    
    @GetMapping("/create")
    public String createRegionForm(Model model, RedirectAttributes redirectAttributes) {
        // Only admin can create regions
        if (!authorizationService.isAdmin()) {
            redirectAttributes.addFlashAttribute("error", "Admin permission required to create regions");
            return "redirect:/regions";
        }
        model.addAttribute("regionForm", new RegionForm());
        return "region-create";
    }
    
    @PostMapping("/create")
    public String createRegion(@ModelAttribute RegionForm form,
                              RedirectAttributes redirectAttributes) {
        // Only admin can create regions
        if (!authorizationService.isAdmin()) {
            redirectAttributes.addFlashAttribute("error", "Admin permission required to create regions");
            return "redirect:/regions";
        }
        
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
    public String purgeRegion(@PathVariable String name,
                             RedirectAttributes redirectAttributes) {
        // Require DELETE permission to purge
        if (!authorizationService.canDelete(name)) {
            redirectAttributes.addFlashAttribute("error", 
                    "You do not have DELETE permission for region '" + name + "'");
            return "redirect:/regions/" + name;
        }
        
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
    public String deleteRegion(@PathVariable String name,
                              RedirectAttributes redirectAttributes) {
        // Only admin can delete regions
        if (!authorizationService.isAdmin()) {
            redirectAttributes.addFlashAttribute("error", "Admin permission required to delete regions");
            return "redirect:/regions/" + name;
        }
        
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
