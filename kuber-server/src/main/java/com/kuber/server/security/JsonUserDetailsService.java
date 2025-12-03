/*
 * Copyright (c) 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 *
 * Patent Pending: Certain architectural patterns and implementations described in
 * this module may be subject to patent applications.
 */
package com.kuber.server.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kuber.server.config.KuberProperties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ResourceLoader;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Simple user details service that reads users from a JSON file.
 * Uses cleartext password comparison for simplicity.
 */
@Slf4j
@Service
public class JsonUserDetailsService implements UserDetailsService {
    
    private final KuberProperties properties;
    private final ResourceLoader resourceLoader;
    private final ObjectMapper objectMapper;
    
    // Cache of users loaded from JSON file
    private final Map<String, JsonUser> usersCache = new ConcurrentHashMap<>();
    
    public JsonUserDetailsService(KuberProperties properties, ResourceLoader resourceLoader) {
        this.properties = properties;
        this.resourceLoader = resourceLoader;
        this.objectMapper = new ObjectMapper();
    }
    
    @PostConstruct
    public void loadUsers() {
        try {
            String usersFilePath = properties.getSecurity().getUsersFile();
            log.info("Loading users from: {}", usersFilePath);
            
            InputStream inputStream = resourceLoader.getResource(usersFilePath).getInputStream();
            JsonNode root = objectMapper.readTree(inputStream);
            JsonNode usersNode = root.get("users");
            
            if (usersNode != null && usersNode.isArray()) {
                for (JsonNode userNode : usersNode) {
                    JsonUser user = new JsonUser();
                    user.setUserId(userNode.get("userId").asText());
                    user.setPassword(userNode.get("password").asText());
                    user.setFullName(userNode.get("fullName").asText());
                    
                    List<String> roles = new ArrayList<>();
                    JsonNode rolesNode = userNode.get("roles");
                    if (rolesNode != null && rolesNode.isArray()) {
                        for (JsonNode roleNode : rolesNode) {
                            roles.add(roleNode.asText());
                        }
                    }
                    user.setRoles(roles);
                    
                    usersCache.put(user.getUserId().toLowerCase(), user);
                    log.debug("Loaded user: {} with roles: {}", user.getUserId(), user.getRoles());
                }
            }
            
            log.info("Loaded {} users from {}", usersCache.size(), usersFilePath);
            
        } catch (Exception e) {
            log.error("Failed to load users from file: {}", e.getMessage());
            // Create a default admin user if file loading fails
            JsonUser admin = new JsonUser();
            admin.setUserId("admin");
            admin.setPassword("admin123");
            admin.setFullName("Default Administrator");
            admin.setRoles(Arrays.asList("ADMIN", "OPERATOR", "USER"));
            usersCache.put("admin", admin);
            log.warn("Using default admin user due to file load failure");
        }
    }
    
    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        JsonUser jsonUser = usersCache.get(username.toLowerCase());
        
        if (jsonUser == null) {
            throw new UsernameNotFoundException("User not found: " + username);
        }
        
        List<SimpleGrantedAuthority> authorities = jsonUser.getRoles().stream()
                .map(role -> new SimpleGrantedAuthority("ROLE_" + role))
                .collect(Collectors.toList());
        
        // Use {noop} prefix to indicate cleartext password (no encoding)
        return User.builder()
                .username(jsonUser.getUserId())
                .password("{noop}" + jsonUser.getPassword())
                .authorities(authorities)
                .build();
    }
    
    /**
     * Get user by ID
     */
    public JsonUser getUser(String userId) {
        return usersCache.get(userId.toLowerCase());
    }
    
    /**
     * Get all users
     */
    public Collection<JsonUser> getAllUsers() {
        return new ArrayList<>(usersCache.values());
    }
    
    /**
     * Reload users from file
     */
    public void reloadUsers() {
        usersCache.clear();
        loadUsers();
    }
    
    /**
     * Simple user data class
     */
    @Data
    public static class JsonUser {
        private String userId;
        private String password;
        private String fullName;
        private List<String> roles = new ArrayList<>();
    }
}
