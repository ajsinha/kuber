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
package com.kuber.server.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Filter that intercepts requests and authenticates via API Key.
 * 
 * API Key can be provided in:
 * 1. X-API-Key header
 * 2. Authorization header with "ApiKey" scheme: "Authorization: ApiKey kub_xxx"
 * 3. Query parameter: ?api_key=kub_xxx
 *
 * @version 1.4.1
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ApiKeyAuthenticationFilter extends OncePerRequestFilter {
    
    public static final String API_KEY_HEADER = "X-API-Key";
    public static final String AUTHORIZATION_HEADER = "Authorization";
    public static final String API_KEY_PREFIX = "ApiKey ";
    public static final String QUERY_PARAM = "api_key";
    
    private final ApiKeyService apiKeyService;
    
    @Override
    protected void doFilterInternal(HttpServletRequest request, 
                                    HttpServletResponse response, 
                                    FilterChain filterChain) throws ServletException, IOException {
        
        // Skip if already authenticated
        if (SecurityContextHolder.getContext().getAuthentication() != null 
                && SecurityContextHolder.getContext().getAuthentication().isAuthenticated()
                && !"anonymousUser".equals(SecurityContextHolder.getContext().getAuthentication().getPrincipal())) {
            filterChain.doFilter(request, response);
            return;
        }
        
        // Try to extract API key from various sources
        String apiKey = extractApiKey(request);
        
        if (apiKey != null) {
            Optional<ApiKey> validatedKey = apiKeyService.validateKey(apiKey);
            
            if (validatedKey.isPresent()) {
                ApiKey key = validatedKey.get();
                
                // Create authentication token
                List<SimpleGrantedAuthority> authorities = key.getRoles().stream()
                        .map(role -> new SimpleGrantedAuthority("ROLE_" + role))
                        .collect(Collectors.toList());
                
                UsernamePasswordAuthenticationToken authentication = 
                        new UsernamePasswordAuthenticationToken(
                                key.getUserId(),
                                null,
                                authorities
                        );
                
                // Store API key info for logging
                authentication.setDetails(new ApiKeyAuthenticationDetails(key.getKeyId(), key.getName()));
                
                SecurityContextHolder.getContext().setAuthentication(authentication);
                
                log.debug("Authenticated via API key: {} ({})", key.getKeyId(), key.getName());
            } else {
                log.debug("Invalid or expired API key provided");
            }
        }
        
        filterChain.doFilter(request, response);
    }
    
    /**
     * Extract API key from request (header, authorization, or query param)
     */
    private String extractApiKey(HttpServletRequest request) {
        // 1. Check X-API-Key header
        String apiKey = request.getHeader(API_KEY_HEADER);
        if (apiKey != null && !apiKey.isEmpty()) {
            return apiKey;
        }
        
        // 2. Check Authorization header with ApiKey scheme
        String authHeader = request.getHeader(AUTHORIZATION_HEADER);
        if (authHeader != null && authHeader.startsWith(API_KEY_PREFIX)) {
            return authHeader.substring(API_KEY_PREFIX.length()).trim();
        }
        
        // 3. Check query parameter
        apiKey = request.getParameter(QUERY_PARAM);
        if (apiKey != null && !apiKey.isEmpty()) {
            return apiKey;
        }
        
        return null;
    }
    
    /**
     * Details object to store API key info in the authentication
     */
    public static class ApiKeyAuthenticationDetails {
        private final String keyId;
        private final String keyName;
        
        public ApiKeyAuthenticationDetails(String keyId, String keyName) {
            this.keyId = keyId;
            this.keyName = keyName;
        }
        
        public String getKeyId() {
            return keyId;
        }
        
        public String getKeyName() {
            return keyName;
        }
        
        @Override
        public String toString() {
            return "ApiKey[" + keyId + ": " + keyName + "]";
        }
    }
}
