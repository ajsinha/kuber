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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kuber.server.config.KuberProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Service for managing API keys.
 * API keys file location is configurable via kuber.security.api-keys-file property.
 * Default: config/apikeys.json
 * 
 * API Key Format: kub_[64 random hex characters]
 * Example: kub_a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0u1v2w3x4y5z6
 *
 * @version 2.4.0
 */
@Slf4j
@Service
public class ApiKeyService {
    
    private static final String API_KEY_PREFIX = "kub_";
    private static final int KEY_LENGTH = 32;
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    
    private final KuberProperties properties;
    private final ObjectMapper objectMapper;
    
    // Cache: keyValue -> ApiKey
    private final Map<String, ApiKey> keyValueCache = new ConcurrentHashMap<>();
    
    // Cache: keyId -> ApiKey
    private final Map<String, ApiKey> keyIdCache = new ConcurrentHashMap<>();
    
    private Path apiKeysFilePath;
    
    public ApiKeyService(KuberProperties properties) {
        this.properties = properties;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
    
    @PostConstruct
    public void initialize() {
        // API keys file path from configuration (default: config/apikeys.json)
        String apiKeysFileSetting = properties.getSecurity().getApiKeysFile();
        apiKeysFilePath = Paths.get(apiKeysFileSetting);
        
        log.info("API Keys file path: {}", apiKeysFilePath);
        
        // Ensure parent directory exists
        try {
            Files.createDirectories(apiKeysFilePath.getParent());
        } catch (IOException e) {
            log.warn("Could not create API keys directory: {}", e.getMessage());
        }
        
        // Load existing keys
        loadApiKeys();
    }
    
    /**
     * Load API keys from file.
     * If file is missing, creates an empty one with a big warning.
     */
    private void loadApiKeys() {
        keyValueCache.clear();
        keyIdCache.clear();
        
        File file = apiKeysFilePath.toFile();
        if (!file.exists()) {
            logMissingFileWarning();
            createEmptyApiKeysFile(file);
            return;
        }
        
        try {
            List<ApiKey> keys = objectMapper.readValue(file, new TypeReference<List<ApiKey>>() {});
            int validKeys = 0;
            for (ApiKey key : keys) {
                if (key.getKeyValue() != null && !key.getKeyValue().isEmpty()) {
                    keyValueCache.put(key.getKeyValue(), key);
                    keyIdCache.put(key.getKeyId(), key);
                    validKeys++;
                    log.info("Loaded API key '{}' ({}): {} [active={}]", 
                            key.getName(), key.getKeyId(), maskKeyValue(key.getKeyValue()), key.isActive());
                } else {
                    log.error("⚠️ API key '{}' ({}) has NULL or EMPTY keyValue - cannot be used for authentication!", 
                            key.getName(), key.getKeyId());
                    keyIdCache.put(key.getKeyId(), key);  // Still put in ID cache for management
                }
            }
            log.info("✅ Loaded {} API keys from {} (valid keys in cache: {})", 
                    keys.size(), apiKeysFilePath, validKeys);
        } catch (IOException e) {
            log.error("Failed to load API keys: {}", e.getMessage());
        }
    }
    
    /**
     * Log a big, bold, unmistakable warning about missing apikeys.json.
     */
    private void logMissingFileWarning() {
        String filePath = apiKeysFilePath.toString();
        
        log.error("");
        log.error("╔═══════════════════════════════════════════════════════════════════════════════════════════════╗");
        log.error("║                                                                                               ║");
        log.error("║     █████╗ ██████╗ ██╗    ██╗  ██╗███████╗██╗   ██╗███████╗    ███╗   ███╗██╗███████╗███████╗ ║");
        log.error("║    ██╔══██╗██╔══██╗██║    ██║ ██╔╝██╔════╝╚██╗ ██╔╝██╔════╝    ████╗ ████║██║██╔════╝██╔════╝ ║");
        log.error("║    ███████║██████╔╝██║    █████╔╝ █████╗   ╚████╔╝ ███████╗    ██╔████╔██║██║███████╗███████╗ ║");
        log.error("║    ██╔══██║██╔═══╝ ██║    ██╔═██╗ ██╔══╝    ╚██╔╝  ╚════██║    ██║╚██╔╝██║██║╚════██║╚════██║ ║");
        log.error("║    ██║  ██║██║     ██║    ██║  ██╗███████╗   ██║   ███████║    ██║ ╚═╝ ██║██║███████║███████║ ║");
        log.error("║    ╚═╝  ╚═╝╚═╝     ╚═╝    ╚═╝  ╚═╝╚══════╝   ╚═╝   ╚══════╝    ╚═╝     ╚═╝╚═╝╚══════╝╚══════╝ ║");
        log.error("║                                                                                               ║");
        log.error("║    ⚠️  WARNING: API KEYS FILE NOT FOUND!  ⚠️                                                  ║");
        log.error("║                                                                                               ║");
        log.error("║    Location: {}", String.format("%-80s║", filePath));
        log.error("║                                                                                               ║");
        log.error("║    IMPACT:                                                                                    ║");
        log.error("║    • All programmatic access (Redis protocol, REST API, clients) will FAIL                   ║");
        log.error("║    • API Key authentication is REQUIRED as of v2.2.0                                         ║");
        log.error("║                                                                                               ║");
        log.error("║    ACTION REQUIRED:                                                                           ║");
        log.error("║    1. Log into Web UI with username/password                                                  ║");
        log.error("║    2. Navigate to Admin → API Keys                                                            ║");
        log.error("║    3. Click 'Generate New Key' to create API keys                                             ║");
        log.error("║                                                                                               ║");
        log.error("║    An empty apikeys.json file will be created automatically.                                  ║");
        log.error("║                                                                                               ║");
        log.error("╚═══════════════════════════════════════════════════════════════════════════════════════════════╝");
        log.error("");
    }
    
    /**
     * Create an empty apikeys.json file.
     */
    private void createEmptyApiKeysFile(File file) {
        try {
            objectMapper.writeValue(file, new ArrayList<ApiKey>());
            log.info("Created empty API keys file at: {}", apiKeysFilePath);
        } catch (IOException e) {
            log.error("Failed to create empty API keys file: {}", e.getMessage());
        }
    }
    
    /**
     * Save API keys to file
     */
    private synchronized void saveApiKeys() {
        try {
            List<ApiKey> keys = new ArrayList<>(keyIdCache.values());
            objectMapper.writeValue(apiKeysFilePath.toFile(), keys);
            log.debug("Saved {} API keys to {}", keys.size(), apiKeysFilePath);
        } catch (IOException e) {
            log.error("Failed to save API keys: {}", e.getMessage());
        }
    }
    
    /**
     * Generate a new API key
     */
    public ApiKey generateApiKey(String name, String userId, List<String> roles, Integer expirationDays) {
        String keyId = generateKeyId();
        String keyValue = generateKeyValue();
        
        Instant expiresAt = null;
        if (expirationDays != null && expirationDays > 0) {
            expiresAt = Instant.now().plusSeconds(expirationDays * 24L * 60L * 60L);
        }
        
        ApiKey apiKey = ApiKey.builder()
                .keyId(keyId)
                .keyValue(keyValue)
                .name(name)
                .userId(userId)
                .roles(roles != null ? roles : List.of("USER"))
                .createdAt(Instant.now())
                .expiresAt(expiresAt)
                .active(true)
                .build();
        
        keyValueCache.put(keyValue, apiKey);
        keyIdCache.put(keyId, apiKey);
        saveApiKeys();
        
        log.info("Generated new API key '{}' for user '{}' with roles {}", name, userId, roles);
        
        return apiKey;
    }
    
    /**
     * Generate a unique key ID
     */
    private String generateKeyId() {
        return "key_" + UUID.randomUUID().toString().substring(0, 8);
    }
    
    /**
     * Generate a secure key value
     */
    private String generateKeyValue() {
        byte[] bytes = new byte[KEY_LENGTH];
        SECURE_RANDOM.nextBytes(bytes);
        StringBuilder sb = new StringBuilder(API_KEY_PREFIX);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    
    /**
     * Validate an API key and return the associated ApiKey object
     */
    public Optional<ApiKey> validateKey(String keyValue) {
        if (keyValue == null || !keyValue.startsWith(API_KEY_PREFIX)) {
            return Optional.empty();
        }
        
        ApiKey apiKey = keyValueCache.get(keyValue);
        if (apiKey == null) {
            return Optional.empty();
        }
        
        if (!apiKey.isValid()) {
            return Optional.empty();
        }
        
        // Update last used timestamp
        apiKey.setLastUsedAt(Instant.now());
        saveApiKeys();
        
        return Optional.of(apiKey);
    }
    
    /**
     * Validate API key without saving/updating lastUsedAt.
     * Use this for high-throughput operations like messaging where
     * saving on every request would be a performance and security issue.
     * 
     * @param keyValue the API key value to validate
     * @return Optional containing the ApiKey if valid, empty otherwise
     */
    public Optional<ApiKey> validateKeyOnly(String keyValue) {
        if (keyValue == null) {
            log.warn("validateKeyOnly: keyValue is null");
            return Optional.empty();
        }
        
        // Trim whitespace that might come from message parsing
        String trimmedKey = keyValue.trim();
        
        if (!trimmedKey.startsWith(API_KEY_PREFIX)) {
            log.warn("validateKeyOnly: key doesn't start with prefix '{}'. Key: {}", 
                    API_KEY_PREFIX, maskKeyValue(trimmedKey));
            return Optional.empty();
        }
        
        log.debug("validateKeyOnly: keyValueCache has {} entries", keyValueCache.size());
        
        // Try with trimmed key
        ApiKey apiKey = keyValueCache.get(trimmedKey);
        if (apiKey == null) {
            log.warn("validateKeyOnly: key NOT FOUND in cache. Looking for: {} (cache size: {})", 
                    maskKeyValue(trimmedKey), keyValueCache.size());
            // Log cached keys for comparison (limit to prevent log spam)
            if (keyValueCache.size() <= 10) {
                keyValueCache.forEach((k, v) -> 
                    log.info("  cached key: {} -> '{}' (active={})", 
                            maskKeyValue(k), v.getName(), v.isActive()));
            }
            return Optional.empty();
        }
        
        if (!apiKey.isValid()) {
            log.warn("validateKeyOnly: key found but not valid (active={}, expired={})", 
                    apiKey.isActive(), apiKey.isExpired());
            return Optional.empty();
        }
        
        log.debug("validateKeyOnly: key validated successfully for '{}'", apiKey.getName());
        // Note: Does NOT update lastUsedAt or save - for performance/security
        return Optional.of(apiKey);
    }
    
    /**
     * Get all API keys (without exposing full key values)
     */
    public List<ApiKey> getAllKeys() {
        return new ArrayList<>(keyIdCache.values());
    }
    
    /**
     * Get API key by ID
     */
    public Optional<ApiKey> getKeyById(String keyId) {
        return Optional.ofNullable(keyIdCache.get(keyId));
    }
    
    /**
     * Revoke (deactivate) an API key
     */
    public boolean revokeKey(String keyId) {
        ApiKey apiKey = keyIdCache.get(keyId);
        if (apiKey == null) {
            return false;
        }
        
        apiKey.setActive(false);
        saveApiKeys();
        
        log.info("Revoked API key: {} ({})", keyId, apiKey.getName());
        return true;
    }
    
    /**
     * Reactivate an API key
     */
    public boolean activateKey(String keyId) {
        ApiKey apiKey = keyIdCache.get(keyId);
        if (apiKey == null) {
            return false;
        }
        
        apiKey.setActive(true);
        saveApiKeys();
        
        log.info("Activated API key: {} ({})", keyId, apiKey.getName());
        return true;
    }
    
    /**
     * Delete an API key permanently
     */
    public boolean deleteKey(String keyId) {
        ApiKey apiKey = keyIdCache.remove(keyId);
        if (apiKey == null) {
            return false;
        }
        
        keyValueCache.remove(apiKey.getKeyValue());
        saveApiKeys();
        
        log.info("Deleted API key: {} ({})", keyId, apiKey.getName());
        return true;
    }
    
    /**
     * Get keys for a specific user
     */
    public List<ApiKey> getKeysForUser(String userId) {
        return keyIdCache.values().stream()
                .filter(k -> k.getUserId().equals(userId))
                .collect(Collectors.toList());
    }
    
    /**
     * Reload API keys from file
     */
    public void reloadKeys() {
        loadApiKeys();
    }
    
    /**
     * Get the number of keys in the value cache.
     */
    public int getKeyCount() {
        return keyValueCache.size();
    }
    
    /**
     * Get statistics about API keys
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("totalKeys", keyIdCache.size());
        stats.put("activeKeys", keyIdCache.values().stream().filter(ApiKey::isActive).count());
        stats.put("expiredKeys", keyIdCache.values().stream().filter(ApiKey::isExpired).count());
        stats.put("filePath", apiKeysFilePath.toString());
        return stats;
    }
    
    /**
     * Mask an API key for display (show only first 8 and last 4 characters)
     */
    public static String maskKeyValue(String keyValue) {
        if (keyValue == null || keyValue.length() < 16) {
            return "****";
        }
        return keyValue.substring(0, 8) + "..." + keyValue.substring(keyValue.length() - 4);
    }
}
