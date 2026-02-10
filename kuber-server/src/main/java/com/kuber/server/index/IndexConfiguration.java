/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Legal Notice: This module and the associated software architecture are proprietary
 * and confidential. Unauthorized copying, distribution, modification, or use is
 * strictly prohibited without explicit written permission from the copyright holder.
 */
package com.kuber.server.index;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import jakarta.annotation.PostConstruct;
import java.io.*;
import java.nio.file.*;
import java.util.*;

/**
 * Loads and manages index configuration from index.yaml file.
 * 
 * <p>Configuration file location is specified via application property:
 * {@code kuber.indexing.config-file} which defaults to ${kuber.base.datadir}/index.yaml
 * 
 * <p>Supports hot-reloading when configuration file changes and
 * auto-saves when indexes are created or removed via the UI/API.
 * 
 * @version 2.1.0
 * @since 1.9.0
 */
@Component
@Slf4j
public class IndexConfiguration {
    
    private final ResourceLoader resourceLoader;
    
    @Value("${kuber.base.datadir:./data}")
    private String baseDataDir;
    
    @Value("${kuber.indexing.config-file:}")
    private String configFilePathOverride;
    
    @Value("${kuber.indexing.watch-for-changes:true}")
    private boolean watchForChanges;
    
    @Value("${kuber.indexing.auto-save:true}")
    private boolean autoSaveEnabled;
    
    // Resolved config file path
    private String configFilePath;
    
    // Parsed configuration
    private volatile IndexingSettings indexingSettings;
    private volatile Map<String, RegionIndexConfig> regionConfigs;
    
    // Listeners for configuration changes
    private final List<IndexConfigurationListener> listeners = new ArrayList<>();
    
    // File watcher thread
    private Thread watcherThread;
    private volatile boolean watching = false;
    
    // Flag to prevent save during load
    private volatile boolean loading = false;
    
    public IndexConfiguration(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
        this.indexingSettings = new IndexingSettings();
        this.regionConfigs = new HashMap<>();
    }
    
    @PostConstruct
    public void init() {
        // Resolve config file path
        if (configFilePathOverride != null && !configFilePathOverride.isEmpty()) {
            configFilePath = configFilePathOverride;
        } else {
            // Default to data directory
            configFilePath = baseDataDir + "/index.yaml";
        }
        
        // Ensure the file path is absolute or file: prefixed for non-classpath
        if (!configFilePath.startsWith("classpath:") && !configFilePath.startsWith("file:")) {
            // Convert to absolute path
            Path path = Paths.get(configFilePath);
            if (!path.isAbsolute()) {
                path = path.toAbsolutePath();
            }
            configFilePath = path.toString();
        }
        
        log.info("Index configuration file: {}", configFilePath);
        
        // Create default config file if it doesn't exist
        createDefaultConfigIfNotExists();
        
        loadConfiguration();
        
        if (watchForChanges && !configFilePath.startsWith("classpath:")) {
            startFileWatcher();
        }
    }
    
    /**
     * Create default index.yaml if it doesn't exist.
     */
    private void createDefaultConfigIfNotExists() {
        if (configFilePath.startsWith("classpath:")) {
            return;
        }
        
        try {
            Path path = Paths.get(configFilePath);
            if (!Files.exists(path)) {
                // Ensure parent directories exist
                Files.createDirectories(path.getParent());
                
                // Write default configuration
                String defaultConfig = """
                    # Kuber Secondary Index Configuration
                    # This file is auto-managed by Kuber. Manual edits are supported.
                    # Changes are detected and applied automatically.
                    #
                    # Documentation: /help/secondary-indexing
                    
                    indexing:
                      enabled: true
                      storage: hybrid           # memory, rocksdb, or hybrid
                      rebuild-on-startup: true  # Rebuild indexes from data on startup
                      rebuild-threads: 4        # Parallel threads for rebuild
                      max-memory-per-region-mb: 256
                      max-indexes-per-region: 20
                      persistence:
                        enabled: true
                        sync-interval-seconds: 30
                    
                    # Define indexes per region
                    # Example:
                    # regions:
                    #   customers:
                    #     indexes:
                    #       - field: status
                    #         type: hash
                    #         description: "Customer status for filtering"
                    #       - field: age
                    #         type: btree
                    #         description: "Age for range queries"
                    
                    regions: {}
                    """;
                
                Files.writeString(path, defaultConfig);
                log.info("Created default index configuration: {}", path);
            }
        } catch (IOException e) {
            log.warn("Could not create default index config file: {}", e.getMessage());
        }
    }
    
    /**
     * Load configuration from YAML file.
     */
    @SuppressWarnings("unchecked")
    public synchronized void loadConfiguration() {
        loading = true;
        try {
            log.info("Loading index configuration from: {}", configFilePath);
            
            InputStream inputStream;
            
            if (configFilePath.startsWith("classpath:")) {
                Resource resource = resourceLoader.getResource(configFilePath);
                if (!resource.exists()) {
                    log.warn("Index configuration file not found: {}. Using defaults.", configFilePath);
                    return;
                }
                inputStream = resource.getInputStream();
            } else {
                Path path = Paths.get(configFilePath);
                if (!Files.exists(path)) {
                    log.warn("Index configuration file not found: {}. Using defaults.", configFilePath);
                    return;
                }
                inputStream = Files.newInputStream(path);
            }
            
            try {
                Yaml yaml = new Yaml();
                Map<String, Object> config = yaml.load(inputStream);
                
                if (config == null) {
                    log.warn("Empty index configuration file");
                    return;
                }
                
                // Parse indexing settings
                Map<String, Object> indexingMap = (Map<String, Object>) config.get("indexing");
                if (indexingMap != null) {
                    indexingSettings = parseIndexingSettings(indexingMap);
                }
                
                // Parse region configurations
                Map<String, Object> regionsMap = (Map<String, Object>) config.get("regions");
                if (regionsMap != null) {
                    regionConfigs = parseRegionConfigs(regionsMap);
                } else {
                    regionConfigs = new HashMap<>();
                }
                
                log.info("╔══════════════════════════════════════════════════════════════╗");
                log.info("║         INDEX CONFIGURATION LOADED                           ║");
                log.info("╠══════════════════════════════════════════════════════════════╣");
                log.info("║  Indexing Enabled:  {}                                       ║", 
                    String.format("%-5s", indexingSettings.isEnabled()));
                log.info("║  Storage Mode:      {}                                    ║", 
                    String.format("%-8s", indexingSettings.getStorage()));
                log.info("║  Regions Defined:   {}                                       ║", 
                    String.format("%-5d", regionConfigs.size()));
                
                int totalIndexes = regionConfigs.values().stream()
                    .mapToInt(r -> r.getIndexes().size())
                    .sum();
                log.info("║  Total Indexes:     {}                                       ║", 
                    String.format("%-5d", totalIndexes));
                log.info("╚══════════════════════════════════════════════════════════════╝");
                
                // Notify listeners
                notifyListeners();
            } finally {
                inputStream.close();
            }
        } catch (IOException e) {
            log.error("Failed to load index configuration: {}", e.getMessage());
        } finally {
            loading = false;
        }
    }
    
    /**
     * Save current configuration to YAML file.
     * Called automatically when indexes are added/removed via UI/API.
     */
    public synchronized void saveConfiguration() {
        if (loading) {
            return; // Don't save while loading
        }
        
        if (configFilePath.startsWith("classpath:")) {
            log.warn("Cannot save to classpath resource. Configure kuber.indexing.config-file to a file path.");
            return;
        }
        
        if (!autoSaveEnabled) {
            log.debug("Auto-save disabled, skipping configuration save");
            return;
        }
        
        try {
            Path path = Paths.get(configFilePath);
            
            // Build configuration map
            Map<String, Object> config = new LinkedHashMap<>();
            
            // Indexing settings
            Map<String, Object> indexingMap = new LinkedHashMap<>();
            indexingMap.put("enabled", indexingSettings.isEnabled());
            indexingMap.put("storage", indexingSettings.getStorage());
            indexingMap.put("rebuild-on-startup", indexingSettings.isRebuildOnStartup());
            indexingMap.put("rebuild-threads", indexingSettings.getRebuildThreads());
            indexingMap.put("max-memory-per-region-mb", indexingSettings.getMaxMemoryPerRegionMb());
            indexingMap.put("max-indexes-per-region", indexingSettings.getMaxIndexesPerRegion());
            
            Map<String, Object> persistence = new LinkedHashMap<>();
            persistence.put("enabled", indexingSettings.isPersistenceEnabled());
            persistence.put("sync-interval-seconds", indexingSettings.getPersistenceSyncIntervalSeconds());
            indexingMap.put("persistence", persistence);
            
            config.put("indexing", indexingMap);
            
            // Region configurations
            Map<String, Object> regionsMap = new LinkedHashMap<>();
            for (Map.Entry<String, RegionIndexConfig> entry : regionConfigs.entrySet()) {
                Map<String, Object> regionMap = new LinkedHashMap<>();
                List<Map<String, Object>> indexesList = new ArrayList<>();
                
                for (IndexDefinition idx : entry.getValue().getIndexes()) {
                    Map<String, Object> indexMap = new LinkedHashMap<>();
                    indexMap.put("field", idx.getField());
                    indexMap.put("type", idx.getType().toString().toLowerCase());
                    if (idx.getDescription() != null && !idx.getDescription().isEmpty()) {
                        indexMap.put("description", idx.getDescription());
                    }
                    indexesList.add(indexMap);
                }
                
                regionMap.put("indexes", indexesList);
                regionsMap.put(entry.getKey(), regionMap);
            }
            config.put("regions", regionsMap);
            
            // Write YAML
            DumperOptions options = new DumperOptions();
            options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            options.setPrettyFlow(true);
            options.setIndent(2);
            
            Yaml yaml = new Yaml(options);
            
            // Write with header comment
            StringBuilder sb = new StringBuilder();
            sb.append("# Kuber Secondary Index Configuration\n");
            sb.append("# Auto-generated by Kuber. Manual edits are supported.\n");
            sb.append("# Last modified: ").append(java.time.Instant.now()).append("\n");
            sb.append("# Documentation: /help/secondary-indexing\n\n");
            sb.append(yaml.dump(config));
            
            // Write atomically
            Path tempPath = path.resolveSibling(path.getFileName() + ".tmp");
            Files.writeString(tempPath, sb.toString());
            Files.move(tempPath, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            
            log.info("Saved index configuration to: {}", path);
            
        } catch (IOException e) {
            log.error("Failed to save index configuration: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Get the configuration file path.
     */
    public String getConfigFilePath() {
        return configFilePath;
    }
    
    @SuppressWarnings("unchecked")
    private IndexingSettings parseIndexingSettings(Map<String, Object> map) {
        IndexingSettings settings = new IndexingSettings();
        
        settings.setEnabled(getBoolean(map, "enabled", true));
        settings.setStorage(getString(map, "storage", "hybrid"));
        settings.setRebuildOnStartup(getBoolean(map, "rebuild-on-startup", true));
        settings.setRebuildThreads(getInt(map, "rebuild-threads", 4));
        settings.setMaxMemoryPerRegionMb(getInt(map, "max-memory-per-region-mb", 256));
        settings.setMaxIndexesPerRegion(getInt(map, "max-indexes-per-region", 20));
        
        // Parse persistence settings
        Map<String, Object> persistence = (Map<String, Object>) map.get("persistence");
        if (persistence != null) {
            settings.setPersistenceEnabled(getBoolean(persistence, "enabled", true));
            settings.setPersistenceSyncIntervalSeconds(getInt(persistence, "sync-interval-seconds", 30));
        }
        
        return settings;
    }
    
    @SuppressWarnings("unchecked")
    private Map<String, RegionIndexConfig> parseRegionConfigs(Map<String, Object> regionsMap) {
        Map<String, RegionIndexConfig> configs = new HashMap<>();
        
        for (Map.Entry<String, Object> entry : regionsMap.entrySet()) {
            String regionName = entry.getKey();
            
            if (entry.getValue() == null) {
                continue;
            }
            
            Map<String, Object> regionMap = (Map<String, Object>) entry.getValue();
            List<Map<String, Object>> indexesList = (List<Map<String, Object>>) regionMap.get("indexes");
            
            if (indexesList == null || indexesList.isEmpty()) {
                continue;
            }
            
            RegionIndexConfig regionConfig = new RegionIndexConfig();
            regionConfig.setRegionName(regionName);
            
            List<IndexDefinition> indexes = new ArrayList<>();
            for (Map<String, Object> indexMap : indexesList) {
                IndexDefinition indexDef = new IndexDefinition();
                indexDef.setRegion(regionName);
                indexDef.setField(getString(indexMap, "field", null));
                indexDef.setType(IndexType.fromString(getString(indexMap, "type", "hash")));
                indexDef.setDescription(getString(indexMap, "description", ""));
                
                if (indexDef.getField() != null) {
                    indexes.add(indexDef);
                    log.debug("Parsed index: region={}, field={}, type={}", 
                        regionName, indexDef.getField(), indexDef.getType());
                }
            }
            
            regionConfig.setIndexes(indexes);
            configs.put(regionName, regionConfig);
        }
        
        return configs;
    }
    
    // Helper methods for parsing
    private String getString(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value != null ? value.toString() : defaultValue;
    }
    
    private boolean getBoolean(Map<String, Object> map, String key, boolean defaultValue) {
        Object value = map.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value != null) {
            return Boolean.parseBoolean(value.toString());
        }
        return defaultValue;
    }
    
    private int getInt(Map<String, Object> map, String key, int defaultValue) {
        Object value = map.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value != null) {
            try {
                return Integer.parseInt(value.toString());
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }
    
    /**
     * Start file watcher for hot-reloading.
     */
    private void startFileWatcher() {
        try {
            Path configPath = Paths.get(configFilePath.replace("file:", ""));
            if (!Files.exists(configPath)) {
                log.debug("Config file path does not exist for watching: {}", configPath);
                return;
            }
            
            Path parentDir = configPath.getParent();
            String fileName = configPath.getFileName().toString();
            
            watching = true;
            watcherThread = new Thread(() -> {
                try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
                    parentDir.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
                    
                    log.info("Watching for index configuration changes: {}", configPath);
                    
                    while (watching) {
                        WatchKey key = watchService.poll(1, java.util.concurrent.TimeUnit.SECONDS);
                        if (key != null) {
                            for (WatchEvent<?> event : key.pollEvents()) {
                                Path changed = (Path) event.context();
                                if (changed.toString().equals(fileName)) {
                                    log.info("Index configuration file changed, reloading...");
                                    Thread.sleep(100); // Wait for file to be fully written
                                    loadConfiguration();
                                }
                            }
                            key.reset();
                        }
                    }
                } catch (IOException | InterruptedException e) {
                    if (watching) {
                        log.error("File watcher error: {}", e.getMessage());
                    }
                }
            }, "index-config-watcher");
            
            watcherThread.setDaemon(true);
            watcherThread.start();
            
        } catch (Exception e) {
            log.warn("Could not start file watcher: {}", e.getMessage());
        }
    }
    
    /**
     * Stop file watcher.
     */
    public void stopWatcher() {
        watching = false;
        if (watcherThread != null) {
            watcherThread.interrupt();
        }
    }
    
    // Getters
    
    public IndexingSettings getIndexingSettings() {
        return indexingSettings;
    }
    
    public Map<String, RegionIndexConfig> getRegionConfigs() {
        return Collections.unmodifiableMap(regionConfigs);
    }
    
    public RegionIndexConfig getRegionConfig(String region) {
        return regionConfigs.get(region);
    }
    
    public List<IndexDefinition> getIndexesForRegion(String region) {
        RegionIndexConfig config = regionConfigs.get(region);
        return config != null ? config.getIndexes() : Collections.emptyList();
    }
    
    public boolean isIndexingEnabled() {
        return indexingSettings.isEnabled();
    }
    
    public String getStorageMode() {
        return indexingSettings.getStorage();
    }
    
    // Configuration change listeners
    
    public void addListener(IndexConfigurationListener listener) {
        listeners.add(listener);
    }
    
    public void removeListener(IndexConfigurationListener listener) {
        listeners.remove(listener);
    }
    
    private void notifyListeners() {
        for (IndexConfigurationListener listener : listeners) {
            try {
                listener.onConfigurationChanged(this);
            } catch (Exception e) {
                log.error("Error notifying configuration listener: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Programmatically add an index definition (runtime).
     * Automatically saves to configuration file.
     */
    public synchronized void addIndex(String region, IndexDefinition indexDef) {
        RegionIndexConfig config = regionConfigs.computeIfAbsent(region, k -> {
            RegionIndexConfig newConfig = new RegionIndexConfig();
            newConfig.setRegionName(region);
            newConfig.setIndexes(new ArrayList<>());
            return newConfig;
        });
        
        // Check if index already exists
        boolean exists = config.getIndexes().stream()
            .anyMatch(i -> i.getField().equals(indexDef.getField()));
        
        if (!exists) {
            config.getIndexes().add(indexDef);
            log.info("Added index definition: region={}, field={}, type={}", 
                region, indexDef.getField(), indexDef.getType());
            
            // Auto-save to file
            saveConfiguration();
        }
    }
    
    /**
     * Programmatically remove an index definition (runtime).
     * Automatically saves to configuration file.
     */
    public synchronized void removeIndex(String region, String field) {
        RegionIndexConfig config = regionConfigs.get(region);
        if (config != null) {
            boolean removed = config.getIndexes().removeIf(i -> i.getField().equals(field));
            if (removed) {
                log.info("Removed index definition: region={}, field={}", region, field);
                
                // Remove empty region configs
                if (config.getIndexes().isEmpty()) {
                    regionConfigs.remove(region);
                }
                
                // Auto-save to file
                saveConfiguration();
            }
        }
    }
    
    /**
     * Remove all indexes for a region.
     * Automatically saves to configuration file.
     */
    public synchronized void removeAllIndexesForRegion(String region) {
        RegionIndexConfig config = regionConfigs.remove(region);
        if (config != null) {
            log.info("Removed all index definitions for region: {}", region);
            saveConfiguration();
        }
    }
    
    // Inner classes
    
    @Data
    public static class IndexingSettings {
        private boolean enabled = true;
        private String storage = "hybrid";
        private boolean rebuildOnStartup = true;
        private int rebuildThreads = 4;
        private int maxMemoryPerRegionMb = 256;
        private int maxIndexesPerRegion = 20;
        private boolean persistenceEnabled = true;
        private int persistenceSyncIntervalSeconds = 30;
    }
    
    @Data
    public static class RegionIndexConfig {
        private String regionName;
        private List<IndexDefinition> indexes = new ArrayList<>();
    }
    
    /**
     * Listener interface for configuration changes.
     */
    public interface IndexConfigurationListener {
        void onConfigurationChanged(IndexConfiguration config);
    }
}
