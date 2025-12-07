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
package com.kuber.server.publishing;

import com.kuber.server.config.KuberProperties;
import com.kuber.server.config.KuberProperties.FilePublisherConfig;
import com.kuber.server.config.KuberProperties.BrokerDefinition;
import com.kuber.server.config.KuberProperties.DestinationConfig;
import com.kuber.server.config.KuberProperties.RegionPublishingConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * File Event Publisher.
 * 
 * Publishes cache events to local or networked file systems.
 * 
 * Supports both:
 * - Legacy per-region configuration (kuber.publishing.regions.X.file.*)
 * - New centralized broker configuration (kuber.publishing.brokers.* + regions.X.destinations[])
 * 
 * This publisher only initializes for brokers where enabled=true.
 * 
 * @version 1.3.10
 */
@Slf4j
@Service
public class FileEventPublisher implements EventPublisher {
    
    public static final String TYPE = "file";
    public static final String DISPLAY_NAME = "File System";
    
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    
    private final KuberProperties properties;
    
    // Writers per binding key
    private final Map<String, FileWriterContext> writers = new ConcurrentHashMap<>();
    
    // Region to destination bindings
    private final Map<String, List<DestinationBinding>> regionBindings = new ConcurrentHashMap<>();
    
    // Statistics
    private final AtomicLong eventsPublished = new AtomicLong(0);
    private final AtomicLong errors = new AtomicLong(0);
    private final AtomicLong filesCreated = new AtomicLong(0);
    
    /**
     * Internal binding configuration.
     */
    private static class DestinationBinding {
        final String directory;
        final String prefix;
        final int maxFileSizeMb;
        final String rotationPolicy;
        final String format;
        final boolean compress;
        final int retentionDays;
        
        DestinationBinding(String directory, String prefix, int maxFileSizeMb,
                          String rotationPolicy, String format, boolean compress, int retentionDays) {
            this.directory = directory;
            this.prefix = prefix;
            this.maxFileSizeMb = maxFileSizeMb;
            this.rotationPolicy = rotationPolicy;
            this.format = format;
            this.compress = compress;
            this.retentionDays = retentionDays;
        }
        
        String bindingKey() {
            return directory + ":" + prefix;
        }
    }
    
    public FileEventPublisher(KuberProperties properties) {
        this.properties = properties;
    }
    
    @Override
    public String getType() {
        return TYPE;
    }
    
    @Override
    public String getDisplayName() {
        return DISPLAY_NAME;
    }
    
    @Override
    public void initialize() {
        log.info("Initializing File Event Publisher...");
        
        Map<String, BrokerDefinition> brokers = properties.getPublishing().getBrokers();
        Map<String, RegionPublishingConfig> regions = properties.getPublishing().getRegions();
        
        for (Map.Entry<String, RegionPublishingConfig> entry : regions.entrySet()) {
            String region = entry.getKey();
            RegionPublishingConfig config = entry.getValue();
            
            if (!config.isEnabled()) continue;
            
            List<DestinationBinding> bindings = new ArrayList<>();
            
            // Check new-style destinations
            if (config.getDestinations() != null) {
                for (DestinationConfig dest : config.getDestinations()) {
                    if (dest.getBroker() == null) continue;
                    BrokerDefinition broker = brokers.get(dest.getBroker());
                    if (broker != null && TYPE.equals(broker.getType()) && broker.isEnabled()) {
                        String directory = broker.getDirectory();
                        String prefix = dest.getTopic();
                        if (prefix == null || prefix.isBlank()) {
                            prefix = region;
                        }
                        
                        bindings.add(new DestinationBinding(
                                directory,
                                prefix,
                                broker.getMaxFileSizeMb(),
                                broker.getRotationPolicy(),
                                broker.getFormat(),
                                broker.isCompress(),
                                broker.getRetentionDays()
                        ));
                        
                        log.info("File destination for region '{}': broker={}, directory={}, prefix={}",
                                region, dest.getBroker(), directory, prefix);
                    }
                }
            }
            
            // Check legacy configuration
            FilePublisherConfig fileConfig = config.getFile();
            if (fileConfig != null && fileConfig.isEnabled()) {
                String directory = fileConfig.getDirectory();
                if (directory == null || directory.isBlank()) {
                    directory = "./events/" + region;
                }
                
                bindings.add(new DestinationBinding(
                        directory,
                        region,
                        fileConfig.getMaxFileSizeMb(),
                        fileConfig.getRotationPolicy(),
                        fileConfig.getFormat(),
                        fileConfig.isCompress(),
                        fileConfig.getRetentionDays()
                ));
                
                log.info("File (legacy) for region '{}': directory={}",
                        region, directory);
            }
            
            if (!bindings.isEmpty()) {
                regionBindings.put(region, bindings);
            }
        }
        
        if (regionBindings.isEmpty()) {
            log.info("No file publishers enabled");
        } else {
            log.info("File publishing configured for {} region(s)", regionBindings.size());
        }
    }
    
    @Override
    public void onStartupOrchestration() {
        // Create output directories for all configured bindings
        for (List<DestinationBinding> bindings : regionBindings.values()) {
            for (DestinationBinding binding : bindings) {
                try {
                    Path directory = Paths.get(binding.directory);
                    if (!Files.exists(directory)) {
                        Files.createDirectories(directory);
                        log.info("Created event output directory: {}", directory.toAbsolutePath());
                    }
                } catch (Exception e) {
                    log.warn("Failed to create output directory '{}': {}", 
                            binding.directory, e.getMessage());
                }
            }
        }
    }
    
    @Override
    public boolean isEnabledForRegion(String region) {
        return regionBindings.containsKey(region);
    }
    
    @Override
    public void publish(String region, CachePublishingEvent event) {
        List<DestinationBinding> bindings = regionBindings.get(region);
        if (bindings == null || bindings.isEmpty()) {
            return;
        }
        
        for (DestinationBinding binding : bindings) {
            publishToDestination(region, event, binding);
        }
    }
    
    private void publishToDestination(String region, CachePublishingEvent event, DestinationBinding binding) {
        try {
            FileWriterContext writerContext = getOrCreateWriter(binding);
            
            // Write event as JSON line
            String jsonLine = event.toJson();
            writerContext.write(jsonLine, binding);
            
            eventsPublished.incrementAndGet();
            log.debug("Published event to file for region '{}', key={}, action={}",
                    region, event.getKey(), event.getAction());
            
        } catch (Exception e) {
            errors.incrementAndGet();
            log.error("Failed to publish event to file for region '{}', key '{}': {}",
                    region, event.getKey(), e.getMessage());
        }
    }
    
    @Override
    public PublisherStats getStats() {
        return new PublisherStats(TYPE, eventsPublished.get(), errors.get(), regionBindings.size());
    }
    
    private FileWriterContext getOrCreateWriter(DestinationBinding binding) throws IOException {
        String key = binding.bindingKey();
        FileWriterContext context = writers.get(key);
        
        if (context == null || context.needsRotation(binding)) {
            synchronized (writers) {
                context = writers.get(key);
                if (context == null || context.needsRotation(binding)) {
                    // Close existing writer
                    if (context != null) {
                        context.close();
                    }
                    
                    // Create new writer
                    context = new FileWriterContext(binding, filesCreated);
                    writers.put(key, context);
                }
            }
        }
        
        return context;
    }
    
    @Override
    public void shutdown() {
        log.info("Shutting down File Event Publisher...");
        
        for (Map.Entry<String, FileWriterContext> entry : writers.entrySet()) {
            try {
                entry.getValue().close();
                log.info("Closed file writer for '{}'", entry.getKey());
            } catch (Exception e) {
                log.warn("Error closing file writer for '{}': {}", 
                        entry.getKey(), e.getMessage());
            }
        }
        writers.clear();
        
        log.info("File Event Publisher shutdown. Total files created: {}", filesCreated.get());
    }
    
    /**
     * Context for managing a file writer with rotation support.
     */
    private static class FileWriterContext {
        private final DestinationBinding binding;
        private final AtomicLong filesCreated;
        private final ReentrantLock writeLock = new ReentrantLock();
        
        private BufferedWriter writer;
        private Path currentFile;
        private LocalDate fileDate;
        private int sequence;
        private long bytesWritten;
        
        FileWriterContext(DestinationBinding binding, AtomicLong filesCreated) throws IOException {
            this.binding = binding;
            this.filesCreated = filesCreated;
            this.fileDate = LocalDate.now();
            this.sequence = 1;
            this.bytesWritten = 0;
            
            createNewFile();
        }
        
        void write(String jsonLine, DestinationBinding currentBinding) throws IOException {
            writeLock.lock();
            try {
                // Check for rotation
                if (needsRotation(currentBinding)) {
                    close();
                    createNewFile();
                }
                
                writer.write(jsonLine);
                writer.newLine();
                writer.flush();
                
                bytesWritten += jsonLine.length() + 1; // +1 for newline
                
            } finally {
                writeLock.unlock();
            }
        }
        
        boolean needsRotation(DestinationBinding currentBinding) {
            // Check date-based rotation
            if ("daily".equalsIgnoreCase(currentBinding.rotationPolicy)) {
                if (!LocalDate.now().equals(fileDate)) {
                    return true;
                }
            }
            
            // Check size-based rotation
            long maxBytes = currentBinding.maxFileSizeMb * 1024L * 1024L;
            if (bytesWritten >= maxBytes) {
                return true;
            }
            
            return false;
        }
        
        private void createNewFile() throws IOException {
            LocalDate today = LocalDate.now();
            if (!today.equals(fileDate)) {
                fileDate = today;
                sequence = 1;
            }
            
            Path directory = Paths.get(binding.directory);
            if (!Files.exists(directory)) {
                Files.createDirectories(directory);
            }
            
            // Generate filename: prefix_date_sequence.jsonl
            String filename = String.format("%s_%s_%03d.jsonl", 
                    binding.prefix, 
                    fileDate.format(DATE_FORMATTER), 
                    sequence);
            
            currentFile = directory.resolve(filename);
            
            // If file exists, increment sequence
            while (Files.exists(currentFile)) {
                sequence++;
                filename = String.format("%s_%s_%03d.jsonl", 
                        binding.prefix, 
                        fileDate.format(DATE_FORMATTER), 
                        sequence);
                currentFile = directory.resolve(filename);
            }
            
            writer = new BufferedWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(currentFile.toFile(), true),
                            StandardCharsets.UTF_8
                    ),
                    8192 // 8KB buffer
            );
            
            bytesWritten = 0;
            filesCreated.incrementAndGet();
            
            log.info("Created event file: {}", currentFile);
        }
        
        void close() {
            if (writer != null) {
                try {
                    writer.flush();
                    writer.close();
                } catch (Exception e) {
                    log.warn("Error closing file writer: {}", e.getMessage());
                }
                writer = null;
            }
        }
    }
}
