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
package com.kuber.server.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.kuber.core.model.CacheRegion;
import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.security.ApiKey;
import com.kuber.server.security.ApiKeyService;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Request/Response Messaging Service.
 * 
 * <p>Manages message broker connections, request processing, and response delivery
 * for cache operations via message queues.</p>
 * 
 * <p>Features:
 * <ul>
 *   <li>Multi-broker support (Kafka, ActiveMQ, RabbitMQ, IBM MQ)</li>
 *   <li>Hot-reload of configuration from request_response.json</li>
 *   <li>Backpressure control with configurable queue depth</li>
 *   <li>Thread pool for asynchronous request processing</li>
 *   <li>API key authentication for all requests</li>
 * </ul>
 * 
 * @version 1.7.0
 */
@Slf4j
@Service
public class RequestResponseService {
    
    private static final String CONFIG_FILE = "request_response.json";
    
    private final KuberProperties properties;
    private final CacheService cacheService;
    private final ApiKeyService apiKeyService;
    private final ObjectMapper objectMapper;
    
    // Configuration
    private MessagingConfig config;
    private File configFile;
    
    // Broker adapters keyed by broker name
    private final Map<String, MessageBrokerAdapter> adapters = new ConcurrentHashMap<>();
    
    // Request processing
    private BlockingQueue<PendingRequest> requestQueue;
    private ExecutorService processorPool;
    private Thread queueMonitorThread;
    
    // State
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean backpressureActive = new AtomicBoolean(false);
    private volatile long backpressureStartTime = 0;
    
    // Statistics
    private final AtomicLong requestsReceived = new AtomicLong(0);
    private final AtomicLong requestsProcessed = new AtomicLong(0);
    private final AtomicLong backpressureActivations = new AtomicLong(0);
    private final AtomicLong authFailures = new AtomicLong(0);
    private final AtomicLong processingErrors = new AtomicLong(0);
    private final AtomicLong requestsDrained = new AtomicLong(0);
    
    // Failed subscriptions tracking
    private final Map<String, FailedSubscription> failedSubscriptions = new ConcurrentHashMap<>();
    
    // File watcher
    private WatchService watchService;
    private Thread watcherThread;
    
    @Autowired
    public RequestResponseService(KuberProperties properties, 
                                   CacheService cacheService,
                                   ApiKeyService apiKeyService,
                                   ObjectMapper objectMapper) {
        this.properties = properties;
        this.cacheService = cacheService;
        this.apiKeyService = apiKeyService;
        this.objectMapper = objectMapper;
    }
    
    @PostConstruct
    public void init() {
        // Determine config file path - use secure folder
        String secureDir = properties.getSecure().getFolder();
        if (secureDir == null || secureDir.isEmpty()) {
            secureDir = "./secure";
        }
        configFile = new File(secureDir, CONFIG_FILE);
        
        log.info("Request/Response messaging config file: {}", configFile.getAbsolutePath());
    }
    
    /**
     * Initialize and start the messaging service.
     * Called by StartupOrchestrator as the last phase.
     */
    public void start() {
        if (initialized.getAndSet(true)) {
            log.warn("RequestResponseService already initialized");
            return;
        }
        
        // Load configuration
        if (!loadConfiguration()) {
            log.warn("Request/Response messaging disabled - no valid configuration");
            return;
        }
        
        if (!config.isEnabled()) {
            log.info("Request/Response messaging disabled in configuration");
            return;
        }
        
        // Initialize request queue with configured depth
        int maxQueueDepth = config.getMaxQueueDepth() > 0 ? config.getMaxQueueDepth() : 100;
        requestQueue = new LinkedBlockingQueue<>(maxQueueDepth);
        
        // Initialize thread pool
        int poolSize = config.getThreadPoolSize() > 0 ? config.getThreadPoolSize() : 10;
        processorPool = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r, "request-processor");
            t.setDaemon(true);
            return t;
        });
        
        // Start queue processor
        startQueueProcessor();
        
        // Start queue monitor (for backpressure)
        startQueueMonitor();
        
        // Connect to brokers and subscribe
        connectAndSubscribe();
        
        // Start file watcher for hot-reload
        startFileWatcher();
        
        running.set(true);
        
        log.info("╔════════════════════════════════════════════════════════════════════╗");
        log.info("║  Request/Response Messaging Service Started                         ║");
        log.info("║    Max Queue Depth: {}                                           ║", String.format("%-4d", maxQueueDepth));
        log.info("║    Thread Pool Size: {}                                          ║", String.format("%-4d", poolSize));
        log.info("║    Brokers Configured: {}                                         ║", String.format("%-4d", config.getBrokers().size()));
        log.info("╚════════════════════════════════════════════════════════════════════╝");
    }
    
    /**
     * Stop the messaging service.
     * Called by ShutdownOrchestrator as the first phase.
     */
    public void stop() {
        if (!running.getAndSet(false)) {
            return;
        }
        
        log.info("Stopping Request/Response Messaging Service...");
        
        // Stop file watcher
        stopFileWatcher();
        
        // Disconnect all brokers
        for (MessageBrokerAdapter adapter : adapters.values()) {
            try {
                adapter.disconnect();
            } catch (Exception e) {
                log.warn("Error disconnecting broker {}: {}", adapter.getBrokerName(), e.getMessage());
            }
        }
        adapters.clear();
        
        // Shutdown processor pool
        if (processorPool != null) {
            processorPool.shutdown();
            try {
                if (!processorPool.awaitTermination(10, TimeUnit.SECONDS)) {
                    processorPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                processorPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // Stop queue monitor
        if (queueMonitorThread != null) {
            queueMonitorThread.interrupt();
        }
        
        log.info("Request/Response Messaging Service stopped");
    }
    
    /**
     * Load configuration from file.
     */
    private boolean loadConfiguration() {
        if (!configFile.exists()) {
            logBigAlertMessage();
            createDefaultConfig();
            return false;
        }
        
        try {
            config = objectMapper.readValue(configFile, MessagingConfig.class);
            log.info("Loaded Request/Response configuration: enabled={}, brokers={}", 
                    config.isEnabled(), config.getBrokers().size());
            return true;
        } catch (Exception e) {
            log.error("Failed to load configuration: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Log a big unmistakable alert message when config file is missing.
     */
    private void logBigAlertMessage() {
        log.warn("╔══════════════════════════════════════════════════════════════════════════════════╗");
        log.warn("║                                                                                  ║");
        log.warn("║   ██╗    ██╗ █████╗ ██████╗ ███╗   ██╗██╗███╗   ██╗ ██████╗                      ║");
        log.warn("║   ██║    ██║██╔══██╗██╔══██╗████╗  ██║██║████╗  ██║██╔════╝                      ║");
        log.warn("║   ██║ █╗ ██║███████║██████╔╝██╔██╗ ██║██║██╔██╗ ██║██║  ███╗                     ║");
        log.warn("║   ██║███╗██║██╔══██║██╔══██╗██║╚██╗██║██║██║╚██╗██║██║   ██║                     ║");
        log.warn("║   ╚███╔███╔╝██║  ██║██║  ██║██║ ╚████║██║██║ ╚████║╚██████╔╝                     ║");
        log.warn("║    ╚══╝╚══╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝╚═╝  ╚═══╝ ╚═════╝                      ║");
        log.warn("║                                                                                  ║");
        log.warn("║   REQUEST/RESPONSE MESSAGING CONFIG FILE NOT FOUND!                              ║");
        log.warn("║                                                                                  ║");
        log.warn("║   Expected file: {}   ║", String.format("%-40s", configFile.getAbsolutePath()));
        log.warn("║                                                                                  ║");
        log.warn("║   A default configuration file has been created.                                 ║");
        log.warn("║   Edit this file to configure message broker connections and restart.            ║");
        log.warn("║                                                                                  ║");
        log.warn("║   The Request/Response Messaging feature is DISABLED until configured.           ║");
        log.warn("║                                                                                  ║");
        log.warn("╚══════════════════════════════════════════════════════════════════════════════════╝");
    }
    
    /**
     * Create default configuration file.
     */
    private void createDefaultConfig() {
        MessagingConfig defaultConfig = new MessagingConfig();
        defaultConfig.setEnabled(false);
        defaultConfig.setMaxQueueDepth(100);
        defaultConfig.setThreadPoolSize(10);
        
        // Add example broker configurations
        MessagingConfig.BrokerConfig kafkaExample = new MessagingConfig.BrokerConfig();
        kafkaExample.setEnabled(false);
        kafkaExample.setType("kafka");
        kafkaExample.setDisplayName("Example Kafka");
        kafkaExample.getConnection().put("bootstrap_servers", "localhost:9092");
        kafkaExample.getConnection().put("group_id", "kuber-request-processor");
        kafkaExample.getRequestTopics().add("ccs_cache_request");
        defaultConfig.getBrokers().put("kafka_example", kafkaExample);
        
        MessagingConfig.BrokerConfig activemqExample = new MessagingConfig.BrokerConfig();
        activemqExample.setEnabled(false);
        activemqExample.setType("activemq");
        activemqExample.setDisplayName("Example ActiveMQ");
        activemqExample.getConnection().put("broker_url", "tcp://localhost:61616");
        activemqExample.getConnection().put("username", "admin");
        activemqExample.getConnection().put("password", "admin");
        activemqExample.getRequestTopics().add("ccs_cache_request");
        defaultConfig.getBrokers().put("activemq_example", activemqExample);
        
        try {
            configFile.getParentFile().mkdirs();
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(configFile, defaultConfig);
            log.info("Created default configuration: {}", configFile.getAbsolutePath());
        } catch (IOException e) {
            log.error("Failed to create default configuration: {}", e.getMessage());
        }
    }
    
    /**
     * Connect to configured brokers and subscribe to request topics.
     */
    private void connectAndSubscribe() {
        if (config == null || config.getBrokers() == null) {
            return;
        }
        
        // Clear previous failed subscriptions
        failedSubscriptions.clear();
        
        for (Map.Entry<String, MessagingConfig.BrokerConfig> entry : config.getBrokers().entrySet()) {
            String brokerName = entry.getKey();
            MessagingConfig.BrokerConfig brokerConfig = entry.getValue();
            
            if (!brokerConfig.isEnabled()) {
                log.info("Broker '{}' is disabled, skipping", brokerName);
                continue;
            }
            
            if (brokerConfig.getRequestTopics() == null || brokerConfig.getRequestTopics().isEmpty()) {
                log.warn("Broker '{}' has no request topics configured", brokerName);
                failedSubscriptions.put(brokerName, new FailedSubscription(
                    brokerName, brokerConfig.getType(), "No request topics configured", 
                    System.currentTimeMillis(), Collections.emptyList()));
                continue;
            }
            
            // Create adapter
            MessageBrokerAdapter adapter = createAdapter(brokerName, brokerConfig);
            if (adapter == null) {
                log.error("Unknown broker type '{}' for broker '{}'", brokerConfig.getType(), brokerName);
                failedSubscriptions.put(brokerName, new FailedSubscription(
                    brokerName, brokerConfig.getType(), 
                    "Unknown broker type: " + brokerConfig.getType(), 
                    System.currentTimeMillis(), brokerConfig.getRequestTopics()));
                continue;
            }
            
            // Connect
            if (!adapter.connect()) {
                log.error("Failed to connect to broker '{}' - system will continue without this broker", brokerName);
                failedSubscriptions.put(brokerName, new FailedSubscription(
                    brokerName, brokerConfig.getType(), 
                    "Connection failed - check broker configuration and connectivity", 
                    System.currentTimeMillis(), brokerConfig.getRequestTopics()));
                continue;
            }
            
            // Subscribe
            if (adapter.subscribe(brokerConfig.getRequestTopics(), this::handleMessage)) {
                adapters.put(brokerName, adapter);
                log.info("Broker '{}' connected and subscribed to: {}", 
                        brokerName, brokerConfig.getRequestTopics());
            } else {
                log.error("Failed to subscribe broker '{}' - system will continue without this broker", brokerName);
                failedSubscriptions.put(brokerName, new FailedSubscription(
                    brokerName, brokerConfig.getType(), 
                    "Subscription failed - topics may not exist", 
                    System.currentTimeMillis(), brokerConfig.getRequestTopics()));
                adapter.disconnect();
            }
        }
        
        // Log summary
        if (!failedSubscriptions.isEmpty()) {
            log.warn("╔════════════════════════════════════════════════════════════════════╗");
            log.warn("║  WARNING: Some broker subscriptions failed                          ║");
            log.warn("║  Failed brokers: {}                                              ║", 
                    String.format("%-4d", failedSubscriptions.size()));
            log.warn("║  System will continue with {} active broker(s)                   ║",
                    String.format("%-4d", adapters.size()));
            log.warn("║  Check Admin UI for details: /admin/messaging                       ║");
            log.warn("╚════════════════════════════════════════════════════════════════════╝");
        }
    }
    
    /**
     * Create appropriate adapter for broker type.
     */
    private MessageBrokerAdapter createAdapter(String brokerName, MessagingConfig.BrokerConfig config) {
        String type = config.getType();
        if (type == null) {
            return null;
        }
        
        switch (type.toLowerCase()) {
            case "kafka":
                return new KafkaBrokerAdapter(brokerName, config);
            case "activemq":
                return new ActiveMqBrokerAdapter(brokerName, config);
            case "rabbitmq":
                return new RabbitMqBrokerAdapter(brokerName, config);
            case "ibmmq":
                return new IbmMqBrokerAdapter(brokerName, config);
            default:
                return null;
        }
    }
    
    /**
     * Handle incoming message from any broker.
     * Uses blocking put to ensure no message is ever rejected.
     * Backpressure is managed by pausing broker consumption.
     */
    private void handleMessage(MessageBrokerAdapter.ReceivedMessage message) {
        requestsReceived.incrementAndGet();
        
        PendingRequest pending = new PendingRequest(
            message.getMessage(),
            message.getTopic(),
            message.getResponseTopic(),
            findAdapterForTopic(message.getTopic()),
            Instant.now()
        );
        
        try {
            // Blocking put - will wait until space is available
            // Backpressure pauses consumption before queue is full, so this rarely blocks
            requestQueue.put(pending);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while queuing request from topic: {}", message.getTopic());
        }
    }
    
    /**
     * Find adapter that is subscribed to the given topic.
     */
    private MessageBrokerAdapter findAdapterForTopic(String topic) {
        for (MessageBrokerAdapter adapter : adapters.values()) {
            if (adapter.getSubscribedTopics().contains(topic)) {
                return adapter;
            }
        }
        return null;
    }
    
    /**
     * Start queue processor threads.
     */
    private void startQueueProcessor() {
        int poolSize = config.getThreadPoolSize() > 0 ? config.getThreadPoolSize() : 10;
        
        for (int i = 0; i < poolSize; i++) {
            processorPool.submit(() -> {
                while (running.get() || !requestQueue.isEmpty()) {
                    try {
                        PendingRequest request = requestQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (request != null) {
                            processRequest(request);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        log.error("Error processing request: {}", e.getMessage());
                        processingErrors.incrementAndGet();
                    }
                }
            });
        }
    }
    
    /**
     * Start queue monitor for backpressure management.
     * Pauses broker consumption when queue reaches high water mark.
     * Resumes consumption when queue drops below low water mark.
     */
    private void startQueueMonitor() {
        int maxDepth = config.getMaxQueueDepth() > 0 ? config.getMaxQueueDepth() : 100;
        int highWaterMark = (int) (maxDepth * 0.8);
        int lowWaterMark = (int) (maxDepth * 0.5);
        
        queueMonitorThread = new Thread(() -> {
            while (running.get()) {
                try {
                    int queueSize = requestQueue.size();
                    
                    if (!backpressureActive.get() && queueSize >= highWaterMark) {
                        // Activate backpressure - pause all adapters
                        backpressureActive.set(true);
                        backpressureStartTime = System.currentTimeMillis();
                        backpressureActivations.incrementAndGet();
                        
                        log.warn("Queue depth {} reached high water mark {}, pausing consumption (backpressure activated)",
                                queueSize, highWaterMark);
                        for (MessageBrokerAdapter adapter : adapters.values()) {
                            adapter.pauseConsumption();
                        }
                    } else if (backpressureActive.get() && queueSize <= lowWaterMark) {
                        // Deactivate backpressure - resume all adapters
                        long duration = System.currentTimeMillis() - backpressureStartTime;
                        backpressureActive.set(false);
                        backpressureStartTime = 0;
                        
                        log.info("Queue depth {} below low water mark {}, resuming consumption (backpressure released after {} ms)",
                                queueSize, lowWaterMark, duration);
                        for (MessageBrokerAdapter adapter : adapters.values()) {
                            adapter.resumeConsumption();
                        }
                    }
                    
                    Thread.sleep(100);
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "queue-monitor");
        queueMonitorThread.setDaemon(true);
        queueMonitorThread.start();
    }
    
    /**
     * Process a single request.
     */
    private void processRequest(PendingRequest pending) {
        Instant processStart = Instant.now();
        
        try {
            // Parse request
            CacheRequest request;
            try {
                request = objectMapper.readValue(pending.getRequestJson(), CacheRequest.class);
            } catch (Exception e) {
                log.error("Failed to parse request: {}", e.getMessage());
                sendErrorResponse(pending, CacheResponse.ErrorCode.PARSE_ERROR, 
                    "Invalid JSON: " + e.getMessage());
                return;
            }
            
            // Validate request
            if (!request.isValid()) {
                sendErrorResponse(pending, request, CacheResponse.ErrorCode.INVALID_REQUEST, 
                    request.getValidationError());
                return;
            }
            
            // Authenticate
            Optional<ApiKey> apiKeyOpt = apiKeyService.validateKey(request.getApiKey());
            if (apiKeyOpt.isEmpty()) {
                authFailures.incrementAndGet();
                sendErrorResponse(pending, request, CacheResponse.ErrorCode.AUTHENTICATION_FAILED, 
                    "Invalid API key");
                return;
            }
            
            // Validate batch limits for MGET
            if (request.isBatchGet()) {
                int maxBatchGetSize = config.getMaxBatchGetSize() > 0 ? config.getMaxBatchGetSize() : 128;
                if (request.getBatchGetSize() > maxBatchGetSize) {
                    sendErrorResponse(pending, request, CacheResponse.ErrorCode.BATCH_LIMIT_EXCEEDED,
                        String.format("MGET batch size %d exceeds maximum allowed %d", 
                            request.getBatchGetSize(), maxBatchGetSize));
                    return;
                }
            }
            
            // Validate batch limits for MSET
            if (request.isBatchSet()) {
                int maxBatchSetSize = config.getMaxBatchSetSize() > 0 ? config.getMaxBatchSetSize() : 128;
                if (request.getBatchSetSize() > maxBatchSetSize) {
                    sendErrorResponse(pending, request, CacheResponse.ErrorCode.BATCH_LIMIT_EXCEEDED,
                        String.format("MSET batch size %d exceeds maximum allowed %d", 
                            request.getBatchSetSize(), maxBatchSetSize));
                    return;
                }
            }
            
            // Execute operation
            OperationResult operationResult = executeOperationWithLimits(request);
            
            // Send success response with optional server message
            Instant sendTime = Instant.now();
            CacheResponse response;
            
            if (operationResult.hasCounts()) {
                response = CacheResponse.successWithCounts(
                    request, operationResult.getResult(),
                    operationResult.getTotalCount(), operationResult.getReturnedCount(),
                    operationResult.getServerMessage(),
                    pending.getReceiveTime(), sendTime
                );
            } else if (operationResult.getServerMessage() != null && !operationResult.getServerMessage().isEmpty()) {
                response = CacheResponse.successWithMessage(
                    request, operationResult.getResult(),
                    operationResult.getServerMessage(),
                    pending.getReceiveTime(), sendTime
                );
            } else {
                response = CacheResponse.success(request, operationResult.getResult(), 
                    pending.getReceiveTime(), sendTime);
            }
            
            sendResponse(pending, response);
            requestsProcessed.incrementAndGet();
            
        } catch (Exception e) {
            log.error("Error processing request: {}", e.getMessage(), e);
            processingErrors.incrementAndGet();
            sendErrorResponse(pending, CacheResponse.ErrorCode.INTERNAL_ERROR, e.getMessage());
        }
    }
    
    /**
     * Result wrapper for operations that may include server messages or counts.
     */
    @Data
    @AllArgsConstructor
    private static class OperationResult {
        private final Object result;
        private final String serverMessage;
        private final int totalCount;
        private final int returnedCount;
        
        static OperationResult simple(Object result) {
            return new OperationResult(result, "", 0, 0);
        }
        
        static OperationResult withMessage(Object result, String message) {
            return new OperationResult(result, message, 0, 0);
        }
        
        static OperationResult withCounts(Object result, int totalCount, int returnedCount, String message) {
            return new OperationResult(result, message, totalCount, returnedCount);
        }
        
        boolean hasCounts() {
            return totalCount > 0 || returnedCount > 0;
        }
    }
    
    /**
     * Represents a key-value pair in response results.
     */
    @Data
    @AllArgsConstructor
    public static class KeyValuePair {
        private String key;
        private Object value;
    }
    
    /**
     * Execute operation with limit handling.
     * Returns key-value pairs for GET, MGET, and KEYS operations.
     */
    private OperationResult executeOperationWithLimits(CacheRequest request) throws Exception {
        String op = request.getOperation().toUpperCase();
        String region = request.getRegion() != null ? request.getRegion() : "default";
        int maxSearchResults = config.getMaxSearchResults() > 0 ? config.getMaxSearchResults() : 10000;
        
        switch (op) {
            case "GET": {
                // GET returns array with one key-value pair, or empty array if not found
                String key = request.getKey();
                Object value = cacheService.get(region, key);
                
                if (value == null) {
                    // Key not found - return empty array with server message
                    return OperationResult.withMessage(
                        new ArrayList<>(),
                        String.format("Key not found: %s", key)
                    );
                }
                
                List<KeyValuePair> result = new ArrayList<>();
                result.add(new KeyValuePair(key, value));
                return OperationResult.simple(result);
            }
            
            case "MGET": {
                // MGET returns array of key-value pairs for found keys
                List<String> keys = request.getKeys();
                List<KeyValuePair> results = new ArrayList<>();
                List<String> missingKeys = new ArrayList<>();
                
                for (String key : keys) {
                    Object value = cacheService.get(region, key);
                    if (value != null) {
                        results.add(new KeyValuePair(key, value));
                    } else {
                        missingKeys.add(key);
                    }
                }
                
                String serverMessage = "";
                if (!missingKeys.isEmpty()) {
                    serverMessage = String.format(
                        "Keys not found (%d of %d): %s",
                        missingKeys.size(), keys.size(),
                        String.join(", ", missingKeys)
                    );
                }
                
                return OperationResult.withCounts(results, keys.size(), results.size(), serverMessage);
            }
            
            case "KEYS": {
                // KEYS returns array of key-value pairs matching pattern
                String pattern = request.getPattern() != null ? request.getPattern() : "*";
                Collection<String> allKeys = cacheService.keys(region, pattern);
                int totalCount = allKeys.size();
                
                // Determine how many keys to process
                int keysToProcess = Math.min(totalCount, maxSearchResults);
                List<KeyValuePair> results = new ArrayList<>();
                
                int count = 0;
                for (String key : allKeys) {
                    if (count >= keysToProcess) break;
                    Object value = cacheService.get(region, key);
                    results.add(new KeyValuePair(key, value));
                    count++;
                }
                
                String serverMessage = "";
                if (totalCount > maxSearchResults) {
                    serverMessage = String.format(
                        "Results truncated: found %d keys matching pattern, returning first %d (max_search_results=%d)",
                        totalCount, maxSearchResults, maxSearchResults
                    );
                }
                
                return OperationResult.withCounts(results, totalCount, results.size(), serverMessage);
            }
            
            case "JSEARCH":
            case "JSON.SEARCH": {
                // JSEARCH already returns key-value results from CacheService
                List<?> allResults = (List<?>) cacheService.jsonSearch(region, request.getQuery());
                int totalCount = allResults.size();
                
                if (totalCount > maxSearchResults) {
                    List<?> truncatedResults = allResults.subList(0, maxSearchResults);
                    String serverMessage = String.format(
                        "Results truncated: found %d matching documents, returning first %d (max_search_results=%d)",
                        totalCount, maxSearchResults, maxSearchResults
                    );
                    return OperationResult.withCounts(truncatedResults, totalCount, maxSearchResults, serverMessage);
                }
                return OperationResult.withCounts(allResults, totalCount, totalCount, "");
            }
            
            default:
                // Delegate to existing operation handler
                return OperationResult.simple(executeOperation(request));
        }
    }
    
    /**
     * Execute cache operation.
     */
    private Object executeOperation(CacheRequest request) throws Exception {
        String op = request.getOperation().toUpperCase();
        String region = request.getRegion() != null ? request.getRegion() : "default";
        
        switch (op) {
            case "PING":
                return "PONG";
                
            case "INFO":
                return cacheService.getAllRegionStats();
                
            case "REGIONS":
                // Convert CacheRegion collection to list of region names
                return cacheService.getAllRegions().stream()
                    .map(r -> r.getName())
                    .collect(Collectors.toList());
                
            case "GET":
                return cacheService.get(region, request.getKey());
                
            case "SET":
                // Convert Object value to String
                String valueStr = convertToString(request.getValue());
                if (request.getTtl() != null && request.getTtl() > 0) {
                    cacheService.set(region, request.getKey(), valueStr, request.getTtl());
                } else {
                    cacheService.set(region, request.getKey(), valueStr);
                }
                return "OK";
                
            case "DELETE":
            case "DEL":
                if (request.getKeys() != null && !request.getKeys().isEmpty()) {
                    int deleted = 0;
                    for (String key : request.getKeys()) {
                        if (cacheService.delete(region, key)) deleted++;
                    }
                    return deleted;
                } else {
                    return cacheService.delete(region, request.getKey()) ? 1 : 0;
                }
                
            case "EXISTS":
                return cacheService.exists(region, request.getKey());
                
            case "KEYS":
                String pattern = request.getPattern() != null ? request.getPattern() : "*";
                return cacheService.keys(region, pattern);
                
            case "MGET":
                return cacheService.mget(region, request.getKeys());
                
            case "MSET":
                if (request.getEntries() != null) {
                    for (Map.Entry<String, Object> entry : request.getEntries().entrySet()) {
                        String entryValueStr = convertToString(entry.getValue());
                        cacheService.set(region, entry.getKey(), entryValueStr);
                    }
                }
                return "OK";
                
            case "TTL":
                return cacheService.ttl(region, request.getKey());
                
            case "EXPIRE":
                return cacheService.expire(region, request.getKey(), 
                    request.getTtl() != null ? request.getTtl() : 0);
                
            case "HGET":
                return cacheService.hget(region, request.getKey(), request.getField());
                
            case "HSET":
                cacheService.hset(region, request.getKey(), 
                    request.getField(), String.valueOf(request.getValue()));
                return 1;
                
            case "HGETALL":
                return cacheService.hgetall(region, request.getKey());
                
            case "HMSET":
                // Set multiple hash fields individually since hmset doesn't exist
                if (request.getFields() != null) {
                    for (Map.Entry<String, String> entry : request.getFields().entrySet()) {
                        cacheService.hset(region, request.getKey(), entry.getKey(), entry.getValue());
                    }
                    return request.getFields().size();
                }
                return 0;
                
            case "JSET":
            case "JSON.SET":
                JsonNode jsonValue = convertToJsonNode(request.getValue());
                cacheService.jsonSet(region, request.getKey(), jsonValue);
                return "OK";
            
            case "JUPDATE":
            case "JSON.UPDATE":
                JsonNode updateJsonValue = convertToJsonNode(request.getValue());
                long ttl = request.getTtl() != null ? request.getTtl() : -1;
                return cacheService.jsonUpdate(region, request.getKey(), updateJsonValue, ttl);
            
            case "JREMOVE":
            case "JSON.REMOVE":
                // Value should be a list of attribute names to remove
                List<String> attributesToRemove = convertToStringList(request.getValue());
                return cacheService.jsonRemoveAttributes(region, request.getKey(), attributesToRemove);
                
            case "JGET":
            case "JSON.GET":
                String path = request.getPath() != null ? request.getPath() : "$";
                return cacheService.jsonGet(region, request.getKey(), path);
                
            case "JSEARCH":
            case "JSON.SEARCH":
                return cacheService.jsonSearch(region, request.getQuery());
                
            default:
                throw new IllegalArgumentException("Unsupported operation: " + op);
        }
    }
    
    /**
     * Convert Object value to list of strings.
     */
    @SuppressWarnings("unchecked")
    private List<String> convertToStringList(Object value) throws Exception {
        if (value == null) {
            return new ArrayList<>();
        }
        if (value instanceof List) {
            List<String> result = new ArrayList<>();
            for (Object item : (List<?>) value) {
                result.add(item != null ? item.toString() : null);
            }
            return result;
        }
        if (value instanceof String) {
            // Try to parse as JSON array
            try {
                return objectMapper.readValue((String) value, 
                    new com.fasterxml.jackson.core.type.TypeReference<List<String>>() {});
            } catch (Exception e) {
                // Single value - return as single-element list
                return List.of((String) value);
            }
        }
        return List.of(value.toString());
    }
    
    /**
     * Convert Object value to String for cache storage.
     */
    private String convertToString(Object value) throws Exception {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        // Convert complex objects to JSON string
        return objectMapper.writeValueAsString(value);
    }
    
    /**
     * Convert Object value to JsonNode for JSON operations.
     */
    private JsonNode convertToJsonNode(Object value) throws Exception {
        if (value == null) {
            return null;
        }
        if (value instanceof JsonNode) {
            return (JsonNode) value;
        }
        // Convert to JsonNode via ObjectMapper
        return objectMapper.valueToTree(value);
    }
    
    /**
     * Send response to the response topic.
     */
    private void sendResponse(PendingRequest pending, CacheResponse response) {
        if (pending.getAdapter() == null) {
            log.error("No adapter available to send response");
            return;
        }
        
        try {
            String responseJson = objectMapper.writeValueAsString(response);
            pending.getAdapter().publish(pending.getResponseTopic(), responseJson);
        } catch (Exception e) {
            log.error("Failed to send response: {}", e.getMessage());
        }
    }
    
    /**
     * Send error response.
     */
    private void sendErrorResponse(PendingRequest pending, String errorCode, String errorMessage) {
        sendErrorResponse(pending, null, errorCode, errorMessage);
    }
    
    private void sendErrorResponse(PendingRequest pending, CacheRequest request, 
                                   String errorCode, String errorMessage) {
        if (pending.getAdapter() == null) {
            return;
        }
        
        CacheResponse response = CacheResponse.error(
            request, errorCode, errorMessage,
            pending.getReceiveTime(), Instant.now()
        );
        
        sendResponse(pending, response);
    }
    
    /**
     * Start file watcher for configuration hot-reload.
     */
    private void startFileWatcher() {
        try {
            watchService = FileSystems.getDefault().newWatchService();
            Path dir = configFile.getParentFile().toPath();
            dir.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY);
            
            watcherThread = new Thread(() -> {
                while (running.get()) {
                    try {
                        WatchKey key = watchService.poll(1, TimeUnit.SECONDS);
                        if (key == null) continue;
                        
                        for (WatchEvent<?> event : key.pollEvents()) {
                            Path changed = (Path) event.context();
                            if (changed.toString().equals(CONFIG_FILE)) {
                                log.info("Configuration file changed, reloading...");
                                Thread.sleep(500); // Brief delay for file to be fully written
                                reloadConfiguration();
                            }
                        }
                        
                        key.reset();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        log.error("Error in file watcher: {}", e.getMessage());
                    }
                }
            }, "config-watcher");
            watcherThread.setDaemon(true);
            watcherThread.start();
            
            log.info("Started configuration file watcher");
            
        } catch (Exception e) {
            log.error("Failed to start file watcher: {}", e.getMessage());
        }
    }
    
    /**
     * Stop file watcher.
     */
    private void stopFileWatcher() {
        if (watcherThread != null) {
            watcherThread.interrupt();
        }
        if (watchService != null) {
            try {
                watchService.close();
            } catch (IOException e) {
                log.warn("Error closing watch service: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Reload configuration and update broker connections.
     */
    private synchronized void reloadConfiguration() {
        try {
            MessagingConfig newConfig = objectMapper.readValue(configFile, MessagingConfig.class);
            
            // Determine what changed
            Set<String> currentBrokers = new HashSet<>(adapters.keySet());
            Set<String> newBrokers = new HashSet<>(newConfig.getBrokers().keySet());
            
            // Disconnect removed brokers
            for (String brokerName : currentBrokers) {
                if (!newBrokers.contains(brokerName) || 
                    !newConfig.getBrokers().get(brokerName).isEnabled()) {
                    MessageBrokerAdapter adapter = adapters.remove(brokerName);
                    if (adapter != null) {
                        log.info("Disconnecting broker '{}' (removed or disabled)", brokerName);
                        adapter.disconnect();
                    }
                }
            }
            
            // Connect new/updated brokers
            for (Map.Entry<String, MessagingConfig.BrokerConfig> entry : newConfig.getBrokers().entrySet()) {
                String brokerName = entry.getKey();
                MessagingConfig.BrokerConfig brokerConfig = entry.getValue();
                
                if (!brokerConfig.isEnabled()) continue;
                
                // Check if broker needs reconnection (config changed)
                MessagingConfig.BrokerConfig oldConfig = config != null ? 
                    config.getBrokers().get(brokerName) : null;
                    
                boolean needsReconnect = !adapters.containsKey(brokerName) ||
                    oldConfig == null ||
                    !Objects.equals(oldConfig.getConnection(), brokerConfig.getConnection()) ||
                    !Objects.equals(oldConfig.getRequestTopics(), brokerConfig.getRequestTopics());
                
                if (needsReconnect) {
                    // Disconnect old adapter
                    MessageBrokerAdapter oldAdapter = adapters.remove(brokerName);
                    if (oldAdapter != null) {
                        oldAdapter.disconnect();
                    }
                    
                    // Create new adapter
                    MessageBrokerAdapter adapter = createAdapter(brokerName, brokerConfig);
                    if (adapter != null && adapter.connect()) {
                        if (adapter.subscribe(brokerConfig.getRequestTopics(), this::handleMessage)) {
                            adapters.put(brokerName, adapter);
                            log.info("Broker '{}' (re)connected: {}", brokerName, 
                                brokerConfig.getRequestTopics());
                        } else {
                            adapter.disconnect();
                        }
                    }
                }
            }
            
            config = newConfig;
            log.info("Configuration reloaded successfully");
            
        } catch (Exception e) {
            log.error("Failed to reload configuration: {}", e.getMessage());
        }
    }
    
    // ==================== Public API for Admin UI ====================
    
    /**
     * Check if service is running.
     */
    public boolean isRunning() {
        return running.get();
    }
    
    /**
     * Check if service is enabled.
     */
    public boolean isEnabled() {
        return config != null && config.isEnabled();
    }
    
    /**
     * Get current configuration.
     */
    public MessagingConfig getConfig() {
        return config;
    }
    
    /**
     * Save configuration.
     */
    public void saveConfig(MessagingConfig newConfig) throws IOException {
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(configFile, newConfig);
        // File watcher will trigger reload
    }
    
    /**
     * Get broker statistics as a Map keyed by broker name.
     */
    public Map<String, MessageBrokerAdapter.BrokerStats> getBrokerStats() {
        Map<String, MessageBrokerAdapter.BrokerStats> stats = new HashMap<>();
        for (MessageBrokerAdapter adapter : adapters.values()) {
            stats.put(adapter.getBrokerName(), adapter.getStats());
        }
        return stats;
    }
    
    /**
     * Get pending requests (for admin UI).
     */
    public List<PendingRequestInfo> getPendingRequests() {
        List<PendingRequestInfo> result = new ArrayList<>();
        if (requestQueue == null) {
            return result;
        }
        int index = 0;
        for (PendingRequest req : requestQueue) {
            result.add(new PendingRequestInfo(
                index++,
                req.getRequestTopic(),
                req.getReceiveTime().toString(),
                System.currentTimeMillis() - req.getReceiveTime().toEpochMilli()
            ));
        }
        return result;
    }
    
    /**
     * Get service statistics.
     */
    public ServiceStats getServiceStats() {
        return new ServiceStats(
            running.get(),
            config != null && config.isEnabled(),
            requestQueue != null ? requestQueue.size() : 0,
            config != null ? config.getMaxQueueDepth() : 100,
            requestsReceived.get(),
            requestsProcessed.get(),
            backpressureActive.get(),
            backpressureActivations.get(),
            backpressureStartTime > 0 ? System.currentTimeMillis() - backpressureStartTime : 0,
            requestsDrained.get(),
            authFailures.get(),
            processingErrors.get(),
            adapters.size(),
            failedSubscriptions.size()
        );
    }
    
    /**
     * Add a new broker configuration.
     */
    public void addBroker(String name, MessagingConfig.BrokerConfig brokerConfig) throws IOException {
        if (config == null) {
            config = new MessagingConfig();
        }
        config.getBrokers().put(name, brokerConfig);
        saveConfig(config);
    }
    
    /**
     * Remove a broker configuration.
     */
    public void removeBroker(String name) throws IOException {
        if (config != null) {
            config.getBrokers().remove(name);
            saveConfig(config);
        }
    }
    
    /**
     * Add a request topic to a broker.
     */
    public void addRequestTopic(String brokerName, String topic) throws IOException {
        if (config != null && config.getBrokers().containsKey(brokerName)) {
            MessagingConfig.BrokerConfig broker = config.getBrokers().get(brokerName);
            if (!broker.getRequestTopics().contains(topic)) {
                broker.getRequestTopics().add(topic);
                saveConfig(config);
            }
        }
    }
    
    /**
     * Remove a request topic from a broker.
     */
    public void removeRequestTopic(String brokerName, String topic) throws IOException {
        if (config != null && config.getBrokers().containsKey(brokerName)) {
            MessagingConfig.BrokerConfig broker = config.getBrokers().get(brokerName);
            broker.getRequestTopics().remove(topic);
            saveConfig(config);
        }
    }
    
    /**
     * Globally enable the Request/Response messaging service.
     * This enables the service and connects all configured brokers.
     * 
     * @return true if successful
     */
    public boolean enableService() {
        if (config == null) {
            log.warn("Cannot enable messaging service - no configuration loaded");
            return false;
        }
        
        if (config.isEnabled() && running.get()) {
            log.info("Messaging service is already enabled and running");
            return true;
        }
        
        config.setEnabled(true);
        try {
            saveConfig(config);
        } catch (IOException e) {
            log.error("Failed to save config after enabling service: {}", e.getMessage());
            return false;
        }
        
        // If not yet started, start the service
        if (!running.get()) {
            initialized.set(false);  // Reset to allow re-initialization
            start();
        }
        
        log.info("Messaging service globally enabled");
        return true;
    }
    
    /**
     * Globally disable the Request/Response messaging service.
     * This disconnects all brokers and stops processing.
     * 
     * @return true if successful
     */
    public boolean disableService() {
        if (config == null) {
            log.warn("Cannot disable messaging service - no configuration loaded");
            return false;
        }
        
        config.setEnabled(false);
        try {
            saveConfig(config);
        } catch (IOException e) {
            log.error("Failed to save config after disabling service: {}", e.getMessage());
            return false;
        }
        
        // Stop the service if running
        if (running.get()) {
            stop();
            initialized.set(false);  // Allow re-initialization if enabled again
        }
        
        log.info("Messaging service globally disabled");
        return true;
    }
    
    /**
     * Check if the service is globally enabled in configuration.
     * 
     * @return true if enabled in config
     */
    public boolean isServiceEnabled() {
        return config != null && config.isEnabled();
    }
    
    /**
     * Drain requests from the queue.
     * 
     * @param count number of requests to drain (0 = drain all)
     * @return number of requests actually drained
     */
    public int drainQueue(int count) {
        if (requestQueue == null) {
            return 0;
        }
        
        List<PendingRequest> drained = new ArrayList<>();
        
        if (count <= 0) {
            // Drain all
            requestQueue.drainTo(drained);
        } else {
            // Drain specific count
            requestQueue.drainTo(drained, count);
        }
        
        // Send error responses for drained requests
        Instant now = Instant.now();
        for (PendingRequest pending : drained) {
            try {
                CacheRequest request = null;
                try {
                    request = objectMapper.readValue(pending.getRequestJson(), CacheRequest.class);
                } catch (Exception e) {
                    // Couldn't parse, create minimal response
                }
                
                CacheResponse response = CacheResponse.error(
                    request, 
                    CacheResponse.ErrorCode.INTERNAL_ERROR,
                    "Request drained by administrator",
                    pending.getReceiveTime(), 
                    now
                );
                
                if (pending.getAdapter() != null) {
                    String responseJson = objectMapper.writeValueAsString(response);
                    pending.getAdapter().publish(pending.getResponseTopic(), responseJson);
                }
            } catch (Exception e) {
                log.warn("Failed to send drain response: {}", e.getMessage());
            }
        }
        
        int drainedCount = drained.size();
        requestsDrained.addAndGet(drainedCount);
        log.info("Drained {} requests from queue", drainedCount);
        
        return drainedCount;
    }
    
    /**
     * Cancel a specific request by position.
     * 
     * @param position position in queue (0-based)
     * @return true if cancelled
     */
    public boolean cancelRequest(int position) {
        if (requestQueue == null || position < 0) {
            return false;
        }
        
        // This is inefficient but queue doesn't support random access removal
        List<PendingRequest> all = new ArrayList<>();
        requestQueue.drainTo(all);
        
        if (position >= all.size()) {
            // Put them all back
            requestQueue.addAll(all);
            return false;
        }
        
        PendingRequest cancelled = all.remove(position);
        requestQueue.addAll(all);
        
        // Send error response
        try {
            CacheRequest request = null;
            try {
                request = objectMapper.readValue(cancelled.getRequestJson(), CacheRequest.class);
            } catch (Exception e) {
                // Couldn't parse
            }
            
            CacheResponse response = CacheResponse.error(
                request,
                CacheResponse.ErrorCode.INTERNAL_ERROR,
                "Request cancelled by administrator",
                cancelled.getReceiveTime(),
                Instant.now()
            );
            
            if (cancelled.getAdapter() != null) {
                String responseJson = objectMapper.writeValueAsString(response);
                cancelled.getAdapter().publish(cancelled.getResponseTopic(), responseJson);
            }
        } catch (Exception e) {
            log.warn("Failed to send cancel response: {}", e.getMessage());
        }
        
        requestsDrained.incrementAndGet();
        log.info("Cancelled request at position {}", position);
        return true;
    }
    
    /**
     * Get failed subscriptions.
     */
    public List<FailedSubscription> getFailedSubscriptions() {
        return new ArrayList<>(failedSubscriptions.values());
    }
    
    /**
     * Retry failed subscription.
     */
    public boolean retryFailedSubscription(String brokerName) {
        FailedSubscription failed = failedSubscriptions.get(brokerName);
        if (failed == null || config == null) {
            return false;
        }
        
        MessagingConfig.BrokerConfig brokerConfig = config.getBrokers().get(brokerName);
        if (brokerConfig == null) {
            return false;
        }
        
        // Try to connect and subscribe again
        MessageBrokerAdapter adapter = createAdapter(brokerName, brokerConfig);
        if (adapter == null) {
            return false;
        }
        
        if (!adapter.connect()) {
            return false;
        }
        
        if (adapter.subscribe(brokerConfig.getRequestTopics(), this::handleMessage)) {
            adapters.put(brokerName, adapter);
            failedSubscriptions.remove(brokerName);
            log.info("Successfully retried subscription for broker '{}'", brokerName);
            return true;
        } else {
            adapter.disconnect();
            return false;
        }
    }
    
    // ==================== Broker Control Methods ====================
    
    /**
     * Enable a broker - connect and start consuming messages.
     * If already connected, does nothing.
     * 
     * @param brokerName the broker name
     * @return true if successful
     */
    public boolean enableBroker(String brokerName) {
        if (config == null || !config.getBrokers().containsKey(brokerName)) {
            log.warn("Broker '{}' not found in configuration", brokerName);
            return false;
        }
        
        // Check if already connected
        if (adapters.containsKey(brokerName)) {
            MessageBrokerAdapter existing = adapters.get(brokerName);
            if (existing.isConnected()) {
                log.info("Broker '{}' is already connected", brokerName);
                return true;
            }
        }
        
        MessagingConfig.BrokerConfig brokerConfig = config.getBrokers().get(brokerName);
        
        // Update config to enabled
        brokerConfig.setEnabled(true);
        try {
            saveConfig(config);
        } catch (IOException e) {
            log.error("Failed to save config after enabling broker: {}", e.getMessage());
        }
        
        // Create and connect adapter
        MessageBrokerAdapter adapter = createAdapter(brokerName, brokerConfig);
        if (adapter == null) {
            log.error("Unknown broker type for '{}'", brokerName);
            failedSubscriptions.put(brokerName, new FailedSubscription(
                brokerName, brokerConfig.getType(), 
                "Unknown broker type: " + brokerConfig.getType(), 
                System.currentTimeMillis(), brokerConfig.getRequestTopics()));
            return false;
        }
        
        if (!adapter.connect()) {
            log.error("Failed to connect to broker '{}'", brokerName);
            failedSubscriptions.put(brokerName, new FailedSubscription(
                brokerName, brokerConfig.getType(), 
                "Connection failed", 
                System.currentTimeMillis(), brokerConfig.getRequestTopics()));
            return false;
        }
        
        if (adapter.subscribe(brokerConfig.getRequestTopics(), this::handleMessage)) {
            adapters.put(brokerName, adapter);
            failedSubscriptions.remove(brokerName);
            log.info("Broker '{}' enabled and connected successfully", brokerName);
            return true;
        } else {
            log.error("Failed to subscribe broker '{}'", brokerName);
            adapter.disconnect();
            failedSubscriptions.put(brokerName, new FailedSubscription(
                brokerName, brokerConfig.getType(), 
                "Subscription failed", 
                System.currentTimeMillis(), brokerConfig.getRequestTopics()));
            return false;
        }
    }
    
    /**
     * Disable a broker - disconnect and stop consuming messages.
     * 
     * @param brokerName the broker name
     * @return true if successful
     */
    public boolean disableBroker(String brokerName) {
        if (config == null || !config.getBrokers().containsKey(brokerName)) {
            log.warn("Broker '{}' not found in configuration", brokerName);
            return false;
        }
        
        // Update config to disabled
        MessagingConfig.BrokerConfig brokerConfig = config.getBrokers().get(brokerName);
        brokerConfig.setEnabled(false);
        try {
            saveConfig(config);
        } catch (IOException e) {
            log.error("Failed to save config after disabling broker: {}", e.getMessage());
        }
        
        // Disconnect adapter if connected
        MessageBrokerAdapter adapter = adapters.remove(brokerName);
        if (adapter != null) {
            try {
                adapter.disconnect();
                log.info("Broker '{}' disabled and disconnected", brokerName);
            } catch (Exception e) {
                log.warn("Error disconnecting broker '{}': {}", brokerName, e.getMessage());
            }
        }
        
        // Remove from failed subscriptions if present
        failedSubscriptions.remove(brokerName);
        
        return true;
    }
    
    /**
     * Pause a broker - stop consuming messages but keep connection open.
     * Messages will queue up at the broker.
     * 
     * @param brokerName the broker name
     * @return true if successful
     */
    public boolean pauseBroker(String brokerName) {
        MessageBrokerAdapter adapter = adapters.get(brokerName);
        if (adapter == null) {
            log.warn("Broker '{}' is not connected, cannot pause", brokerName);
            return false;
        }
        
        if (adapter.isPaused()) {
            log.info("Broker '{}' is already paused", brokerName);
            return true;
        }
        
        adapter.pauseConsumption();
        log.info("Broker '{}' paused - message consumption stopped", brokerName);
        return true;
    }
    
    /**
     * Resume a broker - resume consuming messages.
     * 
     * @param brokerName the broker name
     * @return true if successful
     */
    public boolean resumeBroker(String brokerName) {
        MessageBrokerAdapter adapter = adapters.get(brokerName);
        if (adapter == null) {
            log.warn("Broker '{}' is not connected, cannot resume", brokerName);
            return false;
        }
        
        if (!adapter.isPaused()) {
            log.info("Broker '{}' is not paused", brokerName);
            return true;
        }
        
        adapter.resumeConsumption();
        log.info("Broker '{}' resumed - message consumption restarted", brokerName);
        return true;
    }
    
    /**
     * Get the status of a specific broker.
     * 
     * @param brokerName the broker name
     * @return broker status map
     */
    public Map<String, Object> getBrokerStatus(String brokerName) {
        Map<String, Object> status = new HashMap<>();
        
        if (config == null || !config.getBrokers().containsKey(brokerName)) {
            status.put("exists", false);
            return status;
        }
        
        MessagingConfig.BrokerConfig brokerConfig = config.getBrokers().get(brokerName);
        status.put("exists", true);
        status.put("configEnabled", brokerConfig.isEnabled());
        status.put("type", brokerConfig.getType());
        status.put("displayName", brokerConfig.getDisplayName());
        
        MessageBrokerAdapter adapter = adapters.get(brokerName);
        if (adapter != null) {
            status.put("connected", adapter.isConnected());
            status.put("paused", adapter.isPaused());
            MessageBrokerAdapter.BrokerStats stats = adapter.getStats();
            status.put("messagesReceived", stats.getMessagesReceived());
            status.put("messagesPublished", stats.getMessagesPublished());
            status.put("errors", stats.getErrors());
        } else {
            status.put("connected", false);
            status.put("paused", false);
        }
        
        FailedSubscription failed = failedSubscriptions.get(brokerName);
        if (failed != null) {
            status.put("failed", true);
            status.put("failedReason", failed.getErrorMessage());
        } else {
            status.put("failed", false);
        }
        
        return status;
    }
    
    // ==================== Inner Classes ====================
    
    /**
     * Pending request in queue.
     */
    @Data
    @AllArgsConstructor
    private static class PendingRequest {
        private final String requestJson;
        private final String requestTopic;
        private final String responseTopic;
        private final MessageBrokerAdapter adapter;
        private final Instant receiveTime;
    }
    
    /**
     * Pending request info for admin UI.
     */
    @Data
    @AllArgsConstructor
    public static class PendingRequestInfo {
        private final int position;
        private final String topic;
        private final String receiveTime;
        private final long waitingMs;
    }
    
    /**
     * Failed subscription info.
     */
    @Data
    @AllArgsConstructor
    public static class FailedSubscription {
        private final String brokerName;
        private final String brokerType;
        private final String errorMessage;
        private final long failedAt;
        private final List<String> topics;
    }
    
    /**
     * Service statistics.
     */
    @Data
    @AllArgsConstructor
    public static class ServiceStats {
        private final boolean running;
        private final boolean enabled;
        private final int queueSize;
        private final int maxQueueSize;
        private final long requestsReceived;
        private final long requestsProcessed;
        private final boolean backpressureActive;
        private final long backpressureActivations;
        private final long backpressureDurationMs;
        private final long requestsDrained;
        private final long authFailures;
        private final long processingErrors;
        private final int activeBrokers;
        private final int failedBrokers;
    }
}
