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
package com.kuber.server.network;

import com.kuber.server.config.KuberProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.executor.ExecutorFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Redis protocol server using Apache MINA.
 * Handles incoming Redis protocol connections.
 * 
 * <p>The server is started by {@link com.kuber.server.startup.StartupOrchestrator}
 * after the cache service has been initialized and data has been recovered from
 * persistence. This prevents clients from connecting before the cache is ready.
 * 
 * @version 2.0.0
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisProtocolServer {
    
    private final KuberProperties properties;
    private final RedisProtocolHandler protocolHandler;
    
    private IoAcceptor acceptor;
    private final AtomicBoolean started = new AtomicBoolean(false);
    
    /**
     * Start the Redis protocol server.
     * Called by StartupOrchestrator after cache service is initialized.
     * This ensures clients cannot connect before data recovery is complete.
     */
    public synchronized void startServer() {
        if (started.get()) {
            log.warn("Redis protocol server already started, skipping...");
            return;
        }
        
        try {
            KuberProperties.Network networkConfig = properties.getNetwork();
            
            int processorCount = networkConfig.getIoProcessorCount();
            if (processorCount <= 0) {
                processorCount = Runtime.getRuntime().availableProcessors();
            }
            
            acceptor = new NioSocketAcceptor(processorCount);
            
            // Configure RESP protocol codec with configurable max line length
            RedisProtocolCodecFactory codecFactory = new RedisProtocolCodecFactory(
                    StandardCharsets.UTF_8,
                    networkConfig.getDecoderMaxLineLength()
            );
            
            acceptor.getFilterChain().addLast("codec", 
                    new ProtocolCodecFilter(codecFactory));
            
            // Add executor filter for concurrent handling
            acceptor.getFilterChain().addLast("executor",
                    new ExecutorFilter(Executors.newCachedThreadPool()));
            
            // Configure session settings
            acceptor.getSessionConfig().setReadBufferSize(
                    networkConfig.getReadBufferSize());
            acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE,
                    (int) (networkConfig.getConnectionTimeoutMs() / 1000));
            
            // Set the protocol handler
            acceptor.setHandler(protocolHandler);
            
            // Bind to the configured port
            InetSocketAddress bindAddress = new InetSocketAddress(
                    networkConfig.getBindAddress(),
                    networkConfig.getPort());
            
            acceptor.bind(bindAddress);
            started.set(true);
            
            log.info("Redis protocol server started on {}:{}", 
                    networkConfig.getBindAddress(), 
                    networkConfig.getPort());
            log.info("Max line length: {} bytes", networkConfig.getDecoderMaxLineLength());
            
        } catch (IOException e) {
            log.error("Failed to start Redis protocol server", e);
            throw new RuntimeException("Failed to start Redis protocol server", e);
        }
    }
    
    @PreDestroy
    public void stop() {
        if (acceptor != null) {
            log.info("Stopping Redis protocol server...");
            acceptor.unbind();
            acceptor.dispose();
            log.info("Redis protocol server stopped");
        }
    }
    
    /**
     * Get the number of active connections
     */
    public int getActiveConnections() {
        return acceptor != null ? acceptor.getManagedSessionCount() : 0;
    }
    
    /**
     * Check if the server is running
     */
    public boolean isRunning() {
        return started.get() && acceptor != null && acceptor.isActive();
    }
}
