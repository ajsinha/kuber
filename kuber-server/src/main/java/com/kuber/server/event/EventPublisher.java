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
package com.kuber.server.event;

import com.kuber.core.model.CacheEvent;
import com.kuber.core.protocol.RedisResponse;
import com.kuber.core.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.mina.core.session.IoSession;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Event publisher for cache events and pub/sub functionality.
 * Supports channel subscriptions and pattern-based subscriptions.
 */
@Slf4j
@Component
public class EventPublisher {
    
    // Channel -> Set of subscribers
    private final Map<String, Set<IoSession>> channelSubscribers = new ConcurrentHashMap<>();
    
    // Pattern -> Set of subscribers
    private final Map<String, Set<IoSession>> patternSubscribers = new ConcurrentHashMap<>();
    
    // Event listeners for internal use
    private final List<CacheEventListener> eventListeners = new ArrayList<>();
    
    /**
     * Subscribe a session to a channel
     */
    public void subscribe(String channel, IoSession session) {
        channelSubscribers.computeIfAbsent(channel, k -> new CopyOnWriteArraySet<>())
                .add(session);
        log.debug("Session {} subscribed to channel '{}'", session.getId(), channel);
    }
    
    /**
     * Unsubscribe a session from a channel
     */
    public void unsubscribe(String channel, IoSession session) {
        Set<IoSession> subscribers = channelSubscribers.get(channel);
        if (subscribers != null) {
            subscribers.remove(session);
            if (subscribers.isEmpty()) {
                channelSubscribers.remove(channel);
            }
        }
        log.debug("Session {} unsubscribed from channel '{}'", session.getId(), channel);
    }
    
    /**
     * Subscribe a session to a pattern
     */
    public void psubscribe(String pattern, IoSession session) {
        patternSubscribers.computeIfAbsent(pattern, k -> new CopyOnWriteArraySet<>())
                .add(session);
        log.debug("Session {} subscribed to pattern '{}'", session.getId(), pattern);
    }
    
    /**
     * Unsubscribe a session from a pattern
     */
    public void punsubscribe(String pattern, IoSession session) {
        Set<IoSession> subscribers = patternSubscribers.get(pattern);
        if (subscribers != null) {
            subscribers.remove(session);
            if (subscribers.isEmpty()) {
                patternSubscribers.remove(pattern);
            }
        }
    }
    
    /**
     * Publish a message to a channel
     */
    public int publish(String channel, String message) {
        int count = 0;
        
        // Send to direct channel subscribers
        Set<IoSession> subscribers = channelSubscribers.get(channel);
        if (subscribers != null) {
            for (IoSession session : subscribers) {
                if (session.isConnected()) {
                    sendMessage(session, "message", channel, message);
                    count++;
                }
            }
        }
        
        // Send to pattern subscribers
        for (Map.Entry<String, Set<IoSession>> entry : patternSubscribers.entrySet()) {
            if (matchPattern(entry.getKey(), channel)) {
                for (IoSession session : entry.getValue()) {
                    if (session.isConnected()) {
                        sendPMessage(session, entry.getKey(), channel, message);
                        count++;
                    }
                }
            }
        }
        
        return count;
    }
    
    /**
     * Publish a cache event
     */
    public void publish(CacheEvent event) {
        String channel = event.getChannel();
        String message = JsonUtils.toJson(event);
        
        // Publish to Redis pub/sub
        publish(channel, message);
        
        // Notify internal listeners
        for (CacheEventListener listener : eventListeners) {
            try {
                listener.onEvent(event);
            } catch (Exception e) {
                log.error("Error notifying event listener: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Register an internal event listener
     */
    public void addListener(CacheEventListener listener) {
        eventListeners.add(listener);
    }
    
    /**
     * Remove an internal event listener
     */
    public void removeListener(CacheEventListener listener) {
        eventListeners.remove(listener);
    }
    
    /**
     * Get the number of subscribers for a channel
     */
    public int getSubscriberCount(String channel) {
        Set<IoSession> subscribers = channelSubscribers.get(channel);
        return subscribers != null ? subscribers.size() : 0;
    }
    
    /**
     * Get all channels with subscribers
     */
    public Set<String> getActiveChannels() {
        return new HashSet<>(channelSubscribers.keySet());
    }
    
    /**
     * Get all patterns with subscribers
     */
    public Set<String> getActivePatterns() {
        return new HashSet<>(patternSubscribers.keySet());
    }
    
    /**
     * Get total number of subscriptions
     */
    public int getTotalSubscriptions() {
        int count = 0;
        for (Set<IoSession> subscribers : channelSubscribers.values()) {
            count += subscribers.size();
        }
        for (Set<IoSession> subscribers : patternSubscribers.values()) {
            count += subscribers.size();
        }
        return count;
    }
    
    private void sendMessage(IoSession session, String type, String channel, String message) {
        List<RedisResponse> parts = new ArrayList<>();
        parts.add(RedisResponse.bulkString(type));
        parts.add(RedisResponse.bulkString(channel));
        parts.add(RedisResponse.bulkString(message));
        
        session.write(RedisResponse.array(parts).encode());
    }
    
    private void sendPMessage(IoSession session, String pattern, String channel, String message) {
        List<RedisResponse> parts = new ArrayList<>();
        parts.add(RedisResponse.bulkString("pmessage"));
        parts.add(RedisResponse.bulkString(pattern));
        parts.add(RedisResponse.bulkString(channel));
        parts.add(RedisResponse.bulkString(message));
        
        session.write(RedisResponse.array(parts).encode());
    }
    
    private boolean matchPattern(String pattern, String channel) {
        // Simple glob pattern matching
        String regex = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        return channel.matches(regex);
    }
    
    /**
     * Interface for internal event listeners
     */
    public interface CacheEventListener {
        void onEvent(CacheEvent event);
    }
}
