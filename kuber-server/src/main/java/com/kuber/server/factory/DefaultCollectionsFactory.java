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
package com.kuber.server.factory;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

/**
 * Default implementation of {@link CollectionsFactory} using Java concurrent collections.
 * 
 * <p>This factory creates thread-safe collections using the java.util.concurrent package:
 * <ul>
 *   <li><b>Map</b>: {@link ConcurrentHashMap} - lock-striped concurrent map</li>
 *   <li><b>List</b>: {@link CopyOnWriteArrayList} - snapshot-based concurrent list</li>
 *   <li><b>Set</b>: {@link ConcurrentHashMap#newKeySet()} - concurrent hash set</li>
 *   <li><b>Queue</b>: {@link ConcurrentLinkedQueue} - lock-free concurrent queue</li>
 *   <li><b>Deque</b>: {@link ConcurrentLinkedDeque} - lock-free concurrent deque</li>
 * </ul>
 * 
 * <p>Configuration options:
 * <ul>
 *   <li><b>threadSafe</b>: If false, uses non-thread-safe collections (HashMap, ArrayList, etc.)</li>
 *   <li><b>ordered</b>: If true, uses LinkedHashMap/LinkedHashSet (maintains insertion order)</li>
 *   <li><b>sorted</b>: If true, uses TreeMap/TreeSet (natural ordering)</li>
 * </ul>
 * 
 * @version 1.5.0
 * @since 1.5.0
 */
@Slf4j
@Component
public class DefaultCollectionsFactory implements CollectionsFactory {
    
    private static final String TYPE = "DEFAULT";
    
    // ==================== MAP ====================
    
    @Override
    public <K, V> Map<K, V> createMap(String name) {
        return createMap(name, CollectionsConfig.defaults());
    }
    
    @Override
    public <K, V> Map<K, V> createMap(String name, CollectionsConfig config) {
        Map<K, V> map;
        
        if (config.isSorted()) {
            // Sorted map
            if (config.isThreadSafe()) {
                map = new ConcurrentSkipListMap<>();
                log.debug("Created ConcurrentSkipListMap '{}' (sorted, thread-safe)", name);
            } else {
                map = new TreeMap<>();
                log.debug("Created TreeMap '{}' (sorted, non-thread-safe)", name);
            }
        } else if (config.isOrdered()) {
            // Ordered map (maintains insertion order)
            if (config.isThreadSafe()) {
                // No concurrent ordered map in JDK, use synchronized wrapper
                map = Collections.synchronizedMap(new LinkedHashMap<>(
                        config.getInitialCapacity(), config.getLoadFactor()));
                log.debug("Created synchronized LinkedHashMap '{}' (ordered, thread-safe)", name);
            } else {
                map = new LinkedHashMap<>(config.getInitialCapacity(), config.getLoadFactor());
                log.debug("Created LinkedHashMap '{}' (ordered, non-thread-safe)", name);
            }
        } else {
            // Standard map
            if (config.isThreadSafe()) {
                map = new ConcurrentHashMap<>(
                        config.getInitialCapacity(),
                        config.getLoadFactor(),
                        config.getConcurrencyLevel());
                log.debug("Created ConcurrentHashMap '{}': initialCapacity={}, concurrencyLevel={}", 
                        name, config.getInitialCapacity(), config.getConcurrencyLevel());
            } else {
                map = new HashMap<>(config.getInitialCapacity(), config.getLoadFactor());
                log.debug("Created HashMap '{}' (non-thread-safe)", name);
            }
        }
        
        log.info("Created Map '{}': type={}, threadSafe={}, ordered={}, sorted={}", 
                name, map.getClass().getSimpleName(), config.isThreadSafe(), 
                config.isOrdered(), config.isSorted());
        
        return map;
    }
    
    // ==================== LIST ====================
    
    @Override
    public <E> List<E> createList(String name) {
        return createList(name, CollectionsConfig.defaults());
    }
    
    @Override
    public <E> List<E> createList(String name, CollectionsConfig config) {
        List<E> list;
        
        if (config.isThreadSafe()) {
            list = new CopyOnWriteArrayList<>();
            log.debug("Created CopyOnWriteArrayList '{}' (thread-safe)", name);
        } else {
            list = new ArrayList<>(config.getInitialCapacity());
            log.debug("Created ArrayList '{}' (non-thread-safe)", name);
        }
        
        log.info("Created List '{}': type={}, threadSafe={}", 
                name, list.getClass().getSimpleName(), config.isThreadSafe());
        
        return list;
    }
    
    // ==================== SET ====================
    
    @Override
    public <E> Set<E> createSet(String name) {
        return createSet(name, CollectionsConfig.defaults());
    }
    
    @Override
    public <E> Set<E> createSet(String name, CollectionsConfig config) {
        Set<E> set;
        
        if (config.isSorted()) {
            // Sorted set
            if (config.isThreadSafe()) {
                set = new ConcurrentSkipListSet<>();
                log.debug("Created ConcurrentSkipListSet '{}' (sorted, thread-safe)", name);
            } else {
                set = new TreeSet<>();
                log.debug("Created TreeSet '{}' (sorted, non-thread-safe)", name);
            }
        } else if (config.isOrdered()) {
            // Ordered set (maintains insertion order)
            if (config.isThreadSafe()) {
                // No concurrent ordered set in JDK, use synchronized wrapper
                set = Collections.synchronizedSet(new LinkedHashSet<>(config.getInitialCapacity()));
                log.debug("Created synchronized LinkedHashSet '{}' (ordered, thread-safe)", name);
            } else {
                set = new LinkedHashSet<>(config.getInitialCapacity());
                log.debug("Created LinkedHashSet '{}' (ordered, non-thread-safe)", name);
            }
        } else {
            // Standard set
            if (config.isThreadSafe()) {
                set = ConcurrentHashMap.newKeySet(config.getInitialCapacity());
                log.debug("Created ConcurrentHashMap.KeySetView '{}' (thread-safe)", name);
            } else {
                set = new HashSet<>(config.getInitialCapacity());
                log.debug("Created HashSet '{}' (non-thread-safe)", name);
            }
        }
        
        log.info("Created Set '{}': type={}, threadSafe={}, ordered={}, sorted={}", 
                name, set.getClass().getSimpleName(), config.isThreadSafe(), 
                config.isOrdered(), config.isSorted());
        
        return set;
    }
    
    // ==================== QUEUE ====================
    
    @Override
    public <E> Queue<E> createQueue(String name) {
        return createQueue(name, CollectionsConfig.defaults());
    }
    
    @Override
    public <E> Queue<E> createQueue(String name, CollectionsConfig config) {
        Queue<E> queue;
        
        if (config.isThreadSafe()) {
            queue = new ConcurrentLinkedQueue<>();
            log.debug("Created ConcurrentLinkedQueue '{}' (thread-safe)", name);
        } else {
            queue = new LinkedList<>();
            log.debug("Created LinkedList as Queue '{}' (non-thread-safe)", name);
        }
        
        log.info("Created Queue '{}': type={}, threadSafe={}", 
                name, queue.getClass().getSimpleName(), config.isThreadSafe());
        
        return queue;
    }
    
    // ==================== DEQUE ====================
    
    @Override
    public <E> Deque<E> createDeque(String name) {
        return createDeque(name, CollectionsConfig.defaults());
    }
    
    @Override
    public <E> Deque<E> createDeque(String name, CollectionsConfig config) {
        Deque<E> deque;
        
        if (config.isThreadSafe()) {
            deque = new ConcurrentLinkedDeque<>();
            log.debug("Created ConcurrentLinkedDeque '{}' (thread-safe)", name);
        } else {
            deque = new ArrayDeque<>(config.getInitialCapacity());
            log.debug("Created ArrayDeque '{}' (non-thread-safe)", name);
        }
        
        log.info("Created Deque '{}': type={}, threadSafe={}", 
                name, deque.getClass().getSimpleName(), config.isThreadSafe());
        
        return deque;
    }
    
    // ==================== METADATA ====================
    
    @Override
    public String getType() {
        return TYPE;
    }
}
