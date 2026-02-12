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

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Factory interface for creating thread-safe collection instances.
 * 
 * <p>This factory abstraction allows different collection implementations to be
 * plugged in without changing the consuming code. All collections created by
 * this factory are thread-safe by default.
 * 
 * <p>Supported collection types:
 * <ul>
 *   <li>{@link Map} - Key-value mappings (ConcurrentHashMap, etc.)</li>
 *   <li>{@link List} - Ordered, indexed collections (CopyOnWriteArrayList, etc.)</li>
 *   <li>{@link Set} - Unique element collections (ConcurrentHashMap.newKeySet, etc.)</li>
 *   <li>{@link Queue} - FIFO collections (ConcurrentLinkedQueue, etc.)</li>
 *   <li>{@link Deque} - Double-ended queues (ConcurrentLinkedDeque, etc.)</li>
 * </ul>
 * 
 * <p>Example usage:
 * <pre>{@code
 * CollectionsFactory factory = new DefaultCollectionsFactory();
 * 
 * // Create a thread-safe map
 * Map<String, MyObject> map = factory.createMap("myMap");
 * 
 * // Create a thread-safe list
 * List<String> list = factory.createList("myList");
 * 
 * // Create a thread-safe set with configuration
 * Set<String> set = factory.createSet("mySet", 
 *     CollectionsConfig.builder()
 *         .initialCapacity(1000)
 *         .build());
 * 
 * // Create a thread-safe queue
 * Queue<Task> queue = factory.createQueue("taskQueue");
 * }</pre>
 * 
 * @version 2.3.0
 * @since 1.5.0
 */
public interface CollectionsFactory {
    
    // ==================== MAP ====================
    
    /**
     * Create a new thread-safe Map instance with default configuration.
     * 
     * @param <K> the type of keys maintained by the map
     * @param <V> the type of mapped values
     * @param name the name of the map (for identification/metrics)
     * @return a new thread-safe map instance
     */
    <K, V> Map<K, V> createMap(String name);
    
    /**
     * Create a new thread-safe Map instance with configuration.
     * 
     * @param <K> the type of keys maintained by the map
     * @param <V> the type of mapped values
     * @param name the name of the map
     * @param config the collection configuration
     * @return a new thread-safe map instance
     */
    <K, V> Map<K, V> createMap(String name, CollectionsConfig config);
    
    // ==================== LIST ====================
    
    /**
     * Create a new thread-safe List instance with default configuration.
     * 
     * @param <E> the type of elements in the list
     * @param name the name of the list (for identification/metrics)
     * @return a new thread-safe list instance
     */
    <E> List<E> createList(String name);
    
    /**
     * Create a new thread-safe List instance with configuration.
     * 
     * @param <E> the type of elements in the list
     * @param name the name of the list
     * @param config the collection configuration
     * @return a new thread-safe list instance
     */
    <E> List<E> createList(String name, CollectionsConfig config);
    
    // ==================== SET ====================
    
    /**
     * Create a new thread-safe Set instance with default configuration.
     * 
     * @param <E> the type of elements in the set
     * @param name the name of the set (for identification/metrics)
     * @return a new thread-safe set instance
     */
    <E> Set<E> createSet(String name);
    
    /**
     * Create a new thread-safe Set instance with configuration.
     * 
     * @param <E> the type of elements in the set
     * @param name the name of the set
     * @param config the collection configuration
     * @return a new thread-safe set instance
     */
    <E> Set<E> createSet(String name, CollectionsConfig config);
    
    // ==================== QUEUE ====================
    
    /**
     * Create a new thread-safe Queue instance with default configuration.
     * 
     * @param <E> the type of elements in the queue
     * @param name the name of the queue (for identification/metrics)
     * @return a new thread-safe queue instance
     */
    <E> Queue<E> createQueue(String name);
    
    /**
     * Create a new thread-safe Queue instance with configuration.
     * 
     * @param <E> the type of elements in the queue
     * @param name the name of the queue
     * @param config the collection configuration
     * @return a new thread-safe queue instance
     */
    <E> Queue<E> createQueue(String name, CollectionsConfig config);
    
    // ==================== DEQUE ====================
    
    /**
     * Create a new thread-safe Deque (double-ended queue) instance with default configuration.
     * 
     * @param <E> the type of elements in the deque
     * @param name the name of the deque (for identification/metrics)
     * @return a new thread-safe deque instance
     */
    <E> Deque<E> createDeque(String name);
    
    /**
     * Create a new thread-safe Deque instance with configuration.
     * 
     * @param <E> the type of elements in the deque
     * @param name the name of the deque
     * @param config the collection configuration
     * @return a new thread-safe deque instance
     */
    <E> Deque<E> createDeque(String name, CollectionsConfig config);
    
    // ==================== STACK (via Deque) ====================
    
    /**
     * Create a new thread-safe Stack-like Deque instance.
     * Uses Deque interface as Stack class is legacy.
     * 
     * @param <E> the type of elements in the stack
     * @param name the name of the stack (for identification/metrics)
     * @return a new thread-safe deque usable as a stack (LIFO)
     */
    default <E> Deque<E> createStack(String name) {
        return createDeque(name);
    }
    
    /**
     * Create a new thread-safe Stack-like Deque instance with configuration.
     * 
     * @param <E> the type of elements in the stack
     * @param name the name of the stack
     * @param config the collection configuration
     * @return a new thread-safe deque usable as a stack (LIFO)
     */
    default <E> Deque<E> createStack(String name, CollectionsConfig config) {
        return createDeque(name, config);
    }
    
    // ==================== METADATA ====================
    
    /**
     * Get the type of collections this factory creates.
     * 
     * @return the factory type (e.g., "DEFAULT", "CUSTOM")
     */
    String getType();
    
    /**
     * Collection implementation type enumeration.
     */
    enum CollectionType {
        /** Default Java concurrent collections */
        DEFAULT,
        /** Custom implementation */
        CUSTOM
    }
}
