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

import com.fasterxml.jackson.databind.JsonNode;
import com.kuber.core.constants.KuberConstants;
import com.kuber.core.exception.KuberException;
import com.kuber.core.exception.ReadOnlyException;
import com.kuber.core.model.CacheEntry;
import com.kuber.core.model.CacheRegion;
import com.kuber.core.protocol.RedisCommand;
import com.kuber.core.protocol.RedisResponse;
import com.kuber.core.util.JsonUtils;
import com.kuber.server.cache.CacheService;
import com.kuber.server.config.KuberProperties;
import com.kuber.server.event.EventPublisher;
import com.kuber.server.replication.ReplicationManager;
import com.kuber.server.security.ApiKey;
import com.kuber.server.security.ApiKeyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Handles Redis protocol commands via MINA.
 * Supports standard Redis commands plus Kuber extensions for regions and JSON.
 * 
 * Authentication methods:
 * 1. Password: AUTH password
 * 2. API Key: AUTH APIKEY kub_xxx...
 *
 * @version 1.3.9
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisProtocolHandler extends IoHandlerAdapter {
    
    private static final String SESSION_REGION = "currentRegion";
    private static final String SESSION_AUTHENTICATED = "authenticated";
    private static final String SESSION_AUTH_USER = "authUser";
    private static final String SESSION_IN_MULTI = "inMulti";
    private static final String SESSION_QUEUED_COMMANDS = "queuedCommands";
    private static final String SESSION_SUBSCRIPTIONS = "subscriptions";
    
    private final CacheService cacheService;
    private final KuberProperties properties;
    private final EventPublisher eventPublisher;
    private final ApiKeyService apiKeyService;
    
    @Autowired(required = false)
    private ReplicationManager replicationManager;
    
    // Track active sessions
    private final Map<Long, IoSession> activeSessions = new ConcurrentHashMap<>();
    
    @Override
    public void sessionCreated(IoSession session) {
        log.debug("New connection from: {}", session.getRemoteAddress());
        session.setAttribute(SESSION_REGION, KuberConstants.DEFAULT_REGION);
        session.setAttribute(SESSION_AUTHENTICATED, 
                properties.getSecurity().getRedisPassword() == null);
        activeSessions.put(session.getId(), session);
    }
    
    @Override
    public void sessionClosed(IoSession session) {
        log.debug("Connection closed: {}", session.getRemoteAddress());
        activeSessions.remove(session.getId());
        
        // Clean up subscriptions
        @SuppressWarnings("unchecked")
        Set<String> subscriptions = (Set<String>) session.getAttribute(SESSION_SUBSCRIPTIONS);
        if (subscriptions != null) {
            for (String channel : subscriptions) {
                eventPublisher.unsubscribe(channel, session);
            }
        }
    }
    
    @Override
    public void sessionIdle(IoSession session, IdleStatus status) {
        log.debug("Session idle, closing: {}", session.getRemoteAddress());
        session.closeNow();
    }
    
    @Override
    public void exceptionCaught(IoSession session, Throwable cause) {
        log.error("Error in session {}: {}", session.getRemoteAddress(), cause.getMessage());
        session.write(RedisResponse.error("ERR " + cause.getMessage()).encode());
    }
    
    @Override
    public void messageReceived(IoSession session, Object message) {
        String line = message.toString().trim();
        if (line.isEmpty()) {
            return;
        }
        
        try {
            RedisCommand command = parseCommand(line);
            String region = (String) session.getAttribute(SESSION_REGION);
            command.setRegion(region);
            
            // Check authentication
            if (!isAuthenticated(session) && !isAuthCommand(command)) {
                session.write(RedisResponse.error(KuberConstants.ERR_NOT_AUTHENTICATED).encode());
                return;
            }
            
            // Handle MULTI/EXEC
            if (isInMulti(session) && !isMultiControlCommand(command)) {
                queueCommand(session, command);
                session.write(RedisResponse.queued().encode());
                return;
            }
            
            RedisResponse response = executeCommand(session, command);
            session.write(response.encode());
            
        } catch (ReadOnlyException e) {
            session.write(RedisResponse.error(KuberConstants.ERR_READONLY).encode());
        } catch (KuberException e) {
            session.write(RedisResponse.error(e.getRedisErrorMessage()).encode());
        } catch (Exception e) {
            log.error("Error processing command: {}", e.getMessage(), e);
            session.write(RedisResponse.error("ERR " + e.getMessage()).encode());
        }
    }
    
    private RedisCommand parseCommand(String line) {
        // Handle RESP protocol or inline commands
        List<String> parts = parseCommandLine(line);
        if (parts.isEmpty()) {
            throw new KuberException("Empty command");
        }
        
        String cmd = parts.get(0).toUpperCase();
        List<String> args = parts.size() > 1 ? parts.subList(1, parts.size()) : new ArrayList<>();
        
        RedisCommand command = new RedisCommand();
        command.setCommand(cmd);
        command.setArguments(args);
        command.setWriteOperation(RedisCommand.isWriteCommand(RedisCommand.parseCommandType(cmd)));
        command.setTimestamp(System.currentTimeMillis());
        
        return command;
    }
    
    private List<String> parseCommandLine(String line) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        boolean escaped = false;
        char quoteChar = 0;
        
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            
            if (escaped) {
                switch (c) {
                    case 'n': current.append('\n'); break;
                    case 't': current.append('\t'); break;
                    case 'r': current.append('\r'); break;
                    case '\\': current.append('\\'); break;
                    case '"': current.append('"'); break;
                    case '\'': current.append('\''); break;
                    default: current.append(c);
                }
                escaped = false;
                continue;
            }
            
            if (c == '\\' && inQuotes) {
                escaped = true;
                continue;
            }
            
            if ((c == '"' || c == '\'') && !inQuotes) {
                inQuotes = true;
                quoteChar = c;
                continue;
            }
            
            if (c == quoteChar && inQuotes) {
                inQuotes = false;
                quoteChar = 0;
                continue;
            }
            
            if (c == ' ' && !inQuotes) {
                if (current.length() > 0) {
                    parts.add(current.toString());
                    current = new StringBuilder();
                }
                continue;
            }
            
            current.append(c);
        }
        
        if (current.length() > 0) {
            parts.add(current.toString());
        }
        
        return parts;
    }
    
    private RedisResponse executeCommand(IoSession session, RedisCommand command) {
        String cmd = command.getCommand();
        String region = command.getRegion();
        List<String> args = command.getArguments();
        
        RedisCommand.CommandType cmdType = RedisCommand.parseCommandType(cmd);
        
        switch (cmdType) {
            // Connection commands
            case PING:
                return args.isEmpty() ? RedisResponse.pong() : 
                       RedisResponse.bulkString(args.get(0));
            
            case ECHO:
                return args.isEmpty() ? RedisResponse.error("wrong number of arguments") :
                       RedisResponse.bulkString(args.get(0));
            
            case AUTH:
                return handleAuth(session, args);
            
            case QUIT:
                session.closeOnFlush();
                return RedisResponse.ok();
            
            case SELECT:
                // In Kuber, SELECT changes the region
                return handleRSelect(session, args);
            
            // String commands
            case GET:
                return handleGet(region, args);
            
            case SET:
                return handleSet(region, args);
            
            case SETNX:
                return handleSetNx(region, args);
            
            case SETEX:
                return handleSetEx(region, args);
            
            case MGET:
                return handleMGet(region, args);
            
            case MSET:
                return handleMSet(region, args);
            
            case INCR:
                return handleIncr(region, args);
            
            case INCRBY:
                return handleIncrBy(region, args);
            
            case DECR:
                return handleDecr(region, args);
            
            case DECRBY:
                return handleDecrBy(region, args);
            
            case APPEND:
                return handleAppend(region, args);
            
            case STRLEN:
                return handleStrlen(region, args);
            
            case GETSET:
                return handleGetSet(region, args);
            
            // Key commands
            case DEL:
                return handleDel(region, args);
            
            case EXISTS:
                return handleExists(region, args);
            
            case EXPIRE:
                return handleExpire(region, args);
            
            case TTL:
                return handleTtl(region, args);
            
            case PERSIST:
                return handlePersist(region, args);
            
            case TYPE:
                return handleType(region, args);
            
            case KEYS:
                return handleKeys(region, args);
            
            case RENAME:
                return handleRename(region, args);
            
            // Hash commands
            case HGET:
                return handleHGet(region, args);
            
            case HSET:
                return handleHSet(region, args);
            
            case HGETALL:
                return handleHGetAll(region, args);
            
            case HDEL:
                return handleHDel(region, args);
            
            case HEXISTS:
                return handleHExists(region, args);
            
            case HKEYS:
                return handleHKeys(region, args);
            
            case HVALS:
                return handleHVals(region, args);
            
            case HLEN:
                return handleHLen(region, args);
            
            // JSON commands (Kuber extension)
            case JSET:
                return handleJSet(region, args);
            
            case JGET:
                return handleJGet(region, args);
            
            case JSEARCH:
                return handleJSearch(region, args);
            
            case JDEL:
                return handleJDel(region, args);
            
            // Key search command (Kuber extension)
            case KSEARCH:
                return handleKSearch(region, args);
            
            // Region commands (Kuber extension)
            case REGIONS:
                return handleRegions();
            
            case RCREATE:
                return handleRCreate(args);
            
            case RDROP:
                return handleRDrop(args);
            
            case RPURGE:
                return handleRPurge(args);
            
            case RINFO:
                return handleRInfo(args);
            
            case RSELECT:
                return handleRSelect(session, args);
            
            case RSETMAP:
                return handleRSetMap(region, args);
            
            case RGETMAP:
                return handleRGetMap(region, args);
            
            case RCLEARMAP:
                return handleRClearMap(region, args);
            
            // Server commands
            case INFO:
                return handleInfo();
            
            case DBSIZE:
                return handleDbSize(region);
            
            case FLUSHDB:
                return handleFlushDb(region);
            
            case FLUSHALL:
                return handleFlushAll();
            
            case TIME:
                return handleTime();
            
            // Transaction commands
            case MULTI:
                return handleMulti(session);
            
            case EXEC:
                return handleExec(session);
            
            case DISCARD:
                return handleDiscard(session);
            
            // Pub/Sub commands
            case SUBSCRIBE:
                return handleSubscribe(session, args);
            
            case UNSUBSCRIBE:
                return handleUnsubscribe(session, args);
            
            case PUBLISH:
                return handlePublish(args);
            
            // Replication commands
            case STATUS:
                return handleStatus();
            
            case REPLINFO:
                return handleReplInfo();
            
            default:
                return RedisResponse.error(String.format(KuberConstants.ERR_UNKNOWN_COMMAND, cmd));
        }
    }
    
    // ==================== Command Handlers ====================
    
    /**
     * Handle AUTH command.
     * Supports two formats:
     * 1. AUTH password - traditional password authentication
     * 2. AUTH APIKEY kub_xxx... - API key authentication
     */
    private RedisResponse handleAuth(IoSession session, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'auth' command");
        }
        
        // Check for API key authentication: AUTH APIKEY kub_xxx...
        if (args.size() >= 2 && "APIKEY".equalsIgnoreCase(args.get(0))) {
            String apiKeyValue = args.get(1);
            Optional<ApiKey> validatedKey = apiKeyService.validateKey(apiKeyValue);
            
            if (validatedKey.isPresent()) {
                ApiKey key = validatedKey.get();
                session.setAttribute(SESSION_AUTHENTICATED, true);
                session.setAttribute(SESSION_AUTH_USER, key.getUserId());
                log.info("Redis client authenticated via API key: {} ({})", key.getKeyId(), key.getName());
                return RedisResponse.ok();
            }
            
            log.warn("Invalid API key authentication attempt from {}", session.getRemoteAddress());
            return RedisResponse.error("invalid API key");
        }
        
        // Check for API key directly (if it starts with kub_)
        if (args.get(0).startsWith("kub_")) {
            Optional<ApiKey> validatedKey = apiKeyService.validateKey(args.get(0));
            
            if (validatedKey.isPresent()) {
                ApiKey key = validatedKey.get();
                session.setAttribute(SESSION_AUTHENTICATED, true);
                session.setAttribute(SESSION_AUTH_USER, key.getUserId());
                log.info("Redis client authenticated via API key: {} ({})", key.getKeyId(), key.getName());
                return RedisResponse.ok();
            }
            
            log.warn("Invalid API key authentication attempt from {}", session.getRemoteAddress());
            return RedisResponse.error("invalid API key");
        }
        
        // Traditional password authentication
        String password = properties.getSecurity().getRedisPassword();
        if (password == null || password.equals(args.get(0))) {
            session.setAttribute(SESSION_AUTHENTICATED, true);
            return RedisResponse.ok();
        }
        
        return RedisResponse.error("invalid password");
    }
    
    private RedisResponse handleGet(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'get' command");
        }
        String value = cacheService.get(region, args.get(0));
        return RedisResponse.bulkString(value);
    }
    
    private RedisResponse handleSet(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'set' command");
        }
        
        String key = args.get(0);
        String value = args.get(1);
        long ttl = -1;
        
        // Parse options: EX seconds, PX milliseconds, NX, XX
        for (int i = 2; i < args.size(); i++) {
            String option = args.get(i).toUpperCase();
            switch (option) {
                case "EX":
                    if (i + 1 < args.size()) {
                        ttl = Long.parseLong(args.get(++i));
                    }
                    break;
                case "PX":
                    if (i + 1 < args.size()) {
                        ttl = Long.parseLong(args.get(++i)) / 1000;
                    }
                    break;
                case "NX":
                    if (cacheService.exists(region, key)) {
                        return RedisResponse.nullBulkString();
                    }
                    break;
                case "XX":
                    if (!cacheService.exists(region, key)) {
                        return RedisResponse.nullBulkString();
                    }
                    break;
            }
        }
        
        cacheService.set(region, key, value, ttl);
        return RedisResponse.ok();
    }
    
    private RedisResponse handleSetNx(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'setnx' command");
        }
        boolean result = cacheService.setNx(region, args.get(0), args.get(1));
        return RedisResponse.integer(result ? 1 : 0);
    }
    
    private RedisResponse handleSetEx(String region, List<String> args) {
        if (args.size() < 3) {
            return RedisResponse.error("wrong number of arguments for 'setex' command");
        }
        long ttl = Long.parseLong(args.get(1));
        cacheService.setEx(region, args.get(0), args.get(2), ttl);
        return RedisResponse.ok();
    }
    
    private RedisResponse handleMGet(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'mget' command");
        }
        List<String> values = cacheService.mget(region, args);
        return RedisResponse.arrayFromStrings(values);
    }
    
    private RedisResponse handleMSet(String region, List<String> args) {
        if (args.size() < 2 || args.size() % 2 != 0) {
            return RedisResponse.error("wrong number of arguments for 'mset' command");
        }
        Map<String, String> entries = new HashMap<>();
        for (int i = 0; i < args.size(); i += 2) {
            entries.put(args.get(i), args.get(i + 1));
        }
        cacheService.mset(region, entries);
        return RedisResponse.ok();
    }
    
    private RedisResponse handleIncr(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'incr' command");
        }
        long result = cacheService.incr(region, args.get(0));
        return RedisResponse.integer(result);
    }
    
    private RedisResponse handleIncrBy(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'incrby' command");
        }
        long result = cacheService.incrBy(region, args.get(0), Long.parseLong(args.get(1)));
        return RedisResponse.integer(result);
    }
    
    private RedisResponse handleDecr(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'decr' command");
        }
        long result = cacheService.decr(region, args.get(0));
        return RedisResponse.integer(result);
    }
    
    private RedisResponse handleDecrBy(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'decrby' command");
        }
        long result = cacheService.decrBy(region, args.get(0), Long.parseLong(args.get(1)));
        return RedisResponse.integer(result);
    }
    
    private RedisResponse handleAppend(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'append' command");
        }
        int result = cacheService.append(region, args.get(0), args.get(1));
        return RedisResponse.integer(result);
    }
    
    private RedisResponse handleStrlen(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'strlen' command");
        }
        int result = cacheService.strlen(region, args.get(0));
        return RedisResponse.integer(result);
    }
    
    private RedisResponse handleGetSet(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'getset' command");
        }
        String oldValue = cacheService.getSet(region, args.get(0), args.get(1));
        return RedisResponse.bulkString(oldValue);
    }
    
    private RedisResponse handleDel(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'del' command");
        }
        long count = cacheService.delete(region, args);
        return RedisResponse.integer(count);
    }
    
    private RedisResponse handleExists(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'exists' command");
        }
        long count = cacheService.exists(region, args);
        return RedisResponse.integer(count);
    }
    
    private RedisResponse handleExpire(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'expire' command");
        }
        boolean result = cacheService.expire(region, args.get(0), Long.parseLong(args.get(1)));
        return RedisResponse.integer(result ? 1 : 0);
    }
    
    private RedisResponse handleTtl(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'ttl' command");
        }
        long ttl = cacheService.ttl(region, args.get(0));
        return RedisResponse.integer(ttl);
    }
    
    private RedisResponse handlePersist(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'persist' command");
        }
        boolean result = cacheService.persist(region, args.get(0));
        return RedisResponse.integer(result ? 1 : 0);
    }
    
    private RedisResponse handleType(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'type' command");
        }
        String type = cacheService.type(region, args.get(0));
        return RedisResponse.simpleString(type);
    }
    
    private RedisResponse handleKeys(String region, List<String> args) {
        String pattern = args.isEmpty() ? "*" : args.get(0);
        Set<String> keys = cacheService.keys(region, pattern);
        return RedisResponse.arrayFromStrings(new ArrayList<>(keys));
    }
    
    private RedisResponse handleRename(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'rename' command");
        }
        cacheService.rename(region, args.get(0), args.get(1));
        return RedisResponse.ok();
    }
    
    // Hash commands
    private RedisResponse handleHGet(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'hget' command");
        }
        String value = cacheService.hget(region, args.get(0), args.get(1));
        return RedisResponse.bulkString(value);
    }
    
    private RedisResponse handleHSet(String region, List<String> args) {
        if (args.size() < 3) {
            return RedisResponse.error("wrong number of arguments for 'hset' command");
        }
        cacheService.hset(region, args.get(0), args.get(1), args.get(2));
        return RedisResponse.integer(1);
    }
    
    private RedisResponse handleHGetAll(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'hgetall' command");
        }
        Map<String, String> map = cacheService.hgetall(region, args.get(0));
        List<String> result = new ArrayList<>();
        map.forEach((k, v) -> {
            result.add(k);
            result.add(v);
        });
        return RedisResponse.arrayFromStrings(result);
    }
    
    private RedisResponse handleHDel(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'hdel' command");
        }
        boolean result = cacheService.hdel(region, args.get(0), args.get(1));
        return RedisResponse.integer(result ? 1 : 0);
    }
    
    private RedisResponse handleHExists(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'hexists' command");
        }
        boolean result = cacheService.hexists(region, args.get(0), args.get(1));
        return RedisResponse.integer(result ? 1 : 0);
    }
    
    private RedisResponse handleHKeys(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'hkeys' command");
        }
        Set<String> keys = cacheService.hkeys(region, args.get(0));
        return RedisResponse.arrayFromStrings(new ArrayList<>(keys));
    }
    
    private RedisResponse handleHVals(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'hvals' command");
        }
        Collection<String> vals = cacheService.hvals(region, args.get(0));
        return RedisResponse.arrayFromStrings(new ArrayList<>(vals));
    }
    
    private RedisResponse handleHLen(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'hlen' command");
        }
        int len = cacheService.hlen(region, args.get(0));
        return RedisResponse.integer(len);
    }
    
    // JSON commands
    private RedisResponse handleJSet(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'jset' command");
        }
        String key = args.get(0);
        String jsonStr = args.get(1);
        String path = args.size() > 2 ? args.get(2) : "$";
        long ttl = args.size() > 3 ? Long.parseLong(args.get(3)) : -1;
        
        JsonNode json = JsonUtils.parse(jsonStr);
        cacheService.jsonSet(region, key, path, json, ttl);
        return RedisResponse.ok();
    }
    
    private RedisResponse handleJGet(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'jget' command");
        }
        String key = args.get(0);
        String path = args.size() > 1 ? args.get(1) : "$";
        
        JsonNode result = cacheService.jsonGet(region, key, path);
        if (result == null) {
            return RedisResponse.nullBulkString();
        }
        return RedisResponse.bulkString(JsonUtils.toJson(result));
    }
    
    private RedisResponse handleJSearch(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'jsearch' command");
        }
        String query = args.get(0);
        List<CacheEntry> results = cacheService.jsonSearch(region, query);
        
        List<RedisResponse> items = results.stream()
                .map(e -> RedisResponse.bulkString(e.getKey() + ":" + JsonUtils.toJson(e.getJsonValue())))
                .collect(Collectors.toList());
        
        return RedisResponse.array(items);
    }
    
    private RedisResponse handleJDel(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'jdel' command");
        }
        String key = args.get(0);
        String path = args.size() > 1 ? args.get(1) : "$";
        
        boolean result = cacheService.jsonDelete(region, key, path);
        return RedisResponse.integer(result ? 1 : 0);
    }
    
    /**
     * Handle KSEARCH command - search keys by regex and return key-value pairs as JSON array.
     * Syntax: KSEARCH pattern [LIMIT count]
     * Returns array of JSON objects: [{"key":"...", "value":"...", "type":"...", "ttl":...}, ...]
     */
    private RedisResponse handleKSearch(String region, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'ksearch' command");
        }
        
        String pattern = args.get(0);
        int limit = 1000;
        
        // Parse optional LIMIT
        for (int i = 1; i < args.size() - 1; i++) {
            if ("LIMIT".equalsIgnoreCase(args.get(i))) {
                try {
                    limit = Integer.parseInt(args.get(i + 1));
                } catch (NumberFormatException e) {
                    return RedisResponse.error("invalid limit value");
                }
            }
        }
        
        try {
            List<Map<String, Object>> results = cacheService.searchKeysByRegex(region, pattern, limit);
            
            // Convert to array of JSON strings
            List<RedisResponse> items = results.stream()
                    .map(r -> RedisResponse.bulkString(JsonUtils.toJson(r)))
                    .collect(Collectors.toList());
            
            return RedisResponse.array(items);
        } catch (Exception e) {
            return RedisResponse.error("ERR " + e.getMessage());
        }
    }
    
    // Region commands
    private RedisResponse handleRegions() {
        Collection<CacheRegion> regions = cacheService.getAllRegions();
        List<String> names = regions.stream()
                .map(CacheRegion::getName)
                .collect(Collectors.toList());
        return RedisResponse.arrayFromStrings(names);
    }
    
    private RedisResponse handleRCreate(List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'rcreate' command");
        }
        String name = args.get(0);
        String description = args.size() > 1 ? args.get(1) : "";
        cacheService.createRegion(name, description);
        return RedisResponse.ok();
    }
    
    private RedisResponse handleRDrop(List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'rdrop' command");
        }
        cacheService.deleteRegion(args.get(0));
        return RedisResponse.ok();
    }
    
    private RedisResponse handleRPurge(List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'rpurge' command");
        }
        cacheService.purgeRegion(args.get(0));
        return RedisResponse.ok();
    }
    
    private RedisResponse handleRInfo(List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'rinfo' command");
        }
        CacheRegion region = cacheService.getRegion(args.get(0));
        if (region == null) {
            return RedisResponse.nullBulkString();
        }
        return RedisResponse.bulkString(JsonUtils.toJson(region));
    }
    
    private RedisResponse handleRSelect(IoSession session, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'rselect' command");
        }
        String regionName = args.get(0);
        if (!cacheService.regionExists(regionName)) {
            cacheService.createRegion(regionName, "Auto-created via RSELECT");
        }
        session.setAttribute(SESSION_REGION, regionName);
        return RedisResponse.ok();
    }
    
    /**
     * Handle RSETMAP command - Set attribute mapping for a region.
     * Syntax: RSETMAP [region] json_mapping
     * Example: RSETMAP users {"firstName":"first_name","lastName":"last_name"}
     * If region is omitted, uses current session region.
     */
    private RedisResponse handleRSetMap(String currentRegion, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'rsetmap' command");
        }
        
        String regionName;
        String jsonMapping;
        
        if (args.size() == 1) {
            // Only mapping provided, use current region
            regionName = currentRegion;
            jsonMapping = args.get(0);
        } else {
            // Region and mapping provided
            regionName = args.get(0);
            jsonMapping = args.get(1);
        }
        
        try {
            // Parse the JSON mapping
            Map<String, String> mapping = JsonUtils.getObjectMapper()
                    .readValue(jsonMapping, new com.fasterxml.jackson.core.type.TypeReference<Map<String, String>>() {});
            
            cacheService.setAttributeMapping(regionName, mapping);
            return RedisResponse.ok();
        } catch (Exception e) {
            return RedisResponse.error("ERR invalid JSON mapping: " + e.getMessage());
        }
    }
    
    /**
     * Handle RGETMAP command - Get attribute mapping for a region.
     * Syntax: RGETMAP [region]
     * If region is omitted, uses current session region.
     */
    private RedisResponse handleRGetMap(String currentRegion, List<String> args) {
        String regionName = args.isEmpty() ? currentRegion : args.get(0);
        
        Map<String, String> mapping = cacheService.getAttributeMapping(regionName);
        if (mapping == null || mapping.isEmpty()) {
            return RedisResponse.nullBulkString();
        }
        
        try {
            String json = JsonUtils.getObjectMapper().writeValueAsString(mapping);
            return RedisResponse.bulkString(json);
        } catch (Exception e) {
            return RedisResponse.error("ERR " + e.getMessage());
        }
    }
    
    /**
     * Handle RCLEARMAP command - Clear attribute mapping for a region.
     * Syntax: RCLEARMAP [region]
     * If region is omitted, uses current session region.
     */
    private RedisResponse handleRClearMap(String currentRegion, List<String> args) {
        String regionName = args.isEmpty() ? currentRegion : args.get(0);
        
        try {
            cacheService.clearAttributeMapping(regionName);
            return RedisResponse.ok();
        } catch (Exception e) {
            return RedisResponse.error("ERR " + e.getMessage());
        }
    }
    
    // Server commands
    private RedisResponse handleInfo() {
        Map<String, Object> info = cacheService.getServerInfo();
        StringBuilder sb = new StringBuilder();
        sb.append("# Server\n");
        info.forEach((k, v) -> sb.append(k).append(":").append(v).append("\n"));
        return RedisResponse.bulkString(sb.toString());
    }
    
    private RedisResponse handleDbSize(String region) {
        long size = cacheService.dbSize(region);
        return RedisResponse.integer(size);
    }
    
    private RedisResponse handleFlushDb(String region) {
        cacheService.purgeRegion(region);
        return RedisResponse.ok();
    }
    
    private RedisResponse handleFlushAll() {
        for (CacheRegion region : cacheService.getAllRegions()) {
            if (!region.isCaptive()) {
                cacheService.purgeRegion(region.getName());
            }
        }
        return RedisResponse.ok();
    }
    
    private RedisResponse handleTime() {
        long now = System.currentTimeMillis();
        List<RedisResponse> parts = new ArrayList<>();
        parts.add(RedisResponse.bulkString(String.valueOf(now / 1000)));
        parts.add(RedisResponse.bulkString(String.valueOf((now % 1000) * 1000)));
        return RedisResponse.array(parts);
    }
    
    // Transaction commands
    private RedisResponse handleMulti(IoSession session) {
        session.setAttribute(SESSION_IN_MULTI, true);
        session.setAttribute(SESSION_QUEUED_COMMANDS, new ArrayList<RedisCommand>());
        return RedisResponse.ok();
    }
    
    @SuppressWarnings("unchecked")
    private RedisResponse handleExec(IoSession session) {
        if (!isInMulti(session)) {
            return RedisResponse.error("EXEC without MULTI");
        }
        
        List<RedisCommand> commands = (List<RedisCommand>) session.getAttribute(SESSION_QUEUED_COMMANDS);
        session.setAttribute(SESSION_IN_MULTI, false);
        session.removeAttribute(SESSION_QUEUED_COMMANDS);
        
        List<RedisResponse> results = new ArrayList<>();
        for (RedisCommand cmd : commands) {
            try {
                results.add(executeCommand(session, cmd));
            } catch (Exception e) {
                results.add(RedisResponse.error(e.getMessage()));
            }
        }
        
        return RedisResponse.array(results);
    }
    
    private RedisResponse handleDiscard(IoSession session) {
        session.setAttribute(SESSION_IN_MULTI, false);
        session.removeAttribute(SESSION_QUEUED_COMMANDS);
        return RedisResponse.ok();
    }
    
    // Pub/Sub commands
    private RedisResponse handleSubscribe(IoSession session, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'subscribe' command");
        }
        
        @SuppressWarnings("unchecked")
        Set<String> subscriptions = (Set<String>) session.getAttribute(SESSION_SUBSCRIPTIONS);
        if (subscriptions == null) {
            subscriptions = new HashSet<>();
            session.setAttribute(SESSION_SUBSCRIPTIONS, subscriptions);
        }
        
        List<RedisResponse> responses = new ArrayList<>();
        for (String channel : args) {
            eventPublisher.subscribe(channel, session);
            subscriptions.add(channel);
            
            List<RedisResponse> msg = new ArrayList<>();
            msg.add(RedisResponse.bulkString("subscribe"));
            msg.add(RedisResponse.bulkString(channel));
            msg.add(RedisResponse.integer(subscriptions.size()));
            responses.add(RedisResponse.array(msg));
        }
        
        return RedisResponse.array(responses);
    }
    
    private RedisResponse handleUnsubscribe(IoSession session, List<String> args) {
        @SuppressWarnings("unchecked")
        Set<String> subscriptions = (Set<String>) session.getAttribute(SESSION_SUBSCRIPTIONS);
        
        if (subscriptions == null || subscriptions.isEmpty()) {
            return RedisResponse.array(new ArrayList<>());
        }
        
        List<String> channels = args.isEmpty() ? new ArrayList<>(subscriptions) : args;
        List<RedisResponse> responses = new ArrayList<>();
        
        for (String channel : channels) {
            eventPublisher.unsubscribe(channel, session);
            subscriptions.remove(channel);
            
            List<RedisResponse> msg = new ArrayList<>();
            msg.add(RedisResponse.bulkString("unsubscribe"));
            msg.add(RedisResponse.bulkString(channel));
            msg.add(RedisResponse.integer(subscriptions.size()));
            responses.add(RedisResponse.array(msg));
        }
        
        return RedisResponse.array(responses);
    }
    
    private RedisResponse handlePublish(List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'publish' command");
        }
        int count = eventPublisher.publish(args.get(0), args.get(1));
        return RedisResponse.integer(count);
    }
    
    // Replication commands
    private RedisResponse handleStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("nodeId", properties.getNodeId());
        status.put("isPrimary", replicationManager == null || replicationManager.isPrimary());
        status.put("mode", replicationManager != null ? replicationManager.getMode() : "STANDALONE");
        return RedisResponse.bulkString(JsonUtils.toJson(status));
    }
    
    private RedisResponse handleReplInfo() {
        if (replicationManager == null) {
            return RedisResponse.bulkString("replication:disabled");
        }
        return RedisResponse.bulkString(JsonUtils.toJson(replicationManager.getReplicationInfo()));
    }
    
    // Helper methods
    private boolean isAuthenticated(IoSession session) {
        Boolean auth = (Boolean) session.getAttribute(SESSION_AUTHENTICATED);
        return auth != null && auth;
    }
    
    private boolean isAuthCommand(RedisCommand command) {
        return "AUTH".equalsIgnoreCase(command.getCommand());
    }
    
    private boolean isInMulti(IoSession session) {
        Boolean inMulti = (Boolean) session.getAttribute(SESSION_IN_MULTI);
        return inMulti != null && inMulti;
    }
    
    private boolean isMultiControlCommand(RedisCommand command) {
        String cmd = command.getCommand().toUpperCase();
        return "EXEC".equals(cmd) || "DISCARD".equals(cmd);
    }
    
    @SuppressWarnings("unchecked")
    private void queueCommand(IoSession session, RedisCommand command) {
        List<RedisCommand> queue = (List<RedisCommand>) session.getAttribute(SESSION_QUEUED_COMMANDS);
        if (queue != null) {
            queue.add(command);
        }
    }
}
