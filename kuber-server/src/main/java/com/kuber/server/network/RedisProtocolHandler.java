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
import com.kuber.server.security.*;
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
 * Authentication (v2.1.0):
 * - API Key ONLY: AUTH kub_xxx... or AUTH APIKEY kub_xxx...
 * - Username/password authentication is only for Web UI
 * 
 * Authorization (v1.7.3):
 * - RBAC: Permissions checked based on user's roles and region
 * - Admin users have full access to all operations
 * - Other users need specific roles granting READ/WRITE/DELETE
 *
 * @version 2.1.0
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisProtocolHandler extends IoHandlerAdapter {
    
    private static final String SESSION_REGION = "currentRegion";
    private static final String SESSION_AUTHENTICATED = "authenticated";
    private static final String SESSION_AUTH_USER = "authUser";
    private static final String SESSION_API_KEY = "apiKey";
    private static final String SESSION_IN_MULTI = "inMulti";
    private static final String SESSION_QUEUED_COMMANDS = "queuedCommands";
    private static final String SESSION_SUBSCRIPTIONS = "subscriptions";
    private static final String SESSION_CLIENT_NAME = "clientName";
    private static final String SESSION_TIMEOUT_SECONDS = "timeoutSeconds";
    
    private final CacheService cacheService;
    private final KuberProperties properties;
    private final EventPublisher eventPublisher;
    private final ApiKeyService apiKeyService;
    private final AuthorizationService authorizationService;
    
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
            
            // String commands - READ
            case GET:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleGet(region, args);
            
            // String commands - WRITE
            case SET:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleSet(region, args);
            
            case SETNX:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleSetNx(region, args);
            
            case SETEX:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleSetEx(region, args);
            
            case MGET:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleMGet(region, args);
            
            case MSET:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleMSet(region, args);
            
            case INCR:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleIncr(region, args);
            
            case INCRBY:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleIncrBy(region, args);
            
            case DECR:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleDecr(region, args);
            
            case DECRBY:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleDecrBy(region, args);
            
            case APPEND:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleAppend(region, args);
            
            case STRLEN:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleStrlen(region, args);
            
            case GETSET:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleGetSet(region, args);
            
            // Key commands - DELETE
            case DEL:
                if (!canDelete(session, region)) return permissionDenied("DELETE", region);
                return handleDel(region, args);
            
            // Key commands - READ
            case EXISTS:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleExists(region, args);
            
            // Key commands - WRITE (TTL operations)
            case EXPIRE:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleExpire(region, args);
            
            case TTL:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleTtl(region, args);
            
            case PERSIST:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handlePersist(region, args);
            
            case TYPE:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleType(region, args);
            
            case KEYS:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleKeys(region, args);
            
            case SCAN:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleScan(region, args);
            
            case RENAME:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleRename(region, args);
            
            // Hash commands - READ
            case HGET:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleHGet(region, args);
            
            // Hash commands - WRITE
            case HSET:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleHSet(region, args);
            
            case HGETALL:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleHGetAll(region, args);
            
            case HDEL:
                if (!canDelete(session, region)) return permissionDenied("DELETE", region);
                return handleHDel(region, args);
            
            case HEXISTS:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleHExists(region, args);
            
            case HKEYS:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleHKeys(region, args);
            
            case HVALS:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleHVals(region, args);
            
            case HLEN:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleHLen(region, args);
            
            // JSON commands (Kuber extension) - WRITE
            case JSET:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleJSet(region, args);
            
            // JSON commands - READ
            case JGET:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleJGet(region, args);
            
            case JUPDATE:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleJUpdate(region, args);
            
            case JREMOVE:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleJRemove(region, args);
            
            case JSEARCH:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleJSearch(region, args);
            
            case JDEL:
                if (!canDelete(session, region)) return permissionDenied("DELETE", region);
                return handleJDel(region, args);
            
            // Key search command (Kuber extension) - READ
            case KSEARCH:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleKSearch(region, args);
            
            // Region commands (Kuber extension) - ADMIN only
            case REGIONS:
                return handleRegions();
            
            case RCREATE:
                if (!isAdmin(session)) return RedisResponse.error("NOPERM admin permission required to create regions");
                return handleRCreate(args);
            
            case RDROP:
                if (!isAdmin(session)) return RedisResponse.error("NOPERM admin permission required to drop regions");
                return handleRDrop(args);
            
            case RPURGE:
                if (!canDelete(session, region)) return permissionDenied("DELETE", region);
                return handleRPurge(args);
            
            case RINFO:
                return handleRInfo(args);
            
            case RSELECT:
                return handleRSelect(session, args);
            
            case RSETMAP:
                if (!canWrite(session, region)) return permissionDenied("WRITE", region);
                return handleRSetMap(region, args);
            
            case RGETMAP:
                if (!canRead(session, region)) return permissionDenied("READ", region);
                return handleRGetMap(region, args);
            
            case RCLEARMAP:
                if (!canDelete(session, region)) return permissionDenied("DELETE", region);
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
            
            // Connection/Client commands
            case CLIENT:
                return handleClient(session, args);
            
            default:
                return RedisResponse.error(String.format(KuberConstants.ERR_UNKNOWN_COMMAND, cmd));
        }
    }
    
    // ==================== Command Handlers ====================
    
    /**
     * Handle AUTH command.
     * v2.1.0: API Key authentication ONLY for Redis protocol.
     * 
     * Supported formats:
     * 1. AUTH kub_xxx... - API key direct (recommended)
     * 2. AUTH APIKEY kub_xxx... - API key with keyword prefix
     * 
     * Note: Username/password authentication is only for Web UI.
     * All programmatic access (Redis protocol, REST API) must use API keys.
     */
    private RedisResponse handleAuth(IoSession session, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("wrong number of arguments for 'auth' command");
        }
        
        String apiKeyValue = null;
        
        // Format 1: AUTH APIKEY kub_xxx...
        if (args.size() >= 2 && "APIKEY".equalsIgnoreCase(args.get(0))) {
            apiKeyValue = args.get(1);
        }
        // Format 2: AUTH kub_xxx... (direct API key)
        else if (args.get(0).startsWith("kub_")) {
            apiKeyValue = args.get(0);
        }
        // Invalid format - must be API key
        else {
            log.warn("Invalid auth attempt from {} - API key required (must start with 'kub_')", 
                    session.getRemoteAddress());
            return RedisResponse.error("API key required - use AUTH kub_xxx or AUTH APIKEY kub_xxx");
        }
        
        // Validate API key
        Optional<ApiKey> validatedKey = apiKeyService.validateKey(apiKeyValue);
        
        if (validatedKey.isPresent()) {
            ApiKey key = validatedKey.get();
            session.setAttribute(SESSION_AUTHENTICATED, true);
            session.setAttribute(SESSION_AUTH_USER, key.getUserId());
            session.setAttribute(SESSION_API_KEY, key);
            log.info("Redis client authenticated via API key: {} ({}) for user '{}' from {}", 
                    key.getKeyId(), key.getName(), key.getUserId(), session.getRemoteAddress());
            return RedisResponse.ok();
        }
        
        log.warn("Invalid API key authentication attempt from {}", session.getRemoteAddress());
        return RedisResponse.error("invalid API key");
    }
    
    /**
     * Handle CLIENT command with subcommands.
     * 
     * Supported subcommands:
     * - CLIENT LIST: List connected clients
     * - CLIENT GETNAME: Get current client name
     * - CLIENT SETNAME name: Set client name
     * - CLIENT GETTIMEOUT: Get session timeout in seconds
     * - CLIENT SETTIMEOUT seconds: Set session timeout (60-86400 seconds)
     * - CLIENT KEEPALIVE: Reset idle timer
     * - CLIENT ID: Get session ID
     * - CLIENT INFO: Get current client info
     * 
     * @version 2.1.0
     */
    private RedisResponse handleClient(IoSession session, List<String> args) {
        if (args.isEmpty()) {
            return RedisResponse.error("ERR wrong number of arguments for 'CLIENT' command");
        }
        
        String subCmd = args.get(0).toUpperCase();
        
        switch (subCmd) {
            case "LIST":
                return handleClientList();
                
            case "GETNAME":
                String name = (String) session.getAttribute(SESSION_CLIENT_NAME);
                return name != null ? RedisResponse.bulkString(name) : RedisResponse.nullBulkString();
                
            case "SETNAME":
                if (args.size() < 2) {
                    return RedisResponse.error("ERR wrong number of arguments for 'CLIENT SETNAME'");
                }
                String clientName = args.get(1);
                session.setAttribute(SESSION_CLIENT_NAME, clientName);
                log.debug("Client {} set name to '{}'", session.getRemoteAddress(), clientName);
                return RedisResponse.ok();
                
            case "GETTIMEOUT":
                Integer timeout = (Integer) session.getAttribute(SESSION_TIMEOUT_SECONDS);
                if (timeout == null) {
                    timeout = (int) (properties.getNetwork().getConnectionTimeoutMs() / 1000);
                }
                return RedisResponse.integer(timeout);
                
            case "SETTIMEOUT":
                if (args.size() < 2) {
                    return RedisResponse.error("ERR wrong number of arguments for 'CLIENT SETTIMEOUT'");
                }
                try {
                    int timeoutSeconds = Integer.parseInt(args.get(1));
                    // Allow 60 seconds to 24 hours (86400 seconds), or 0 to disable timeout
                    if (timeoutSeconds != 0 && (timeoutSeconds < 60 || timeoutSeconds > 86400)) {
                        return RedisResponse.error("ERR timeout must be 0 (no timeout) or between 60 and 86400 seconds");
                    }
                    session.setAttribute(SESSION_TIMEOUT_SECONDS, timeoutSeconds);
                    // Update MINA session idle time
                    session.getConfig().setIdleTime(IdleStatus.BOTH_IDLE, timeoutSeconds);
                    log.info("Client {} set timeout to {} seconds", session.getRemoteAddress(), timeoutSeconds);
                    return RedisResponse.ok();
                } catch (NumberFormatException e) {
                    return RedisResponse.error("ERR invalid timeout value");
                }
                
            case "KEEPALIVE":
                // Reset the idle time by updating last activity - MINA handles this automatically
                // but we can explicitly reset by setting idle time again
                Integer currentTimeout = (Integer) session.getAttribute(SESSION_TIMEOUT_SECONDS);
                if (currentTimeout == null) {
                    currentTimeout = (int) (properties.getNetwork().getConnectionTimeoutMs() / 1000);
                }
                session.getConfig().setIdleTime(IdleStatus.BOTH_IDLE, currentTimeout);
                return RedisResponse.simpleString("PONG");
                
            case "ID":
                return RedisResponse.integer(session.getId());
                
            case "INFO":
                return handleClientInfo(session);
                
            case "HELP":
                return handleClientHelp();
                
            default:
                return RedisResponse.error("ERR unknown CLIENT subcommand '" + subCmd + "'. Try CLIENT HELP.");
        }
    }
    
    /**
     * Handle CLIENT LIST - return info about all connected clients
     */
    private RedisResponse handleClientList() {
        StringBuilder sb = new StringBuilder();
        for (IoSession s : activeSessions.values()) {
            sb.append("id=").append(s.getId());
            sb.append(" addr=").append(s.getRemoteAddress());
            String clientName = (String) s.getAttribute(SESSION_CLIENT_NAME);
            if (clientName != null) {
                sb.append(" name=").append(clientName);
            }
            String user = (String) s.getAttribute(SESSION_AUTH_USER);
            if (user != null) {
                sb.append(" user=").append(user);
            }
            String region = (String) s.getAttribute(SESSION_REGION);
            sb.append(" db=").append(region);
            Integer timeout = (Integer) s.getAttribute(SESSION_TIMEOUT_SECONDS);
            if (timeout != null) {
                sb.append(" timeout=").append(timeout);
            }
            sb.append("\n");
        }
        return RedisResponse.bulkString(sb.toString());
    }
    
    /**
     * Handle CLIENT INFO - return info about current client
     */
    private RedisResponse handleClientInfo(IoSession session) {
        StringBuilder sb = new StringBuilder();
        sb.append("id=").append(session.getId());
        sb.append(" addr=").append(session.getRemoteAddress());
        String clientName = (String) session.getAttribute(SESSION_CLIENT_NAME);
        if (clientName != null) {
            sb.append(" name=").append(clientName);
        }
        String user = (String) session.getAttribute(SESSION_AUTH_USER);
        if (user != null) {
            sb.append(" user=").append(user);
        }
        String region = (String) session.getAttribute(SESSION_REGION);
        sb.append(" db=").append(region);
        Integer timeout = (Integer) session.getAttribute(SESSION_TIMEOUT_SECONDS);
        if (timeout == null) {
            timeout = (int) (properties.getNetwork().getConnectionTimeoutMs() / 1000);
        }
        sb.append(" timeout=").append(timeout);
        sb.append(" idle=").append(session.getIdleCount(IdleStatus.BOTH_IDLE));
        return RedisResponse.bulkString(sb.toString());
    }
    
    /**
     * Handle CLIENT HELP - return help text
     */
    private RedisResponse handleClientHelp() {
        List<RedisResponse> help = new ArrayList<>();
        help.add(RedisResponse.bulkString("CLIENT LIST -- List connected clients"));
        help.add(RedisResponse.bulkString("CLIENT ID -- Get current client ID"));
        help.add(RedisResponse.bulkString("CLIENT INFO -- Get current client info"));
        help.add(RedisResponse.bulkString("CLIENT GETNAME -- Get client name"));
        help.add(RedisResponse.bulkString("CLIENT SETNAME <name> -- Set client name"));
        help.add(RedisResponse.bulkString("CLIENT GETTIMEOUT -- Get session timeout in seconds"));
        help.add(RedisResponse.bulkString("CLIENT SETTIMEOUT <seconds> -- Set session timeout (0=no timeout, 60-86400)"));
        help.add(RedisResponse.bulkString("CLIENT KEEPALIVE -- Reset idle timer (returns PONG)"));
        help.add(RedisResponse.bulkString("CLIENT HELP -- Show this help"));
        return RedisResponse.array(help);
    }
    
    // ==================== RBAC Authorization ====================
    
    /**
     * Check if session user has read permission for region
     */
    private boolean canRead(IoSession session, String region) {
        if (!properties.getSecurity().isRbacEnabled()) {
            return true;
        }
        String userId = (String) session.getAttribute(SESSION_AUTH_USER);
        if (userId == null) {
            return false;
        }
        return authorizationService.hasPermission(userId, region, KuberPermission.READ);
    }
    
    /**
     * Check if session user has write permission for region
     */
    private boolean canWrite(IoSession session, String region) {
        if (!properties.getSecurity().isRbacEnabled()) {
            return true;
        }
        String userId = (String) session.getAttribute(SESSION_AUTH_USER);
        if (userId == null) {
            return false;
        }
        return authorizationService.hasPermission(userId, region, KuberPermission.WRITE);
    }
    
    /**
     * Check if session user has delete permission for region
     */
    private boolean canDelete(IoSession session, String region) {
        if (!properties.getSecurity().isRbacEnabled()) {
            return true;
        }
        String userId = (String) session.getAttribute(SESSION_AUTH_USER);
        if (userId == null) {
            return false;
        }
        return authorizationService.hasPermission(userId, region, KuberPermission.DELETE);
    }
    
    /**
     * Check if session user is admin
     */
    private boolean isAdmin(IoSession session) {
        if (!properties.getSecurity().isRbacEnabled()) {
            return true;
        }
        String userId = (String) session.getAttribute(SESSION_AUTH_USER);
        if (userId == null) {
            return false;
        }
        return authorizationService.isAdmin(userId);
    }
    
    /**
     * Return error for permission denied
     */
    private RedisResponse permissionDenied(String operation, String region) {
        return RedisResponse.error("NOPERM " + operation + " permission denied for region '" + region + "'");
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
    
    /**
     * Handle SCAN command - iterate through keys with cursor-based pagination.
     * Syntax: SCAN cursor [MATCH pattern] [COUNT count]
     * Returns: [nextCursor, [key1, key2, ...]]
     */
    private RedisResponse handleScan(String region, List<String> args) {
        int cursor = 0;
        String pattern = "*";
        int count = 10;
        
        // Parse cursor (first argument)
        if (!args.isEmpty()) {
            try {
                cursor = Integer.parseInt(args.get(0));
            } catch (NumberFormatException e) {
                return RedisResponse.error("invalid cursor");
            }
        }
        
        // Parse optional MATCH and COUNT
        for (int i = 1; i < args.size() - 1; i++) {
            String option = args.get(i).toUpperCase();
            if ("MATCH".equals(option) && i + 1 < args.size()) {
                pattern = args.get(i + 1);
                i++;
            } else if ("COUNT".equals(option) && i + 1 < args.size()) {
                try {
                    count = Integer.parseInt(args.get(i + 1));
                } catch (NumberFormatException e) {
                    return RedisResponse.error("invalid COUNT value");
                }
                i++;
            }
        }
        
        // Get all matching keys
        Set<String> allKeys = cacheService.keys(region, pattern);
        List<String> keyList = new ArrayList<>(allKeys);
        
        // Calculate pagination
        int startIndex = cursor;
        int endIndex = Math.min(startIndex + count, keyList.size());
        int nextCursor = endIndex >= keyList.size() ? 0 : endIndex;
        
        // Get keys for this batch
        List<String> batchKeys = startIndex < keyList.size() 
            ? keyList.subList(startIndex, endIndex) 
            : new ArrayList<>();
        
        // Build response: [nextCursor, [keys...]]
        List<RedisResponse> response = new ArrayList<>();
        response.add(RedisResponse.bulkString(String.valueOf(nextCursor)));
        response.add(RedisResponse.arrayFromStrings(batchKeys));
        
        return RedisResponse.array(response);
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
    
    /**
     * Handle JUPDATE command - Update/merge JSON value.
     * Syntax: JUPDATE key json [TTL seconds]
     * 
     * Behavior:
     * - If key doesn't exist: set the JSON value
     * - If key exists and is valid JSON: merge new JSON onto existing (new values override)
     * - If key exists but is not valid JSON: replace with new JSON value
     * 
     * Returns: OK on success, or the merged JSON string
     */
    private RedisResponse handleJUpdate(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'jupdate' command");
        }
        String key = args.get(0);
        String jsonStr = args.get(1);
        long ttl = args.size() > 2 ? Long.parseLong(args.get(2)) : -1;
        
        // Validate that the input is valid JSON
        JsonNode newJson;
        try {
            newJson = JsonUtils.parse(jsonStr);
            if (newJson == null) {
                return RedisResponse.error("ERR invalid JSON");
            }
        } catch (Exception e) {
            return RedisResponse.error("ERR invalid JSON: " + e.getMessage());
        }
        
        // Perform the update/merge operation
        JsonNode result = cacheService.jsonUpdate(region, key, newJson, ttl);
        
        if (result != null) {
            return RedisResponse.bulkString(JsonUtils.toJson(result));
        }
        return RedisResponse.ok();
    }
    
    /**
     * Handle JREMOVE command - Remove attributes from JSON value.
     * Syntax: JREMOVE key ["attr1", "attr2", ...]
     * 
     * Behavior:
     * - If key exists and has valid JSON object: removes specified attributes and saves
     * - If key doesn't exist or value is not JSON: returns 0 (nothing done)
     * 
     * Returns: Number of attributes removed, or the updated JSON string
     */
    private RedisResponse handleJRemove(String region, List<String> args) {
        if (args.size() < 2) {
            return RedisResponse.error("wrong number of arguments for 'jremove' command");
        }
        String key = args.get(0);
        String jsonStr = args.get(1);
        
        // Parse the attributes array
        JsonNode attributesJson;
        try {
            attributesJson = JsonUtils.parse(jsonStr);
            if (attributesJson == null || !attributesJson.isArray()) {
                return RedisResponse.error("ERR second argument must be a JSON array of attribute names");
            }
        } catch (Exception e) {
            return RedisResponse.error("ERR invalid JSON array: " + e.getMessage());
        }
        
        // Extract attribute names from array
        List<String> attributes = new java.util.ArrayList<>();
        for (JsonNode node : attributesJson) {
            if (node.isTextual()) {
                attributes.add(node.asText());
            } else {
                return RedisResponse.error("ERR attribute names must be strings");
            }
        }
        
        if (attributes.isEmpty()) {
            return RedisResponse.integer(0);
        }
        
        // Perform the remove operation
        JsonNode result = cacheService.jsonRemoveAttributes(region, key, attributes);
        
        if (result != null) {
            return RedisResponse.bulkString(JsonUtils.toJson(result));
        }
        return RedisResponse.integer(0);
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
