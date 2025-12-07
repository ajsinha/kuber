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
package com.kuber.server.controller;

import com.kuber.server.config.KuberProperties;
import com.kuber.server.persistence.PersistenceStore;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Custom error controller that provides detailed error information.
 * 
 * Features:
 * - Detailed error pages for web requests
 * - JSON error responses for API requests
 * - Exception details and stack traces (configurable)
 * - System status information for 5xx errors
 * - Request tracking with unique IDs
 * 
 * @author Ashutosh Sinha
 * @version 1.4.1
 */
@Slf4j
@Controller
public class CustomErrorController implements ErrorController {
    
    private static final String ERROR_PATH = "/error";
    
    @Autowired
    private KuberProperties properties;
    
    @Autowired(required = false)
    private PersistenceStore persistenceStore;
    
    /**
     * Handle error requests for web pages (HTML response).
     */
    @RequestMapping(value = ERROR_PATH, produces = MediaType.TEXT_HTML_VALUE)
    public String handleErrorHtml(HttpServletRequest request, Model model) {
        // Generate unique request ID for tracking
        String requestId = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        
        // Get error attributes
        Integer statusCode = getStatusCode(request);
        String errorMessage = getErrorMessage(request);
        String path = getPath(request);
        Throwable exception = getException(request);
        
        // Log the error
        logError(statusCode, path, errorMessage, exception, requestId);
        
        // Populate model with error details
        model.addAttribute("status", statusCode);
        model.addAttribute("error", getErrorName(statusCode));
        model.addAttribute("errorTitle", getErrorTitle(statusCode));
        model.addAttribute("message", errorMessage);
        model.addAttribute("path", path);
        model.addAttribute("timestamp", LocalDateTime.now());
        model.addAttribute("requestId", requestId);
        model.addAttribute("nodeId", properties.getNodeId());
        
        // Add exception details if available
        if (exception != null) {
            model.addAttribute("exception", exception.getClass().getName());
            model.addAttribute("exceptionMessage", exception.getMessage());
            
            // Include stack trace (can be disabled via configuration)
            if (shouldIncludeStackTrace()) {
                model.addAttribute("trace", getStackTrace(exception));
            }
        }
        
        // Add system status for 5xx errors
        if (statusCode >= 500) {
            addSystemStatus(model);
        }
        
        return "error";
    }
    
    /**
     * Handle error requests for API calls (JSON response).
     */
    @RequestMapping(value = ERROR_PATH, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> handleErrorJson(HttpServletRequest request) {
        // Generate unique request ID for tracking
        String requestId = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        
        // Get error attributes
        Integer statusCode = getStatusCode(request);
        String errorMessage = getErrorMessage(request);
        String path = getPath(request);
        Throwable exception = getException(request);
        
        // Log the error
        logError(statusCode, path, errorMessage, exception, requestId);
        
        // Build error response
        Map<String, Object> errorResponse = new LinkedHashMap<>();
        errorResponse.put("timestamp", Instant.now().toString());
        errorResponse.put("status", statusCode);
        errorResponse.put("error", getErrorName(statusCode));
        errorResponse.put("message", errorMessage);
        errorResponse.put("path", path);
        errorResponse.put("requestId", requestId);
        errorResponse.put("nodeId", properties.getNodeId());
        
        // Add exception details if available
        if (exception != null) {
            errorResponse.put("exception", exception.getClass().getName());
            errorResponse.put("exceptionMessage", exception.getMessage());
            
            if (shouldIncludeStackTrace()) {
                errorResponse.put("trace", getStackTrace(exception));
            }
        }
        
        return ResponseEntity.status(statusCode).body(errorResponse);
    }
    
    /**
     * Get HTTP status code from request.
     */
    private Integer getStatusCode(HttpServletRequest request) {
        Object status = request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
        if (status != null) {
            return Integer.valueOf(status.toString());
        }
        return 500;
    }
    
    /**
     * Get error message from request.
     */
    private String getErrorMessage(HttpServletRequest request) {
        // First try to get the exception message
        Throwable exception = getException(request);
        if (exception != null && exception.getMessage() != null) {
            return exception.getMessage();
        }
        
        // Then try the error message attribute
        Object message = request.getAttribute(RequestDispatcher.ERROR_MESSAGE);
        if (message != null && !message.toString().isEmpty()) {
            return message.toString();
        }
        
        // Return default message based on status code
        Integer statusCode = getStatusCode(request);
        return getDefaultMessage(statusCode);
    }
    
    /**
     * Get request path that caused the error.
     */
    private String getPath(HttpServletRequest request) {
        Object path = request.getAttribute(RequestDispatcher.ERROR_REQUEST_URI);
        if (path != null) {
            return path.toString();
        }
        return request.getRequestURI();
    }
    
    /**
     * Get exception from request.
     */
    private Throwable getException(HttpServletRequest request) {
        return (Throwable) request.getAttribute(RequestDispatcher.ERROR_EXCEPTION);
    }
    
    /**
     * Get standard HTTP error name.
     */
    private String getErrorName(Integer statusCode) {
        HttpStatus status = HttpStatus.resolve(statusCode);
        if (status != null) {
            return status.getReasonPhrase();
        }
        return "Unknown Error";
    }
    
    /**
     * Get user-friendly error title.
     */
    private String getErrorTitle(Integer statusCode) {
        return switch (statusCode) {
            case 400 -> "Bad Request";
            case 401 -> "Authentication Required";
            case 403 -> "Access Denied";
            case 404 -> "Page Not Found";
            case 405 -> "Method Not Allowed";
            case 408 -> "Request Timeout";
            case 409 -> "Conflict";
            case 415 -> "Unsupported Media Type";
            case 429 -> "Too Many Requests";
            case 500 -> "Internal Server Error";
            case 501 -> "Not Implemented";
            case 502 -> "Bad Gateway";
            case 503 -> "Service Unavailable";
            case 504 -> "Gateway Timeout";
            default -> "Something Went Wrong";
        };
    }
    
    /**
     * Get default message for status code.
     */
    private String getDefaultMessage(Integer statusCode) {
        return switch (statusCode) {
            case 400 -> "The request could not be understood by the server.";
            case 401 -> "You need to log in to access this resource.";
            case 403 -> "You don't have permission to access this resource.";
            case 404 -> "The requested page or resource could not be found.";
            case 405 -> "The request method is not supported for this resource.";
            case 408 -> "The server timed out waiting for the request.";
            case 409 -> "The request conflicts with the current state of the resource.";
            case 415 -> "The media type of the request is not supported.";
            case 429 -> "You have sent too many requests. Please wait and try again.";
            case 500 -> "An unexpected error occurred on the server.";
            case 501 -> "The requested feature is not implemented.";
            case 502 -> "The server received an invalid response from an upstream server.";
            case 503 -> "The server is temporarily unavailable. Please try again later.";
            case 504 -> "The server did not receive a timely response from an upstream server.";
            default -> "An unexpected error occurred while processing your request.";
        };
    }
    
    /**
     * Get stack trace as string.
     */
    private String getStackTrace(Throwable exception) {
        if (exception == null) {
            return null;
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append(exception.getClass().getName()).append(": ").append(exception.getMessage()).append("\n");
        
        for (StackTraceElement element : exception.getStackTrace()) {
            sb.append("    at ").append(element.toString()).append("\n");
        }
        
        // Include caused by
        Throwable cause = exception.getCause();
        if (cause != null) {
            sb.append("Caused by: ").append(cause.getClass().getName()).append(": ").append(cause.getMessage()).append("\n");
            for (StackTraceElement element : cause.getStackTrace()) {
                sb.append("    at ").append(element.toString()).append("\n");
            }
        }
        
        return sb.toString();
    }
    
    /**
     * Check if stack traces should be included.
     * This could be made configurable via properties.
     */
    private boolean shouldIncludeStackTrace() {
        // Always include in development, could add configuration for production
        return true;
    }
    
    /**
     * Add system status information to model.
     */
    private void addSystemStatus(Model model) {
        // Persistence status
        if (persistenceStore != null) {
            model.addAttribute("persistenceAvailable", persistenceStore.isAvailable());
        } else {
            model.addAttribute("persistenceAvailable", false);
        }
        
        // Memory status
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        long maxMemory = runtime.maxMemory();
        int heapUsagePercent = (int) ((usedMemory * 100) / maxMemory);
        model.addAttribute("heapUsagePercent", heapUsagePercent);
        
        // Uptime
        long uptimeMs = ManagementFactory.getRuntimeMXBean().getUptime();
        model.addAttribute("uptime", formatUptime(uptimeMs));
    }
    
    /**
     * Format uptime in human-readable form.
     */
    private String formatUptime(long uptimeMs) {
        Duration duration = Duration.ofMillis(uptimeMs);
        long days = duration.toDays();
        long hours = duration.toHoursPart();
        long minutes = duration.toMinutesPart();
        
        if (days > 0) {
            return days + "d " + hours + "h " + minutes + "m";
        } else if (hours > 0) {
            return hours + "h " + minutes + "m";
        } else {
            return minutes + "m";
        }
    }
    
    /**
     * Log error for debugging and monitoring.
     */
    private void logError(Integer statusCode, String path, String message, Throwable exception, String requestId) {
        if (statusCode >= 500) {
            log.error("[{}] {} {} - {} - {}", requestId, statusCode, path, message, 
                    exception != null ? exception.getClass().getName() : "no exception", exception);
        } else if (statusCode >= 400) {
            log.warn("[{}] {} {} - {}", requestId, statusCode, path, message);
        } else {
            log.debug("[{}] {} {} - {}", requestId, statusCode, path, message);
        }
    }
}
