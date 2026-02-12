/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.util;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.servlet.HandlerMapping;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * Resolves cache keys from request URIs, handling keys that contain forward
 * slashes (e.g. {@code employee/EMP001}).
 *
 * <p>When a cache key contains {@code /}, clients URL-encode it as {@code %2F}.
 * Depending on Tomcat's {@code encodedSolidusHandling} setting, {@code %2F} may
 * arrive in the request as:
 * <ul>
 *   <li><b>PASSTHROUGH</b> — {@code %2F} remains literal in the URI → single path segment</li>
 *   <li><b>DECODE</b> — {@code %2F} is decoded to {@code /} → multiple path segments</li>
 * </ul>
 *
 * <p>This utility handles both cases by extracting the key portion from the
 * {@code /**} wildcard match, which naturally concatenates across path segments.
 *
 * @version 2.3.0
 */
public final class SlashKeyResolver {

    private static final AntPathMatcher PATH_MATCHER = new AntPathMatcher();

    private SlashKeyResolver() { }

    /**
     * Extract the cache key from a {@code /**} catch-all mapping.
     *
     * <p>Uses Spring's {@code HandlerMapping} attributes to determine the
     * matched pattern and full path, then extracts and URL-decodes the
     * remaining portion (the key).
     *
     * @param request current HTTP request (must have been matched by a {@code /**} pattern)
     * @return decoded cache key, or {@code null} if extraction fails
     */
    public static String resolveKey(HttpServletRequest request) {
        String fullPath = (String) request.getAttribute(
                HandlerMapping.PATH_WITHIN_HANDLER_MAPPING_ATTRIBUTE);
        String bestPattern = (String) request.getAttribute(
                HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);

        if (fullPath == null || bestPattern == null) {
            return null;
        }

        String extracted = PATH_MATCHER.extractPathWithinPattern(bestPattern, fullPath);
        if (extracted == null || extracted.isEmpty()) {
            return null;
        }

        return decode(extracted);
    }

    /**
     * URL-decode a value, handling already-decoded values gracefully.
     */
    private static String decode(String value) {
        if (value == null) return null;
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (Exception e) {
            // Already decoded or not URL-encoded
            return value;
        }
    }
}
