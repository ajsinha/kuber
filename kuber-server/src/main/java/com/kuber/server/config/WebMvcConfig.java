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
package com.kuber.server.config;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.HandlerMapping;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.util.UrlPathHelper;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Spring MVC configuration for handling cache keys that contain forward slashes.
 *
 * <h3>Problem</h3>
 * Cache keys like {@code employee/EMP001} are URL-encoded as {@code employee%2FEMP001}.
 * Spring's default {@code PathPatternParser} (Spring Boot 3.x) decodes {@code %2F}
 * <em>before</em> pattern matching, splitting a URL like
 * {@code /api/v1/json/myregion/employee%2FEMP001} into three segments instead of two,
 * causing a 400 or 404 response.
 *
 * <h3>Four-layer solution</h3>
 * <ol>
 *   <li><strong>application.properties</strong> — {@code spring.mvc.pathmatch.matching-strategy=ant-path-matcher}
 *       tells Spring Boot's auto-configuration to use {@code AntPathMatcher} instead of
 *       {@code PathPatternParser}. This is the critical setting: without it,
 *       {@code setPatternParser(null)} in a {@code WebMvcConfigurer} is silently
 *       ignored by Spring Boot 3.2.x.</li>
 *   <li><strong>This class</strong> — sets {@code UrlPathHelper.urlDecode=false} so the
 *       AntPathMatcher operates on the raw (encoded) URI. {@code %2F} stays as a
 *       literal string and does not create an extra path segment.</li>
 *   <li><strong>{@code TomcatConfig}</strong> — sets Tomcat's
 *       {@code encodedSolidusHandling=passthrough} so Tomcat does not reject
 *       {@code %2F} before it reaches Spring.</li>
 *   <li><strong>{@code SecurityConfig}</strong> — calls
 *       {@code StrictHttpFirewall.setAllowUrlEncodedSlash(true)} so Spring Security's
 *       firewall permits the encoded slash.</li>
 * </ol>
 *
 * <h3>Path variable decoding</h3>
 * Because {@code urlDecode=false} affects <em>all</em> path variables, a
 * {@code HandlerInterceptor} is registered that URL-decodes every extracted
 * {@code @PathVariable} value before the controller method is invoked.
 * Normal (unencoded) path variables pass through unchanged.
 *
 * @version 2.6.0
 * @see TomcatConfig
 * @see SecurityConfig
 */
@Slf4j
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    @Override
    public void configurePathMatch(PathMatchConfigurer configurer) {
        UrlPathHelper helper = new UrlPathHelper();
        helper.setUrlDecode(false);
        helper.setAlwaysUseFullPath(true);
        configurer.setUrlPathHelper(helper);
        log.info("Path matching: urlDecode=false, alwaysUseFullPath=true (slash keys supported)");
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new PathVariableDecodingInterceptor());
    }

    /**
     * Interceptor that URL-decodes all path variables extracted by the handler mapping.
     *
     * Because {@code UrlPathHelper.urlDecode=false}, path variables are extracted
     * from the raw (encoded) URL. This interceptor transparently decodes them so
     * controllers see the human-readable values via {@code @PathVariable}.
     */
    private static class PathVariableDecodingInterceptor implements HandlerInterceptor {

        @Override
        @SuppressWarnings("unchecked")
        public boolean preHandle(HttpServletRequest request,
                                 HttpServletResponse response,
                                 Object handler) {
            Map<String, String> pathVars = (Map<String, String>) request.getAttribute(
                    HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE);
            if (pathVars != null && !pathVars.isEmpty()) {
                Map<String, String> decoded = new HashMap<>(pathVars.size());
                for (Map.Entry<String, String> entry : pathVars.entrySet()) {
                    decoded.put(entry.getKey(), decode(entry.getValue()));
                }
                request.setAttribute(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE, decoded);
            }
            return true;
        }

        private String decode(String value) {
            if (value == null) return null;
            try {
                return URLDecoder.decode(value, StandardCharsets.UTF_8);
            } catch (Exception e) {
                return value;
            }
        }
    }
}
