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

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.security.web.firewall.StrictHttpFirewall;

/**
 * Allows encoded forward slashes ({@code %2F}) in URL paths so that cache keys
 * containing slashes (e.g. {@code employee/EMP001}) work end-to-end.
 *
 * <h3>Four independent layers are configured:</h3>
 * <ol>
 *   <li><b>JVM system property</b> — {@code org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH=true}
 *       set in a static initializer before Tomcat starts, preventing the low-level
 *       URL decoder from rejecting {@code %2F}.</li>
 *   <li><b>Tomcat connector</b> — {@code EncodedSolidusHandling.PASSTHROUGH} set via
 *       {@link WebServerFactoryCustomizer} on the Connector, ensuring
 *       {@code %2F} is neither rejected nor decoded but passed through as-is.</li>
 *   <li><b>Spring Security firewall</b> — {@link StrictHttpFirewall} configured to
 *       allow URL-encoded slashes via {@link WebSecurityCustomizer}.</li>
 *   <li><b>Spring MVC fallback</b> — {@code /**} catch-all endpoints in ApiController
 *       handle the case where {@code %2F} is decoded to {@code /} by reassembling
 *       the key from the remaining path segments.</li>
 * </ol>
 *
 * @version 2.3.0
 */
@Slf4j
@Configuration
public class TomcatConfig {

    /*
     * CRITICAL: This system property must be set BEFORE Tomcat's UDecoder class
     * is loaded. A static initializer in a @Configuration class runs early enough
     * in the Spring Boot lifecycle to guarantee this.
     */
    static {
        System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
    }

    /**
     * Tomcat connector layer: set encoded solidus handling to PASSTHROUGH.
     *
     * <p>{@code Connector.setEncodedSolidusHandling(String)} is the Tomcat 10.1
     * API that controls how {@code %2F} is handled in URL paths.
     *
     * <p>Fallback chain:
     * <ol>
     *   <li>Connector.setEncodedSolidusHandling("passthrough") — direct API</li>
     *   <li>Connector.setProperty() — generic property setter</li>
     *   <li>Reflective call — last resort for non-standard Connector implementations</li>
     * </ol>
     */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public WebServerFactoryCustomizer<TomcatServletWebServerFactory> encodedSlashCustomizer() {
        return factory -> factory.addConnectorCustomizers(connector -> {
            boolean configured = false;

            // Method 1: Direct Connector API (Tomcat 10.1+)
            try {
                connector.setEncodedSolidusHandling("passthrough");
                log.info("Tomcat: encodedSolidusHandling = PASSTHROUGH (connector API)");
                configured = true;
            } catch (Exception e) {
                log.debug("Connector.setEncodedSolidusHandling() failed: {}", e.getMessage());
            }

            // Method 2: Generic property setter on Connector
            if (!configured) {
                try {
                    connector.setProperty("encodedSolidusHandling", "passthrough");
                    log.info("Tomcat: encodedSolidusHandling = PASSTHROUGH (setProperty)");
                    configured = true;
                } catch (Exception e) {
                    log.debug("setProperty method failed: {}", e.getMessage());
                }
            }

            // Method 3: Reflective fallback
            if (!configured) {
                try {
                    var method = connector.getClass().getMethod(
                            "setEncodedSolidusHandling", String.class);
                    method.invoke(connector, "passthrough");
                    log.info("Tomcat: encodedSolidusHandling = PASSTHROUGH (reflection)");
                    configured = true;
                } catch (Exception e) {
                    log.warn("All methods to set encodedSolidusHandling failed. " +
                            "Keys containing '/' rely on /**-pattern fallback. Last error: {}",
                            e.getMessage());
                }
            }
        });
    }

    /**
     * Spring Security layer: wire a custom firewall that allows {@code %2F}.
     *
     * In Spring Security 6.x the firewall must be set via {@link WebSecurityCustomizer}
     * AND as a standalone bean so both the {@code WebSecurity} object and the
     * auto-configured filter chain pick it up.
     */
    @Bean
    public StrictHttpFirewall httpFirewall() {
        StrictHttpFirewall firewall = new StrictHttpFirewall();
        firewall.setAllowUrlEncodedSlash(true);       // %2F  (employee/EMP001)
        firewall.setAllowUrlEncodedDoubleSlash(true);  // %2F%2F
        firewall.setAllowUrlEncodedPercent(true);      // %25  (keys with literal %)
        firewall.setAllowBackSlash(true);              // backslash in keys
        firewall.setAllowSemicolon(true);              // semicolons in keys
        log.info("Spring Security firewall: URL-encoded slashes (%2F) and special characters allowed");
        return firewall;
    }

    @Bean
    public WebSecurityCustomizer webSecurityCustomizer(StrictHttpFirewall firewall) {
        return web -> web.httpFirewall(firewall);
    }
}
