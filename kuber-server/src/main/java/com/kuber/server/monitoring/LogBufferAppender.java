/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 *
 * Custom Logback appender that feeds log events into the LogBufferService.
 * This appender is registered programmatically at application startup.
 *
 * @version 2.4.0
 */
package com.kuber.server.monitoring;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.AppenderBase;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Logback appender that captures log events into the in-memory LogBufferService.
 * Automatically registered at startup to the root logger.
 *
 * @version 2.4.0
 */
@Component
@RequiredArgsConstructor
public class LogBufferAppender extends AppenderBase<ILoggingEvent> {

    private final LogBufferService logBufferService;

    @PostConstruct
    public void init() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

        this.setContext(loggerContext);
        this.setName("LOG_BUFFER");
        this.start();

        Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(this);
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (logBufferService == null) return;

        String stackTrace = null;
        IThrowableProxy tp = event.getThrowableProxy();
        if (tp != null) {
            stackTrace = ThrowableProxyUtil.asString(tp);
        }

        logBufferService.addEntry(
                event.getLevel().toString(),
                event.getLoggerName(),
                event.getFormattedMessage(),
                event.getThreadName(),
                event.getTimeStamp(),
                stackTrace
        );
    }
}
