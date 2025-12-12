/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.network;

import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * RESP (Redis Serialization Protocol) codec factory for Apache MINA.
 * Handles both RESP array format and inline commands.
 * 
 * @version 1.7.4
 */
public class RedisProtocolCodecFactory implements ProtocolCodecFactory {
    
    private final RedisProtocolEncoder encoder;
    private final RedisProtocolDecoder decoder;
    
    public RedisProtocolCodecFactory() {
        this(StandardCharsets.UTF_8, 1024 * 1024); // 1MB default max line
    }
    
    public RedisProtocolCodecFactory(Charset charset, int maxLineLength) {
        this.encoder = new RedisProtocolEncoder(charset);
        this.decoder = new RedisProtocolDecoder(charset, maxLineLength);
    }
    
    @Override
    public ProtocolEncoder getEncoder(IoSession session) {
        return encoder;
    }
    
    @Override
    public ProtocolDecoder getDecoder(IoSession session) {
        return decoder;
    }
}
