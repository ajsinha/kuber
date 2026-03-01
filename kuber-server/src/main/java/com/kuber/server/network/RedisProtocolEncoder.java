/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.network;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * RESP protocol encoder for Apache MINA.
 * Writes response strings directly without adding extra delimiters.
 * The response string already contains proper RESP formatting with \r\n.
 * 
 * @version 2.6.3
 */
public class RedisProtocolEncoder extends ProtocolEncoderAdapter {
    
    private final Charset charset;
    
    public RedisProtocolEncoder() {
        this(StandardCharsets.UTF_8);
    }
    
    public RedisProtocolEncoder(Charset charset) {
        this.charset = charset;
    }
    
    @Override
    public void encode(IoSession session, Object message, ProtocolEncoderOutput out) throws Exception {
        String response;
        
        if (message instanceof String) {
            response = (String) message;
        } else {
            response = message.toString();
        }
        
        // Write directly without adding any extra delimiters
        // The response string already has \r\n from RESP protocol encoding
        byte[] bytes = response.getBytes(charset);
        IoBuffer buffer = IoBuffer.allocate(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        out.write(buffer);
    }
}
