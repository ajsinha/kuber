/*
 * Copyright © 2025-2030, All Rights Reserved
 * Ashutosh Sinha | Email: ajsinha@gmail.com
 */
package com.kuber.server.network;

import lombok.extern.slf4j.Slf4j;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * RESP protocol decoder for Apache MINA.
 * Handles both RESP array format (*n$len...) and inline commands.
 * 
 * <p>RESP format examples:
 * <ul>
 *   <li>Simple string: +OK\r\n</li>
 *   <li>Error: -ERR message\r\n</li>
 *   <li>Integer: :1000\r\n</li>
 *   <li>Bulk string: $6\r\nfoobar\r\n</li>
 *   <li>Array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n</li>
 * </ul>
 * 
 * <p>Inline format: COMMAND arg1 arg2\r\n
 * 
 * @version 2.0.0
 */
@Slf4j
public class RedisProtocolDecoder extends CumulativeProtocolDecoder {
    
    private final Charset charset;
    private final int maxLineLength;
    
    private static final String DECODER_STATE = "REDIS_DECODER_STATE";
    
    public RedisProtocolDecoder() {
        this(StandardCharsets.UTF_8, 1024 * 1024);
    }
    
    public RedisProtocolDecoder(Charset charset, int maxLineLength) {
        this.charset = charset;
        this.maxLineLength = maxLineLength;
    }
    
    @Override
    protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
        // Mark the current position
        int startPos = in.position();
        
        if (!in.hasRemaining()) {
            return false;
        }
        
        // Peek at first byte to determine format
        byte firstByte = in.get(startPos);
        
        if (firstByte == '*') {
            // RESP array format
            return decodeRespArray(session, in, out);
        } else if (firstByte == '+' || firstByte == '-' || firstByte == ':' || firstByte == '$') {
            // Other RESP types (simple string, error, integer, bulk string)
            return decodeRespSimple(session, in, out);
        } else {
            // Inline command format
            return decodeInline(session, in, out);
        }
    }
    
    /**
     * Decode RESP array format: *n\r\n$len\r\ndata\r\n...
     */
    private boolean decodeRespArray(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
        int startPos = in.position();
        
        // Read array count line: *n\r\n
        String countLine = readLine(in);
        if (countLine == null) {
            in.position(startPos);
            return false;
        }
        
        int count;
        try {
            count = Integer.parseInt(countLine.substring(1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid RESP array count: " + countLine);
        }
        
        if (count < 0) {
            // Null array
            out.write("");
            return true;
        }
        
        // Read each bulk string element
        List<String> elements = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String element = readBulkString(in);
            if (element == null) {
                // Not enough data, reset and wait for more
                in.position(startPos);
                return false;
            }
            elements.add(element);
        }
        
        // Convert to inline command format for the handler
        String command = String.join(" ", elements);
        out.write(command);
        return true;
    }
    
    /**
     * Read a bulk string: $len\r\ndata\r\n
     */
    private String readBulkString(IoBuffer in) {
        int startPos = in.position();
        
        // Read length line
        String lenLine = readLine(in);
        if (lenLine == null) {
            in.position(startPos);
            return null;
        }
        
        if (!lenLine.startsWith("$")) {
            throw new IllegalArgumentException("Expected bulk string, got: " + lenLine);
        }
        
        int len;
        try {
            len = Integer.parseInt(lenLine.substring(1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid bulk string length: " + lenLine);
        }
        
        if (len < 0) {
            // Null bulk string
            return null;
        }
        
        // Check if we have enough data (len bytes + \r\n)
        if (in.remaining() < len + 2) {
            in.position(startPos);
            return null;
        }
        
        // Read the data
        byte[] data = new byte[len];
        in.get(data);
        
        // Read and verify \r\n
        if (in.remaining() < 2) {
            in.position(startPos);
            return null;
        }
        
        byte cr = in.get();
        byte lf = in.get();
        if (cr != '\r' || lf != '\n') {
            throw new IllegalArgumentException("Bulk string not terminated with \\r\\n");
        }
        
        return new String(data, charset);
    }
    
    /**
     * Decode other RESP types (simple string, error, integer, single bulk string)
     */
    private boolean decodeRespSimple(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
        String line = readLine(in);
        if (line == null) {
            return false;
        }
        
        // For simple types, just pass the line content
        out.write(line.substring(1)); // Remove type prefix
        return true;
    }
    
    /**
     * Decode inline command format: COMMAND arg1 arg2\r\n
     */
    private boolean decodeInline(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
        String line = readLine(in);
        if (line == null) {
            return false;
        }
        
        // Pass the line as-is to the handler
        out.write(line);
        return true;
    }
    
    /**
     * Read a line terminated by \r\n or \n
     * Returns null if complete line not available
     */
    private String readLine(IoBuffer in) {
        int startPos = in.position();
        int endPos = -1;
        boolean hasCr = false;
        
        // Scan for line terminator
        int scanPos = startPos;
        int remaining = in.remaining();
        
        for (int i = 0; i < remaining && i < maxLineLength; i++) {
            byte b = in.get(scanPos + i);
            if (b == '\n') {
                endPos = scanPos + i;
                hasCr = (i > 0 && in.get(scanPos + i - 1) == '\r');
                break;
            }
        }
        
        if (endPos < 0) {
            // No complete line yet
            return null;
        }
        
        // Calculate line length (excluding \r\n or \n)
        int lineLen = endPos - startPos - (hasCr ? 1 : 0);
        
        // Read the line
        byte[] lineBytes = new byte[lineLen];
        in.get(lineBytes);
        
        // Skip \r if present
        if (hasCr) {
            in.get(); // Skip \r
        }
        in.get(); // Skip \n
        
        return new String(lineBytes, charset);
    }
}
