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
package com.kuber.server.autoload;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

/**
 * Utility class for detecting and handling file encodings.
 * 
 * <p>Provides defensive encoding detection that:
 * <ul>
 *   <li>Detects BOM (Byte Order Mark) for UTF-8, UTF-16, UTF-32</li>
 *   <li>Falls back to trying common encodings if no BOM is found</li>
 *   <li>Validates encoding by attempting to decode a sample of the file</li>
 *   <li>Supports explicit encoding specification</li>
 * </ul>
 * 
 * <p>Supported BOM patterns:
 * <ul>
 *   <li>UTF-8: EF BB BF</li>
 *   <li>UTF-16 BE: FE FF</li>
 *   <li>UTF-16 LE: FF FE</li>
 *   <li>UTF-32 BE: 00 00 FE FF</li>
 *   <li>UTF-32 LE: FF FE 00 00</li>
 * </ul>
 * 
 * @version 1.8.1
 * @since 1.7.9
 */
@Slf4j
public class EncodingDetector {
    
    // BOM signatures
    private static final byte[] UTF8_BOM = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
    private static final byte[] UTF16_BE_BOM = {(byte) 0xFE, (byte) 0xFF};
    private static final byte[] UTF16_LE_BOM = {(byte) 0xFF, (byte) 0xFE};
    private static final byte[] UTF32_BE_BOM = {(byte) 0x00, (byte) 0x00, (byte) 0xFE, (byte) 0xFF};
    private static final byte[] UTF32_LE_BOM = {(byte) 0xFF, (byte) 0xFE, (byte) 0x00, (byte) 0x00};
    
    // Common encodings to try (in order of likelihood)
    private static final List<Charset> COMMON_ENCODINGS = Arrays.asList(
            StandardCharsets.UTF_8,
            StandardCharsets.ISO_8859_1,      // Latin-1
            Charset.forName("windows-1252"),   // Windows Western European
            StandardCharsets.US_ASCII,
            StandardCharsets.UTF_16,
            StandardCharsets.UTF_16BE,
            StandardCharsets.UTF_16LE
    );
    
    // Sample size for encoding validation (read first N bytes)
    private static final int SAMPLE_SIZE = 8192;
    
    /**
     * Result of encoding detection containing the detected charset and BOM length.
     */
    public record EncodingResult(Charset charset, int bomLength, String detectionMethod) {
        
        /**
         * Create a result for a detected encoding.
         */
        public static EncodingResult of(Charset charset, int bomLength, String method) {
            return new EncodingResult(charset, bomLength, method);
        }
        
        /**
         * Create a result for default/fallback encoding.
         */
        public static EncodingResult defaultEncoding(Charset charset) {
            return new EncodingResult(charset, 0, "default");
        }
    }
    
    /**
     * Detect the encoding of a file.
     * 
     * <p>Detection order:
     * <ol>
     *   <li>Check for BOM (Byte Order Mark)</li>
     *   <li>Try the default encoding</li>
     *   <li>Try common encodings</li>
     *   <li>Fall back to default encoding</li>
     * </ol>
     * 
     * @param file the file to detect encoding for
     * @param defaultEncoding the default encoding to use if detection fails
     * @return EncodingResult containing the detected charset and BOM length
     */
    public static EncodingResult detectEncoding(Path file, Charset defaultEncoding) {
        try {
            // Read file header for BOM detection
            byte[] header = readFileHeader(file, 4);
            if (header == null || header.length == 0) {
                log.debug("Empty file or unable to read header: {}", file);
                return EncodingResult.defaultEncoding(defaultEncoding);
            }
            
            // Check for BOM
            EncodingResult bomResult = detectBom(header);
            if (bomResult != null) {
                log.debug("Detected encoding via BOM for {}: {} (BOM length: {})", 
                        file.getFileName(), bomResult.charset(), bomResult.bomLength());
                return bomResult;
            }
            
            // No BOM found - try to validate with default encoding first
            if (isValidEncoding(file, defaultEncoding)) {
                log.debug("File {} is valid with default encoding: {}", file.getFileName(), defaultEncoding);
                return EncodingResult.of(defaultEncoding, 0, "validated-default");
            }
            
            // Try common encodings
            for (Charset charset : COMMON_ENCODINGS) {
                if (!charset.equals(defaultEncoding) && isValidEncoding(file, charset)) {
                    log.info("Detected encoding for {} via validation: {}", file.getFileName(), charset);
                    return EncodingResult.of(charset, 0, "validated-fallback");
                }
            }
            
            // Fall back to default
            log.warn("Could not detect encoding for {}, using default: {}", file.getFileName(), defaultEncoding);
            return EncodingResult.defaultEncoding(defaultEncoding);
            
        } catch (Exception e) {
            log.warn("Error detecting encoding for {}: {}", file.getFileName(), e.getMessage());
            return EncodingResult.defaultEncoding(defaultEncoding);
        }
    }
    
    /**
     * Detect BOM (Byte Order Mark) from file header bytes.
     * 
     * @param header the first bytes of the file
     * @return EncodingResult if BOM detected, null otherwise
     */
    private static EncodingResult detectBom(byte[] header) {
        if (header.length >= 4) {
            // Check UTF-32 first (4 bytes)
            if (startsWith(header, UTF32_BE_BOM)) {
                return EncodingResult.of(Charset.forName("UTF-32BE"), 4, "BOM-UTF32BE");
            }
            if (startsWith(header, UTF32_LE_BOM)) {
                return EncodingResult.of(Charset.forName("UTF-32LE"), 4, "BOM-UTF32LE");
            }
        }
        
        if (header.length >= 3) {
            // Check UTF-8 (3 bytes)
            if (startsWith(header, UTF8_BOM)) {
                return EncodingResult.of(StandardCharsets.UTF_8, 3, "BOM-UTF8");
            }
        }
        
        if (header.length >= 2) {
            // Check UTF-16 (2 bytes) - check after UTF-32 LE since it shares prefix
            if (startsWith(header, UTF16_BE_BOM)) {
                return EncodingResult.of(StandardCharsets.UTF_16BE, 2, "BOM-UTF16BE");
            }
            // UTF-16 LE check needs to ensure it's not UTF-32 LE
            if (startsWith(header, UTF16_LE_BOM) && 
                    (header.length < 4 || header[2] != 0 || header[3] != 0)) {
                return EncodingResult.of(StandardCharsets.UTF_16LE, 2, "BOM-UTF16LE");
            }
        }
        
        return null;
    }
    
    /**
     * Check if array starts with the given prefix.
     */
    private static boolean startsWith(byte[] array, byte[] prefix) {
        if (array.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (array[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Read the first N bytes of a file.
     */
    private static byte[] readFileHeader(Path file, int length) {
        try (InputStream is = Files.newInputStream(file)) {
            byte[] header = new byte[length];
            int read = is.read(header);
            if (read < length) {
                return Arrays.copyOf(header, read);
            }
            return header;
        } catch (IOException e) {
            log.debug("Error reading file header: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Validate if a file can be read with the given encoding.
     * 
     * <p>Reads a sample of the file and checks for decoding errors.
     * 
     * @param file the file to validate
     * @param charset the charset to try
     * @return true if the file can be decoded without errors
     */
    public static boolean isValidEncoding(Path file, Charset charset) {
        try (InputStream is = Files.newInputStream(file)) {
            // Read sample bytes
            byte[] sample = new byte[SAMPLE_SIZE];
            int read = is.read(sample);
            if (read <= 0) {
                return true; // Empty file is valid
            }
            if (read < sample.length) {
                sample = Arrays.copyOf(sample, read);
            }
            
            // Try to decode with strict error handling
            CharsetDecoder decoder = charset.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            
            try {
                decoder.decode(java.nio.ByteBuffer.wrap(sample));
                return true;
            } catch (java.nio.charset.CharacterCodingException e) {
                log.trace("Encoding {} failed for file {}: {}", charset, file.getFileName(), e.getMessage());
                return false;
            }
        } catch (IOException e) {
            log.debug("Error validating encoding for {}: {}", file.getFileName(), e.getMessage());
            return false;
        }
    }
    
    /**
     * Create a BufferedReader that handles BOM and encoding properly.
     * 
     * <p>This method:
     * <ul>
     *   <li>Detects the encoding (BOM or validation)</li>
     *   <li>Skips the BOM if present</li>
     *   <li>Returns a reader with the appropriate charset</li>
     * </ul>
     * 
     * @param file the file to read
     * @param defaultEncoding the default encoding to use
     * @return a BufferedReader configured with the detected encoding
     * @throws IOException if the file cannot be read
     */
    public static BufferedReader createReader(Path file, Charset defaultEncoding) throws IOException {
        EncodingResult result = detectEncoding(file, defaultEncoding);
        
        log.debug("Creating reader for {} with encoding {} (detection: {}, BOM skip: {} bytes)", 
                file.getFileName(), result.charset(), result.detectionMethod(), result.bomLength());
        
        InputStream is = Files.newInputStream(file);
        
        // Skip BOM if detected
        if (result.bomLength() > 0) {
            long skipped = is.skip(result.bomLength());
            if (skipped != result.bomLength()) {
                log.warn("Expected to skip {} BOM bytes but skipped {}", result.bomLength(), skipped);
            }
        }
        
        return new BufferedReader(new InputStreamReader(is, result.charset()));
    }
    
    /**
     * Create a BufferedReader with explicit encoding and optional BOM skipping.
     * 
     * @param file the file to read
     * @param charset the charset to use
     * @param skipBom whether to detect and skip BOM
     * @return a BufferedReader configured with the specified charset
     * @throws IOException if the file cannot be read
     */
    public static BufferedReader createReader(Path file, Charset charset, boolean skipBom) throws IOException {
        InputStream is = Files.newInputStream(file);
        
        if (skipBom) {
            // Check for BOM
            byte[] header = readFileHeader(file, 4);
            EncodingResult bomResult = header != null ? detectBom(header) : null;
            
            if (bomResult != null && bomResult.bomLength() > 0) {
                long skipped = is.skip(bomResult.bomLength());
                log.debug("Skipped {} byte BOM for file {}", skipped, file.getFileName());
            }
        }
        
        return new BufferedReader(new InputStreamReader(is, charset));
    }
    
    /**
     * Read all lines from a file with automatic encoding detection.
     * 
     * @param file the file to read
     * @param defaultEncoding the default encoding to use
     * @return list of lines from the file
     * @throws IOException if the file cannot be read
     */
    public static List<String> readAllLines(Path file, Charset defaultEncoding) throws IOException {
        EncodingResult result = detectEncoding(file, defaultEncoding);
        
        log.debug("Reading all lines from {} with encoding {} (detection: {})", 
                file.getFileName(), result.charset(), result.detectionMethod());
        
        try (BufferedReader reader = createReader(file, defaultEncoding)) {
            return reader.lines().toList();
        }
    }
    
    /**
     * Read entire file content as string with automatic encoding detection.
     * 
     * @param file the file to read
     * @param defaultEncoding the default encoding to use
     * @return file content as string
     * @throws IOException if the file cannot be read
     */
    public static String readString(Path file, Charset defaultEncoding) throws IOException {
        EncodingResult result = detectEncoding(file, defaultEncoding);
        
        log.debug("Reading string from {} with encoding {} (detection: {})", 
                file.getFileName(), result.charset(), result.detectionMethod());
        
        try (BufferedReader reader = createReader(file, defaultEncoding)) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (sb.length() > 0) {
                    sb.append('\n');
                }
                sb.append(line);
            }
            return sb.toString();
        }
    }
    
    /**
     * Parse a charset name safely, returning the default if parsing fails.
     * 
     * @param charsetName the charset name to parse
     * @param defaultCharset the default charset to use if parsing fails
     * @return the parsed Charset or default
     */
    public static Charset parseCharset(String charsetName, Charset defaultCharset) {
        if (charsetName == null || charsetName.isBlank()) {
            return defaultCharset;
        }
        
        try {
            return Charset.forName(charsetName.trim());
        } catch (Exception e) {
            log.warn("Invalid charset name '{}', using default: {}", charsetName, defaultCharset);
            return defaultCharset;
        }
    }
    
    /**
     * Get a list of common charset names for reference.
     * 
     * @return list of common charset names
     */
    public static List<String> getCommonCharsetNames() {
        return Arrays.asList(
                "UTF-8",
                "UTF-16",
                "UTF-16BE",
                "UTF-16LE",
                "UTF-32",
                "UTF-32BE",
                "UTF-32LE",
                "US-ASCII",
                "ISO-8859-1",     // Latin-1
                "ISO-8859-15",    // Latin-9 (with Euro sign)
                "windows-1252",   // Windows Western European
                "windows-1251",   // Windows Cyrillic
                "Shift_JIS",      // Japanese
                "EUC-JP",         // Japanese
                "GB2312",         // Simplified Chinese
                "GBK",            // Simplified Chinese Extended
                "Big5",           // Traditional Chinese
                "EUC-KR"          // Korean
        );
    }
}
