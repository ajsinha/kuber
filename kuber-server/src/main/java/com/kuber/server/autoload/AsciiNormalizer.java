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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.extern.slf4j.Slf4j;

import java.text.Normalizer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Utility class for normalizing text to US-ASCII.
 * 
 * <p>Provides comprehensive ASCII normalization that:
 * <ul>
 *   <li>Converts accented characters to their base forms (é → e, ü → u, ñ → n)</li>
 *   <li>Transliterates special characters (ß → ss, æ → ae, ø → o)</li>
 *   <li>Handles ligatures (ﬁ → fi, ﬂ → fl)</li>
 *   <li>Converts currency symbols (€ → EUR, £ → GBP, ¥ → YEN)</li>
 *   <li>Normalizes quotes and dashes to ASCII equivalents</li>
 *   <li>Removes or replaces characters that cannot be transliterated</li>
 * </ul>
 * 
 * <p>This is useful for:
 * <ul>
 *   <li>Ensuring data consistency across different systems</li>
 *   <li>Search optimization (searching for "cafe" finds "café")</li>
 *   <li>Legacy system compatibility</li>
 *   <li>URL-safe key generation</li>
 * </ul>
 * 
 * <p>Example transformations:
 * <pre>
 * "Café Müller"      → "Cafe Muller"
 * "naïve résumé"     → "naive resume"
 * "Æsop's Fœtus"     → "Aesop's Foetus"
 * "Größe"            → "Groesse"
 * "北京 (Beijing)"   → " (Beijing)"  (CJK removed, can be configured)
 * "Price: €100"      → "Price: EUR100"
 * </pre>
 * 
 * @version 1.7.7
 * @since 1.7.7
 */
@Slf4j
public class AsciiNormalizer {
    
    // Pattern to match non-ASCII characters after normalization
    private static final Pattern NON_ASCII_PATTERN = Pattern.compile("[^\\x00-\\x7F]");
    
    // Pattern to match combining diacritical marks (used after NFD normalization)
    private static final Pattern DIACRITICAL_MARKS = Pattern.compile("\\p{InCombiningDiacriticalMarks}+");
    
    // Special character replacements (before NFD normalization)
    private static final Map<String, String> SPECIAL_REPLACEMENTS = new HashMap<>();
    
    // Single character replacements
    private static final Map<Character, String> CHAR_REPLACEMENTS = new HashMap<>();
    
    static {
        // German special characters
        CHAR_REPLACEMENTS.put('\u00DF', "ss"); // LATIN SMALL LETTER SHARP S
        CHAR_REPLACEMENTS.put('\u1E9E', "SS"); // LATIN CAPITAL LETTER SHARP S
        
        // Nordic/Scandinavian
        CHAR_REPLACEMENTS.put('\u00E6', "ae"); // LATIN SMALL LETTER AE
        CHAR_REPLACEMENTS.put('\u00C6', "AE"); // LATIN CAPITAL LETTER AE
        CHAR_REPLACEMENTS.put('\u0153', "oe"); // LATIN SMALL LIGATURE OE
        CHAR_REPLACEMENTS.put('\u0152', "OE"); // LATIN CAPITAL LIGATURE OE
        CHAR_REPLACEMENTS.put('\u00F8', "o");  // LATIN SMALL LETTER O WITH STROKE
        CHAR_REPLACEMENTS.put('\u00D8', "O");  // LATIN CAPITAL LETTER O WITH STROKE
        CHAR_REPLACEMENTS.put('\u00E5', "a");  // LATIN SMALL LETTER A WITH RING ABOVE
        CHAR_REPLACEMENTS.put('\u00C5', "A");  // LATIN CAPITAL LETTER A WITH RING ABOVE
        CHAR_REPLACEMENTS.put('\u00F0', "d");  // LATIN SMALL LETTER ETH
        CHAR_REPLACEMENTS.put('\u00D0', "D");  // LATIN CAPITAL LETTER ETH
        CHAR_REPLACEMENTS.put('\u00FE', "th"); // LATIN SMALL LETTER THORN
        CHAR_REPLACEMENTS.put('\u00DE', "TH"); // LATIN CAPITAL LETTER THORN
        
        // Polish
        CHAR_REPLACEMENTS.put('\u0142', "l");  // LATIN SMALL LETTER L WITH STROKE
        CHAR_REPLACEMENTS.put('\u0141', "L");  // LATIN CAPITAL LETTER L WITH STROKE
        
        // Currency symbols
        CHAR_REPLACEMENTS.put('\u20AC', "EUR"); // EURO SIGN
        CHAR_REPLACEMENTS.put('\u00A3', "GBP"); // POUND SIGN
        CHAR_REPLACEMENTS.put('\u00A5', "YEN"); // YEN SIGN
        CHAR_REPLACEMENTS.put('\u00A2', "c");   // CENT SIGN
        CHAR_REPLACEMENTS.put('\u20B9', "INR"); // INDIAN RUPEE SIGN
        CHAR_REPLACEMENTS.put('\u20BD', "RUB"); // RUBLE SIGN
        CHAR_REPLACEMENTS.put('\u20A9', "KRW"); // WON SIGN
        CHAR_REPLACEMENTS.put('\u20AA', "ILS"); // NEW SHEQEL SIGN
        CHAR_REPLACEMENTS.put('\u0E3F', "THB"); // THAI CURRENCY SYMBOL BAHT
        CHAR_REPLACEMENTS.put('\u20AB', "VND"); // DONG SIGN
        
        // Typographic quotes and apostrophes
        CHAR_REPLACEMENTS.put('\u2018', "'");  // LEFT SINGLE QUOTATION MARK
        CHAR_REPLACEMENTS.put('\u2019', "'");  // RIGHT SINGLE QUOTATION MARK
        CHAR_REPLACEMENTS.put('\u201A', "'");  // SINGLE LOW-9 QUOTATION MARK
        CHAR_REPLACEMENTS.put('\u201C', "\""); // LEFT DOUBLE QUOTATION MARK
        CHAR_REPLACEMENTS.put('\u201D', "\""); // RIGHT DOUBLE QUOTATION MARK
        CHAR_REPLACEMENTS.put('\u201E', "\""); // DOUBLE LOW-9 QUOTATION MARK
        CHAR_REPLACEMENTS.put('\u2039', "<");  // SINGLE LEFT-POINTING ANGLE QUOTATION MARK
        CHAR_REPLACEMENTS.put('\u203A', ">");  // SINGLE RIGHT-POINTING ANGLE QUOTATION MARK
        CHAR_REPLACEMENTS.put('\u00AB', "<<"); // LEFT-POINTING DOUBLE ANGLE QUOTATION MARK
        CHAR_REPLACEMENTS.put('\u00BB', ">>"); // RIGHT-POINTING DOUBLE ANGLE QUOTATION MARK
        
        // Dashes and hyphens
        CHAR_REPLACEMENTS.put('\u2013', "-");  // EN DASH
        CHAR_REPLACEMENTS.put('\u2014', "-");  // EM DASH
        CHAR_REPLACEMENTS.put('\u2015', "-");  // HORIZONTAL BAR
        CHAR_REPLACEMENTS.put('\u2012', "-");  // FIGURE DASH
        CHAR_REPLACEMENTS.put('\u2212', "-");  // MINUS SIGN
        
        // Ellipsis
        CHAR_REPLACEMENTS.put('\u2026', "...");
        
        // Spaces
        CHAR_REPLACEMENTS.put('\u00A0', " ");  // non-breaking space
        CHAR_REPLACEMENTS.put('\u2003', " ");  // em space
        CHAR_REPLACEMENTS.put('\u2002', " ");  // en space
        CHAR_REPLACEMENTS.put('\u2009', " ");  // thin space
        CHAR_REPLACEMENTS.put('\u200B', "");   // zero-width space (remove)
        CHAR_REPLACEMENTS.put('\uFEFF', "");   // BOM (remove)
        
        // Ligatures
        CHAR_REPLACEMENTS.put('\uFB01', "fi");  // LATIN SMALL LIGATURE FI
        CHAR_REPLACEMENTS.put('\uFB02', "fl");  // LATIN SMALL LIGATURE FL
        CHAR_REPLACEMENTS.put('\uFB00', "ff");  // LATIN SMALL LIGATURE FF
        CHAR_REPLACEMENTS.put('\uFB03', "ffi"); // LATIN SMALL LIGATURE FFI
        CHAR_REPLACEMENTS.put('\uFB04', "ffl"); // LATIN SMALL LIGATURE FFL
        
        // Math symbols
        CHAR_REPLACEMENTS.put('\u00D7', "x");     // MULTIPLICATION SIGN
        CHAR_REPLACEMENTS.put('\u00F7', "/");     // DIVISION SIGN
        CHAR_REPLACEMENTS.put('\u00B1', "+/-");   // PLUS-MINUS SIGN
        CHAR_REPLACEMENTS.put('\u2264', "<=");    // LESS-THAN OR EQUAL TO
        CHAR_REPLACEMENTS.put('\u2265', ">=");    // GREATER-THAN OR EQUAL TO
        CHAR_REPLACEMENTS.put('\u2260', "!=");    // NOT EQUAL TO
        CHAR_REPLACEMENTS.put('\u2248', "~");     // ALMOST EQUAL TO
        CHAR_REPLACEMENTS.put('\u00B0', " deg");  // DEGREE SIGN
        CHAR_REPLACEMENTS.put('\u2032', "'");     // PRIME
        CHAR_REPLACEMENTS.put('\u2033', "\"");    // DOUBLE PRIME
        CHAR_REPLACEMENTS.put('\u00A9', "(c)");   // COPYRIGHT SIGN
        CHAR_REPLACEMENTS.put('\u00AE', "(R)");   // REGISTERED SIGN
        CHAR_REPLACEMENTS.put('\u2122', "(TM)");  // TRADE MARK SIGN
        CHAR_REPLACEMENTS.put('\u2116', "No.");   // NUMERO SIGN
        CHAR_REPLACEMENTS.put('\u00A7', "S");     // SECTION SIGN
        CHAR_REPLACEMENTS.put('\u00B6', "P");     // PILCROW SIGN
        CHAR_REPLACEMENTS.put('\u2020', "+");     // DAGGER
        CHAR_REPLACEMENTS.put('\u2021', "++");    // DOUBLE DAGGER
        CHAR_REPLACEMENTS.put('\u2022', "*");     // BULLET
        CHAR_REPLACEMENTS.put('\u00B7', ".");     // MIDDLE DOT
        
        // Fractions
        CHAR_REPLACEMENTS.put('\u00BD', "1/2"); // VULGAR FRACTION ONE HALF
        CHAR_REPLACEMENTS.put('\u00BC', "1/4"); // VULGAR FRACTION ONE QUARTER
        CHAR_REPLACEMENTS.put('\u00BE', "3/4"); // VULGAR FRACTION THREE QUARTERS
        CHAR_REPLACEMENTS.put('\u2153', "1/3"); // VULGAR FRACTION ONE THIRD
        CHAR_REPLACEMENTS.put('\u2154', "2/3"); // VULGAR FRACTION TWO THIRDS
        CHAR_REPLACEMENTS.put('\u215B', "1/8"); // VULGAR FRACTION ONE EIGHTH
        CHAR_REPLACEMENTS.put('\u215C', "3/8"); // VULGAR FRACTION THREE EIGHTHS
        CHAR_REPLACEMENTS.put('\u215D', "5/8"); // VULGAR FRACTION FIVE EIGHTHS
        CHAR_REPLACEMENTS.put('\u215E', "7/8"); // VULGAR FRACTION SEVEN EIGHTHS
        
        // Superscripts and subscripts
        CHAR_REPLACEMENTS.put('\u00B9', "1"); // SUPERSCRIPT ONE
        CHAR_REPLACEMENTS.put('\u00B2', "2"); // SUPERSCRIPT TWO
        CHAR_REPLACEMENTS.put('\u00B3', "3"); // SUPERSCRIPT THREE
        CHAR_REPLACEMENTS.put('\u2070', "0"); // SUPERSCRIPT ZERO
        CHAR_REPLACEMENTS.put('\u2074', "4"); // SUPERSCRIPT FOUR
        CHAR_REPLACEMENTS.put('\u2075', "5"); // SUPERSCRIPT FIVE
        CHAR_REPLACEMENTS.put('\u2076', "6"); // SUPERSCRIPT SIX
        CHAR_REPLACEMENTS.put('\u2077', "7"); // SUPERSCRIPT SEVEN
        CHAR_REPLACEMENTS.put('\u2078', "8"); // SUPERSCRIPT EIGHT
        CHAR_REPLACEMENTS.put('\u2079', "9"); // SUPERSCRIPT NINE
        
        // Greek letters (common in science/math)
        CHAR_REPLACEMENTS.put('\u03B1', "alpha");   // GREEK SMALL LETTER ALPHA
        CHAR_REPLACEMENTS.put('\u03B2', "beta");    // GREEK SMALL LETTER BETA
        CHAR_REPLACEMENTS.put('\u03B3', "gamma");   // GREEK SMALL LETTER GAMMA
        CHAR_REPLACEMENTS.put('\u03B4', "delta");   // GREEK SMALL LETTER DELTA
        CHAR_REPLACEMENTS.put('\u03B5', "epsilon"); // GREEK SMALL LETTER EPSILON
        CHAR_REPLACEMENTS.put('\u03C0', "pi");      // GREEK SMALL LETTER PI
        CHAR_REPLACEMENTS.put('\u03C3', "sigma");   // GREEK SMALL LETTER SIGMA
        CHAR_REPLACEMENTS.put('\u03BC', "mu");      // GREEK SMALL LETTER MU
        CHAR_REPLACEMENTS.put('\u03A9', "Omega");   // GREEK CAPITAL LETTER OMEGA
        CHAR_REPLACEMENTS.put('\u03BB', "lambda");  // GREEK SMALL LETTER LAMBDA
        CHAR_REPLACEMENTS.put('\u03B8', "theta");   // GREEK SMALL LETTER THETA
        CHAR_REPLACEMENTS.put('\u03C6', "phi");     // GREEK SMALL LETTER PHI
        CHAR_REPLACEMENTS.put('\u03C8', "psi");     // GREEK SMALL LETTER PSI
        CHAR_REPLACEMENTS.put('\u03C9', "omega");   // GREEK SMALL LETTER OMEGA
        
        // Inverted punctuation (Spanish)
        CHAR_REPLACEMENTS.put('\u00A1', "!"); // INVERTED EXCLAMATION MARK
        CHAR_REPLACEMENTS.put('\u00BF', "?"); // INVERTED QUESTION MARK
    }
    
    /**
     * Normalize a string to US-ASCII.
     * 
     * <p>Process:
     * <ol>
     *   <li>Apply special character replacements (ß → ss, € → EUR, etc.)</li>
     *   <li>Apply Unicode NFD normalization (decompose accented chars)</li>
     *   <li>Remove combining diacritical marks (accents)</li>
     *   <li>Remove any remaining non-ASCII characters</li>
     * </ol>
     * 
     * @param input the input string (may contain non-ASCII characters)
     * @return ASCII-only string
     */
    public static String normalize(String input) {
        return normalize(input, true);
    }
    
    /**
     * Normalize a string to US-ASCII with option to preserve or remove unknown characters.
     * 
     * @param input the input string
     * @param removeUnknown if true, remove characters that can't be transliterated;
     *                      if false, replace with '?'
     * @return ASCII-only string
     */
    public static String normalize(String input, boolean removeUnknown) {
        if (input == null) {
            return null;
        }
        
        if (input.isEmpty()) {
            return input;
        }
        
        // Check if already ASCII (fast path)
        if (isAscii(input)) {
            return input;
        }
        
        StringBuilder result = new StringBuilder(input.length());
        
        // Step 1: Apply character replacements
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            
            // Check for special replacement
            String replacement = CHAR_REPLACEMENTS.get(c);
            if (replacement != null) {
                result.append(replacement);
            } else {
                result.append(c);
            }
        }
        
        String processed = result.toString();
        
        // Step 2: NFD normalize (decompose accented characters)
        // This separates base characters from combining marks
        // e.g., "é" becomes "e" + combining acute accent
        processed = Normalizer.normalize(processed, Normalizer.Form.NFD);
        
        // Step 3: Remove combining diacritical marks
        processed = DIACRITICAL_MARKS.matcher(processed).replaceAll("");
        
        // Step 4: Handle any remaining non-ASCII
        if (!isAscii(processed)) {
            if (removeUnknown) {
                processed = NON_ASCII_PATTERN.matcher(processed).replaceAll("");
            } else {
                processed = NON_ASCII_PATTERN.matcher(processed).replaceAll("?");
            }
        }
        
        return processed;
    }
    
    /**
     * Check if a string contains only ASCII characters.
     * 
     * @param input the string to check
     * @return true if all characters are in the ASCII range (0-127)
     */
    public static boolean isAscii(String input) {
        if (input == null || input.isEmpty()) {
            return true;
        }
        
        for (int i = 0; i < input.length(); i++) {
            if (input.charAt(i) > 127) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Normalize all string values in a JSON object to ASCII.
     * 
     * <p>Recursively processes:
     * <ul>
     *   <li>Object fields (both keys and values)</li>
     *   <li>Array elements</li>
     *   <li>Nested structures</li>
     * </ul>
     * 
     * <p>Non-string values (numbers, booleans, null) are preserved unchanged.
     * 
     * @param node the JSON node to normalize
     * @param objectMapper ObjectMapper for creating new nodes
     * @return new JsonNode with all strings normalized to ASCII
     */
    public static JsonNode normalizeJson(JsonNode node, ObjectMapper objectMapper) {
        return normalizeJson(node, objectMapper, true, true);
    }
    
    /**
     * Normalize all string values in a JSON object to ASCII with options.
     * 
     * @param node the JSON node to normalize
     * @param objectMapper ObjectMapper for creating new nodes
     * @param normalizeKeys if true, also normalize object keys
     * @param removeUnknown if true, remove untransliterable chars; if false, use '?'
     * @return new JsonNode with strings normalized to ASCII
     */
    public static JsonNode normalizeJson(JsonNode node, ObjectMapper objectMapper, 
                                          boolean normalizeKeys, boolean removeUnknown) {
        if (node == null || node.isNull()) {
            return node;
        }
        
        if (node.isTextual()) {
            // Normalize text value
            String normalized = normalize(node.asText(), removeUnknown);
            return new TextNode(normalized);
        }
        
        if (node.isObject()) {
            ObjectNode result = objectMapper.createObjectNode();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String key = normalizeKeys ? normalize(field.getKey(), removeUnknown) : field.getKey();
                JsonNode value = normalizeJson(field.getValue(), objectMapper, normalizeKeys, removeUnknown);
                result.set(key, value);
            }
            
            return result;
        }
        
        if (node.isArray()) {
            ArrayNode result = objectMapper.createArrayNode();
            for (JsonNode element : node) {
                result.add(normalizeJson(element, objectMapper, normalizeKeys, removeUnknown));
            }
            return result;
        }
        
        // Numbers, booleans, etc. - return as-is
        return node;
    }
    
    /**
     * Normalize an ObjectNode in place (modifies the original node).
     * 
     * <p>This is more efficient than creating a new node when you don't
     * need to preserve the original.
     * 
     * @param node the ObjectNode to normalize
     * @param objectMapper ObjectMapper for creating nodes
     * @param normalizeKeys if true, also normalize keys (requires rebuilding)
     */
    public static void normalizeJsonInPlace(ObjectNode node, ObjectMapper objectMapper, boolean normalizeKeys) {
        if (node == null) {
            return;
        }
        
        if (normalizeKeys) {
            // If normalizing keys, we need to rebuild the object
            Map<String, JsonNode> normalized = new HashMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String key = normalize(field.getKey(), true);
                JsonNode value = normalizeJson(field.getValue(), objectMapper, true, true);
                normalized.put(key, value);
            }
            
            node.removeAll();
            normalized.forEach(node::set);
        } else {
            // Just normalize values
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String fieldName = field.getKey();
                JsonNode value = field.getValue();
                
                if (value.isTextual()) {
                    String normalized = normalize(value.asText(), true);
                    node.put(fieldName, normalized);
                } else if (value.isObject()) {
                    normalizeJsonInPlace((ObjectNode) value, objectMapper, false);
                } else if (value.isArray()) {
                    ArrayNode arrayNode = (ArrayNode) value;
                    for (int i = 0; i < arrayNode.size(); i++) {
                        JsonNode element = arrayNode.get(i);
                        if (element.isTextual()) {
                            arrayNode.set(i, new TextNode(normalize(element.asText(), true)));
                        } else if (element.isObject()) {
                            normalizeJsonInPlace((ObjectNode) element, objectMapper, false);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Get statistics about non-ASCII content in a string.
     * 
     * @param input the string to analyze
     * @return map containing statistics (totalChars, asciiChars, nonAsciiChars, percentAscii)
     */
    public static Map<String, Object> getAsciiStats(String input) {
        Map<String, Object> stats = new HashMap<>();
        
        if (input == null || input.isEmpty()) {
            stats.put("totalChars", 0);
            stats.put("asciiChars", 0);
            stats.put("nonAsciiChars", 0);
            stats.put("percentAscii", 100.0);
            return stats;
        }
        
        int total = input.length();
        int ascii = 0;
        
        for (int i = 0; i < total; i++) {
            if (input.charAt(i) <= 127) {
                ascii++;
            }
        }
        
        stats.put("totalChars", total);
        stats.put("asciiChars", ascii);
        stats.put("nonAsciiChars", total - ascii);
        stats.put("percentAscii", (ascii * 100.0) / total);
        
        return stats;
    }
    
    /**
     * Normalize a cache key to ASCII.
     * 
     * <p>This is a convenience method for normalizing keys that also:
     * <ul>
     *   <li>Trims whitespace</li>
     *   <li>Collapses multiple spaces to single space</li>
     *   <li>Removes control characters</li>
     * </ul>
     * 
     * @param key the cache key to normalize
     * @return normalized ASCII key
     */
    public static String normalizeKey(String key) {
        if (key == null) {
            return null;
        }
        
        // First normalize to ASCII
        String normalized = normalize(key, true);
        
        // Remove control characters
        normalized = normalized.replaceAll("[\\x00-\\x1F\\x7F]", "");
        
        // Collapse whitespace
        normalized = normalized.replaceAll("\\s+", " ");
        
        // Trim
        return normalized.trim();
    }
}
