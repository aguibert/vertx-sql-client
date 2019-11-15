package io.vertx.db2client.impl.util;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.sql.SQLException;

public class CCSIDHelper {

    /* DRDA CCSID levels for UTF8 and EBCDIC */
    static final int UTF8_CCSID = 1208;
    
    public static final CCSID EBCDIC = new EBCDIC();
    public static final CCSID UTF8 = new UTF8();

    public static interface CCSID {

        public byte space();

        public byte dot();

        // Convert a Java String into bytes for a particular ccsid.
        //
        // @param sourceString A Java String to convert.
        // @return A new byte array representing the String in a particular ccsid.
        public byte[] convertFromJavaString(String sourceString) throws SQLException;

        // Convert a byte array representing characters in a particular ccsid into a
        // Java String.
        //
        // @param sourceBytes An array of bytes to be converted.
        // @param offset An offset indicating first byte to convert.
        // @param numToConvert The number of bytes to be converted.
        // @return A new Java String Object created after conversion.
        String convertToJavaString(byte[] sourceBytes, int offset, int numToConvert);

        /**
         * Initialize this instance for encoding a new string. This method resets any
         * internal state that may be left after earlier calls to {@link #encode} on
         * this instance. For example, it may reset the internal
         * {@code java.nio.charset.CharsetEncoder}, if the implementation uses one to do
         * the encoding.
         */
        public void startEncoding();

        /**
         * Encode the contents of a {@code CharBuffer} into a {@code ByteBuffer}. The
         * method will return {@code true} if all the characters were encoded and copied
         * to the destination. If the receiving byte buffer is too small to hold the
         * entire encoded representation of the character buffer, the method will return
         * {@code false}. The caller should then allocate a larger byte buffer, copy the
         * contents from the old byte buffer to the new one, and then call this method
         * again to get the remaining characters encoded.
         *
         * @param src
         *            buffer holding the characters to encode
         * @param dest
         *            buffer receiving the encoded bytes
         * @param agent
         *            where to report errors
         * @return {@code true} if all characters were encoded, {@code false} if the
         *         destination buffer is full and there still are more characters to
         *         encode
         * @throws SQLException
         *             if the characters cannot be encoded using this CCSID manager's
         *             character encoding
         */
        public boolean encode(CharBuffer src, ByteBuffer dest) throws SQLException;
    }

    private static class EBCDIC implements CCSID {
        private static final int[] conversionArrayToEbcdic = { 0x0000, 0x0001, 0x0002, 0x0003, 0x0037, 0x002d, 0x002e,
                0x002f, 0x0016, 0x0005, 0x0025, 0x000b, 0x000c, 0x000d, 0x000e, 0x000f, 0x0010, 0x0011, 0x0012, 0x0013,
                0x003c, 0x003d, 0x0032, 0x0026, 0x0018, 0x0019, 0x003f, 0x0027, 0x001c, 0x001d, 0x001e, 0x001f, 0x0040,
                0x004f, 0x007f, 0x007b, 0x005b, 0x006c, 0x0050, 0x007d, 0x004d, 0x005d, 0x005c, 0x004e, 0x006b, 0x0060,
                0x004b, 0x0061, 0x00f0, 0x00f1, 0x00f2, 0x00f3, 0x00f4, 0x00f5, 0x00f6, 0x00f7, 0x00f8, 0x00f9, 0x007a,
                0x005e, 0x004c, 0x007e, 0x006e, 0x006f, 0x007c, 0x00c1, 0x00c2, 0x00c3, 0x00c4, 0x00c5, 0x00c6, 0x00c7,
                0x00c8, 0x00c9, 0x00d1, 0x00d2, 0x00d3, 0x00d4, 0x00d5, 0x00d6, 0x00d7, 0x00d8, 0x00d9, 0x00e2, 0x00e3,
                0x00e4, 0x00e5, 0x00e6, 0x00e7, 0x00e8, 0x00e9, 0x004a, 0x00e0, 0x005a, 0x005f, 0x006d, 0x0079, 0x0081,
                0x0082, 0x0083, 0x0084, 0x0085, 0x0086, 0x0087, 0x0088, 0x0089, 0x0091, 0x0092, 0x0093, 0x0094, 0x0095,
                0x0096, 0x0097, 0x0098, 0x0099, 0x00a2, 0x00a3, 0x00a4, 0x00a5, 0x00a6, 0x00a7, 0x00a8, 0x00a9, 0x00c0,
                0x00bb, 0x00d0, 0x00a1, 0x0007, 0x0020, 0x0021, 0x0022, 0x0023, 0x0024, 0x0015, 0x0006, 0x0017, 0x0028,
                0x0029, 0x002a, 0x002b, 0x002c, 0x0009, 0x000a, 0x001b, 0x0030, 0x0031, 0x001a, 0x0033, 0x0034, 0x0035,
                0x0036, 0x0008, 0x0038, 0x0039, 0x003a, 0x003b, 0x0004, 0x0014, 0x003e, 0x00ff, 0x0041, 0x00aa, 0x00b0,
                0x00b1, 0x009f, 0x00b2, 0x006a, 0x00b5, 0x00bd, 0x00b4, 0x009a, 0x008a, 0x00ba, 0x00ca, 0x00af, 0x00bc,
                0x0090, 0x008f, 0x00ea, 0x00fa, 0x00be, 0x00a0, 0x00b6, 0x00b3, 0x009d, 0x00da, 0x009b, 0x008b, 0x00b7,
                0x00b8, 0x00b9, 0x00ab, 0x0064, 0x0065, 0x0062, 0x0066, 0x0063, 0x0067, 0x009e, 0x0068, 0x0074, 0x0071,
                0x0072, 0x0073, 0x0078, 0x0075, 0x0076, 0x0077, 0x00ac, 0x0069, 0x00ed, 0x00ee, 0x00eb, 0x00ef, 0x00ec,
                0x00bf, 0x0080, 0x00fd, 0x00fe, 0x00fb, 0x00fc, 0x00ad, 0x00ae, 0x0059, 0x0044, 0x0045, 0x0042, 0x0046,
                0x0043, 0x0047, 0x009c, 0x0048, 0x0054, 0x0051, 0x0052, 0x0053, 0x0058, 0x0055, 0x0056, 0x0057, 0x008c,
                0x0049, 0x00cd, 0x00ce, 0x00cb, 0x00cf, 0x00cc, 0x00e1, 0x0070, 0x00dd, 0x00de, 0x00db, 0x00dc, 0x008d,
                0x008e, 0x00df };

        private static final int[] conversionArrayToUCS2 = { 0x0000, 0x0001, 0x0002, 0x0003, 0x009C, 0x0009, 0x0086,
                0x007F, 0x0097, 0x008D, 0x008E, 0x000B, 0x000C, 0x000D, 0x000E, 0x000F, 0x0010, 0x0011, 0x0012, 0x0013,
                0x009D, 0x0085, 0x0008, 0x0087, 0x0018, 0x0019, 0x0092, 0x008F, 0x001C, 0x001D, 0x001E, 0x001F, 0x0080,
                0x0081, 0x0082, 0x0083, 0x0084, 0x000A, 0x0017, 0x001B, 0x0088, 0x0089, 0x008A, 0x008B, 0x008C, 0x0005,
                0x0006, 0x0007, 0x0090, 0x0091, 0x0016, 0x0093, 0x0094, 0x0095, 0x0096, 0x0004, 0x0098, 0x0099, 0x009A,
                0x009B, 0x0014, 0x0015, 0x009E, 0x001A, 0x0020, 0x00A0, 0x00E2, 0x00E4, 0x00E0, 0x00E1, 0x00E3, 0x00E5,
                0x00E7, 0x00F1, 0x005B, 0x002E, 0x003C, 0x0028, 0x002B, 0x0021, 0x0026, 0x00E9, 0x00EA, 0x00EB, 0x00E8,
                0x00ED, 0x00EE, 0x00EF, 0x00EC, 0x00DF, 0x005D, 0x0024, 0x002A, 0x0029, 0x003B, 0x005E, 0x002D, 0x002F,
                0x00C2, 0x00C4, 0x00C0, 0x00C1, 0x00C3, 0x00C5, 0x00C7, 0x00D1, 0x00A6, 0x002C, 0x0025, 0x005F, 0x003E,
                0x003F, 0x00F8, 0x00C9, 0x00CA, 0x00CB, 0x00C8, 0x00CD, 0x00CE, 0x00CF, 0x00CC, 0x0060, 0x003A, 0x0023,
                0x0040, 0x0027, 0x003D, 0x0022, 0x00D8, 0x0061, 0x0062, 0x0063, 0x0064, 0x0065, 0x0066, 0x0067, 0x0068,
                0x0069, 0x00AB, 0x00BB, 0x00F0, 0x00FD, 0x00FE, 0x00B1, 0x00B0, 0x006A, 0x006B, 0x006C, 0x006D, 0x006E,
                0x006F, 0x0070, 0x0071, 0x0072, 0x00AA, 0x00BA, 0x00E6, 0x00B8, 0x00C6, 0x00A4, 0x00B5, 0x007E, 0x0073,
                0x0074, 0x0075, 0x0076, 0x0077, 0x0078, 0x0079, 0x007A, 0x00A1, 0x00BF, 0x00D0, 0x00DD, 0x00DE, 0x00AE,
                0x00A2, 0x00A3, 0x00A5, 0x00B7, 0x00A9, 0x00A7, 0x00B6, 0x00BC, 0x00BD, 0x00BE, 0x00AC, 0x007C, 0x00AF,
                0x00A8, 0x00B4, 0x00D7, 0x007B, 0x0041, 0x0042, 0x0043, 0x0044, 0x0045, 0x0046, 0x0047, 0x0048, 0x0049,
                0x00AD, 0x00F4, 0x00F6, 0x00F2, 0x00F3, 0x00F5, 0x007D, 0x004A, 0x004B, 0x004C, 0x004D, 0x004E, 0x004F,
                0x0050, 0x0051, 0x0052, 0x00B9, 0x00FB, 0x00FC, 0x00F9, 0x00FA, 0x00FF, 0x005C, 0x00F7, 0x0053, 0x0054,
                0x0055, 0x0056, 0x0057, 0x0058, 0x0059, 0x005A, 0x00B2, 0x00D4, 0x00D6, 0x00D2, 0x00D3, 0x00D5, 0x0030,
                0x0031, 0x0032, 0x0033, 0x0034, 0x0035, 0x0036, 0x0037, 0x0038, 0x0039, 0x00B3, 0x00DB, 0x00DC, 0x00D9,
                0x00DA, 0x009F };

        // EBCDIC ()
        // {
        // super ((byte) 0x40, // 0x40 is the ebcdic space character
        // (byte) 0x4B,
        // new byte[] {
        // // '0', '1', '2', '3', '4',
        // (byte)0xf0,(byte)0xf1,(byte)0xf2,(byte)0xf3,(byte)0xf4,
        // // '5', '6', '7', '8', '9',
        // (byte)0xf5,(byte)0xf6,(byte)0xf7,(byte)0xf8,(byte)0xf9,
        // // 'A', 'B', 'C', 'D', 'E',
        // (byte)0xc1,(byte)0xc2,(byte)0xc3,(byte)0xc4,(byte)0xc5,
        // // 'F', 'G', 'H', 'I', 'J',
        // (byte)0xc6,(byte)0xc7,(byte)0xc8,(byte)0xc9,(byte)0xd1,
        // // 'K', 'L', 'M', 'N', 'O',
        // (byte)0xd2,(byte)0xd3,(byte)0xd4,(byte)0xd5,(byte)0xd6,
        // // 'P'
        // (byte)0xd7
        // }
        // );
        // }

        @Override
        public byte dot() {
            return 0x4B;
        }

        @Override
        public byte space() {
            return 0x40;
        }

        public byte[] convertFromJavaString(String sourceString) throws SQLException {
            CharBuffer src = CharBuffer.wrap(sourceString);
            ByteBuffer dest = ByteBuffer.allocate(sourceString.length());
            startEncoding();
            encode(src, dest);
            return dest.array();
        }

        public void startEncoding() {
            // We don't have a CharsetEncoder instance to reset, or any other
            // internal state associated with earlier encode() calls. Do nothing.
        }

        public boolean encode(CharBuffer src, ByteBuffer dest) throws SQLException {
            // Encode as many characters as the destination buffer can hold.
            int charsToEncode = Math.min(src.remaining(), dest.remaining());
            for (int i = 0; i < charsToEncode; i++) {
                char c = src.get();
                if (c > 0xff) {
                    throw new SQLException("CANT_CONVERT_UNICODE_TO_EBCDIC");
                } else {
                    dest.put((byte) conversionArrayToEbcdic[c]);
                }
            }

            if (src.remaining() == 0) {
                // All characters have been encoded. We're done.
                return true;
            } else {
                // We still have more characters to encode, but no room in
                // destination buffer.
                return false;
            }
        }

        public String convertToJavaString(byte[] sourceBytes, int offset, int numToConvert) {
            int i = 0, j = 0;
            char[] theChars = new char[numToConvert];
            int num = 0;

            for (i = offset; i < (offset + numToConvert); i++) {
                num = (sourceBytes[i] < 0) ? (sourceBytes[i] + 256) : sourceBytes[i];
                theChars[j] = (char) conversionArrayToUCS2[num];
                j++;
            }
            return new String(theChars);
        }
    }
    
    private static class UTF8 implements CCSID {
        private final static String UTF8 = "UTF-8";
        private final static Charset UTF8_CHARSET = Charset.forName(UTF8);
        private final CharsetEncoder encoder = UTF8_CHARSET.newEncoder();

//        public Utf8CcsidManager() {
//            super((byte) ' ', // 0x40 is the ebcdic space character
//                    (byte) '.',
//                    new byte[]{//02132002jev begin
//                        //     '0',       '1',       '2',       '3',      '4',
//                        (byte) 0xf0, (byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4,
//                        //     '5',       '6',       '7',       '8',      '9',
//                        (byte) 0xf5, (byte) 0xf6, (byte) 0xf7, (byte) 0xf8, (byte) 0xf9,
//                        //     'A',       'B',       'C',       'D',      'E',
//                        (byte) 0xc1, (byte) 0xc2, (byte) 0xc3, (byte) 0xc4, (byte) 0xc5,
//                        //      'F'
//                        (byte) 0xc6},
//                    new byte[]{
//                        //     'G',       'H',       'I',       'J',      'K',
//                        (byte) 0xc7, (byte) 0xc8, (byte) 0xc9, (byte) 0xd1, (byte) 0xd2,
//                        //     'L',       'M',       'N',       '0',      'P',
//                        (byte) 0xd3, (byte) 0xd4, (byte) 0xd5, (byte) 0xd6, (byte) 0xd7,
//                        //     'A',       'B',       'C',       'D',      'E',
//                        (byte) 0xc1, (byte) 0xc2, (byte) 0xc3, (byte) 0xc4, (byte) 0xc5,
//                        //      'F'
//                        (byte) 0xc6}                     //02132002jev end
//            );
//        }
        
        @Override
        public byte space() {
            return ' ';
        }
        
        @Override
        public byte dot() {
            return '.';
        }
        
        public byte[] convertFromJavaString(String sourceString)
                throws SQLException {
            try {
                ByteBuffer buf = encoder.encode(CharBuffer.wrap(sourceString));

                if (buf.limit() == buf.capacity()) {
                    // The length of the encoded representation of the string
                    // matches the length of the returned buffer, so just return
                    // the backing array.
                    return buf.array();
                }

                // Otherwise, copy the interesting bytes into an array with the
                // correct length.
                byte[] bytes = new byte[buf.limit()];
                buf.get(bytes);
                return bytes;
            } catch (CharacterCodingException cce) {
                throw new SQLException("SQLState.CANT_CONVERT_UNICODE_TO_UTF8",
                        cce);
            }
        }

        /**
         * Offset and numToConvert are given in terms of bytes! Not characters!
         */
        public String convertToJavaString(byte[] sourceBytes, int offset, int numToConvert) {
            return new String(sourceBytes, offset, numToConvert, UTF8_CHARSET);
        }

        public void startEncoding() {
            encoder.reset();
        }

        public boolean encode(CharBuffer src, ByteBuffer dest)
                throws SQLException {
            CoderResult result = encoder.encode(src, dest, true);
            if (result == CoderResult.UNDERFLOW) {
                // We've exhausted the input buffer, which means we're done if
                // we just get everything flushed to the destination buffer.
                result = encoder.flush(dest);
            }

            if (result == CoderResult.UNDERFLOW) {
                // Input buffer is exhausted and everything is flushed to the
                // destination. We're done.
                return true;
            } else if (result == CoderResult.OVERFLOW) {
                // Need more room in the output buffer.
                return false;
            } else {
                // Something in the input buffer couldn't be encoded.
                throw new SQLException("SQLState.CANT_CONVERT_UNICODE_TO_UTF8");
            }
        }
    }

}
