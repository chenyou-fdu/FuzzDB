package org.chenyou.fuzzdb.util;

import com.google.common.base.Preconditions;
import sun.misc.Unsafe;

import java.nio.ByteOrder;
import java.util.zip.CRC32C;

// almost based on JDK9/JDK10 CRC32C implements
//   but with different interface
public class FuzzCRC32C {
    public final static CRC32C crc32C = new CRC32C();

    private static final int kMaskDelta = 0xa282ead8;
    // return a masked representation of crc.
    //  Motivation: it is problematic to compute the CRC of a string that
    //  contains embedded CRCs.  Therefore we recommend that CRCs stored
    //  somewhere (e.g., in files) should be masked before being stored.
    public static int mask(int crc) {
        // Rotate right by 15 bits and add a constant.
        return ((crc >>> 15) | (crc << 17)) + kMaskDelta;
    }

    // Return the crc whose masked representation is maskedCrc.
    public static int unmask(int maskedCrc) {
        int rot = maskedCrc - kMaskDelta;
        return ((rot >>> 17) | (rot << 15));
    }

    public static long value(int data) {
        crc32C.reset();
        crc32C.update(data);
        return crc32C.getValue();
    }

    public static long value(int data, byte[] data2, int n) {
        crc32C.reset();
        crc32C.update(data);
        crc32C.update(data2, 0, n);
        return crc32C.getValue();
    }

    public static long value(int data, byte[] data2, int offset, int n) {
        crc32C.reset();
        crc32C.update(data);
        crc32C.update(data2, offset, n);
        return crc32C.getValue();
    }
    public static long value(byte[] data, byte[] data2) {
        crc32C.reset();
        crc32C.update(data);
        crc32C.update(data2);
        return crc32C.getValue();
    }
    public static long value(byte[] data) {
        crc32C.reset();
        crc32C.update(data);
        return crc32C.getValue();
    }

    public static long value(final byte[] data, int n) {
        Preconditions.checkNotNull(data);
        crc32C.reset();
        crc32C.update(data, 0, n);
        return crc32C.getValue();
    }

    public static long value(final byte[] data, int offset, int n) {
        Preconditions.checkNotNull(data);
        Preconditions.checkArgument(n <= data.length - offset);
        crc32C.reset();
        crc32C.update(data, offset, n);
        return crc32C.getValue();
    }


    public static class FuzzCRC32COld {

        private int crc = 0xFFFFFFFF;
        private static final int CRC32C_POLY = 0x1EDC6F41;
        private static final int REVERSED_CRC32C_POLY = Integer.reverse(CRC32C_POLY);

        private static final Unsafe UNSAFE = UnsafeHelper.getUnsafe();

        // Lookup tables
        // Lookup table for single byte calculations
        private static final int[] byteTable;
        // Lookup tables for bulk operations in 'slicing-by-8' algorithm
        private static final int[][] byteTables = new int[8][256];
        private static final int[] byteTable0 = byteTables[0];
        private static final int[] byteTable1 = byteTables[1];
        private static final int[] byteTable2 = byteTables[2];
        private static final int[] byteTable3 = byteTables[3];
        private static final int[] byteTable4 = byteTables[4];
        private static final int[] byteTable5 = byteTables[5];
        private static final int[] byteTable6 = byteTables[6];
        private static final int[] byteTable7 = byteTables[7];

        static {
            // Generate lookup tables
            // High-order polynomial term stored in LSB of r.
            for (int index = 0; index < byteTables[0].length; index++) {
                int r = index;
                for (int i = 0; i < Byte.SIZE; i++) {
                    if ((r & 1) != 0) {
                        r = (r >>> 1) ^ REVERSED_CRC32C_POLY;
                    } else {
                        r >>>= 1;
                    }
                }
                byteTables[0][index] = r;
            }

            for (int index = 0; index < byteTables[0].length; index++) {
                int r = byteTables[0][index];

                for (int k = 1; k < byteTables.length; k++) {
                    r = byteTables[0][r & 0xFF] ^ (r >>> 8);
                    byteTables[k][index] = r;
                }
            }

            if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
                byteTable = byteTables[0];
            } else { // ByteOrder.BIG_ENDIAN
                byteTable = new int[byteTable0.length];
                System.arraycopy(byteTable0, 0, byteTable, 0, byteTable0.length);
                for (int[] table : byteTables) {
                    for (int index = 0; index < table.length; index++) {
                        table[index] = Integer.reverseBytes(table[index]);
                    }
                }
            }
        }

        private static final int kMaskDelta = 0xa282ead8;

        // return a masked representation of crc.
        //  Motivation: it is problematic to compute the CRC of a string that
        //  contains embedded CRCs.  Therefore we recommend that CRCs stored
        //  somewhere (e.g., in files) should be masked before being stored.
        public static int mask(int crc) {
            // Rotate right by 15 bits and add a constant.
            return ((crc >>> 15) | (crc << 17)) + kMaskDelta;
        }

        // Return the crc whose masked representation is maskedCrc.
        public static int unmask(int maskedCrc) {
            int rot = maskedCrc - kMaskDelta;
            return ((rot >>> 17) | (rot << 15));
        }

        public static int value(final byte[] data, int n) {
            Preconditions.checkNotNull(data);
            int preRes = extend(0xFFFFFFFF, data, 0, n);
            return (int) ((~preRes) & 0xFFFFFFFFL);
        }

        public static int value(final byte[] data, int offset, int n) {
            Preconditions.checkNotNull(data);
            Preconditions.checkArgument(n <= data.length - offset);
            int preRes = extend(0xFFFFFFFF, data, offset, offset + n);
            return (int) ((~preRes) & 0xFFFFFFFFL);
        }

        public static int extend(int crcValue, final byte[] data, int n) {
            int preCrc = (int) ((~crcValue) & 0xFFFFFFFFL);
            int preRes = extend(preCrc, data, 0, n);
            return (int) ((~preRes) & 0xFFFFFFFFL);
        }

        private static int extend(int crc, byte[] b, int off, int end) {

            // Do only byte reads for arrays so short they can't be aligned
            // or if bytes are stored with a larger witdh than one byte.
            if (end - off >= 8 && Unsafe.ARRAY_BYTE_INDEX_SCALE == 1) {

                // align on 8 bytes
                int alignLength
                        = (8 - ((Unsafe.ARRAY_BYTE_BASE_OFFSET + off) & 0x7)) & 0x7;
                for (int alignEnd = off + alignLength; off < alignEnd; off++) {
                    crc = (crc >>> 8) ^ byteTable[(crc ^ b[off]) & 0xFF];
                }

                if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
                    crc = Integer.reverseBytes(crc);
                }

                // slicing-by-8
                for (; off < (end - Long.BYTES); off += Long.BYTES) {
                    int firstHalf;
                    int secondHalf;
                    if (Unsafe.ADDRESS_SIZE == 4) {
                        // On 32 bit platforms read two ints instead of a single 64bit long
                        firstHalf = UNSAFE.getInt(b, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + off);
                        secondHalf = UNSAFE.getInt(b, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + off
                                + Integer.BYTES);
                    } else {
                        long value = UNSAFE.getLong(b, (long) Unsafe.ARRAY_BYTE_BASE_OFFSET + off);
                        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
                            firstHalf = (int) value;
                            secondHalf = (int) (value >>> 32);
                        } else { // ByteOrder.BIG_ENDIAN
                            firstHalf = (int) (value >>> 32);
                            secondHalf = (int) value;
                        }
                    }
                    crc ^= firstHalf;
                    if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
                        crc = byteTable7[crc & 0xFF]
                                ^ byteTable6[(crc >>> 8) & 0xFF]
                                ^ byteTable5[(crc >>> 16) & 0xFF]
                                ^ byteTable4[crc >>> 24]
                                ^ byteTable3[secondHalf & 0xFF]
                                ^ byteTable2[(secondHalf >>> 8) & 0xFF]
                                ^ byteTable1[(secondHalf >>> 16) & 0xFF]
                                ^ byteTable0[secondHalf >>> 24];
                    } else { // ByteOrder.BIG_ENDIAN
                        crc = byteTable0[secondHalf & 0xFF]
                                ^ byteTable1[(secondHalf >>> 8) & 0xFF]
                                ^ byteTable2[(secondHalf >>> 16) & 0xFF]
                                ^ byteTable3[secondHalf >>> 24]
                                ^ byteTable4[crc & 0xFF]
                                ^ byteTable5[(crc >>> 8) & 0xFF]
                                ^ byteTable6[(crc >>> 16) & 0xFF]
                                ^ byteTable7[crc >>> 24];
                    }
                }

                if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
                    crc = Integer.reverseBytes(crc);
                }
            }

            // Tail
            for (; off < end; off++) {
                crc = (crc >>> 8) ^ byteTable[(crc ^ b[off]) & 0xFF];
            }

            return crc;
        }


    }


}
