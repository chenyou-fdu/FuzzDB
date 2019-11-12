package org.chenyou.fuzzdb.util;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ChenYou on 2017/11/12.
 */
public class Coding {
    static public void EncodeFixed32(byte[] data, int value) {
        Preconditions.checkArgument(data.length >= 4);
        data[0] = (byte) (value & 0xff);
        data[1] = (byte) ((value >> 8) & 0xff);
        data[2] = (byte) ((value >> 16) & 0xff);
        data[3] = (byte) ((value >> 24) & 0xff);
    }

    static public Integer DecodeFixed32(byte[] data, int sPos) {
        int byte0 = (((int) data[sPos]) & 0xFF);
        int byte1 = (((int) data[sPos + 1]) & 0xFF) << 8;
        int byte2 = (((int) data[sPos + 2]) & 0xFF) << 16;
        int byte3 = (((int) data[sPos + 3]) & 0xFF) << 24;
        return (byte0 | byte1 | byte2 | byte3);
    }

    static public void PutFixed32(List<Byte> dst, int value) {
        byte buf[] = new byte[4];
        EncodeFixed32(buf, value);
        for (byte b : buf) {
            dst.add(b);
        }
    }

    static public void EncodeFixed64(byte[] data, long value) {
        Preconditions.checkArgument(data.length >= 8);
        data[0] = (byte) (value & 0xff);
        data[1] = (byte) ((value >> 8) & 0xff);
        data[2] = (byte) ((value >> 16) & 0xff);
        data[3] = (byte) ((value >> 24) & 0xff);
        data[4] = (byte) ((value >> 32) & 0xff);
        data[5] = (byte) ((value >> 40) & 0xff);
        data[6] = (byte) ((value >> 48) & 0xff);
        data[7] = (byte) ((value >> 56) & 0xff);
    }

    static public void EncodeVarint32(List<Byte> data, int v) {
        final int B = 128;
        if (v < (1 << 7)) {
            data.add((byte)v);
        } else if (v < (1 << 14)) {
            data.add((byte)(v | B));
            data.add((byte)(v >>> 7));
        } else if (v < (1 << 21)) {
            data.add((byte)(v | B));
            data.add((byte)((v >>> 7) | B));
            data.add((byte)(v >>> 14));
        } else if (v < (1 << 28)) {
            data.add((byte)(v | B));
            data.add((byte)((v >>> 7) | B));
            data.add((byte)((v >>> 14) | B));
            data.add((byte)(v >>> 21));
        } else {
            data.add((byte)(v | B));
            data.add((byte)((v >>> 7) | B));
            data.add((byte)((v >>> 14) | B));
            data.add((byte)((v >>> 21) | B));
            data.add((byte)(v >>> 28));
        }
    }

    static public void EncodeVarint64(List<Byte> data, long v) {
        final int B = 128;
        int cnt = 0;
        while (v >= B) {
            //data[cnt] = (byte) (v | B);
            data.add((byte) (v | B));
            v >>= 7;
            cnt++;
        }
        //data[cnt] = (byte) v;
        data.add((byte)v);
    }

    static public Long DecodeFixed64(byte[] data, int sPos) {
        long byte0 = (((long) data[sPos]) & 0xFF);
        long byte1 = (((long) data[sPos + 1]) & 0xFF) << 8;
        long byte2 = (((long) data[sPos + 2]) & 0xFF) << 16;
        long byte3 = (((long) data[sPos + 3]) & 0xFF) << 24;
        long byte4 = (((long) data[sPos + 4]) & 0xFF) << 32;
        long byte5 = (((long) data[sPos + 5]) & 0xFF) << 40;
        long byte6 = (((long) data[sPos + 6]) & 0xFF) << 48;
        long byte7 = (((long) data[sPos + 7]) & 0xFF) << 56;
        return (byte0 | byte1 | byte2 | byte3 | byte4 | byte5 | byte6 | byte7);
    }

    static public void PutFixed64(List<Byte> dst, long value) {
        byte[] buf = new byte[8];
        EncodeFixed64(buf, value);
        for (byte b : buf) {
            dst.add(b);
        }
    }

    static public void PutVarint64(List<Byte> dst, long v) {
        //byte[] buf = new byte[10];
        List<Byte> buf = new ArrayList<>();
        EncodeVarint64(buf, v);
        for(Byte b : buf) {
            dst.add(b);
        }
    }
    static public class CodingEntry {
        public int lastPos;
        public Long realValue;
        public CodingEntry(Long rV, int lP) {
            lastPos = lP;
            realValue = rV;
        }
    }

    static public CodingEntry GetVarint64(List<Byte> src, int pPos, int length) {
        long res = 0;
        int cnt = pPos;
        for(int shift = 0; shift <= 63 && pPos < length; shift += 7) {
            long b = (long)src.get(cnt);
            cnt++;
            if((b & 128) != 0) {
                // More bytes are present
                res |= ((b & 127) << shift);
            } else {
                res |= (b << shift);
                return new CodingEntry(res, cnt);
            }
        }
        return new CodingEntry(null, cnt);
    }

    // Returns the length of the varint32 or varint64 encoding of "v"
    static public int VarintLength(long v) {
        int len = 1;
        while (v >= 128) {
            v >>= 7;
            len++;
        }
        return len;
    }
}
