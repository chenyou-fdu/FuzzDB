package org.chenyou.fuzzdb.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

/**
 * Created by ChenYou on 2017/11/12.
 */
public class Coding {
    static public byte[] EncodeFixed32(Integer value) {
        return new byte[]{
                (byte)(value & 0xff),
                (byte)((value >> 8) & 0xff),
                (byte)((value >> 16) & 0xff),
                (byte)((value >> 24) & 0xff)
        };
    }
    static public Integer DecodeFixed32(byte data[], Integer sPos) {
        int byte0 = (((int)data[sPos]) & 0xFF );
        int byte1 = (((int)data[sPos + 1]) & 0xFF) << 8;
        int byte2 = (((int)data[sPos + 2]) & 0xFF) << 16;
        int byte3 = (((int)data[sPos + 3]) & 0xFF) << 24;
        return (byte0 | byte1 | byte2 | byte3);
    }

    static public void PutFixed32(List<Byte> dst, Integer value) {
        byte buf[] = EncodeFixed32(value);
        for(byte b : buf) {
            dst.add(b);
        }
    }

    static public byte[] EncodeFixed64(Long value) {
        return new byte[] {
                (byte)(value & 0xff),
                (byte)((value >> 8) & 0xff),
                (byte)((value >> 16) & 0xff),
                (byte)((value >> 24) & 0xff),
                (byte)((value >> 32) & 0xff),
                (byte)((value >> 40) & 0xff),
                (byte)((value >> 48) & 0xff),
                (byte)((value >> 56) & 0xff)
        };
    }

    static public Long DecodeFixed64(byte data[], Integer sPos) {
        long byte0 = (((long)data[sPos]) & 0xFF );
        long byte1 = (((long)data[sPos + 1]) & 0xFF) << 8;
        long byte2 = (((long)data[sPos + 2]) & 0xFF) << 16;
        long byte3 = (((long)data[sPos + 3]) & 0xFF) << 24;
        long byte4 = (((long)data[sPos + 4]) & 0xFF) << 32;
        long byte5 = (((long)data[sPos + 5]) & 0xFF) << 40;
        long byte6 = (((long)data[sPos + 6]) & 0xFF) << 48;
        long byte7 = (((long)data[sPos + 7]) & 0xFF) << 56;
        return (byte0 | byte1 | byte2 | byte3 | byte4 | byte5 | byte6 | byte7);
    }

    static public void PutFixed64(List<Byte> dst, Long value) {
        byte buf[] = EncodeFixed64(value);
        for(byte b : buf) {
            dst.add(b);
        }
    }
}
