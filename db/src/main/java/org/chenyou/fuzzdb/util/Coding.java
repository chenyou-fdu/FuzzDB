package org.chenyou.fuzzdb.util;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Created by ChenYou on 2017/11/12.
 */
public class Coding {
    static public void EncodeFixed32(byte[] data, int value) {
        Preconditions.checkArgument(data.length >= 4);
        data[0] = (byte)(value & 0xff);
        data[1] = (byte)((value >> 8) & 0xff);
        data[2] = (byte)((value >> 16) & 0xff);
        data[3] = (byte)((value >> 24) & 0xff);
    }
    static public Integer DecodeFixed32(byte[] data, int sPos) {
        int byte0 = (((int)data[sPos]) & 0xFF );
        int byte1 = (((int)data[sPos + 1]) & 0xFF) << 8;
        int byte2 = (((int)data[sPos + 2]) & 0xFF) << 16;
        int byte3 = (((int)data[sPos + 3]) & 0xFF) << 24;
        return (byte0 | byte1 | byte2 | byte3);
    }

    static public void PutFixed32(List<Byte> dst, int value) {
        byte buf[] = new byte[4];
        EncodeFixed32(buf, value);
        for(byte b : buf) {
            dst.add(b);
        }
    }

    static public void EncodeFixed64(byte[] data, long value) {
        Preconditions.checkArgument(data.length >= 8);
        data[0] = (byte)(value & 0xff);
        data[1] = (byte)((value >> 8) & 0xff);
        data[2] = (byte)((value >> 16) & 0xff);
        data[3] = (byte)((value >> 24) & 0xff);
        data[4] = (byte)((value >> 32) & 0xff);
        data[5] = (byte)((value >> 40) & 0xff);
        data[6] = (byte)((value >> 48) & 0xff);
        data[7] = (byte)((value >> 56) & 0xff);
    }

    static public Long DecodeFixed64(byte[] data, int sPos) {
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

    static public void PutFixed64(List<Byte> dst, long value) {
        byte[] buf = new byte[8];
        EncodeFixed64(buf, value);
        for(byte b : buf) {
            dst.add(b);
        }
    }
}
