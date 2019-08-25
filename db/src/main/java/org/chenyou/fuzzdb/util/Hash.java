package org.chenyou.fuzzdb.util;

/**
 * Created by ChenYou on 2017/11/12.
 */
public class Hash {
    static public Integer hash(byte data[], Integer n, Integer seed) {
        Integer m = 0xc6a4a793;
        Integer r = 24;
        Integer h = (seed ^ (n * m));

        Integer i = 0;
        for(; i + 4 <= data.length; i += 4) {
            Integer w = Coding.DecodeFixed32(data, i);
            h += w;
            h *= m;
            h ^= (h >>> 16);
        }
        Integer offset = data.length - i;
        if (offset == 3) {
            h += (((int)data[i + 2]) & 0xFF) << 16;
            offset--;
        }
        if (offset == 2) {
            h += (((int)data[i + 1]) & 0xFF) << 8;
            offset--;
        }
        if (offset == 1) {
            h += ((int)data[i]) & 0xFF;
            h *= m;
            h ^= (h >>> r);
        }
        return h;

    }
}
