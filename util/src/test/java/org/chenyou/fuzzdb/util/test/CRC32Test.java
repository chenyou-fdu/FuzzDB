package org.chenyou.fuzzdb.util.test;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.zip.CRC32;
public class CRC32Test {
    private CRC32 crc32;

    @Before
    public void setup() {
        crc32 = new CRC32();
    }

    @Test
    public void CRC32TestZero() {
        byte[] buf = new byte[32];
        Arrays.fill(buf, (byte) 0);
        for(Integer i = 0; i < 32; i++)
            buf[i] = 0;
        crc32.update(buf);

        Long tis = crc32.getValue();
        Assert.assertEquals(0x8a9136aaL, crc32.getValue());

        return;
    }
}
