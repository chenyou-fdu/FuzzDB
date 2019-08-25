package org.chenyou.fuzzdb.util;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class FuzzCRC32CTest {

    @Test
    public void FuzzCRC32CTest1() {
        Assert.assertNotEquals(FuzzCRC32C.value("liam".getBytes(), 4),
                FuzzCRC32C.value("bonehead".getBytes(), 4));
    }

    @Test
    public void FuzzCRC32CTest2() {
        Assert.assertNotEquals(FuzzCRC32C.value("liam".getBytes(), 4),
                FuzzCRC32C.value("liam".getBytes(), 2));
    }

    @Test
    public void FuzzCRC32CTest3() {
        Assert.assertEquals(FuzzCRC32C.value("liam".getBytes(), 4),
                FuzzCRC32C.value("liam".getBytes(), 4));
    }

    @Test
    public void FuzzCRC32CTest4() {
        Assert.assertEquals(FuzzCRC32C.value("liam".getBytes(), 4),
                FuzzCRC32C.value("liamGallagher".getBytes(), 4));
    }


    @Test
    public void FuzzCRC32CTest5() {
        //long crcValue = FuzzCRC32C.value("liam".getBytes(), 4);
        //crcValue = FuzzCRC32C.extend(crcValue, "Gallagher".getBytes(), 9);
        long crcValue = FuzzCRC32C.value("liam".getBytes(), "Gallagher".getBytes());
        Assert.assertEquals(FuzzCRC32C.value("liamGallagher".getBytes()),
                crcValue);

    }

    @Test
    public void FuzzCRC32CTest6() {
        byte[] data = new byte[32];
        Assert.assertEquals(0x8a9136aaL, FuzzCRC32C.value(data, 32));
    }

    @Test
    public void FuzzCRC32CTest7() {
        byte[] data = new byte[32];
        Arrays.fill(data, (byte) 0xff);
        Assert.assertEquals( 0x62a8ab43, FuzzCRC32C.value(data, 32));
    }

    @Test
    public void FuzzCRC32CTest8() {
        byte[] data = new byte[32];
        for (int i = 0; i < 32; i++) {
            data[i] = (byte) i;
        }
        Assert.assertEquals(0x46dd794e, FuzzCRC32C.value(data, 32));
    }

    @Test
    public void FuzzCRC32CTest9() {
        byte[] data = new byte[32];
        for (int i = 0; i < 32; i++) {
            data[i] = (byte)(31 - i);
        }
        Assert.assertEquals(0x113fdb5c, FuzzCRC32C.value(data, 32));
    }

    @Test
    public void FuzzCRC32CTest10() {
        byte[] buf = {
                0x01, -64, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x14, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x04, 0x00,
                0x00, 0x00, 0x00, 0x14,
                0x00, 0x00, 0x00, 0x18,
                0x28, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
                0x02, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00,
        };
        Assert.assertEquals(0xd9963a56L, FuzzCRC32C.value(buf, buf.length));
    }

    @Test
    public void FuzzCRC32Test11() {
        Assert.assertEquals(FuzzCRC32C.value("liam".getBytes(), 2,2),
                FuzzCRC32C.value("am".getBytes(), 2));
    }

    @Test
    public void FuzzCRC32MaskTest() {
        Long crc32cValueLong = FuzzCRC32C.value("liam".getBytes(), 4);
        int crc32cValue = crc32cValueLong.intValue();

        Assert.assertNotEquals(crc32cValue, FuzzCRC32C.mask(crc32cValue));
        Assert.assertNotEquals(crc32cValue, FuzzCRC32C.mask(FuzzCRC32C.mask(crc32cValue)));
        Assert.assertEquals(crc32cValue, FuzzCRC32C.unmask(FuzzCRC32C.mask(crc32cValue)));
        Assert.assertEquals(crc32cValue, FuzzCRC32C.unmask(FuzzCRC32C.mask(crc32cValue)));
        Assert.assertEquals(crc32cValue,
                FuzzCRC32C.unmask(FuzzCRC32C.unmask(FuzzCRC32C.mask(FuzzCRC32C.mask(crc32cValue)))));
    }
}
