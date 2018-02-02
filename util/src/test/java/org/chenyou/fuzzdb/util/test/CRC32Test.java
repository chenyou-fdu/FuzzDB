package org.chenyou.fuzzdb.util.test;
import org.junit.Before;
import org.junit.Test;
import org.chenyou.fuzzdb.util.FuzzCRC32C;
public class CRC32Test {

    @Before
    public void setup() {
    }

    @Test
    public void CRC32TestZero() {
        FuzzCRC32C fuzzCRC32 = new FuzzCRC32C();
        byte[] tmp = new byte[32];
        fuzzCRC32.update(tmp, 0, 32);
        long k = fuzzCRC32.getValue();
        return;
    }
}
