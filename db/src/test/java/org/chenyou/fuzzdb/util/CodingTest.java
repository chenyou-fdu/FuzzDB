package org.chenyou.fuzzdb.util;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedLong;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ChenYou on 2017/11/12.
 */
public class CodingTest {
    private final Logger logger = LoggerFactory.getLogger(CodingTest.class);
    @Test
    public void TestFixed32() {
        List<Byte> p = new ArrayList<>();
        for(Integer i = 0; i < 100000; i++) {
            Coding.PutFixed32(p, i);
        }
        byte data[] = Bytes.toArray(p);
        Integer index = 0;
        for(Integer i = 0; i < 100000; i++) {
            Integer actual = Coding.DecodeFixed32(data, index);
            Assert.assertEquals(actual, i);
            index += 4;
        }
    }

    @Test
    public void TestFixed64() {
        List<Byte> p = new ArrayList<>();
        for (Integer power = 0; power <= 63; power++) {
            Long v = (1L << power);
            logger.debug("power {} : save v - 1 {} v + 0 {} v + 1 {}", power, v - 1, v, v + 1);
            Coding.PutFixed64(p, v - 1);
            Coding.PutFixed64(p, v);
            Coding.PutFixed64(p, v + 1);
        }
        byte[] pBytes = Bytes.toArray(p);
        Integer pos = 0;
        for (Integer power = 0; power <= 63; power++) {
            Long v = (1L << power);
            Long actual = Coding.DecodeFixed64(pBytes, pos);

            logger.debug("power {} : compare v - 1 {} v.s. actual {}", power,
                    UnsignedLong.fromLongBits((v - 1)), UnsignedLong.fromLongBits(actual));
            Assert.assertEquals(UnsignedLong.fromLongBits((v - 1)), UnsignedLong.fromLongBits(actual));
            pos += 8;
            actual = Coding.DecodeFixed64(pBytes, pos);
            logger.debug("power {} : compare v + 0 {} v.s. actual {}", power,
                    UnsignedLong.fromLongBits((v)), UnsignedLong.fromLongBits(actual));
            Assert.assertEquals(UnsignedLong.fromLongBits(v), UnsignedLong.fromLongBits(actual));
            pos += 8;
            actual = Coding.DecodeFixed64(pBytes, pos);
            logger.debug("power {} : compare v + 1 {} v.s. actual {}", power,
                    UnsignedLong.fromLongBits(v).plus(UnsignedLong.fromLongBits(1L)),  UnsignedLong.fromLongBits(actual));
            Assert.assertEquals(UnsignedLong.fromLongBits(v).plus(UnsignedLong.fromLongBits(1L)), UnsignedLong.fromLongBits(actual));
            pos += 8;
        }
    }

}
