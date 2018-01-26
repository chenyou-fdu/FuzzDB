package org.chenyou.fuzzdb.util.test;

import org.chenyou.fuzzdb.util.BloomFilter;
import org.chenyou.fuzzdb.util.Brick;
import org.chenyou.fuzzdb.util.FilterPolicy;
import org.chenyou.fuzzdb.util.Coding;
import com.google.common.primitives.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Created by ChenYou on 2017/11/25.
 */
public class BloomFilterTest {
    private final Logger logger = LoggerFactory.getLogger(BloomFilterTest.class);
    private FilterPolicy policy;
    private List<Brick> keys;
    private List<Byte> filter;
    @Before
    public void setup() {
        policy = BloomFilter.newBloomFilterPolicy(10);
        keys = new ArrayList<>();
        filter = new ArrayList<>();
    }

    private void reset() {
        keys.clear();
        filter.clear();
    }

    private void add(final Brick b) {
        keys.add(b);
    }

    private void build() {
        filter.clear();
        policy.createFilter(keys, keys.size(), filter);
        keys.clear();
    }

    private Boolean match(final Brick b) {
        if(!keys.isEmpty()) {
            build();
        }
        return policy.keyMatchWith(b, new Brick(filter));
    }

    private Brick key(Integer i) {
        Brick tmp = new Brick(Bytes.asList(Coding.EncodeFixed32(i)));
        return tmp;
    }

    private Double falsePositiveRate() {
        Integer result = 0;
        for(Integer i = 0; i < 10000; i++) {
            if(match(key(i + 1000000000))) {
                result++;
            }
        }
        return result / 10000.0;
    }
    static private Integer nextLen(Integer len) {
        if(len < 10) len += 1;
        else if(len < 100) len += 10;
        else if(len < 1000) len += 100;
        else len += 1000;
        return len;
    }

    private Integer filterSize() {
        return filter.size();
    }

    @Test
    public void EmptyTest() {
        Assert.assertFalse(match(new Brick("hello")));
        Assert.assertFalse(match(new Brick("world")));
    }

    @Test
    public void SmallTest() {
        add(new Brick("hello"));
        add(new Brick("world"));
        add(new Brick(Bytes.asList(Coding.EncodeFixed32(128))));
        Assert.assertTrue(match(new Brick("hello")));
        Assert.assertTrue(match(new Brick("world")));
        Assert.assertTrue(match(new Brick(Bytes.asList(Coding.EncodeFixed32(128)))));
        Assert.assertFalse(match(new Brick("xxx")));
        Assert.assertFalse(match(new Brick("!")));
    }

    @Test
    public void VaryingTest() {
        Integer badFilters = 0;
        Integer goodFilters = 0;
        for(Integer len = 0; len <= 10000; len = nextLen(len)) {
            reset();
            for(Integer i = 0; i < len; i++) {
                add(key(i));
            }
            build();
            Integer cmpRes1 = Integer.compareUnsigned(filter.size(), (len * 10 / 8) + 40);
            Assert.assertTrue(cmpRes1 == 0 || cmpRes1 == -1);
            for(Integer i = 0; i < len; i++) {
                Assert.assertTrue(match(key(i)));
            }

            Double rate = falsePositiveRate();
            logger.debug("False positive: {}% length: {} bytes: {} ", rate*100, len, filterSize());
            Assert.assertTrue( rate <= 0.02);
            if(rate > 0.0125) badFilters++;
            else goodFilters++;
        }
        logger.debug("Filters: {} good, {} bad", goodFilters, badFilters);
        Assert.assertTrue(badFilters <= goodFilters/5);
    }
}
