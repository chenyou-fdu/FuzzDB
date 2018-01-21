package org.chenyou.fuzzdb.db.test;

import org.chenyou.fuzzdb.db.SkipList;
import org.chenyou.fuzzdb.db.SkipList.SkipListIterator;
import org.chenyou.fuzzdb.util.Random;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;

import java.util.*;

/**
 * Created by ChenYou on 2018/1/2.
 */
public class SkipListTest {
    private final Logger logger = LoggerFactory.getLogger(SkipListTest.class);
    private Comparator<Long> cmp = new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            if(o1 < o2) return -1;
            else if(o1 > o2) return 1;
            else return 0;
        }
    };

    @Test
    public void emptyTest() {
        SkipList<Long> skipList = new SkipList<>(cmp);
        Assert.assertFalse(skipList.contains(10L));
        SkipListIterator<Long> iter = skipList.iterator();
        Assert.assertFalse(iter.valid());
        iter.seekToFirst();
        Assert.assertFalse(iter.valid());
        iter.seek(1000L);
        Assert.assertFalse(iter.valid());
        iter.seekToLast();
        Assert.assertFalse(iter.valid());
    }

    @Test
    public void simpleInsertAndSearchTest() {
        SkipList<Long> skipList = new SkipList<>(cmp);
        final Long N = 10L;
        for(Long i = 1L; i <= N; i++)
            skipList.insert(i);
        for(Long i = 1L; i <= N; i++)
            Assert.assertTrue(skipList.contains(i));

    }

    @Test
    public void insertAndSearchTest() {
        final Integer N = 2000;
        final Integer R = 5000;
        Random rnd = new Random(1000);
        TreeSet<Long> keys = new TreeSet<>();
        SkipList<Long> skipList = new SkipList<>(cmp);

        for(Integer i = 0; i < N; i++) {
            Long key = (long) rnd.next() % R;
            if(keys.add(key))
                skipList.insert(key);
        }

        for (Long i = 0L; i < R; i++) {
            if(skipList.contains(i)) {
                Assert.assertTrue(keys.contains(i));
            } else {
                Assert.assertFalse(keys.contains(i));
            }
        }

        // simple iterator tests
        {
            SkipListIterator<Long> iter = skipList.iterator();

            Assert.assertFalse(iter.valid());
            iter.seek(0L);
            Assert.assertTrue(iter.valid());
            Assert.assertEquals(keys.first(), iter.key());

            iter.seekToFirst();
            Assert.assertTrue(iter.valid());
            Assert.assertEquals(keys.first(), iter.key());

            iter.seekToLast();
            Assert.assertTrue(iter.valid());
            Assert.assertEquals(keys.last(), iter.key());
        }

        // forward iterator test
        for(Long i = 0L; i < R; i++) {
            SkipListIterator<Long> iter = skipList.iterator();
            iter.seek(i);

            Iterator<Long> setIter = keys.tailSet(i).iterator();
            for(Integer j = 0; j < 3; j++) {
                if(!setIter.hasNext()) {
                    Assert.assertFalse(iter.valid());
                    break;
                } else {
                    Assert.assertTrue(iter.valid());
                    Long setVal = setIter.next();
                    Long skipListVal = iter.key();
                    logger.debug("set val is {} vs skiplist val is {}", setVal, skipListVal);
                    Assert.assertEquals(setVal, skipListVal);
                    iter.iterNext();
                }
            }
        }
        // backward iterator test
        {
            SkipListIterator<Long> iter = skipList.iterator();
            iter.seekToLast();
            Iterator<Long> setIter = keys.descendingIterator();
            while(setIter.hasNext()) {
                Assert.assertTrue(iter.valid());
                Long setVal = setIter.next();
                Long skipListVal = iter.key();
                iter.prev();
                logger.debug("set val is {} vs skiplist val is {}", setVal, skipListVal);
                Assert.assertEquals(setVal, skipListVal);
            }
        }
    }


}
