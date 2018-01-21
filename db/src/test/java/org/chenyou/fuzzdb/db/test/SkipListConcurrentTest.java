package org.chenyou.fuzzdb.db.test;

import com.google.common.base.Preconditions;
import org.chenyou.fuzzdb.db.SkipList;
import org.chenyou.fuzzdb.db.SkipList.SkipListIterator;
import org.chenyou.fuzzdb.util.Coding;
import org.chenyou.fuzzdb.util.Helper;
import org.chenyou.fuzzdb.util.Random;
import org.chenyou.fuzzdb.util.Hash;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by ChenYou on 2018/1/7.
 */
public class SkipListConcurrentTest {
    private final static Logger logger = LoggerFactory.getLogger(SkipListConcurrentTest.class);

    private Comparator<Long> cmp = new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            if(o1 < o2) return -1;
            else if(o1 > o2) return 1;
            else return 0;
        }
    };
    private enum ReaderState {
        STARTING, RUNNING, DONE
    }

    private class ConcurrentTest {


        /**
         *  We generate multi-part keys:
         *  <key,gen,hash>
         *  where:
         *  key is in range [0..K-1]
         *  gen is a generation number for key
         *  hash is hash(key,gen)
        */
        private final Integer K = 4;
        private State current;
        private SkipList<Long> skipList;

        private Long key(Long key) {
            return key >>> 40;
        }
        private Long gen(Long key) {
            return (key >>> 8) & 0xffffffffL;
        }
        private Long hash(Long key) {
            return key & 0xff;
        }
        private Long hashNumbers(Long k, Long g) {
            byte[] data = new byte[16];
            byte[] num1 = Coding.EncodeFixed64(k);
            byte[] num2 = Coding.EncodeFixed64(g);
            for(Integer i = 0; i < 8; i++) {
                data[i] = num1[i];
                data[i + 8] = num2[i];
            }
            return (long)Hash.hash(data, 16, 0);
        }
        // key's bits representation [63 ... 40] gen [39 ... 8] hash [7 ... 0]
        private Long makeKey(Long k, Long g) {
            Preconditions.checkArgument(k <= K && g <= 0xffffffffL);
            return ((k << 40) | (g << 8) | (hashNumbers(k, g) & 0xff));
        }
        private Boolean isValidKey(Long k) {
            return hash(k) == (hashNumbers(key(k), gen(k)) & 0xff);
        }
        private Long randomTarget(Random rnd) {
            switch (rnd.next() % 10) {
                case 0:
                    // seek to begining
                    return makeKey(0L, 0L);
                case 1:
                    // seek to end
                    return makeKey((long)K, 0L);
                default:
                    // seek to middle
                    return makeKey((long)(rnd.next() % K), 0L);
            }
        }
        private class State {
            private List<AtomicReference<Long>> generation;
            private void set(Integer k, Long v) {
                generation.get(k).set(v);
            }
            private Long get(Integer k) {
                return generation.get(k).get();
            }
            private State() {
                generation = new ArrayList<>(K);
                for(Integer i = 0; i < K; i++) {
                    generation.add(new AtomicReference<>(0L));
                }
            }
        }

        // REQUIRES: External synchronization
        private void writeStep(Random rnd) {
            final Integer k = rnd.next() % K;
            final Long g = this.current.get(k) + 1;
            final Long insertedKey = makeKey((long)k, g);
            logger.debug("insert key: {} gen: {} hash: {}", key(insertedKey), gen(insertedKey), hash(insertedKey));
            this.skipList.insert(insertedKey);
            this.current.set(k, g);
        }

        private void readStep(Random rnd) {
            // snapshot last inserted generation number for each key
            State initialState = new State();
            for(Integer k = 0; k < K; k++) {
                initialState.set(k, current.get(k));
            }

            // random position key to start
            //   random position key always has 0 generation
            Long pos = randomTarget(rnd);
            SkipListIterator<Long> iter = this.skipList.iterator();
            iter.seek(pos);
            while(true) {
                Long cur;
                if(!iter.valid()) {
                    // last key with 0 gen
                    cur = makeKey((long)K, 0L);
                } else {
                    cur = iter.key();
                    Assert.assertTrue(isValidKey(cur));
                }
                logger.debug("random pos key: {} gen: {} hash: {} v.s. " +
                        "{} cur key: {} gen: {} hash: {}", key(pos), gen(pos), hash(pos),
                        iter.valid() ? "seeked" : "last", key(cur), gen(cur), hash(cur));
                Assert.assertTrue(pos <= cur);

                // Verify that everything in [pos,current)
                //   was not present in initialState.
                while(pos < cur) {
                    logger.debug("inital state for pos key: {} gen: {}", key(pos), initialState.get(key(pos).intValue()));

                    Assert.assertTrue(key(pos) < K);
                    Assert.assertTrue((gen(pos) == 0) ||
                        gen(pos) > initialState.get(key(pos).intValue())
                    );
                    if(key(pos) < key(cur)) {
                        pos = makeKey(key(pos) + 1, 0L);
                        logger.debug("next key of pos key: {} gen: {} hash: {}", key(pos), gen(pos), hash(pos));
                    } else {
                        pos = makeKey(key(pos), gen(pos) + 1);
                        logger.debug("next gen of pos key: {} gen: {} hash: {}", key(pos), gen(pos), hash(pos));
                    }
                }

                if(!iter.valid()) {
                    logger.debug("iterator has searched to the end");
                    break;
                }

                if((rnd.next() % 2) != 0) {
                    iter.iterNext();
                    pos = makeKey(key(pos), gen(pos)+1);
                } else {
                    Long newTarget = randomTarget(rnd);
                    if(newTarget > pos) {
                        pos = newTarget;
                        iter.seek(newTarget);
                    }
                }
                logger.debug("new pos key: {} gen: {} hash: {}", key(pos), gen(pos), hash(pos));
            }
        }

        private ConcurrentTest() {
            this.skipList = new SkipList<>(cmp);
            this.current = new State();
        }
    }


    private class TestState {
        private Integer seed;
        private ConcurrentTest ct;
        private ReaderState state;
        private ReentrantLock mu;
        private Condition cond;
        private AtomicReference<Object> quitFlag;
        private TestState(Integer s) {
            this.seed = s;
            this.state = ReaderState.STARTING;
            this.mu = new ReentrantLock();
            this.cond = this.mu.newCondition();
            this.quitFlag = new AtomicReference<>(null);
            this.ct = new ConcurrentTest();
        }

        private void waitState(ReaderState s) {
            this.mu.lock();
            try {
                while (this.state != s) {
                    this.cond.await();
                }
            } catch (InterruptedException e) {
                logger.error("{}", e);
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                this.mu.unlock();
            }
        }
        private void changeState(ReaderState s) {
            this.mu.lock();
            this.state = s;
            this.cond.signal();
            logger.debug("TestState change to {}", this.state);
            this.mu.unlock();
        }

    }

    private static void concurrentReader(Object arg) {
        TestState state = (TestState) arg;
        Random rnd = new Random(state.seed);
        state.changeState(ReaderState.RUNNING);
        while(state.quitFlag.get() == null) {
            state.ct.readStep(rnd);
        }
        state.changeState(ReaderState.DONE);
    }

    private class concurrentReaderTask implements Runnable {
        private Object state;
        private concurrentReaderTask(final TestState state) {
            this.state = state;
        }
        @Override
        public void run() {
            concurrentReader(this.state);
        }
    }

    private void runConcurrent(Integer run) {
        final Integer seed = 301 + (run * 100);
        Random rnd = new Random(seed);
        final Integer N = 1000;
        final Integer kSize = 1000;
        Helper helper = Helper.getHelper();
        for(Integer i = 0; i < N; i++) {
            if((i % 100) == 0) {
                logger.info("Run {} of {}", i, N);
            }
            TestState state = new TestState(seed + 1);
            helper.Schedule(new concurrentReaderTask(state));
            state.waitState(ReaderState.RUNNING);
            for(Integer j = 0; j < kSize; j++) {
                state.ct.writeStep(rnd);
            }
            state.quitFlag.set(new Object());
            state.waitState(ReaderState.DONE);
        }

    }

    @Test
    public void simpleTestOnThread() {
        ConcurrentTest ct = new ConcurrentTest();
        Random rnd = new Random(301);
        for(Integer i = 0; i < 10000; i++) {
            ct.readStep(rnd);
            ct.writeStep(rnd);
        }
    }

    @Test
    public void concurrentTestOne() {
        runConcurrent(1);
    }
    @Test
    public void concurrentTestTwo() {
        runConcurrent(2);
    }
    @Test
    public void concurrentTestThree() {
        runConcurrent(3);
    }
    @Test
    public void concurrentTestFour() {
        runConcurrent(4);
    }
    @Test
    public void concurrentTestFive() {
        runConcurrent(5);
    }

}
