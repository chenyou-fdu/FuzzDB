package org.chenyou.fuzzdb.util;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class HelperTest {
    private final Logger logger = LoggerFactory.getLogger(HelperTest.class);
    private class SimpleRunnableTask implements Runnable {
        private Integer taskNum;
        private BlockingDeque<Integer> res;
        private SimpleRunnableTask(Integer taskNum, final BlockingDeque<Integer> res) {
            this.taskNum = taskNum;
            this.res = res;
        }
        @Override
        public void run() {
            logger.info("task number : {}", this.taskNum);
            res.add(this.taskNum * 10);
        }
    }

    @Test
    public void simpleScheduleTest() {
        Helper helper = Helper.getHelper();
        BlockingDeque<Integer> res = new LinkedBlockingDeque<>();
        for(Integer i = 1; i <= 10; i++) {
            helper.Schedule(new SimpleRunnableTask(i, res));
        }
        try {
            for (Integer i = 0; i < 10; i++) {
                Integer eachRes = res.takeFirst();
                Assert.assertTrue(eachRes >= 10);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

}
