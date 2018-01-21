package org.chenyou.fuzzdb.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class Helper {
    private final Logger logger = LoggerFactory.getLogger(Helper.class);
    private ReentrantLock mu;
    private Boolean startedBGThread;
    private Thread bgThread;
    private Runnable backgroudTask;
    private BlockingDeque<Runnable> queue;
    private static class SingletonHelperHolder {
        private static final Helper instance = new Helper();
    }
    public static Helper getHelper() {
        return SingletonHelperHolder.instance;
    }
    private Helper() {
        this.startedBGThread = false;
        this.mu = new ReentrantLock();
        this.queue = new LinkedBlockingDeque<>();
        this.backgroudTask = () -> {
            try {
                this.logger.debug("start background thread");
                while(true) {
                    Runnable task = this.queue.takeFirst();
                    task.run();
                }
            } catch (InterruptedException e) {
                this.logger.error("{}", e);
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        };
    }
    public void Schedule(Runnable task) {
        if(!this.startedBGThread) {
            this.startedBGThread = true;
            this.bgThread = new Thread(this.backgroudTask);
            this.bgThread.start();
        }
        this.queue.add(task);
    }

}
