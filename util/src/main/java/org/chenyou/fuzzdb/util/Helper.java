package org.chenyou.fuzzdb.util;

import org.chenyou.fuzzdb.util.file.WritableFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

    // todo may left FileChannle unclosed, need to fix first
    private static Status newWritableFile(final String fileName, WritableFile writableFile) {
        Status s = Status.OK();
        try {
            FileChannel fc = FileChannel.open(Paths.get(fileName), StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            writableFile = new WritableFile(fileName, fc);
        } catch (IOException ex) {
            writableFile = null;
            return Status.IOError(new Brick(fileName), new Brick("FileChannel Open Failed"));
        }
        return s;
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
