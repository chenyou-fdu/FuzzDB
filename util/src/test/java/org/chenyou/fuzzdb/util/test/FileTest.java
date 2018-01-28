package org.chenyou.fuzzdb.util.test;

import org.chenyou.fuzzdb.util.Brick;

import org.chenyou.fuzzdb.util.Random;
import org.chenyou.fuzzdb.util.file.WritableFile;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;


public class FileTest {
    private String testTmpFilePath = "D://filetesttmp";
    private final Logger logger = LoggerFactory.getLogger(FileTest.class);
    private Random rnd;

    @Before
    public void setup() {
        this.rnd = new Random(1);
    }
    @After
    public void clean() {
        File testTmpFile = new File(testTmpFilePath);
        if(testTmpFile.exists() && testTmpFile.isFile()) {
            testTmpFile.deleteOnExit();
            logger.debug("{} deleted", this.testTmpFilePath);
        }
    }

    @Test
    public void WritableFileTest1() {
        WritableFile writableFile = null;
        Integer lineNum = 50000;
        StringBuilder sb = new StringBuilder();
        for(Integer i = 0; i < lineNum; i++) {
            String eachText = rnd.next().toString();
            sb.append(eachText);
        }
        String text = sb.toString();
        try {
            FileChannel fd = FileChannel.open(Paths.get(testTmpFilePath), StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            writableFile = new WritableFile(testTmpFilePath, fd);
            Assert.assertTrue(writableFile.append(new Brick(text)).ok());
            Assert.assertTrue(writableFile.flush().ok());
            logger.debug("write {} string lager than buffer ", lineNum);
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage());
        } finally {
            if(writableFile != null) {
                Assert.assertTrue(writableFile.close().ok());
            }
        }
        try {
            byte[] rawBytes = Files.readAllBytes(Paths.get(testTmpFilePath));
            String readText = new String(rawBytes);
            Assert.assertEquals(readText, text);
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    @Test
    public void WritableFileTest2() {
        WritableFile writableFile = null;
        Integer lineNum = 1000;
        StringBuilder sb = new StringBuilder();
        try {
            FileChannel fd = FileChannel.open(Paths.get(testTmpFilePath), StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            writableFile = new WritableFile(testTmpFilePath, fd);
            for(Integer i = 0; i < lineNum; i++) {
                String eachText = rnd.next().toString();
                sb.append(eachText);
                Assert.assertTrue(writableFile.append(new Brick(eachText)).ok());
            }
            Assert.assertTrue(writableFile.flush().ok());
            logger.debug("append all flush once {} number string", lineNum);
            for(Integer i = 0; i < lineNum; i++) {
                String eachText = rnd.next().toString();
                sb.append(eachText);
                Assert.assertTrue(writableFile.append(new Brick(eachText)).ok());
                Assert.assertTrue(writableFile.flush().ok());
            }
            logger.debug("append once flush once {} string", lineNum);
            for(Integer i = 0; i < lineNum; i++) {
                String eachText = rnd.next().toString();
                sb.append(eachText);
                Assert.assertTrue(writableFile.append(new Brick(eachText)).ok());
                Assert.assertTrue(writableFile.sync().ok());
            }
            logger.debug("append once sync once {} string", lineNum);
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage());
        } finally {
            if(writableFile != null) {
                Assert.assertTrue(writableFile.close().ok());
            }
        }
        String text = sb.toString();
        try {
            byte[] rawBytes = Files.readAllBytes(Paths.get(testTmpFilePath));
            String readText = new String(rawBytes);
            Assert.assertEquals(text,readText);
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    @Test
    public void WritableFileTest3() {
        WritableFile writableFile = null;
        Integer lineNum = 100;
        try {
            FileChannel fd = FileChannel.open(Paths.get(testTmpFilePath), StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            writableFile = new WritableFile(testTmpFilePath, fd);
            for(Integer i = 0; i < lineNum; i++) {
                String eachText = rnd.next().toString();
                Assert.assertTrue(writableFile.append(new Brick(eachText)).ok());
                Assert.assertTrue(writableFile.flush().ok());
                logger.debug("write test string {}", eachText);
                byte[] rawBytes = Files.readAllBytes(Paths.get(testTmpFilePath));
                String readText = new String(rawBytes);
                Assert.assertEquals(eachText,readText);
            }

        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage());
        } finally {
            if(writableFile != null) {
                Assert.assertTrue(writableFile.close().ok());
            }
        }
    }
}
