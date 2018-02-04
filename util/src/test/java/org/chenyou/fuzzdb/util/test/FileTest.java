package org.chenyou.fuzzdb.util.test;

import org.chenyou.fuzzdb.util.Slice;

import org.chenyou.fuzzdb.util.Random;
import org.chenyou.fuzzdb.util.file.FuzzSequentialFile;
import org.chenyou.fuzzdb.util.file.FuzzWritableFile;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;


public class FileTest {
    private String testTmpFilePath = "filetesttmp";
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
        FuzzWritableFile writableFile = null;
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
            writableFile = new FuzzWritableFile(testTmpFilePath, fd);
            Assert.assertTrue(writableFile.append(new Slice(text)).ok());
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
        FuzzWritableFile writableFile = null;
        Integer lineNum = 1000;
        StringBuilder sb = new StringBuilder();
        try {
            FileChannel fd = FileChannel.open(Paths.get(testTmpFilePath), StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            writableFile = new FuzzWritableFile(testTmpFilePath, fd);
            for(Integer i = 0; i < lineNum; i++) {
                String eachText = rnd.next().toString();
                sb.append(eachText);
                Assert.assertTrue(writableFile.append(new Slice(eachText)).ok());
            }
            Assert.assertTrue(writableFile.flush().ok());
            logger.debug("append all flush once {} number string", lineNum);
            for(Integer i = 0; i < lineNum; i++) {
                String eachText = rnd.next().toString();
                sb.append(eachText);
                Assert.assertTrue(writableFile.append(new Slice(eachText)).ok());
                Assert.assertTrue(writableFile.flush().ok());
            }
            logger.debug("append once flush once {} string", lineNum);
            for(Integer i = 0; i < lineNum; i++) {
                String eachText = rnd.next().toString();
                sb.append(eachText);
                Assert.assertTrue(writableFile.append(new Slice(eachText)).ok());
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
        Integer lineNum = 100;
        for(Integer i = 0; i < lineNum; i++) {
            String eachText = rnd.next().toString();
            FuzzWritableFile writableFile = null;
            try {
                FileChannel fd = FileChannel.open(Paths.get(testTmpFilePath), StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                writableFile = new FuzzWritableFile(testTmpFilePath, fd);
                Assert.assertTrue(writableFile.append(new Slice(eachText)).ok());
                Assert.assertTrue(writableFile.flush().ok());
            } catch (IOException ex) {
                throw new RuntimeException(ex.getMessage());
            } finally {
                if (writableFile != null) {
                    Assert.assertTrue(writableFile.close().ok());
                }
            }
            try {
                byte[] rawBytes = Files.readAllBytes(Paths.get(testTmpFilePath));
                String readText = new String(rawBytes);
                Assert.assertEquals(eachText, readText);
            } catch (IOException ex) {
                throw new RuntimeException(ex.getMessage());
            }
        }
    }

    @Test
    public void SequentialFileTest1() {
        Integer lineNum = 50000;
        StringBuilder sb = new StringBuilder();
        for(Integer i = 0; i < lineNum; i++) {
            String eachText = rnd.next().toString();
            sb.append(eachText);
        }
        String text = sb.toString();
        try {
            Files.write(Paths.get(testTmpFilePath), text.getBytes(), StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage());
        }
        FuzzSequentialFile sequentialFile = null;
        try {
            FileChannel fd = FileChannel.open(Paths.get(testTmpFilePath), StandardOpenOption.READ);
            sequentialFile = new FuzzSequentialFile(testTmpFilePath, fd);
            Slice res = new Slice();
            Assert.assertTrue(sequentialFile.read(text.length(), res).ok());
            Assert.assertEquals(text, new String(res.getData()));
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage());
        } finally {
            if(sequentialFile != null) {
                Assert.assertTrue(sequentialFile.close().ok());
            }
        }
    }

    @Test
    public void SequentialFileTest2() {
        Integer lineNum = 512301;
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        for(Integer i = 0; i < lineNum; i++) {
            String eachText = rnd.next().toString();
            if(i > lineNum/2) sb1.append(eachText);
            sb2.append(eachText);
        }
        String text1 = sb1.toString();
        String text2 = sb2.toString();
        try {
            Files.write(Paths.get(testTmpFilePath), text2.getBytes(), StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage());
        }
        FuzzSequentialFile sequentialFile = null;
        try {
            FileChannel fd = FileChannel.open(Paths.get(testTmpFilePath), StandardOpenOption.READ);
            sequentialFile = new FuzzSequentialFile(testTmpFilePath, fd);
            Slice res = new Slice();
            Assert.assertTrue(sequentialFile.skip((long)(text2.length()-text1.length())).ok());
            Assert.assertTrue(sequentialFile.read(text1.length(), res).ok());
            Assert.assertEquals(text1, new String(res.getData()));
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage());
        } finally {
            if(sequentialFile != null) {
                Assert.assertTrue(sequentialFile.close().ok());
            }
        }
    }
}
