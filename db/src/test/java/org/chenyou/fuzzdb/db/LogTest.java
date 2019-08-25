package org.chenyou.fuzzdb.db;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;
import org.chenyou.fuzzdb.util.*;
import org.chenyou.fuzzdb.util.file.SequentialFile;
import org.chenyou.fuzzdb.util.file.WritableFile;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

public class LogTest {
    @Test
    public void TestEOF() {
        LogTestHelper logTestHelper = new LogTestHelper();
        Assert.assertEquals("EOF", logTestHelper.read());
    }

    @Test
    public void TestReadWrite() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write("foo");
        logTestHelper.write("bar");
        logTestHelper.write("");
        logTestHelper.write("xxxx");
        Assert.assertEquals("foo", logTestHelper.read());
        Assert.assertEquals("bar", logTestHelper.read());
        Assert.assertEquals("", logTestHelper.read());
        Assert.assertEquals("xxxx", logTestHelper.read());
        Assert.assertEquals("EOF", logTestHelper.read());
        Assert.assertEquals("EOF", logTestHelper.read());
    }

    @Test
    public void testManyBlocks() {
        LogTestHelper logTestHelper = new LogTestHelper();
        for (Integer i = 0; i < 10000; i++) {
            logTestHelper.write(i.toString());
        }
        for (Integer i = 0; i < 10000; i++) {
            Assert.assertEquals(i.toString(), logTestHelper.read());
        }
        Assert.assertEquals("EOF", logTestHelper.read());
    }

    @Test
    public void testFragmentation() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write("small");
        logTestHelper.write(bigNumber("medium", 50000));
        logTestHelper.write(bigNumber("large", 100000));
        Assert.assertEquals("small", logTestHelper.read());
        Assert.assertEquals(bigNumber("medium", 50000), logTestHelper.read());
        Assert.assertEquals(bigNumber("large", 100000), logTestHelper.read());
        Assert.assertEquals("EOF", logTestHelper.read());
    }

    @Test
    public void testMarginalTrailer() {
        LogTestHelper logTestHelper = new LogTestHelper();
        // Make a trailer that is exactly the same length as an empty record.
        final int n = LogFormat.K_BLOCK_SIZE - 2 * LogFormat.K_HEADER_SIZE;
        logTestHelper.write(bigNumber("foo", n));
        // write payload size is n, plus header size K_HEADER_SIZE
        Assert.assertEquals(LogFormat.K_BLOCK_SIZE - LogFormat.K_HEADER_SIZE, logTestHelper.writtenBytes());
        logTestHelper.write("");
        logTestHelper.write("bar");
        Assert.assertEquals(bigNumber("foo", n), logTestHelper.read());
        Assert.assertEquals("", logTestHelper.read());
        Assert.assertEquals("bar", logTestHelper.read());
        Assert.assertEquals("EOF", logTestHelper.read());
    }

    @Test
    public void testMarginalTrailer2() {
        LogTestHelper logTestHelper = new LogTestHelper();
        final int n = LogFormat.K_BLOCK_SIZE - 2 * LogFormat.K_HEADER_SIZE;
        logTestHelper.write(bigNumber("foo", n));
        Assert.assertEquals(LogFormat.K_BLOCK_SIZE - LogFormat.K_HEADER_SIZE, logTestHelper.writtenBytes());
        logTestHelper.write("bar");
        Assert.assertEquals(bigNumber("foo", n), logTestHelper.read());
        Assert.assertEquals("bar", logTestHelper.read());
        Assert.assertEquals("EOF", logTestHelper.read());
        Assert.assertEquals(0, logTestHelper.droppedBytes());
        Assert.assertEquals("", logTestHelper.reportMessage());
    }

    @Test
    public void testShortTrailer() {
        LogTestHelper logTestHelper = new LogTestHelper();
        final int n = LogFormat.K_BLOCK_SIZE - 2 * LogFormat.K_HEADER_SIZE + 4;
        logTestHelper.write(bigNumber("foo", n));
        Assert.assertEquals(LogFormat.K_BLOCK_SIZE - LogFormat.K_HEADER_SIZE + 4, logTestHelper.writtenBytes());
        logTestHelper.write("");
        logTestHelper.write("bar");
        Assert.assertEquals(bigNumber("foo", n), logTestHelper.read());
        Assert.assertEquals("", logTestHelper.read());
        Assert.assertEquals("bar", logTestHelper.read());
        Assert.assertEquals("EOF", logTestHelper.read());
    }

    @Test
    public void testAlignedEOF() {
        LogTestHelper logTestHelper = new LogTestHelper();
        final int n = LogFormat.K_BLOCK_SIZE - 2 * LogFormat.K_HEADER_SIZE + 4;
        logTestHelper.write(bigNumber("foo", n));
        Assert.assertEquals(LogFormat.K_BLOCK_SIZE - LogFormat.K_HEADER_SIZE + 4, logTestHelper.writtenBytes());
        Assert.assertEquals(bigNumber("foo", n), logTestHelper.read());
        Assert.assertEquals("EOF", logTestHelper.read());
    }

    @Test
    public void testOpenForAppend() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write("hello");
        logTestHelper.reopenForAppender();
        logTestHelper.write("world");
    }

    @Test
    public void testRandomRead() {
        LogTestHelper logTestHelper = new LogTestHelper();
        int n = 500;
        Random writeRandom = new Random(301);
        for (Integer i = 0; i < n; i++) {
            logTestHelper.write(randomNumber(i.toString(), writeRandom));
        }
        Random readRandom = new Random(301);
        for (Integer i = 0; i < n; i++) {
            Assert.assertEquals(randomNumber(i.toString(), readRandom), logTestHelper.read());
        }
        Assert.assertEquals("EOF", logTestHelper.read());
    }

    @Test
    public void testReadError() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write("foo");
        logTestHelper.forceError();
        Assert.assertEquals("EOF", logTestHelper.read());
        Assert.assertEquals(LogFormat.K_BLOCK_SIZE, logTestHelper.droppedBytes());
        Assert.assertEquals("OK", logTestHelper.matchError("read error"));
    }

    @Test
    public void testBadRecordType() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write("foo");
        logTestHelper.incrementByte(6, (byte) 100);
        logTestHelper.fixChecksum(0, 3);
        Assert.assertEquals("EOF", logTestHelper.read());
        Assert.assertEquals(3, logTestHelper.droppedBytes());
        Assert.assertEquals("OK", logTestHelper.matchError("unknown record type"));
    }

    @Test
    public void testTruncatedTrailingRecordIsIgnored() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write("foo");
        // Drop all payload as well as a header byte
        logTestHelper.shrinkSize(4);
        Assert.assertEquals("EOF", logTestHelper.read());
        // Truncated last record is ignored, not treated as an error.
        Assert.assertEquals(0, logTestHelper.droppedBytes());
        Assert.assertEquals("", logTestHelper.reportMessage());
    }

    @Test
    public void testBadLength() {
        final int kPayloadSize = LogFormat.K_BLOCK_SIZE - LogFormat.K_HEADER_SIZE;
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write(bigNumber("bar", kPayloadSize));
        logTestHelper.write("foo");
        // Least significant size byte is stored in header[4].
        logTestHelper.incrementByte(4, (byte) 1);
        Assert.assertEquals("foo", logTestHelper.read());
        Assert.assertEquals(LogFormat.K_BLOCK_SIZE, logTestHelper.droppedBytes());
        Assert.assertEquals("OK", logTestHelper.matchError("bad record length"));
    }

    @Test
    public void testBadLengthAtEndIsIgnored() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write("foo");
        logTestHelper.shrinkSize(1);
        Assert.assertEquals("EOF", logTestHelper.read());
        Assert.assertEquals(0, logTestHelper.droppedBytes());
        Assert.assertEquals("", logTestHelper.reportMessage());
    }

    @Test
    public void testUnexpectedMiddleType() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write("foo");
        logTestHelper.setByte(6, (byte) LogFormat.RecordType.kMiddleType.getValue());
        logTestHelper.fixChecksum(0, 3);
        Assert.assertEquals("EOF", logTestHelper.read());
        Assert.assertEquals(3, logTestHelper.droppedBytes());
        Assert.assertEquals("OK", logTestHelper.matchError("missing start"));
    }

    @Test
    public void testUnexpectedFullType() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write("foo");
        logTestHelper.write("bar");
        logTestHelper.setByte(6, (byte) LogFormat.RecordType.kFirstType.getValue());
        logTestHelper.fixChecksum(0, 3);
        Assert.assertEquals("bar", logTestHelper.read());
        Assert.assertEquals("EOF", logTestHelper.read());
        Assert.assertEquals(3, logTestHelper.droppedBytes());
        Assert.assertEquals("OK", logTestHelper.matchError("partial record without end"));
    }

    @Test
    public void testUnexpectedFirstType() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write("foo");
        logTestHelper.write(bigNumber("bar", 100000));
        logTestHelper.setByte(6, (byte) LogFormat.RecordType.kFirstType.getValue());
        logTestHelper.fixChecksum(0, 3);
        Assert.assertEquals(bigNumber("bar", 100000), logTestHelper.read());
        Assert.assertEquals("EOF", logTestHelper.read());
        Assert.assertEquals(3, logTestHelper.droppedBytes());
        Assert.assertEquals("OK", logTestHelper.matchError("partial record without end"));
    }

    @Test
    public void testMissingLastIsIgnored() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write(bigNumber("bar", LogFormat.K_BLOCK_SIZE));
        logTestHelper.shrinkSize(14);
        Assert.assertEquals("EOF", logTestHelper.read());
        Assert.assertEquals("", logTestHelper.reportMessage());
        Assert.assertEquals(0, logTestHelper.droppedBytes());
    }

    @Test
    public void testPartialLastIsIgnored() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write(bigNumber("bar", LogFormat.K_BLOCK_SIZE));
        logTestHelper.shrinkSize(1);
        Assert.assertEquals("EOF", logTestHelper.read());
        Assert.assertEquals("", logTestHelper.reportMessage());
        Assert.assertEquals(0, logTestHelper.droppedBytes());
    }

    @Test
    public void testSkipIntoMultiRecord() {
        // Consider a fragmented record:
        //    first(R1), middle(R1), last(R1), first(R2)
        // If initial_offset points to a record after first(R1) but before first(R2)
        // incomplete fragment errors are not actual errors, and must be suppressed
        // until a new first or full record is encountered.
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write(bigNumber("foo", 3 * LogFormat.K_BLOCK_SIZE));
        logTestHelper.write("correct");
        logTestHelper.startReadingAt(LogFormat.K_BLOCK_SIZE);
        Assert.assertEquals("correct", logTestHelper.read());
        Assert.assertEquals("", logTestHelper.reportMessage());
        Assert.assertEquals(0, logTestHelper.droppedBytes());
        Assert.assertEquals("EOF", logTestHelper.read());
    }

    @Test
    public void testErrorJoinsRecords() {
        // Consider two fragmented records:
        //    first(R1) last(R1) first(R2) last(R2)
        // where the middle two fragments disappear.  We do not want
        // first(R1),last(R2) to get joined and returned as a valid record.

        // Write records that span two blocks
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.write(bigNumber("foo", LogFormat.K_BLOCK_SIZE));
        logTestHelper.write(bigNumber("bar", LogFormat.K_BLOCK_SIZE));
        logTestHelper.write("correct");

        for (int offset = LogFormat.K_BLOCK_SIZE; offset < 2 * LogFormat.K_BLOCK_SIZE; offset++) {
            logTestHelper.setByte(offset, (byte) 'x');
        }

        Assert.assertEquals("correct", logTestHelper.read());
        Assert.assertEquals("EOF", logTestHelper.read());
        final int dropped = logTestHelper.droppedBytes();
        Assert.assertTrue(dropped <= 2 * LogFormat.K_BLOCK_SIZE + 100);
        Assert.assertTrue(dropped >= 2 * LogFormat.K_BLOCK_SIZE);
    }

    @Test
    public void testReadStart() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkInitialOffsetRecord(0, 0);
    }

    @Test
    public void testReadSecondOneOff() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkInitialOffsetRecord(1, 1);
    }

    @Test
    public void testReadSecondTenThousand() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkInitialOffsetRecord(10000, 1);
    }

    @Test
    public void testReadSecondStart() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkInitialOffsetRecord(10007, 1);
    }

    @Test
    public void testReadThirdOneOff() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkInitialOffsetRecord(10008, 2);
    }

    @Test
    public void testReadThirdStart() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkInitialOffsetRecord(20014, 2);
    }

    @Test
    public void testReadFourthOneOff() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkInitialOffsetRecord(20015, 3);
    }

    @Test
    public void testReadFourthFirstBlockTrailer() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkInitialOffsetRecord(LogFormat.K_BLOCK_SIZE - 4, 3);
    }

    @Test
    public void testReadFourthMiddleBlock() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkInitialOffsetRecord(LogFormat.K_BLOCK_SIZE + 1, 3);
    }

    @Test
    public void testReadFourthLastBlock() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkInitialOffsetRecord(2 * LogFormat.K_BLOCK_SIZE + 1, 3);
    }

    @Test
    public void testReadFourthStart() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkInitialOffsetRecord(
                2 * (LogFormat.K_HEADER_SIZE+ 1000) + (2 * LogFormat.K_BLOCK_SIZE- 1000) + 3 * LogFormat.K_HEADER_SIZE,
                3);
    }

    @Test
    public void testReadInitialOffsetIntoBlockPadding() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkInitialOffsetRecord(3 * LogFormat.K_BLOCK_SIZE - 3, 5);
    }

    @Test
    public void testReadEnd() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkOffsetPastEndReturnsNoRecords(0);
    }

    @Test
    public void testReadPastEnd() {
        LogTestHelper logTestHelper = new LogTestHelper();
        logTestHelper.checkOffsetPastEndReturnsNoRecords(5);
    }

    private String bigNumber(String baseStr, int n) {
        StringBuilder stringBuilder = new StringBuilder();
        int cnt = 0;
        while (stringBuilder.length() < n) {
            for (int i = 0; i < baseStr.length(); i++) {
                stringBuilder.append(baseStr.charAt(i));
                cnt++;
                if (cnt >= n) {
                    Preconditions.checkArgument(stringBuilder.length() == n);
                    return stringBuilder.toString();
                }
            }
        }
        return stringBuilder.toString();
    }

    private String randomNumber(String baseStr, Random random) {
        return bigNumber(baseStr, random.skewed(17));
    }


static class ReportCollector extends LogReader.Reporter {
    public ReportCollector() {
        droppedBytes = 0;
        message = "";
    }

    @Override
    void corruption(int bytes, final Status status) {
        droppedBytes += bytes;
        message += status.toString();
    }

    private int droppedBytes;
    private String message;
}

class StringDest extends WritableFile {
    public StringDest() {
        this.byteList = new ArrayList<>();
    }

    @Override
    public Status close() {
        return Status.OK();
    }

    @Override
    public Status flush() {
        return Status.OK();
    }

    @Override
    public Status sync() {
        return Status.OK();
    }

    @Override
    public Status append(final Slice data) {
        int size = data.getSize();
        byte[] dataArray = data.getData();
        //for (byte eachByte : data.getData()) {
        for (int i = 0; i < size; i++) {
            byteList.add(dataArray[i]);
        }
        return Status.OK();
    }

    public List<Byte> byteList;
}

static class StringSource extends SequentialFile {
    private boolean forceError;
    private boolean returnedPartial;

    public StringSource() {
        this.forceError = false;
        this.returnedPartial = false;
    }

    @Override
    public Status read(int n, Slice result) {
        Preconditions.checkArgument(!returnedPartial, "must not read() after eof/error");
        if (forceError) {
            this.forceError = false;
            this.returnedPartial = true;
            return Status.Corruption(new Slice("read error"));
        }

        if (content.getSize() < n) {
            n = content.getSize();
            this.returnedPartial = true;
        }
        result.clear();
        result.setData(content.getData(), n);
        content.removePrefix(n);
        return Status.OK();
    }

    @Override
    public Status skip(long n) {
        if (n > content.getSize()) {
            content.clear();
            return Status.NotFound(new Slice("in-memory file skipped past end"));
        }
        content.removePrefix((int) n);
        return Status.OK();
    }

    @Override
    public Status close() {
        return Status.OK();
    }

    Slice content;
}

public class LogTestHelper {
    public LogTestHelper() {
        this.reading = false;
        this.reportCollector = new ReportCollector();
        this.dest = new StringDest();
        this.source = new StringSource();
        this.writer = new LogWriter(this.dest);
        this.reader = new LogReader(this.source, this.reportCollector, true, 0);
        this.initArray();
        numInitialOffsetRecord = this.initialOffsetLastRecordOffset.length;
    }

    public void forceError() {
        this.source.forceError = true;
    }

    public String matchError(String msg) {
        if (!reportCollector.message.contains(msg)) {
            return reportCollector.message;
        } else {
            return "OK";
        }
    }

    public void startReadingAt(long initialOffset) {
        this.reader = new LogReader(this.source, this.reportCollector, true /*checksum*/, initialOffset);
    }


    private void writeInitialOffsetLog() {
        for (int i = 0; i < this.numInitialOffsetRecord; i++) {
            int size = initialOffsetRecordSizes[i];
            String eachStr = (char) ((int) 'a' + i) + "";
            this.write(bigNumber(eachStr, size));
        }
    }

    public void checkInitialOffsetRecord(long initialOffset, int exceptRecordOffset) {
        this.writeInitialOffsetLog();
        this.reading = true;
        this.source.content = new Slice(dest.byteList);
        LogReader offsetReader = new LogReader(this.source, this.reportCollector, true, initialOffset);

        // Read all records from expected_record_offset through the last one.
        Assert.assertTrue(exceptRecordOffset < numInitialOffsetRecord);
        for (; exceptRecordOffset < numInitialOffsetRecord; ++exceptRecordOffset) {
            Slice record = new Slice();
            List<Byte> scratch = new ArrayList<>();
            Assert.assertTrue(offsetReader.readRecord(record, scratch));
            Assert.assertEquals(initialOffsetRecordSizes[exceptRecordOffset], record.getSize());
            Assert.assertEquals(initialOffsetLastRecordOffset[exceptRecordOffset], offsetReader.getLastRecordOffset());
            Assert.assertEquals((char) ((int) 'a' + exceptRecordOffset), record.getData()[0]);
        }
    }
    public void checkOffsetPastEndReturnsNoRecords(long offsetPastEnd) {
        writeInitialOffsetLog();
        this.reading = true;
        this.source.content = new Slice(dest.byteList);
        LogReader offsetReader = new LogReader(source, reportCollector, true, writtenBytes() + offsetPastEnd);
        Slice record = new Slice();
        List<Byte> scratch = new ArrayList<>();
        Assert.assertFalse(offsetReader.readRecord(record, scratch));
    }

    public void reopenForAppender() {
        this.writer = new LogWriter(this.dest, this.dest.byteList.size());
    }

    public int droppedBytes() {
        return this.reportCollector.droppedBytes;
    }

    public void incrementByte(int offset, byte delta) {
        byte res = this.dest.byteList.get(offset);
        res += delta;
        this.dest.byteList.set(offset, res);
    }

    public void setByte(int offset, byte newByte) {
        this.dest.byteList.set(offset, newByte);
    }

    public void shrinkSize(int bytes) {
        int oldSize = this.dest.byteList.size();
        this.dest.byteList = this.dest.byteList.subList(0, oldSize - bytes);
    }

    public void fixChecksum(int headerOffset, int len) {
        int oldSize = this.dest.byteList.size();
        Byte[] tmpArray = new Byte[oldSize - (headerOffset + 6)];
        this.dest.byteList.subList(headerOffset + 6, oldSize).toArray(tmpArray);
        byte[] tmpByte = ArrayUtils.toPrimitive(tmpArray);
        Long tmpCrc = FuzzCRC32C.value(tmpByte, len + 1);
        int crc = FuzzCRC32C.mask(tmpCrc.intValue());
        byte[] tmpRes = new byte[4];
        Coding.EncodeFixed32(tmpByte, crc);
        for (int i = 0; i < 4; i++) {
            this.dest.byteList.set(headerOffset + i, tmpByte[i]);
        }
    }

    public String reportMessage() {
        return this.reportCollector.message;
    }

    public void write(final String msg) {
        Preconditions.checkArgument(!reading, "Write() after starting to read");
        this.writer.addRecord(new Slice(msg));
    }

    public int writtenBytes() {
        return dest.byteList.size();
    }

    public String read() {
        if (!this.reading) {
            reading = true;
            source.content = new Slice(dest.byteList);
        }
        List<Byte> scratch = new ArrayList<>();
        Slice record = new Slice();
        if (reader.readRecord(record, scratch)) {
            return record.toString();
        } else {
            return "EOF";
        }
    }

    public void initArray() {
        this.initialOffsetRecordSizes = new int[]{
                10000,
                10000,
                2 * LogFormat.K_BLOCK_SIZE - 1000,
                1,
                13716,
                LogFormat.K_BLOCK_SIZE - LogFormat.K_HEADER_SIZE
        };
        this.initialOffsetLastRecordOffset = new long[]{
                0,
                LogFormat.K_HEADER_SIZE + 10000,
                2 * (LogFormat.K_HEADER_SIZE + 10000),
                2 * (LogFormat.K_HEADER_SIZE + 10000) + (2 * LogFormat.K_BLOCK_SIZE - 1000) + 3 * LogFormat.K_HEADER_SIZE,
                2 * (LogFormat.K_HEADER_SIZE + 10000) + (2 * LogFormat.K_BLOCK_SIZE - 1000) + 3 * LogFormat.K_HEADER_SIZE + LogFormat.K_HEADER_SIZE + 1,
                3 * LogFormat.K_BLOCK_SIZE
        };
    }

    int[] initialOffsetRecordSizes;
    long[] initialOffsetLastRecordOffset;
    int numInitialOffsetRecord;
    boolean reading;
    StringDest dest;
    StringSource source;
    LogWriter writer;
    LogReader reader;
    ReportCollector reportCollector;
}
}
