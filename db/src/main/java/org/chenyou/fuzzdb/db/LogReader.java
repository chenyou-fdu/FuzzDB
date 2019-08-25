package org.chenyou.fuzzdb.db;

import org.apache.commons.lang3.ArrayUtils;
import org.chenyou.fuzzdb.util.Coding;
import org.chenyou.fuzzdb.util.FuzzCRC32C;
import org.chenyou.fuzzdb.util.Slice;
import org.chenyou.fuzzdb.util.Status;
import org.chenyou.fuzzdb.util.file.SequentialFile;
import java.util.Arrays;
import java.util.List;

public class LogReader {
    static abstract class Reporter {
        abstract void corruption(int byteSize, final Status status);
    }

    private SequentialFile sequentialFile;
    private Reporter reporter;
    private boolean checkSum;
    private byte[] backingStore;
    private Slice buffer;
    private boolean eof;

    // offset of last read
    private long lastRecordOffset;
    private long endOfBufferOffset;
    // where to read the first record
    private long initOffset;

    private boolean reSync;

    // create a reader that will return log records from "sequentialFile".
    //  "sequentialFile" must remain live while this Reader is in use.
    //
    // If "reporter" is non-null, it is notified whenever some data is
    //  dropped due to a detected corruption. "reporter" must remain
    //  live while this Reader is in use.
    //
    // If "checksum" is true, verify checksums if available.
    //
    // The Reader will start reading at the first record located at physical
    //  position >= initial_offset within the file.
    public LogReader(SequentialFile sequentialFile, Reporter reporter, boolean checkSum, long initOffset) {
        this.sequentialFile = sequentialFile;
        this.reporter = reporter;
        this.checkSum = checkSum;
        this.initOffset = initOffset;

        this.backingStore = new byte[LogFormat.K_BLOCK_SIZE];
        this.eof = false;
        this.lastRecordOffset = 0L;
        this.endOfBufferOffset = 0L;
        this.reSync = this.initOffset > 0;

        this.buffer = new Slice();
    }

    public Boolean skipToInitalBlock() {
        // logic offset in a block
        int offsetInBlock = (int) (initOffset % LogFormat.K_BLOCK_SIZE);
        // physical offset of the block start pos
        Long blockStartPos = initOffset - offsetInBlock;

        // don't search a block if we'd be in the end of a block (less than header size in the end)
        if (offsetInBlock > LogFormat.K_BLOCK_SIZE - 6) {
            // no need to use offsetInBlock again
            // offsetInBlock = 0;
            // move to next block
            blockStartPos += LogFormat.K_BLOCK_SIZE;
        }

        endOfBufferOffset = blockStartPos;

        // skip seqFile to start of first block that can contain the initial record
        if (blockStartPos > 0) {
            Status skipStatus = sequentialFile.skip(blockStartPos);
            // report failure
            if (!skipStatus.ok()) {
                reportDrop(blockStartPos, skipStatus);
                return false;
            }
        }
        return true;
    }

    private void reportDrop(Long bytes, final Status reason) {
        // WTF is this?
        // compareUnsigned(x, y): x == y ret 0, x > y return pos, x < y return neg
        int unsignedCompareResult = Long.compareUnsigned(endOfBufferOffset - buffer.getSize() - bytes, initOffset);
        if (reporter != null && unsignedCompareResult >= 0) {
            this.reporter.corruption(bytes.intValue(), reason);
        }
    }

    private void reportCorruption(long bytes, String reason) {
        reportDrop(bytes, Status.Corruption(new Slice(reason), new Slice("")));
    }

    public boolean readRecord(Slice record, List<Byte> scratch) {
        // if last offset less than the start pos of this read, do skip
        if (this.lastRecordOffset < initOffset) {
            // skip to init offset of a block
            if (!skipToInitalBlock()) {
                return false;
            }
        }
        record.clear();
        // TODO is correct?
        //scratch.setLength(0);
        scratch.clear();
        boolean inFragmentedRecord = false;
        // Record offset of the logical record that we're reading
        long prospectiveRecordOffset = 0;
        Slice fragment = new Slice();
        while (true) {
            final int recordTypeValue = readPhysicalRecord(fragment);
            // ReadPhysicalRecord may have only had an empty trailer remaining in its
            // internal buffer. Calculate the offset of the next physical record now
            // that it has returned, properly accounting for its header size.
            long physicalRecordOffset = this.endOfBufferOffset - this.buffer.getSize() - LogFormat.K_HEADER_SIZE - fragment.getSize();

            // reSync means initOffset > 0
            if (reSync) {
                if (recordTypeValue == LogFormat.RecordType.kMiddleType.getValue()) {
                    continue;
                } else if (recordTypeValue == LogFormat.RecordType.kLastType.getValue()) {
                    reSync = false;
                    continue;
                } else {
                    reSync = false;
                }
            }
            if (recordTypeValue == LogFormat.RecordType.kFullType.getValue()) {
                if (inFragmentedRecord) {
                    // Handle bug in earlier versions of log::Writer where
                    // it could emit an empty kFirstType record at the tail end
                    // of a block followed by a kFullType or kFirstType record
                    // at the beginning of the next block.
                    if (scratch.size() != 0) {
                        reportCorruption(scratch.size(), "partial record without end(1)");
                    }
                }
                prospectiveRecordOffset = physicalRecordOffset;
                //scratch.setLength(0);
                scratch.clear();
                //record = fragment;
                record.setData(fragment.getData());
                // TODO why
                lastRecordOffset = prospectiveRecordOffset;
                return true;
            } else if (recordTypeValue == LogFormat.RecordType.kFirstType.getValue()) {
                if (inFragmentedRecord) {
                    // Handle bug in earlier versions of log::Writer where
                    // it could emit an empty kFirstType record at the tail end
                    // of a block followed by a kFullType or kFirstType record
                    // at the beginning of the next block.
                    if (scratch.size() != 0) {
                        reportCorruption(scratch.size(), "partial record without end(2)");
                    }
                }
                prospectiveRecordOffset = physicalRecordOffset;
                // TODO assign string
                //scratch.append(Arrays.toString(fragment.getData()));
                scratch.clear();
                scratch.addAll(Arrays.asList(ArrayUtils.toObject(fragment.getData())));
                inFragmentedRecord = true;
            } else if (recordTypeValue == LogFormat.RecordType.kMiddleType.getValue()) {
                if (!inFragmentedRecord) {
                    reportCorruption(fragment.getSize(), "missing start of fragmented record(1)");
                } else {
                    // TODO append data from fragment to string?
                    scratch.addAll(Arrays.asList(ArrayUtils.toObject(fragment.getData())));
                }
            } else if (recordTypeValue == LogFormat.RecordType.kLastType.getValue()) {
                if (!inFragmentedRecord) {
                    reportCorruption(fragment.getSize(), "missing start of fragmented record(2)");
                } else {
                    // TODO may have better
                    scratch.addAll(Arrays.asList(ArrayUtils.toObject(fragment.getData())));
                    Byte[] fullData = new Byte[scratch.size()];
                    scratch.toArray(fullData);
                    record.setData(ArrayUtils.toPrimitive(fullData));
                    lastRecordOffset = prospectiveRecordOffset;
                    return true;
                }
            } else if (recordTypeValue == LogFormat.RecordType.kEof.getValue()) {
                if (inFragmentedRecord) {
                    // This can be caused by the writer dying immediately after
                    // writing a physical record but before completing the next; don't
                    // treat it as a corruption, just ignore the entire logical record.
                    scratch.clear();
                }
                return false;
            } else if (recordTypeValue == LogFormat.RecordType.kBadRecord.getValue()) {
                if (inFragmentedRecord) {
                    reportCorruption(scratch.size(), "error in middle of record");
                    inFragmentedRecord = false;
                    scratch.clear();
                }
            } else {
                // TODO report unknown
                reportCorruption(fragment.getSize() + (inFragmentedRecord ? scratch.size() : 0),
                        "unknown record type " + recordTypeValue);
                inFragmentedRecord = false;
                scratch.clear();
            }
        }
    }

    // do physical read from file to result, each time a block
    private int readPhysicalRecord(Slice result) {
        while(true) {
            if (buffer.getSize() < LogFormat.K_HEADER_SIZE) {
                // if not read to end of file, re-read
                if (!eof) {
                    // last time buffer has read a full record
                    buffer.clear();
                    Status status = sequentialFile.read(LogFormat.K_BLOCK_SIZE, buffer);
                    // move end with buffer size
                    endOfBufferOffset += buffer.getSize();
                    if (!status.ok()) {
                        buffer.clear();
                        reportDrop((long) LogFormat.K_BLOCK_SIZE, status);
                        eof = true;
                        return LogFormat.RecordType.kEof.getValue();
                    } else if (buffer.getSize() < LogFormat.K_BLOCK_SIZE) {
                        // if read less than a block, means we met eof
                        eof = true;
                    }
                    continue;
                } else {
                    // Note that if buffer is non-empty, we have a truncated header at the
                    //   end of the file, which can be caused by the writer crashing in the
                    //   middle of writing the header. Instead of considering this an error,
                    //   just report EOF.
                    buffer.clear();
                    return LogFormat.RecordType.kEof.getValue();
                }
            }
            // parse header
            byte[] rawBuffer = buffer.getData();
            final int a = ((int) rawBuffer[4]) & 0xff;
            final int b = ((int) rawBuffer[5]) & 0xff;
            final int type = rawBuffer[6];
            final int length = a | (b << 8);
            if (LogFormat.K_HEADER_SIZE + length > buffer.getSize()) {
                // header size + record size large than the data just read
                long dropSize = buffer.getSize();
                buffer.clear();
                if (!eof) {
                    reportCorruption(dropSize, "bad record length");
                    return LogFormat.RecordType.kBadRecord.getValue();
                } else {
                    // If the end of the file has been reached without reading |length| bytes
                    //  of payload, assume the writer died in the middle of writing the record.
                    //  don't report a corruption.
                    return LogFormat.RecordType.kEof.getValue();
                }
            }

            // TODO it this type in java?
            if (type == LogFormat.RecordType.kZeroType.getValue() && length == 0) {
                // Skip zero length record without reporting any drops since
                // such records are produced by the mmap based writing code in
                // env_posix.cc that preallocates file regions.
                buffer.clear();
                return LogFormat.RecordType.kBadRecord.getValue();
            }

            if (checkSum) {
                int expectedCrc = FuzzCRC32C.unmask(Coding.DecodeFixed32(rawBuffer, 0));
                //int actuallCrc = FuzzCRC32C.value(rawBuffer, 6, length + 1);
                Long actuallCrcLong = FuzzCRC32C.value(rawBuffer, 6, length + 1);
                int actuallCrc = actuallCrcLong.intValue();
                if (actuallCrc != expectedCrc) {
                    // Drop the rest of the buffer since "length" itself may have
                    //  been corrupted and if we trust it, we could find some
                    //  fragment of a real log record that just happens to look
                    //  like a valid log record.
                    long dropSize = buffer.getSize();
                    buffer.clear();
                    reportCorruption(dropSize, "checksum mismatch");
                    return LogFormat.RecordType.kBadRecord.getValue();
                }
            }

            // remove this time read content
            buffer.removePrefix(LogFormat.K_HEADER_SIZE + length);

            // check if has data read before initOffset
            // Skip physical record that started before initOffset
            if (endOfBufferOffset - buffer.getSize() - LogFormat.K_HEADER_SIZE - length < initOffset) {
                result.clear();
                return LogFormat.RecordType.kBadRecord.getValue();
            }

            // store the real payload
            byte[] payLoad = new byte[length];
            System.arraycopy(rawBuffer, LogFormat.K_HEADER_SIZE, payLoad, 0, length);
        /*
        for (int i = 0; i < length; i++) {
            payLoad[i] = header[LogFormat.K_HEADER_SIZE + i];
        }*/
            result.setData(payLoad);
            return type;
        }
    }

    public long getLastRecordOffset() {
        return lastRecordOffset;
    }
}
