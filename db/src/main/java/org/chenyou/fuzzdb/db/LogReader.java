package org.chenyou.fuzzdb.db;

import org.chenyou.fuzzdb.util.Slice;
import org.chenyou.fuzzdb.util.Status;
import org.chenyou.fuzzdb.util.file.SequentialFile;
import sun.rmi.runtime.Log;

public class LogReader {
    private static abstract class Reporter {
        public abstract void corruption(Integer byteSize, final Status status);
    }

    private SequentialFile sequentialFile;
    private Reporter reporter;
    private Boolean checkSum;
    private byte[] backingStore;
    private Slice buffer;
    private Boolean eof;

    private Long lastRecordOffset;
    private Long endOfBufferOffset;
    private Long initOffset;

    private Boolean reSync;

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
    public LogReader(SequentialFile sequentialFile, Reporter reporter, Boolean checkSum, Long initOffset) {
        this.sequentialFile = sequentialFile;
        this.reporter = reporter;
        this.checkSum = checkSum;
        this.initOffset = initOffset;

        this.backingStore = new byte[LogFormat.kBlockSize];
        this.eof = false;
        this.lastRecordOffset = 0L;
        this.endOfBufferOffset = 0L;
        this.reSync = this.initOffset > 0;

        this.buffer = null;
    }

    public Boolean skipToInitalBlock() {
        Integer offsetInBlock = (int)(initOffset % LogFormat.kBlockSize);
        Long blockStartPos = initOffset - offsetInBlock;

        // don't search a block if we'd be in the trailer
        if(offsetInBlock > LogFormat.kBlockSize - 6) {
            offsetInBlock = 0;
            blockStartPos += LogFormat.kBlockSize;
        }

        endOfBufferOffset = blockStartPos;

        // skip to start of first block that can contain the initial record
        if (blockStartPos > 0) {
            Status skipStatus = sequentialFile.skip(blockStartPos);
            if (!skipStatus.ok()) {
                reportDrop(blockStartPos, skipStatus);
                return false;
            }
        }
        return true;
    }

    public void reportDrop(Long bytes, final Status reason) {
        if(reporter != null && endOfBufferOffset - buffer.getSize() - bytes >= initOffset) {
            this.reporter.corruption(bytes.intValue(), reason);
        }
    }

    public void reportCorruption(Long bytes, String reason) {
        reportDrop(bytes, Status.Corruption(new Slice(reason), null));
    }

    public LogFormat.RecordType readPhysicalRecord(Slice result) {
        while(true) {
            if(buffer.getSize() < LogFormat.kHeaderSize) {
                // if not read to end of file, re-read
                if(!eof) {
                    buffer.clear();
                    Status status = sequentialFile.read(LogFormat.kBlockSize, buffer);
                    endOfBufferOffset += buffer.getSize();
                    if(!status.ok()) {
                        buffer.clear();
                        reportDrop((long)LogFormat.kBlockSize, status);
                        eof = true;
                        return LogFormat.RecordType.kEof;
                    } else if(buffer.getSize() < LogFormat.kBlockSize) {
                        eof = true;
                    }
                } else {
                    // Note that if buffer is non-empty, we have a truncated header at the
                    //   end of the file, which can be caused by the writer crashing in the
                    //   middle of writing the header. Instead of considering this an error,
                    //   just report EOF.
                    buffer.clear();
                    return LogFormat.RecordType.kEof;
                }
            }
            byte[] header = buffer.getData();
            final Integer a = ((int)header[4]) & 0xff;
            final Integer b = ((int)header[5]) & 0xff;
            final Integer type = (int)header[6];
            final Integer length = a | (b << 8);
            if(LogFormat.kHeaderSize + length > buffer.getSize()) {
                Integer dropSize = buffer.getSize();
                buffer.clear();
                if(!eof) {
                    reportCorruption((long)dropSize, "bad record length");
                    return LogFormat.RecordType.kBadRecord;
                }
            }

        }

    }
}
