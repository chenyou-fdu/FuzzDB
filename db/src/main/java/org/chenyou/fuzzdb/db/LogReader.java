package org.chenyou.fuzzdb.db;

import org.chenyou.fuzzdb.util.Slice;
import org.chenyou.fuzzdb.util.Status;
import org.chenyou.fuzzdb.util.file.SequentialFile;

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
}
