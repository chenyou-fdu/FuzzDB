package org.chenyou.fuzzdb.db;

import com.google.common.base.Preconditions;
import org.chenyou.fuzzdb.util.Coding;
import org.chenyou.fuzzdb.util.FuzzCRC32C;
import org.chenyou.fuzzdb.util.Slice;
import org.chenyou.fuzzdb.util.Status;
import org.chenyou.fuzzdb.util.file.WritableFile;


public class LogWriter  {

    private WritableFile writableFile;
    private Integer blockOffset;
    private int[] typeCrc = new int[LogFormat.kMaxRecordType+1];

    private static void initTypeCrc(int[] typeCrc) {
        Preconditions.checkNotNull(typeCrc);
        for(int i = 0; i < LogFormat.kMaxRecordType; i++) {
            byte[] tmp = {(byte) i};
            typeCrc[i] = FuzzCRC32C.value(tmp, 1);
        }
    }

    // Create a LogWriter that will append data to "dest".
    //   "dest" must have initial length "destLength".
    //   "dest" must remain OPEN while this LogWriter is in use.
    public LogWriter(WritableFile dest, Long destLength) {
        this.writableFile = dest;
        this.blockOffset = (int)(destLength % LogFormat.kBlockSize);
        initTypeCrc(this.typeCrc);
    }

    // Create a LogWriter that will append data to "dest".
    //   "dest" must have initially empty.
    //   "dest" must remain OPEN while this LogWriter is in use.
    public LogWriter(WritableFile dest) {
        this.writableFile = dest;
        this.blockOffset = 0;
        initTypeCrc(this.typeCrc);
    }

    public Status addRecord(final Slice slice) {
        byte[] data = slice.getData();
        Integer left = slice.getSize();
        // Fragment the record if necessary and emit it.  Note that if slice
        //  is empty, we still want to iterate once to emit a single
        //  zero-length record
        Status s = null;
        Boolean begin = true;
        do {
            final Integer leftOver = LogFormat.kBlockSize - blockOffset;
            Preconditions.checkArgument(leftOver >= 0);
            if(leftOver < LogFormat.kHeaderSize) {
                // if leftover size is less than header size,
                //   switch to a new block
                if(leftOver > 0) {
                    // fill the trailer using zero
                    // todo may have better padding char
                    Preconditions.checkArgument(LogFormat.kHeaderSize == 7);
                    writableFile.append(new Slice(new byte[LogFormat.kHeaderSize]));
                }
                this.blockOffset = 0;
            }
            // we never leave < kHeaderSize bytes in a block.
            Preconditions.checkArgument(LogFormat.kBlockSize - this.blockOffset >= LogFormat.kHeaderSize);

            Integer availableSize = LogFormat.kBlockSize - this.blockOffset - LogFormat.kHeaderSize;
            Integer fragmentSize = (left < availableSize) ? left : availableSize;

            LogFormat.RecordType type = null;
            final Boolean end = (left == fragmentSize);

            if(begin && end) {
                type = LogFormat.RecordType.kFullType;
            } else if(begin) {
                type = LogFormat.RecordType.kFirstType;
            } else if(end) {
                type = LogFormat.RecordType.kLastType;
            } else {
                type = LogFormat.RecordType.kMiddleType;
            }

        } while(true);
    }

    public Status emitPhysicalRecord(LogFormat.RecordType t, byte[] data, Integer n) {
        // must fit in two bytes
        Preconditions.checkArgument(n <= 0xffff);
        Preconditions.checkArgument(this.blockOffset + LogFormat.kHeaderSize + n <= LogFormat.kBlockSize);

        byte[] buf = new byte[LogFormat.kHeaderSize];
        buf[4] = (byte)(n & 0xff);
        buf[5] = (byte)(n >> 8);
        buf[6] = (byte)t.getValue();

        // compute the crc of the record type and the payload
        Integer crc = FuzzCRC32C.extend(this.typeCrc[t.getValue()], data, n);
        crc = FuzzCRC32C.Mask(crc);
        byte[] crcArray = Coding.EncodeFixed32(crc);
        // save crc32c value to first 4 bytes of header
        for(Integer i = 0; i < 4; i++)
            buf[i] = crcArray[i];

        // write header
        Status s = writableFile.append(new Slice(buf));
        if(s.ok()) {
            // write payload
            s = writableFile.append(new Slice(data));
            if(s.ok()) writableFile.flush();
        }
        this.blockOffset += LogFormat.kHeaderSize + n;
        return s;
    }
}
