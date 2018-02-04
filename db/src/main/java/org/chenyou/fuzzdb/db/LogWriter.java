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
            // leftOver means unused size in a block
            //   since the header info can't be stored in different block
            //   so when leftOver < kHeaderSize, we store this record to next block
            final Integer leftOver = LogFormat.kBlockSize - blockOffset;
            Preconditions.checkArgument(leftOver >= 0);
            if(leftOver < LogFormat.kHeaderSize) {
                // if leftover size is less than header size,
                //   switch to a new block
                if(leftOver > 0) {
                    // fill this block using zero
                    // todo may have better padding char
                    Preconditions.checkArgument(LogFormat.kHeaderSize == 7);
                    writableFile.append(new Slice(new byte[leftOver]));
                }
                this.blockOffset = 0;
            }
            // double check block offset
            //   CAUTION: it's acceptable to stored full head info in the tail of a block
            Preconditions.checkArgument(LogFormat.kBlockSize - this.blockOffset >= LogFormat.kHeaderSize);

            Integer availableSize = LogFormat.kBlockSize - this.blockOffset - LogFormat.kHeaderSize;
            // left means un-append part of slice data
            Integer fragmentSize = (left < availableSize) ? left : availableSize;

            LogFormat.RecordType type = null;
            final Boolean end = (left == fragmentSize);
            // full means a full record stored in one block
            // begin means this part is the first part of one record
            // middle means this part is one of the middle part of one record
            // end means this part is the last part of one record
            if(begin && end) {
                type = LogFormat.RecordType.kFullType;
            } else if(begin) {
                type = LogFormat.RecordType.kFirstType;
            } else if(end) {
                type = LogFormat.RecordType.kLastType;
            } else {
                type = LogFormat.RecordType.kMiddleType;
            }
            Integer dataOffset = slice.getSize() - left;
            s = emitPhysicalRecord(type, data, dataOffset, fragmentSize);
            left -= fragmentSize;
            begin = false;
        } while(s.ok() && left > 0);
        return s;
    }

    public Status emitPhysicalRecord(LogFormat.RecordType t, byte[] data, Integer offset, Integer n) {
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
        // save crc32c value to first 4 bytes of header
        Coding.EncodeFixed32(buf, crc);

        // write header
        Status s = writableFile.append(new Slice(buf));
        if(s.ok()) {
            // write payload
            s = writableFile.append(new Slice(data, offset, n));
            if(s.ok()) writableFile.flush();
        }
        this.blockOffset += LogFormat.kHeaderSize + n;
        return s;
    }
}
