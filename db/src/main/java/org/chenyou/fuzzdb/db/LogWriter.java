package org.chenyou.fuzzdb.db;

import com.google.common.base.Preconditions;
import org.chenyou.fuzzdb.util.Coding;
import org.chenyou.fuzzdb.util.FuzzCRC32C;
import org.chenyou.fuzzdb.util.Slice;
import org.chenyou.fuzzdb.util.Status;
import org.chenyou.fuzzdb.util.file.WritableFile;


public class LogWriter {

    private WritableFile writableFile;
    private int blockOffset;
    //private int[] typeCrc = new int[LogFormat.K_MAX_RECORD_TYPE + 1];

    /*private static void initTypeCrc(int[] typeCrc) {
        Preconditions.checkNotNull(typeCrc);
        for (int i = 0; i <= LogFormat.K_MAX_RECORD_TYPE; i++) {
            byte[] tmp = {(byte) i};
            typeCrc[i] = FuzzCRC32C.value(tmp, 1);
        }
    }*/

    // Create a LogWriter that will append data to "dest".
    //   "dest" must have initial length "destLength".
    //   "dest" must remain OPEN while this LogWriter is in use.
    public LogWriter(WritableFile dest, long destLength) {
        this.writableFile = dest;
        this.blockOffset = (int) (destLength % LogFormat.K_BLOCK_SIZE);
        //initTypeCrc(this.typeCrc);
    }

    // Create a LogWriter that will append data to "dest".
    //   "dest" must have initially empty.
    //   "dest" must remain OPEN while this LogWriter is in use.
    public LogWriter(WritableFile dest) {
        this.writableFile = dest;
        this.blockOffset = 0;
        //initTypeCrc(this.typeCrc);
    }

    public Status addRecord(final Slice slice) {
        byte[] data = slice.getData();
        int left = slice.getSize();
        // Fragment the record if necessary and emit it.  Note that if slice
        //  is empty, we still want to iterate once to emit a single
        //  zero-length record
        Status s = null;
        boolean begin = true;
        do {
            // leftOver means unused size in a block
            //   since the header info can't be stored in different block
            //   so when leftOver < kHeaderSize, we store this record to next block
            final int leftOver = LogFormat.K_BLOCK_SIZE - blockOffset;
            Preconditions.checkArgument(leftOver >= 0);
            if (leftOver < LogFormat.K_HEADER_SIZE) {
                // if leftover size is less than header size,
                //   switch to a new block
                if (leftOver > 0) {
                    // fill this block using zero
                    Preconditions.checkArgument(LogFormat.K_HEADER_SIZE == 7);
                    // todo may have better padding char or recheck length
                    // writableFile.append(new Slice(new byte[leftOver]));
                    writableFile.append(new Slice("\\x00\\x00\\x00\\x00\\x00\\x00", leftOver));
                }
                this.blockOffset = 0;
            }
            // double check block offset
            //   we never leave < kHeaderSize bytes in a block.
            //   so it's acceptable to stored full head info in the tail of a block
            Preconditions.checkArgument((LogFormat.K_BLOCK_SIZE - this.blockOffset) >= LogFormat.K_HEADER_SIZE);

            int availableSize = LogFormat.K_BLOCK_SIZE - this.blockOffset - LogFormat.K_HEADER_SIZE;
            // left means un-append part of slice data
            int fragmentSize = Math.min(left, availableSize);

            LogFormat.RecordType type = null;
            final boolean end = (left == fragmentSize);
            // kFullType means a full record stored in one block
            // kFirstType means this part is the first part of one record
            // kMiddleType means this part is one of the middle part of one record
            // kLastType means this part is the last part of one record
            if (begin && end) {
                type = LogFormat.RecordType.kFullType;
            } else if (begin) {
                type = LogFormat.RecordType.kFirstType;
            } else if (end) {
                type = LogFormat.RecordType.kLastType;
            } else {
                type = LogFormat.RecordType.kMiddleType;
            }
            // how many data of this slice is append
            int dataOffset = slice.getSize() - left;
            // will write Header and payload to WriteableFile
            s = emitPhysicalRecord(type, data, dataOffset, fragmentSize);
            left -= fragmentSize;
            // begin is true only in first round of addRecord
            begin = false;
        } while (s.ok() && left > 0);
        return s;
    }

    public Status emitPhysicalRecord(LogFormat.RecordType t, byte[] data, int offset, int n) {
        // must fit in two bytes
        Preconditions.checkArgument(n <= 0xffff);
        Preconditions.checkArgument(this.blockOffset + LogFormat.K_HEADER_SIZE + n <= LogFormat.K_BLOCK_SIZE);

        byte[] buf = new byte[LogFormat.K_HEADER_SIZE];
        buf[4] = (byte) (n & 0xff);
        buf[5] = (byte) (n >> 8);
        buf[6] = (byte) t.getValue();

        // compute the crc of the record type and the payload
        //Integer crc = FuzzCRC32C.extend(this.typeCrc[t.getValue()], data, n);
        Long crcLong = FuzzCRC32C.value(t.getValue(), data, offset, n);
        int crc = FuzzCRC32C.mask(crcLong.intValue());
        // save crc32c value to first 4 bytes of header
        Coding.EncodeFixed32(buf, crc);

        // write header
        Status s = writableFile.append(new Slice(buf));
        if (s.ok()) {
            // write payload
            s = writableFile.append(new Slice(data, offset, n));
            if (s.ok()) {
                writableFile.flush();
            }
        }
        this.blockOffset += LogFormat.K_HEADER_SIZE + n;
        return s;
    }
}
