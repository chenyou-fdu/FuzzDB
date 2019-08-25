package org.chenyou.fuzzdb.db;

public class LogFormat {
    public enum RecordType {
        // kZeroType is reserved for preallocated files
        kZeroType(0),
        kFullType(1),
        // for fragments
        kFirstType(2), kMiddleType(3), kLastType(4),
        kEof(5),
        kBadRecord(6);

        private int recordTypeValue;
        RecordType(int i) {
            this.recordTypeValue = i;
        }
        public int getValue() {
            return this.recordTypeValue;
        }
    }

    public static final int K_MAX_RECORD_TYPE = RecordType.kLastType.getValue();
    public static final int K_BLOCK_SIZE = 32768;
    // header is checksum (4 bytes), length (2 bytes), type (1 byte).
    public static final int K_HEADER_SIZE = 4 + 2 + 1;
}
