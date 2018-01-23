package org.chenyou.fuzzdb.db;

public class LogOperator {
    public enum RecordType {
        // kZeroType is reserved for preallocated files
        kZeroType,
        kFullType,
        // for fragments
        kFirstType, kMiddleType, kLastType
    }

    public static final RecordType kMaxRecordType = RecordType.kLastType;
    public static final Integer kBlobSize = 32768;
    // header is checksum (4 bytes), length (2 bytes), type (1 byte).
    public static final Integer kHeaderSize = 4 + 2 + 1;
}
