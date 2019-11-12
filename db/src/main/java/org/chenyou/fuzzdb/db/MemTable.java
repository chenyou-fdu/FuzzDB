package org.chenyou.fuzzdb.db;

import org.chenyou.fuzzdb.util.Slice;
import org.chenyou.fuzzdb.util.comparator.InternalKeyComparator;

public class MemTable {
    static Slice getLengthPrefixedSlice(byte[] data) {
        int len;
        //p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
        //return Slice(p, len);
        return null;
    }
    public MemTable(InternalKeyComparator internalKeyComparator) {

    }

    private class KeyComparator {
        InternalKeyComparator comparator;
        KeyComparator(InternalKeyComparator c) {
            this.comparator = c;
        }
    }
}
