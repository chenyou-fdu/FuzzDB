package org.chenyou.fuzzdb.db;

import com.google.common.base.Preconditions;
import org.chenyou.fuzzdb.util.Coding;
import org.chenyou.fuzzdb.util.ParsedInternalKey;
import org.chenyou.fuzzdb.util.Slice;

import java.util.List;

public class DBFormat {
    // Value types encoded as the last component of internal keys.
    // DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
    // data structures.
    public enum ValueType {
        kTypeDeletion(0),
        kTypeValue(1);
        private int valueTypeValue;

        ValueType(int i) {
            this.valueTypeValue = i;
        }

        public int getValue() {
            return this.valueTypeValue;
        }
    }

    static public final ValueType kValueTypeForSeek = ValueType.kTypeValue;
    static public final long kMaxSequenceNumber = ((0x1L << 56) - 1);

    // Returns the user key portion of an internal key.
    static public Slice extractUserKey(final Slice internalKey) {
        Preconditions.checkArgument(internalKey.getSize() >= 8);
        Slice res = new Slice();
        res.setData(internalKey.getData(), internalKey.getSize() - 8);
        return res;
    }

    static public long packSequenceAndType(long seq, ValueType t) {
        Preconditions.checkArgument(seq <= kMaxSequenceNumber);
        Preconditions.checkArgument(t.getValue() <= kValueTypeForSeek.getValue());
        return ((seq << 8) | t.getValue());
    }

    static public void appendInternalKey(List<Byte> result, final ParsedInternalKey key) {
        Slice userKey = key.userKey;
        for(int i = 0; i < userKey.getSize(); i++) {
            result.add(userKey.getData()[i]);
        }
        Coding.PutFixed64(result, packSequenceAndType(key.sequence, key.type));
    }

    static public boolean parseInternalKey(final Slice internalKey, ParsedInternalKey result) {
        final int len = internalKey.getSize();
        if(len < 8) {
            return false;
        }
        long num = Coding.DecodeFixed64(internalKey.getData(), len - 8);
        byte c = (byte)(num & 0xff);
        result.sequence = num >> 8;
        result.type = (c == 0 ? ValueType.kTypeDeletion : ValueType.kTypeValue);
        Slice newUserKey = new Slice();
        newUserKey.setData(internalKey.getData(), len - 8);
        result.userKey = newUserKey;
        return (c <= ValueType.kTypeValue.getValue());
    }


}
