package org.chenyou.fuzzdb.util;

import org.chenyou.fuzzdb.db.DBFormat.ValueType;

public class ParsedInternalKey {
    public Slice userKey;
    public long sequence;
    public ValueType type;
    public ParsedInternalKey() {}
    public ParsedInternalKey(final Slice u, long seq, ValueType t) {
        this.userKey = u;
        this.sequence = seq;
        this.type = t;
    }


}
