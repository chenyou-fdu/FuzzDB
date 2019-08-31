package org.chenyou.fuzzdb.util.comparator;

import org.apache.commons.lang3.ArrayUtils;
import org.chenyou.fuzzdb.db.DBFormat;
import org.chenyou.fuzzdb.util.ParsedInternalKey;
import org.chenyou.fuzzdb.util.Slice;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.chenyou.fuzzdb.db.DBFormat.ValueType.kTypeDeletion;
import static org.chenyou.fuzzdb.db.DBFormat.ValueType.kTypeValue;

public class ComparatorTest {

    static String IKey(String userKey, long seq, DBFormat.ValueType vt) {
        return IKey(new Slice(userKey), seq, vt);
    }

    static String IKey(Slice userKey, long seq, DBFormat.ValueType vt) {
        List<Byte> IKeyRes = IKeyInner(userKey, seq, vt);
        Byte[] iKeyByte = new Byte[IKeyRes.size()];
        IKeyRes.toArray(iKeyByte);
        return new String(ArrayUtils.toPrimitive(iKeyByte));
    }

    static List<Byte> IKeyInner(Slice userKey, long seq, DBFormat.ValueType vt) {
        List<Byte> encoded = new ArrayList<>();
        DBFormat.appendInternalKey(encoded, new ParsedInternalKey(userKey, seq, vt));
        return encoded;
    }

    static Slice Shorten(String result, Slice limit) {
        InternalKeyComparator internalKeyComparator = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
        return internalKeyComparator.findShortestSeparator(result, limit);
    }

    static Slice ShortSuccessor(String s) {
        Slice sSlice = new Slice(s);
        InternalKeyComparator internalKeyComparator = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
        Slice result = internalKeyComparator.findShortSuccessor(sSlice);
        return result;
    }
    static Slice ShortSuccessor(List<Byte> sList) {
        Slice sSlice = new Slice(sList);
        InternalKeyComparator internalKeyComparator = new InternalKeyComparator(BytewiseComparatorImpl.getInstance());
        Slice result = internalKeyComparator.findShortSuccessor(sSlice);
        return result;
    }


    static void TestKey(Slice key, long seq, DBFormat.ValueType vt) {
        List<Byte> encode = IKeyInner(key, seq, vt);
        Slice in = new Slice(encode);
        ParsedInternalKey decoded = new ParsedInternalKey(new Slice(""), 0, kTypeValue);
        Assert.assertTrue(DBFormat.parseInternalKey(in, decoded));
        Assert.assertEquals(key.toString(), decoded.userKey.toString());
        Assert.assertEquals(seq, decoded.sequence);
        Assert.assertEquals(vt, decoded.type);
        Assert.assertFalse(DBFormat.parseInternalKey(new Slice("bar"), decoded));
    }

    @Test
    public void testInternalKeyDecodeEncode() {
        String[] keys = new String[] {"", "k", "hello", "longggggggggggggggggggggg"};
        long[] seq = new long[] {1, 2, 3,
                (1L << 8) - 1,
                1L << 8,
                (1L << 8) + 1,
                (1L << 16) - 1,
                1L << 16,
                (1L << 16) + 1,
                (1L << 32) - 1,
                1L << 32,
                (1L << 32) + 1};
        for (int k = 0; k < keys.length; k++) {
            for (int s = 0; s < seq.length; s++) {
                TestKey(new Slice(keys[k]), seq[s], kTypeValue);
                TestKey(new Slice("hello"), 1, kTypeDeletion);
            }
        }
    }

    @Test
    public void testInternalKeyShortSeparator() {
        // When user keys are same
        Assert.assertEquals(IKey("foo", 100, kTypeValue), Shorten(IKey("foo", 100, kTypeValue), new Slice(IKey("foo", 99, kTypeValue))).toString());
        Assert.assertEquals(IKey("foo", 100, kTypeValue), Shorten(IKey("foo", 100, kTypeValue), new Slice(IKey("foo", 101, kTypeValue))).toString());
        Assert.assertEquals(IKey("foo", 100, kTypeValue), Shorten(IKey("foo", 100, kTypeValue), new Slice(IKey("foo", 100, kTypeValue))).toString());
        // When user keys are misordered
        Assert.assertEquals(IKey("foo", 100, kTypeValue), Shorten(IKey("foo", 100, kTypeValue), new Slice(IKey("foo", 99, kTypeValue))).toString());
        // When user keys are different, but correctly ordered
        Assert.assertEquals(IKey("g", DBFormat.kMaxSequenceNumber, DBFormat.kValueTypeForSeek), Shorten(IKey("foo", 100, kTypeValue), new Slice(IKey("hello", 200, kTypeValue))).toString());
        // When start user key is prefix of limit user key
        Assert.assertEquals(IKey("foo", 100, kTypeValue), Shorten(IKey("foo", 100, kTypeValue), new Slice(IKey("foobar", 200, kTypeValue))).toString());
        // When limit user key is prefix of start user key
        Assert.assertEquals(IKey("foobar", 100, kTypeValue), Shorten(IKey("foobar", 100, kTypeValue), new Slice(IKey("foo", 200, kTypeValue))).toString());
    }

    @Test
    public void testInternalKeyShortestSuccessor() {
        List<Byte> leftList = IKeyInner(new Slice(new byte[]{(byte)0xff, (byte)0xff}), 100, kTypeValue);
        List<Byte> rightInner = IKeyInner(new Slice(new byte[]{(byte)0xff, (byte)0xff}), 100, kTypeValue);
        Slice right = ShortSuccessor(rightInner);
        Assert.assertEquals(new Slice(leftList), right);
    }

    /*
    private static class TestComparator implements FuzzComparator {
        @Override
        public String getName() {
            return "fuzzdb.TestComparator";
        }

        @Override
        public Slice findShortestSeparator(String start, Slice limit) {
            return BytewiseComparatorImpl.getInstance().findShortestSeparator(start, limit);
        }

        @Override
        public Slice findShortSuccessor(String key) {
            return BytewiseComparatorImpl.getInstance().findShortSuccessor(key);
        }

        @Override
        public int compare(Object o, Object t1) {
            return BytewiseComparatorImpl.getInstance().compare(o, t1);
        }
    }
     */
}
