package org.chenyou.fuzzdb.util.comparator;

import com.google.common.base.Preconditions;
import org.chenyou.fuzzdb.util.Slice;


public class BytewiseComparatorImpl implements FuzzComparator {
    public BytewiseComparatorImpl() {

    }

    static private BytewiseComparatorImpl bytewiseComparator;
    static public synchronized BytewiseComparatorImpl getInstance() {
        if(bytewiseComparator == null) {
            bytewiseComparator = new BytewiseComparatorImpl();
        }
        return bytewiseComparator;
    }

    @Override
    public String getName() {
        return "fuzzdb.BytewiseComparator";
    }

    @Override
    public Slice findShortestSeparator(String start, Slice limit) {
        // Find length of common prefix
        int minLen = Math.min(start.length(), limit.getSize());
        int diffIndex = 0;
        while ((diffIndex < minLen) && (start.getBytes()[diffIndex] == limit.getData()[diffIndex])) {
            diffIndex++;
        }

        if (diffIndex >= minLen) {
            // Do not shorten if one string is a prefix of the other
        } else {
            byte diffByte = start.getBytes()[diffIndex];
            int res1 = Byte.compareUnsigned(diffByte, (byte) 0xff);
            int res2 = Byte.compareUnsigned((byte) (diffByte + 1), limit.getData()[diffIndex]);
            if (res1 < 0 && res2 < 0) {
                byte[] res = new byte[diffIndex + 1];
                for (int i = 0; i < diffIndex + 1; i++) {
                    if (i == diffIndex) {
                        res[i] = (byte)(start.getBytes()[i] + 1);
                    } else {
                        res[i] = start.getBytes()[i];
                    }
                }
                Slice comSlice = new Slice(res);
                Preconditions.checkArgument(compare(comSlice, limit) < 0);
                return new Slice(res);
            }
        }
        return new Slice(start);
    }

    @Override
    public Slice findShortSuccessor(final Slice key) {
        int n = key.getSize();
        byte[] res = new byte[n];
        for (int i = 0; i < n; i++) {
            final byte b = key.getData()[i];
            res[i] = b;
            if(Byte.compareUnsigned(b, (byte)0xff) != 0) {
                res[i] = (byte)(b + 1);
                Slice resSlice = new Slice();
                resSlice.setData(res, i+1);
                return resSlice;
            }
        }
        return new Slice(res);
    }

    @Override
    public int compare(Object a, Object b) {
        Preconditions.checkArgument(a instanceof Slice);
        Preconditions.checkArgument(b instanceof Slice);
        Slice as = (Slice) a;
        Slice bs = (Slice) b;
        return as.compare(bs);
    }

}
