package org.chenyou.fuzzdb.util;

import com.google.common.primitives.Bytes;
import java.util.List;

/**
 * Created by ChenYou on 2017/11/12.
 */
public class BloomFilter implements FilterPolicy {
    // num of bits for each key,
    // larger bitsPerKey less false positive rate
    private Integer bitsPerKey;
    // num of hash function simulate here
    private Integer k;

    BloomFilter(Integer bitsPerKey) {
        this.bitsPerKey = bitsPerKey;
        this.k = new Double(this.bitsPerKey * 0.69).intValue();
        if(this.k < 1) this.k = 1;
        if(this.k > 30) this.k = 30;
    }

    @Override
    public String getFilterName() {
        return "FuzzDB.BloomFilter";
    }

    @Override
    public void createFilter(List<Slice> keys, Integer n, List<Byte> dst) {
        Integer bits = n * bitsPerKey;

        // min bloom filter size to reduce false positive rate
        if(bits < 64) bits = 64;

        // since dst stores bytes ( 8 bits )
        // we rounds up bits to multiplier of eight
        Integer bytes = (bits + 7)  / 8;
        bits = bytes * 8;

        // allocate space at the back of dst,
        // to insert new filter to the back of total filters
        Integer init_size = dst.size();

        //dst.setLength(init_size + bytes);
        for(Integer i = init_size; i < init_size + bytes; i++) {
            dst.add((byte)0);
        }

        // record num of probes in the end of dst
        dst.add(k.byteValue());

        // store n keys in filter
        for(Integer i = 0; i < n; i++) {
            // first hash
            Integer h = bloomHash(keys.get(i));
            // rotate right 17 bits to create new hash
            // each result of hash function : j * delta + h
            // we can view this process as a double hash
            Integer delta = (h >>> 17) | (h << 15);
            for(Integer j = 0; j < k; j++) {
                Integer bitpos = Integer.remainderUnsigned(h, bits);//h % bits;
                Integer bitposDiv8 = Integer.divideUnsigned(bitpos, 8);
                Integer bitposMod8 = Integer.remainderUnsigned(bitpos, 8);
                Byte oriVal = dst.get(init_size + bitposDiv8);
                Byte hashBit = (byte)(1 << bitposMod8);
                // set bit pos caculated by hash to one
                dst.set(init_size + bitposDiv8, (byte)(oriVal | hashBit));
                h += delta;
            }
        }

    }

    @Override
    public Boolean keyMatchWith(final Slice key, final Slice filter) {
        Integer len = filter.getSize();
        if(len < 2) return false;
        //List<Byte> array = filter.getData();
        byte[] array = filter.getData();
        Integer bits = (len - 1) * 8;
        Integer k = (int)array[len-1];
        if(k > 30) return true;
        Integer h = bloomHash(key);
        Integer delta = (h >>> 17) | (h << 15);
        for(Integer i = 0; i < k; i++) {
            Integer bitpos = Integer.remainderUnsigned(h, bits);
            Integer bitposDiv8 = Integer.divideUnsigned(bitpos, 8);
            Integer bitposMod8 = Integer.remainderUnsigned(bitpos, 8);
            if((array[bitposDiv8] & (1 << bitposMod8)) == 0) return false;
            h += delta;
        }
        return true;
    }

    static private Integer bloomHash(final Slice key) {
        //List<Byte> keyData = key.getData();
        return Hash.hash(key.getData(), key.getSize(), 0xbc9f1d34);
    }

    static public FilterPolicy newBloomFilterPolicy(Integer bitsPerKey) {
        return new BloomFilter(bitsPerKey);
    }
}
