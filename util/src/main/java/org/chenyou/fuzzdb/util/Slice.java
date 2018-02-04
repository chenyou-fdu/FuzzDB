package org.chenyou.fuzzdb.util;

import java.util.ArrayList;
import com.google.common.primitives.Bytes;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ChenYou on 2017/11/5.
 */
public class Slice {
    private byte[] data;
    private Integer size;
    private Integer offset = 0;
    public Slice() {
        this.data = null;
        this.size = 0;
    }

    public Slice(String data, Integer size) {
        this.data = data.getBytes();
        this.size = size;
    }

    public Slice(String data) {
        this.data = data.getBytes();
        this.size = data.length();
    }
    public Slice(byte[] data) {
        this.data = data;
        this.size = data.length;
    }

    public Slice(byte[] data, Integer offset, Integer size) {
        this.data = new byte[size];
        for(Integer i = 0; i < size; i++) {
            this.data[i] = data[i+offset];
        }
        this.size = size;
    }

    public byte[] getData() {
        return this.data;
    }
    public Integer getSize() {
        return size;
    }
    public Boolean isEmpty() {
        return this.size == 0;
    }
    public byte get(Integer idx) {
        if(idx >= size) throw new IndexOutOfBoundsException("get index: " + idx + ", all length: " + size);
        return this.data[idx];
    }
    public void clear() {
        this.size = 0;
        this.data = null;
    }
    public void setData(byte[] data) {
        this.data = data;
        this.size = data.length;
    }

    // may not that efficient
    public void removePrefix(Integer n) {
        if(n < 0 || n > size) throw new IndexOutOfBoundsException("remove length: " + n + ", all length: " + size);
        size -= n;
        byte[] newArray = new byte[size];
        System.arraycopy(data, n, newArray, 0, size);
        this.data = newArray;
    }
    @Override
    public String toString() {
        return new String(this.data);
    }

    @Override
    public boolean equals(Object o) {
        if(o == this) return true;
        if(!(o instanceof Slice)) return false;
        Slice b = (Slice) o;
        return Arrays.equals(getData(), b.getData());
    }

    public Boolean notEquals(Object o) {
        return !equals(o);
    }

    // this < b : neg ; this == b : zero ; this > b : pos
    public Integer compare(Slice b) {
        Integer minLen = (getSize() < b.getSize()) ? size : b.getSize();
        Integer r = memcmp(this, b, minLen);
        if(r == 0) {
            if(getSize() < b.getSize()) r = -1;
            else if(getSize() > b.getSize()) r = 1;
        }
        return r;
    }

    public Boolean startWith(Slice b) {
        return (getSize() >= b.getSize()) && (memcmp(this, b, b.getSize()) == 0);
    }

    static public Integer memcmp(Slice a, Slice b, Integer n) {
        Integer r = 0;
        for(int i = 0; i < n; i++) {
            if (a.get(i) != (b.get(i))) {
                r = a.get(i) - b.get(i);
                break;
            }
        }
        return r;
    }
}
