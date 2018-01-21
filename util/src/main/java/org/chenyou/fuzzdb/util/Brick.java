package org.chenyou.fuzzdb.util;

import java.util.ArrayList;
import com.google.common.primitives.Bytes;
import java.util.List;

/**
 * Created by ChenYou on 2017/11/5.
 */
public class Brick {
    private List<Byte> data;
    private Integer size;
    public Brick() {
        this.data = new ArrayList<>();
        this.size = 0;
    }

    public Brick(String data, int size) {
        this.data = new ArrayList<>(Bytes.asList(data.getBytes()));
        this.size = size;
    }

    public Brick(String data) {
        this.data = new ArrayList<>(Bytes.asList(data.getBytes()));
        this.size = data.length();
    }

    public Brick(List<Byte> data) {
        this.data = data;
        this.size = data.size();
    }

    public List<Byte> getData() {
        return data;
    }

    public Integer getSize() {
        return size;
    }

    public Boolean isEmpty() {
        return size == 0;
    }

    public Byte get(Integer idx) {
        if(idx >= size) throw new IndexOutOfBoundsException();
        return this.data.get(idx);
    }

    public void clear() {
        size = 0;
        data.clear();
    }

    public void removePrefix(Integer n) {
        if(n > size) throw new IndexOutOfBoundsException();
        List<Byte> newData = new ArrayList<>();
        for(Integer i = n; i < size; i++) {
            newData.add(data.get(i));
        }
        data = newData;
        size -= n;
    }

    @Override
    public String toString() {
        return new String(Bytes.toArray(data));
    }

    // this < b : neg ; this == b : zero ; this > b : pos
    public Integer compare(Brick b) {
        Integer minLen = (getSize() < b.getSize()) ? size : b.getSize();
        Integer r = memcmp(this, b, minLen);
        if(r == 0) {
            if(getSize() < b.getSize()) r = -1;
            else if(getSize() > b.getSize()) r = 1;
        }
        return r;
    }

    public Boolean startWith(Brick b) {
        return (getSize() >= b.getSize()) && (memcmp(this, b, b.getSize()) == 0);
    }

    @Override
    public boolean equals(Object o) {
        if(o == this) return true;
        if(!(o instanceof Brick)) return false;
        Brick b = (Brick) o;
        return getData().equals(b.getData());
    }

    public Boolean notEquals(Object o) {
        return !equals(o);
    }

    static public Integer memcmp(Brick a, Brick b, Integer n) {
        Integer r = 0;
        for(int i = 0; i < n; i++) {
            if (!a.get(i).equals(b.get(i))) {
                r = a.get(i) - b.get(i);
                break;
            }
        }
        return r;
    }
}
