package org.chenyou.fuzzdb.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by ChenYou on 2017/11/7.
 */
public class SliceTest {
    @Test
    public void EmptyTest() {
        Slice emptySlice = new Slice();
        Boolean isEmpty = emptySlice.isEmpty();
        Assert.assertTrue(isEmpty);
        Slice loadedSlice = new Slice("EmptyTest");
        isEmpty = loadedSlice.isEmpty();
        Assert.assertFalse(isEmpty);
    }

    @Test
    public void GetTest() {
        Slice loadedSlice = new Slice("GetTest");
        Assert.assertEquals('G', loadedSlice.get(0));
        Assert.assertEquals('t', loadedSlice.get(2));
        Assert.assertEquals('s', loadedSlice.get(5));
        boolean isEx = false;
        try {
            loadedSlice.get(100);
        } catch (IllegalArgumentException ex) {
            isEx = true;
        }
        Assert.assertTrue(isEx);
    }

    @Test
    public void ClearTest() {
        Slice loadedSlice = new Slice("ClearTest");
        Assert.assertFalse(loadedSlice.isEmpty());
        loadedSlice.clear();
        Assert.assertTrue(loadedSlice.isEmpty());
    }

    @Test
    public void GetStringTest() {
        String getTestStr = "GetStringTest";
        Slice loadedSlice = new Slice(getTestStr);
        Assert.assertEquals(getTestStr, loadedSlice.toString());
    }

    @Test
    public void RemovePrefixTest() {
        //List<Byte> data = new ArrayList<>(Bytes.asList("RemovePrefixTest".getBytes()));
        Slice loadedSlice = new Slice("RemovePrefixTest".getBytes());
        loadedSlice.removePrefix(5);
        Assert.assertEquals("ePrefixTest", loadedSlice.toString());
        loadedSlice.removePrefix(3);
        Assert.assertEquals("efixTest", loadedSlice.toString());
        loadedSlice.removePrefix(8);
        Assert.assertEquals("", loadedSlice.toString());
        Assert.assertTrue(loadedSlice.isEmpty());
        boolean isEx = false;
        try {
            loadedSlice.removePrefix(100);
        } catch (IllegalArgumentException ex) {
            isEx = true;
        }
        Assert.assertTrue(isEx);
    }

    @Test
    public void MemCmpTest() {
        Slice aSlice = new Slice("MemCmpTest");
        Slice bSlice = new Slice("MemCmpTest");
        Slice cSlice = new Slice("PemCmpTest");
        Slice dSlice = new Slice("MemCmpTest$$$$", 10);
        Assert.assertEquals((Integer)0, Slice.memcmp(aSlice, bSlice, 10));
        Assert.assertEquals((Integer)('M'-'P'), Slice.memcmp(bSlice, cSlice, 10));
        boolean isEx = false;
        try {
            Slice.memcmp(aSlice, bSlice, 110);
        } catch (IllegalArgumentException ex) {
            isEx = true;
        }
        Assert.assertTrue(isEx);
    }

    @Test
    public void CompareTest() {
        Slice aSlice = new Slice("CompareTest");
        Slice bSlice = new Slice("CompareTest");
        Slice cSlice = new Slice("DompareTest");
        Slice dSlice = new Slice("AompareTest");
        Slice eSlice = new Slice("Comp");
        Assert.assertEquals((Integer)0, aSlice.compare(bSlice));
        Assert.assertTrue(aSlice.compare(cSlice) < 0);
        Assert.assertTrue(aSlice.compare(dSlice) > 0);
        Assert.assertTrue(aSlice.compare(eSlice) > 0);
    }

    @Test
    public void StartWithTest() {
        Slice aSlice = new Slice("CompareTest");

        Assert.assertTrue(aSlice.startWith(new Slice("Com")));
        Assert.assertTrue(aSlice.startWith(new Slice("Compare")));
        Assert.assertFalse(aSlice.startWith(new Slice("Dick")));
    }

    @Test
    public void EqualTest() {
        Slice aSlice = new Slice("12345");
        Slice bSlice = new Slice("abcde");
        String cString = "test";
        Assert.assertFalse(aSlice.equals(bSlice));
        Assert.assertTrue(aSlice.notEquals(bSlice));
        Assert.assertTrue(aSlice.notEquals(cString));
    }

    @Test
    public void SetTest() {
        Slice aSlice = new Slice("abcde");
        aSlice.setData("12345".getBytes());
        Slice bSlice = new Slice("12345");
        Assert.assertEquals(aSlice, bSlice);
    }
}
