package org.chenyou.fuzzdb.util.test;

import com.google.common.primitives.Bytes;
import org.chenyou.fuzzdb.util.Brick;
import org.junit.Test;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ChenYou on 2017/11/7.
 */
public class BrickTest {
    @Test
    public void EmptyTest() {
        Brick emptyBrick = new Brick();
        Boolean isEmpty = emptyBrick.isEmpty();
        Assert.assertTrue(isEmpty);
        Brick loadedBrick = new Brick("EmptyTest");
        isEmpty = loadedBrick.isEmpty();
        Assert.assertFalse(isEmpty);
    }

    @Test
    public void GetTest() {
        Brick loadedBrick = new Brick("GetTest");
        Assert.assertEquals((Byte)(byte)'G', loadedBrick.get(0));
        Assert.assertEquals((Byte)(byte)'t', loadedBrick.get(2));
        Assert.assertEquals((Byte)(byte)'s', loadedBrick.get(5));
        Boolean isEx = false;
        try {
            loadedBrick.get(100);
        } catch (IndexOutOfBoundsException ex) {
            isEx = true;
        }
        Assert.assertTrue(isEx);
    }

    @Test
    public void ClearTest() {
        Brick loadedBrick = new Brick("ClearTest");
        Assert.assertFalse(loadedBrick.isEmpty());
        loadedBrick.clear();
        Assert.assertTrue(loadedBrick.isEmpty());
    }

    @Test
    public void GetStringTest() {
        String getTestStr = "GetStringTest";
        Brick loadedBrick = new Brick(getTestStr);
        Assert.assertEquals(getTestStr, loadedBrick.toString());
    }

    @Test
    public void RemovePrefixTest() {
        List<Byte> data = new ArrayList<>(Bytes.asList("RemovePrefixTest".getBytes()));
        Brick loadedBrick = new Brick(data);
        loadedBrick.removePrefix(5);
        Assert.assertEquals("ePrefixTest", loadedBrick.toString());
        loadedBrick.removePrefix(3);
        Assert.assertEquals("efixTest", loadedBrick.toString());
        loadedBrick.removePrefix(8);
        Assert.assertEquals("", loadedBrick.toString());
        Assert.assertTrue(loadedBrick.isEmpty());
        Boolean isEx = false;
        try {
            loadedBrick.removePrefix(100);
        } catch (IndexOutOfBoundsException ex) {
            isEx = true;
        }
        Assert.assertTrue(isEx);
    }

    @Test
    public void MemCmpTest() {
        Brick aBrick = new Brick("MemCmpTest");
        Brick bBrick = new Brick("MemCmpTest");
        Brick cBrick = new Brick("PemCmpTest");
        Brick dBrick = new Brick("MemCmpTest$$$$", 10);
        Assert.assertEquals((Integer)0, Brick.memcmp(aBrick, bBrick, 10));
        Assert.assertEquals((Integer)('M'-'P'), Brick.memcmp(bBrick, cBrick, 10));
        Boolean isEx = false;
        try {
            Brick.memcmp(aBrick, bBrick, 110);
        } catch (IndexOutOfBoundsException ex) {
            isEx = true;
        }
        Assert.assertTrue(isEx);
    }

    @Test
    public void CompareTest() {
        Brick aBrick = new Brick("CompareTest");
        Brick bBrick = new Brick("CompareTest");
        Brick cBrick = new Brick("DompareTest");
        Brick dBrick = new Brick("AompareTest");
        Brick eBrick = new Brick("Comp");
        Assert.assertEquals((Integer)0, aBrick.compare(bBrick));
        Assert.assertTrue(aBrick.compare(cBrick) < 0);
        Assert.assertTrue(aBrick.compare(dBrick) > 0);
        Assert.assertTrue(aBrick.compare(eBrick) > 0);
    }

    @Test
    public void StartWithTest() {
        Brick aBrick = new Brick("CompareTest");

        Assert.assertTrue(aBrick.startWith(new Brick("Com")));
        Assert.assertTrue(aBrick.startWith(new Brick("Compare")));
        Assert.assertFalse(aBrick.startWith(new Brick("Dick")));
    }

    @Test
    public void EqualTest() {
        Brick aBrick = new Brick("12345");
        Brick bBrick = new Brick("abcde");
        String cString = "test";
        Assert.assertFalse(aBrick.equals(bBrick));
        Assert.assertTrue(aBrick.notEquals(bBrick));
        Assert.assertTrue(aBrick.notEquals(cString));
    }
}
