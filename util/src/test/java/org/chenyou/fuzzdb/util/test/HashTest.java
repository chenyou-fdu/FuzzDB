package org.chenyou.fuzzdb.util.test;

import org.chenyou.fuzzdb.util.Hash;
import org.junit.Assert;
import org.junit.Test;


/**
 * Created by ChenYou on 2017/11/12.
 */
public class HashTest {
    @Test
    public void EmptyTest() {
        byte[] test = {};
        Assert.assertEquals(Hash.hash(test, test.length, 0xbc9f1d34), (Integer)0xbc9f1d34);
    }

    @Test
    public void OneTest() {
        byte[] test = { 0x62 };
        Assert.assertEquals(Hash.hash(test, test.length, 0xbc9f1d34), (Integer)0xef1345c4);
    }

    @Test
    public void TwoTest() {
        byte[] test = {(byte)0xc3, (byte)0x97};
        Assert.assertEquals(Hash.hash(test, test.length, 0xbc9f1d34), (Integer)0x5b663814);
    }

    @Test
    public void ThreeTest() {
        byte[] test = {(byte)0xe2, (byte)0x99, (byte)0xa5};
        Assert.assertEquals(Hash.hash(test, test.length, 0xbc9f1d34), (Integer)0x323c078f);
    }

    @Test
    public void FourTest() {
        byte[] test = {(byte)0xe1, (byte)0x80, (byte)0xb9, (byte)0x32};
        Assert.assertEquals(Hash.hash(test, test.length, 0xbc9f1d34), (Integer)0xed21633a);
    }

    @Test
    public void FourEightTest() {
        byte[] test = {(byte)0x01, (byte)0xc0, (byte)0x00, (byte)0x00,
                (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
                (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
                (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
                (byte)0x14, (byte)0x00, (byte)0x00, (byte)0x00,
                (byte)0x00, (byte)0x00, (byte)0x04, (byte)0x00,
                (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x14,
                (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x18,
                (byte)0x28, (byte)0x00, (byte)0x00, (byte)0x00,
                (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
                (byte)0x02, (byte)0x00, (byte)0x00, (byte)0x00,
                (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00};
        Assert.assertEquals(Hash.hash(test, test.length, 0x12345678), (Integer)0xf333dabb);
    }
}
