package org.chenyou.fuzzdb.util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeHelper {

    private final static Unsafe UNSAFE;
    private final static Throwable UNSUPPORTED;
    static {
        Unsafe unsafe = null;
        Throwable unsupported = null;
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = (Unsafe) theUnsafe.get(null);
        } catch (Throwable e) {
            unsupported = e;
        }

        UNSAFE = unsafe;
        UNSUPPORTED = unsupported;
    }


    public static Unsafe getUnsafe() throws UnsupportedOperationException {
        Unsafe u = UNSAFE;
        if (u == null) {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }
        return u;
    }
}
