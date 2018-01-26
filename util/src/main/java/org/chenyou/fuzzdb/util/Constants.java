package org.chenyou.fuzzdb.util;

public class Constants {
    private Constants() {}

    public static final String OS_NAME = System.getProperty("os.name");

    // Linux
    public static final Boolean LINUX = OS_NAME.startsWith("Linux");
    // Windows
    public static final Boolean WINDOWS = OS_NAME.startsWith("Windows");
    // SunOS
    public static final Boolean SUN_OS = OS_NAME.startsWith("SunOS");
    // Mac OS X
    public static final Boolean MAC_OS_X = OS_NAME.startsWith("Mac OS X");
    // FreeBSD
    public static final Boolean FREE_BSD = OS_NAME.startsWith("FreeBSD");
}
