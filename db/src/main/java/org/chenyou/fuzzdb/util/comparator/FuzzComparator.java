package org.chenyou.fuzzdb.util.comparator;

import org.chenyou.fuzzdb.util.Slice;

import java.util.Comparator;

public interface FuzzComparator extends Comparator {
    String getName();
    Slice findShortestSeparator(String start, final Slice limit);
    Slice findShortSuccessor(final Slice key);
}
