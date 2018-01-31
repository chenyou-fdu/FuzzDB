package org.chenyou.fuzzdb.util;

import java.util.List;

/**
 * Created by ChenYou on 2017/11/12.
 */
public interface FilterPolicy {
    String getFilterName();

    void createFilter(List<Slice> keys, Integer n, List<Byte> dst);

    Boolean keyMatchWith(final Slice key, final Slice filter);
}
