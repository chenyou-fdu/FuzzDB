package org.chenyou.fuzzdb.util;

import java.util.List;

/**
 * Created by ChenYou on 2017/11/12.
 */
public interface FilterPolicy {
    String getFilterName();

    void createFilter(List<Brick> keys, Integer n, List<Byte> dst);

    Boolean keyMatchWith(final Brick key, final Brick filter);
}
