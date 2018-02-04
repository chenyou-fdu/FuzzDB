package org.chenyou.fuzzdb.util.file;

import org.chenyou.fuzzdb.util.Slice;
import org.chenyou.fuzzdb.util.Status;

public abstract class SequentialFile {
    public abstract Status read(Integer n, Slice result);
    public abstract Status skip(Long n);
    public abstract Status close();
}
