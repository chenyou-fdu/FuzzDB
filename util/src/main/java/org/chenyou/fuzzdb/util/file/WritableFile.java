package org.chenyou.fuzzdb.util.file;

import org.chenyou.fuzzdb.util.Slice;
import org.chenyou.fuzzdb.util.Status;

public abstract class WritableFile {
    public abstract Status append(final Slice data);
    public abstract Status close();
    public abstract Status flush();
    public abstract Status sync();
}
