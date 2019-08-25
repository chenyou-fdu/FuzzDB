package org.chenyou.fuzzdb.util.file;

import org.chenyou.fuzzdb.util.Slice;
import org.chenyou.fuzzdb.util.Status;

public abstract class SequentialFile {
    // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
    // written by this routine.  Sets "result" to the data that was
    // read (including if fewer than "n" bytes were successfully read).
    // May set "*result" to point at data in "scratch[0..n-1]", so
    // "scratch[0..n-1]" must be live when "*result" is used.
    // If an error was encountered, returns a non-OK status.
    //
    // REQUIRES: External synchronization
    public abstract Status read(int n, Slice result);

    // Skip "n" bytes from the file. This is guaranteed to be no
    // slower that reading the same data, but may be faster.
    //
    // If end of file is reached, skipping will stop at the end of the
    // file, and Skip will return OK.
    //
    // REQUIRES: External synchronization
    public abstract Status skip(long n);
    public abstract Status close();
}
