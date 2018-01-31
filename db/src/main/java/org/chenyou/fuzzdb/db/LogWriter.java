package org.chenyou.fuzzdb.db;

import org.chenyou.fuzzdb.util.file.WritableFile;

public class LogWriter  {

    // Create a LogWriter that will append data to "dest".
    //   "dest" must have initial length "destLength".
    //   "dest" must remain OPEN while this LogWriter is in use.
    public LogWriter(WritableFile dest, Long destLenght) {

    }

    // Create a LogWriter that will append data to "dest".
    //   "dest" must have initially empty.
    //   "dest" must remain OPEN while this LogWriter is in use.
    public LogWriter(WritableFile dest) {

    }
}
