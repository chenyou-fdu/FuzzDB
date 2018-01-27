package org.chenyou.fuzzdb.util.file;

import org.chenyou.fuzzdb.util.Brick;
import org.chenyou.fuzzdb.util.Status;

import java.io.FileInputStream;
import java.io.IOException;

public class SequentialFile {
    private String fileName;
    private FileInputStream fd;

    public SequentialFile(String fileName, FileInputStream fd) {
        this.fileName = fileName;
        this.fd = fd;
    }

    public Status read(Integer n, Brick result) {
        Status s = Status.OK();
        byte[] scratch = new byte[n];
        try {
            this.fd.read(scratch);
        } catch (IOException ex) {
            return Status.IOError(new Brick(this.fileName), new Brick("read failed"));
        }
        return s;
    }

}
