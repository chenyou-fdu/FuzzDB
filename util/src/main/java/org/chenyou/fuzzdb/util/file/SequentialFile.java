package org.chenyou.fuzzdb.util.file;

import org.chenyou.fuzzdb.util.Brick;
import org.chenyou.fuzzdb.util.Status;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;

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
        Integer r = 0;
        Integer offset = 0;
        while(r != -1) {
            try {
                r = this.fd.read(scratch, offset, n);
                n -= r;
                offset += r;
            } catch (IOException ex) {
                return Status.IOError(new Brick(this.fileName), new Brick("read failed"));
            }
        }
        result = new Brick(new String(scratch));
        return s;
    }

    public Status skip(Long n) {
        Long sNum = 0L;
        while(sNum < n) {
            try {
                sNum += this.fd.skip(n);
            } catch (IOException ex) {
                return Status.IOError(new Brick(this.fileName), new Brick("skip failed"));
            }
        }
        return Status.OK();
    }
}
