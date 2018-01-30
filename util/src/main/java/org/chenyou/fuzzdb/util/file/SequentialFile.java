package org.chenyou.fuzzdb.util.file;

import org.chenyou.fuzzdb.util.Brick;
import org.chenyou.fuzzdb.util.Status;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;

public class SequentialFile {
    private String fileName;
    private FileChannel fd;

    public SequentialFile(String fileName, FileChannel fd) {
        this.fileName = fileName;
        this.fd = fd;
    }

    public Status read(Integer n, Brick result) {
        Status s = Status.OK();
        ByteBuffer scratch = ByteBuffer.allocate(n);
        Integer r = 0;
        while(n > 0) {
            try {
                r = this.fd.read(scratch);
                n -= r;
            } catch (IOException ex) {
                return Status.IOError(new Brick(this.fileName), new Brick("read failed"));
            }
        }
        result = new Brick(new String(scratch.array()));
        return s;
    }

    public Status skip(Long n) {
        try {
            this.fd.position(n);
        } catch (IOException ex) {
            return Status.IOError(new Brick(this.fileName), new Brick("skip failed"));
        }
        return Status.OK();
    }
}
