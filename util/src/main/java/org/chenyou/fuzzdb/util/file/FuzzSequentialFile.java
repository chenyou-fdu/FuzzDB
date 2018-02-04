package org.chenyou.fuzzdb.util.file;

import org.chenyou.fuzzdb.util.Slice;
import org.chenyou.fuzzdb.util.Status;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FuzzSequentialFile extends SequentialFile{
    private String fileName;
    private FileChannel fd;

    public FuzzSequentialFile(String fileName, FileChannel fd) {
        this.fileName = fileName;
        this.fd = fd;
    }

    @Override
    public Status read(Integer n, Slice result) {
        Status s = Status.OK();
        ByteBuffer scratch = ByteBuffer.allocate(n);
        Integer r = 0;
        while(n > 0) {
            try {
                r = this.fd.read(scratch);
                n -= r;
            } catch (IOException ex) {
                return Status.IOError(new Slice(this.fileName), new Slice("read failed"));
            }
        }
        //result = new Slice(new String(scratch.array()));
        result.setData(scratch.array());
        return s;
    }

    @Override
    public Status skip(Long n) {
        try {
            this.fd.position(n);
        } catch (IOException ex) {
            return Status.IOError(new Slice(this.fileName), new Slice("skip failed"));
        }
        return Status.OK();
    }

    @Override
    public Status close() {
        try {
            this.fd.close();
        } catch (IOException ex) {
            return Status.IOError(new Slice(this.fileName), null);
        }
        return Status.OK();
    }
}
