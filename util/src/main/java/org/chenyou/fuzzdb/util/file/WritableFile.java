package org.chenyou.fuzzdb.util.file;

import org.chenyou.fuzzdb.util.Brick;
import org.chenyou.fuzzdb.util.Status;

import java.io.FileOutputStream;
import java.io.IOException;

import com.google.common.primitives.Bytes;

public class WritableFile {
    private String fileName;
    private FileOutputStream fd;
    private Integer pos;
    private byte[] buf;
    private final static Integer kBufSize = 65536;
    private Status flushBuffered() {
        Status s = writeRaw(buf, 0, pos);
        pos = 0;
        return s;
    }

    private Status writeRaw(byte[] b, Integer offset, Integer n) {
        try {
            this.fd.write(b, offset, n);
        } catch (IOException ex) {
            return Status.IOError(new Brick(this.fileName), null);
        }
        return Status.OK();
    }
    public WritableFile(String fileName, FileOutputStream fd) {
        this.fd = fd;
        this.fileName = fileName;
        this.buf = new byte[kBufSize];
    }

    public Status append(final Brick data) {
        Integer n = data.getSize();
        byte[] p = Bytes.toArray(data.getData());
        int copy = Math.min(n, kBufSize - pos);
        System.arraycopy(p, 0, buf, pos, copy);
        n -= copy;
        pos += copy;
        if(n == 0) return Status.OK();

        Status s = flushBuffered();
        if(!s.ok()) {
            return s;
        }

        if(n < kBufSize) {
            System.arraycopy(p, copy, buf, pos, n);
            pos = n;
            return Status.OK();
        }
        return writeRaw(p, copy, n);
    }

    public Status close() {
        Status result = flushBuffered();
        try {
            this.fd.close();
        } catch (IOException ex) {
            return Status.IOError(new Brick(this.fileName), null);
        }
        return result;
    }

    public Status syncDirIfManifest() {
        Integer lastPos = this.fileName.lastIndexOf('/');
        String dir;
        Brick baseName;
        if(lastPos == -1) {
            dir = ".";
            baseName = new Brick(this.fileName);
        } else {
            dir = this.fileName.substring(0, lastPos);
            baseName = new Brick(this.fileName.substring(lastPos+1));
        }
        Status s;
        if(baseName.startWith(new Brick("MANIFEST"))) {
        }
    }

}
