package org.chenyou.fuzzdb.util.file;

import com.google.common.base.Preconditions;
import org.chenyou.fuzzdb.util.Brick;
import org.chenyou.fuzzdb.util.Constants;
import org.chenyou.fuzzdb.util.Status;

import java.io.FileNotFoundException;
import java.nio.channels.FileChannel;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.Condition;

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
        if(baseName.startWith(new Brick("MANIFEST"))) {
            try {
                // implements ref to Lucene-Core IOUtil.fsync
                final FileChannel tmpFd = FileChannel.open(Paths.get(dir), StandardOpenOption.READ);
                // fsync on dir
                tmpFd.force(true);
            }
            catch (FileNotFoundException ex) {
                return Status.NotFound(new Brick(dir), new Brick("directory not found"));
            }
            catch (IOException ex) {
                Preconditions.checkArgument(Constants.WINDOWS,
                        "Windows platform doesn't support fsync");
                return Status.OK();
            }
        }
        return Status.OK();
    }

    public Status sync() {
        Status s = syncDirIfManifest();
        if(!s.ok()) {
            return s;
        }
        s = flushBuffered();
        if(s.ok()) {
            // fdatasync on file
            try {
                this.fd.getChannel().force(false);
            } catch (FileNotFoundException ex) {
                return Status.NotFound(new Brick(this.fileName), new Brick("file not found"));
            }
            catch (IOException ex) {
                Preconditions.checkArgument(Constants.WINDOWS,
                        "Windows platform doesn't support fsync");
                return Status.OK();
            }
        }
        return s;
    }
}
