package org.chenyou.fuzzdb.util.file;

import com.google.common.base.Preconditions;
import org.chenyou.fuzzdb.util.Slice;
import org.chenyou.fuzzdb.util.Constants;
import org.chenyou.fuzzdb.util.Status;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import com.google.common.primitives.Bytes;

public class WritableFile {
    private String fileName;
    private FileChannel fd;
    private Integer pos;
    private ByteBuffer buf;
    private final static Integer kBufSize = 65536;

    private Status flushBuffered() {
        buf.flip();
        Status s = writeRaw(buf);
        this.pos = 0;
        this.buf.clear();
        return s;
    }

    private Status writeRaw(ByteBuffer b) {
        while(b.hasRemaining()) {
            try {
                this.fd.write(b);
            } catch (IOException ex) {
                return Status.IOError(new Slice(this.fileName), null);
            }
        }
        return Status.OK();
    }

    public WritableFile(String fileName, FileChannel fd) {
        this.fd = fd;
        this.fileName = fileName;
        this.buf = ByteBuffer.allocate(kBufSize);
        this.pos = 0;
    }

    public Status append(final Slice data) {
        Integer n = data.getSize();
        byte[] p = data.getData();
        int copy = Math.min(n, kBufSize - pos);
        this.buf.put(p, 0, copy);
        n -= copy;
        pos += copy;
        if(n == 0) return Status.OK();
        Status s = flushBuffered();
        if(!s.ok()) {
            return s;
        }
        if(n < kBufSize) {
            this.buf.put(p, copy, n);
            pos = n;
            return Status.OK();
        }
        // write all bytes at once, rather than put into buffer
        ByteBuffer largeBuf = ByteBuffer.allocate(n);
        largeBuf.put(p, copy, n);
        largeBuf.flip();
        return writeRaw(largeBuf);
    }

    public Status close() {
        Status result = flushBuffered();
        try {
            this.fd.close();
        } catch (IOException ex) {
            return Status.IOError(new Slice(this.fileName), null);
        }
        return result;
    }

    public Status flush() {
        return flushBuffered();
    }

    public Status syncDirIfManifest() {
        Integer lastPos = this.fileName.lastIndexOf('/');
        String dir;
        Slice baseName;
        if(lastPos == -1) {
            dir = ".";
            baseName = new Slice(this.fileName);
        } else {
            dir = this.fileName.substring(0, lastPos);
            baseName = new Slice(this.fileName.substring(lastPos+1));
        }
        if(baseName.startWith(new Slice("MANIFEST"))) {
            try {
                // implements ref to Lucene-Core IOUtil.fsync
                final FileChannel tmpFd = FileChannel.open(Paths.get(dir), StandardOpenOption.READ);
                // fsync on dir
                tmpFd.force(true);
            }
            catch (FileNotFoundException ex) {
                return Status.NotFound(new Slice(dir), new Slice("directory not found"));
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
                this.fd.force(false);
            } catch (FileNotFoundException ex) {
                return Status.NotFound(new Slice(this.fileName), new Slice("file not found"));
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
