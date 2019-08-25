package org.chenyou.fuzzdb.util.file;

import com.google.common.base.Preconditions;
import org.chenyou.fuzzdb.util.Constants;
import org.chenyou.fuzzdb.util.Slice;
import org.chenyou.fuzzdb.util.Status;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FuzzWritableFile extends WritableFile {
    private String fileName;
    private FileChannel fd;
    private Integer pos;
    private ByteBuffer buf;
    private boolean isManifest;
    private String dirName;
    private final static Integer K_BUF_SIZE = 65536;

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

    private static Slice getBasename(String fileName) {
        int lastPos = fileName.lastIndexOf('/');
        if (lastPos == -1) {
            return new Slice(fileName);
        }
        // The filename component should not contain a path separator. If it does,
        // the splitting was done incorrectly.
        Preconditions.checkArgument(fileName.lastIndexOf('/', lastPos + 1) == -1);

        return new Slice(fileName.substring(lastPos+1));
    }

    // Returns the directory name in a path pointing to a file.
    // Returns "." if the path does not contain any directory separator.
    private static String getDirName(String fileName) {
        int lastPos = fileName.lastIndexOf('/');
        if (lastPos == -1) {
            return ".";
        }
        // The filename component should not contain a path separator. If it does,
        // the splitting was done incorrectly.
        Preconditions.checkArgument(fileName.lastIndexOf('/', lastPos + 1) == -1);
        return fileName.substring(0, lastPos);
    }

    private static boolean isManifestJudge(String fileName) {
        return getBasename(fileName).startWith(new Slice("MANIFEST"));
    }

    public FuzzWritableFile(String fileName, FileChannel fd) {
        this.fd = fd;
        this.fileName = fileName;
        this.buf = ByteBuffer.allocate(K_BUF_SIZE);
        this.pos = 0;
        this.isManifest = isManifestJudge(fileName);
        this.dirName = getDirName(fileName);
    }

    @Override
    public Status append(final Slice data) {
        int n = data.getSize();
        byte[] p = data.getData();
        // no more than buffer size
        int copy = Math.min(n, K_BUF_SIZE - pos);
        this.buf.put(p, 0, copy);
        n -= copy;
        pos += copy;
        // full data copy to buffer
        if(n == 0) {
            return Status.OK();
        }
        // flush full buffer to disk
        Status s = flushBuffered();
        if(!s.ok()) {
            return s;
        }
        // put remains data into buffer
        if(n < K_BUF_SIZE) {
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

    @Override
    public Status close() {
        Status result = flushBuffered();
        try {
            this.fd.close();
        } catch (IOException ex) {
            return Status.IOError(new Slice(this.fileName), null);
        }
        return result;
    }

    @Override
    public Status flush() {
        return flushBuffered();
    }

    public Status syncDirIfManifest() {
        if(this.isManifest) {
            try {
                // implements ref to Lucene-Core IOUtil.fsync
                // TODO potentially in consistent > JDK 8
                final FileChannel tmpFd = FileChannel.open(Paths.get(this.dirName), StandardOpenOption.READ);
                // fsync on dir
                tmpFd.force(true);
            }
            catch (FileNotFoundException ex) {
                return Status.NotFound(new Slice(this.dirName), new Slice("directory not found"));
            }
            catch (IOException ex) {
                Preconditions.checkArgument(Constants.WINDOWS,
                        "Windows platform doesn't support fsync");
                return Status.OK();
            }
        }
        return Status.OK();
    }

    @Override
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
            } catch (IOException ex) {
                Preconditions.checkArgument(Constants.WINDOWS,
                        "Windows platform doesn't support fsync");
                return Status.OK();
            }
        }
        return s;
    }
}
