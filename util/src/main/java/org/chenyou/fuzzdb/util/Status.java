package org.chenyou.fuzzdb.util;

import com.google.common.base.Preconditions;

public class Status {
    private enum Code {
        kOk, kNotFound, kCorruption,
        kNotSupported, kInvalidArgument, kIOError
    }
    private Code status;
    private String msg;
    private Code code() {
        return this.status;
    }

    static public Status OK() {
        return new Status();
    }

    static public Status NotFound(final Brick msg, final Brick msg2) {
        return new Status(Code.kNotFound, msg, msg2);
    }

    static public Status Corruption(final Brick msg, final Brick msg2) {
        return new Status(Code.kCorruption, msg, msg2);
    }

    static public Status NotSupported(final Brick msg, final Brick msg2) {
        return new Status(Code.kNotSupported, msg, msg2);
    }

    static public Status InvalidArgument(final Brick msg, final Brick msg2) {
        return new Status(Code.kInvalidArgument, msg, msg2);
    }

    static public Status IOError(final Brick msg, final Brick msg2) {
        return new Status(Code.kIOError, msg, msg2);
    }

    private Status() {
        this.status = null;
        this.msg = null;
    }
    private Status(Code code, final Brick msg, final Brick msg2) {
        Preconditions.checkArgument(code != Code.kOk && msg != null);
        this.status = code;
        StringBuilder sb = new StringBuilder();
        sb.append(msg.toString());
        if(msg2 != null) {
            sb.append(":");
            sb.append(msg2.toString());
        }
        this.msg = sb.toString();
    }
    public Boolean ok() {
        return this.status == null;
    }

    public Boolean isNotFound() {
        return code() == Code.kNotFound;
    }

    public Boolean isCorruption() {
        return code() == Code.kCorruption;
    }

    public Boolean isIOErrer() {
        return code() == Code.kIOError;
    }

    public Boolean isNotSupportedError() {
        return code() == Code.kNotSupported;
    }

    public Boolean IsInvalidArgument() {
        return code() == Code.kInvalidArgument;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if(this.status == null) {
            return "OK";
        } else {
            switch (code()) {
                case kOk:
                    return "OK";
                case kNotFound:
                    sb.append("NotFound: ");
                    break;
                case kCorruption:
                    sb.append("Corruption: ");
                    break;
                case kNotSupported:
                    sb.append("Not implemented: ");
                    break;
                case kInvalidArgument:
                    sb.append("Invalid argument: ");
                    break;
                case kIOError:
                    sb.append("IO error: ");
                    break;
                default:
                    sb.append(String.format("Unknown code(%d): ", code().ordinal()));
            }
            sb.append(this.msg);
            return sb.toString();
        }
    }
}
