package org.chenyou.fuzzdb.util.test;
import org.chenyou.fuzzdb.util.Brick;
import org.chenyou.fuzzdb.util.Status;
import org.junit.Test;
import org.junit.Assert;
public class StatusTest {

    @Test
    public void createStatusTest() {
        String msg1 = "StatusMsg1";
        String msg2 = "StatusMsg2";
        Status okStatus = Status.OK();
        Assert.assertEquals(okStatus.toString(), "OK");
        String notFoundMsg = "NotFound: StatusMsg1:StatusMsg2";
        Status notFoundStatus = Status.NotFound(new Brick(msg1), new Brick(msg2));
        Assert.assertEquals(notFoundMsg, notFoundStatus.toString());
        String corruptionMsg = "Corruption: StatusMsg1";
        Status corruptionStatus = Status.Corruption(new Brick(msg1), null);
        Assert.assertEquals(corruptionMsg, corruptionStatus.toString());
        String notSupportedMsg = "Not implemented: StatusMsg1:StatusMsg2";
        Status notSupportedStatus = Status.NotSupported(new Brick(msg1), new Brick(msg2));
        Assert.assertEquals(notSupportedMsg, notSupportedStatus.toString());
        String invalidArgumentMsg = "Invalid argument: StatusMsg1";
        Status invalidArgumentStatus = Status.InvalidArgument(new Brick(msg1), null);
        Assert.assertEquals(invalidArgumentMsg, invalidArgumentStatus.toString());
        String ioErrorMsg = "IO error: StatusMsg1:StatusMsg2";
        Status ioErrorStatus = Status.IOError(new Brick(msg1), new Brick(msg2));
        Assert.assertEquals(ioErrorMsg, ioErrorStatus.toString());
    }

    @Test
    public void getStatusTest() {
        String msg1 = "StatusMsg1";
        String msg2 = "StatusMsg2";
        Status okStatus = Status.OK();
        Assert.assertTrue(okStatus.ok());
        Assert.assertFalse(okStatus.isIOErrer());
        Status notFoundStatus = Status.NotFound(new Brick(msg1), new Brick(msg2));
        Assert.assertTrue(notFoundStatus.isNotFound());
        Assert.assertFalse(notFoundStatus.IsInvalidArgument());
        Status corruptionStatus = Status.Corruption(new Brick(msg1), null);
        Assert.assertTrue(corruptionStatus.isCorruption());
        Assert.assertFalse(corruptionStatus.isNotSupportedError());
        Status notSupportedStatus = Status.NotSupported(new Brick(msg1), new Brick(msg2));
        Assert.assertTrue(notSupportedStatus.isNotSupportedError());
        Assert.assertFalse(notFoundStatus.isCorruption());
        Status invalidArgumentStatus = Status.InvalidArgument(new Brick(msg1), null);
        Assert.assertTrue(invalidArgumentStatus.IsInvalidArgument());
        Status ioErrorStatus = Status.IOError(new Brick(msg1), new Brick(msg2));
        Assert.assertTrue(ioErrorStatus.isIOErrer());
    }
}
