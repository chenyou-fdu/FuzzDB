package org.chenyou.fuzzdb.util.comparator;

import com.google.common.base.Preconditions;
import org.chenyou.fuzzdb.util.Coding;
import org.chenyou.fuzzdb.util.Slice;

import java.util.ArrayList;
import java.util.List;

import static org.chenyou.fuzzdb.db.DBFormat.*;

public class InternalKeyComparator implements FuzzComparator{
    public InternalKeyComparator(FuzzComparator userComparator) {
        this.userComparator = userComparator;
    }

    @Override
    public String getName() {
        return "FuzzDB.InternalKeyComparator";
    }

    @Override
    public Slice findShortSuccessor(final Slice key) {
        Slice userKey = extractUserKey(key);
        Slice tmp = new Slice();
        tmp.setData(userKey.getData(), userKey.getSize());
        tmp = userComparator.findShortSuccessor(tmp);
        if(tmp.getSize() < userKey.getSize() && userComparator.compare(userKey, tmp) < 0) {
            byte[] tmpByteArray = tmp.getData();
            List<Byte> tmpByteList = new ArrayList<>(tmp.getSize());
            for(int i = 0; i < tmp.getSize(); i++) {
                //tmpByteList.set(i, tmpByteArray[i]);
                tmpByteList.add(tmpByteArray[i]);
            }
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            Coding.PutFixed64(tmpByteList, packSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
            Slice resSlice = new Slice(tmpByteList);
            Preconditions.checkArgument(this.compare(key, resSlice) < 0);
            return resSlice;
        }
        return key;
    }

    @Override
    public int compare(Object aKeyObj, Object bKeyObj) {
        Preconditions.checkArgument(aKeyObj instanceof Slice);
        Preconditions.checkArgument(bKeyObj instanceof Slice);
        // Order by:
        //    increasing user key (according to user-supplied comparator)
        //    decreasing sequence number
        //    decreasing type (though sequence# should be enough to disambiguate)
        Slice aKey = (Slice) aKeyObj;
        Slice bKey = (Slice) bKeyObj;
        int r = userComparator.compare(extractUserKey(aKey), extractUserKey(bKey));
        if(r == 0) {
            long anum = Coding.DecodeFixed64(aKey.getData(), aKey.getSize() - 8);
            long bnum = Coding.DecodeFixed64(bKey.getData(), bKey.getSize() - 8);
            if(anum > bnum) {
                r = -1;
            } else if(anum < bnum) {
                r = +1;
            }
        }
        return r;
    }

    @Override
    public Slice findShortestSeparator(String start, final Slice limit) {
        Slice userStart = extractUserKey(new Slice(start));
        Slice userLimit = extractUserKey(limit);
        String tmp = userStart.toString();
        Slice tmpSlice = userComparator.findShortestSeparator(tmp, userLimit);

        if(tmpSlice.getSize() < userStart.getSize() && userComparator.compare(userStart, tmpSlice) < 0) {
            List<Byte> tmpByteList = new ArrayList<>(tmpSlice.getSize());
            for(int i = 0; i < tmpSlice.getSize(); i++){
                //tmpByteList.set(i, tmpSlice.getData()[i]);
                tmpByteList.add(tmpSlice.getData()[i]);
            }
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            Coding.PutFixed64(tmpByteList, packSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
            Slice resSlice = new Slice(tmpByteList);
            Preconditions.checkArgument(this.compare(new Slice(start), resSlice) < 0);
            Preconditions.checkArgument(this.compare(resSlice, limit) < 0);
            return resSlice;
        }
        return new Slice(start);
    }

    private FuzzComparator userComparator;
}
