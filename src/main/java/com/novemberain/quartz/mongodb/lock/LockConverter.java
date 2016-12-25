package com.novemberain.quartz.mongodb.lock;

import com.novemberain.quartz.mongodb.Constants;
import com.novemberain.quartz.mongodb.util.Keys;
import org.bson.Document;

public class LockConverter {

    public static Lock fromDocument(Document bson) {
        if (bson == null) {
            return null;
        }
        return new Lock(bson.getObjectId("_id"),
                Keys.LockType.valueOf(bson.getString(Keys.LOCK_TYPE)),
                bson.getString(Keys.KEY_NAME),
                bson.getString(Keys.KEY_GROUP),
                bson.getString(Constants.LOCK_INSTANCE_ID),
                bson.getDate(Constants.LOCK_TIME)
        );
    }

    public static Document toDocument(Lock lock) {
        if (lock == null) {
            return null;
        }
        return new Document()
                .append("_id", lock.getId())
                .append(Keys.LOCK_TYPE, lock.getLockType().name())
                .append(Keys.KEY_NAME, lock.getKeyName())
                .append(Keys.KEY_GROUP, lock.getKeyGrup())
                .append(Constants.LOCK_INSTANCE_ID, lock.getInstanceId())
                .append(Constants.LOCK_TIME, lock.getDate());
    }
}
