package com.novemberain.quartz.mongodb.lock;

import com.novemberain.quartz.mongodb.util.Keys;
import org.bson.types.ObjectId;

import java.util.Date;

public class Lock {

    private final ObjectId id;
    private final Keys.LockType lockType;
    private final String keyName;
    private final String keyGrup;
    private final String instanceId;
    private final Date time;

    public Lock(ObjectId id, Keys.LockType lockType, String keyName, String keyGrup, String instanceId, Date time) {
        this.id = id;
        this.lockType = lockType;
        this.keyName = keyName;
        this.keyGrup = keyGrup;
        this.instanceId = instanceId;
        this.time = time;
    }

    public Keys.LockType getLockType() {
        return lockType;
    }

    public ObjectId getId() {
        return id;
    }

    public String getKeyName() {
        return keyName;
    }

    public String getKeyGrup() {
        return keyGrup;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public Date getDate() {
        return time;
    }
}
