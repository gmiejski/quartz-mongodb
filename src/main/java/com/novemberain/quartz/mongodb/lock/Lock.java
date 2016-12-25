package com.novemberain.quartz.mongodb.lock;

import com.novemberain.quartz.mongodb.util.Keys;
import org.bson.types.ObjectId;

import java.util.Date;

public class Lock {

    private final ObjectId id;
    private final Keys.LockType lockType;
    private final String keyName;
    private final String keyGroup;
    private final String instanceId;
    private final Date time;

    public Lock(ObjectId id, Keys.LockType lockType, String keyName, String keyGroup, String instanceId, Date time) {
        this.id = id;
        this.lockType = lockType;
        this.keyName = keyName;
        this.keyGroup = keyGroup;
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

    public String getKeyGroup() {
        return keyGroup;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public Date getDate() {
        return time;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Lock{");
        sb.append("id=").append(id);
        sb.append(", lockType=").append(lockType);
        sb.append(", keyName='").append(keyName).append('\'');
        sb.append(", keyGroup='").append(keyGroup).append('\'');
        sb.append(", instanceId='").append(instanceId).append('\'');
        sb.append(", time=").append(time);
        sb.append('}');
        return sb.toString();
    }
}
