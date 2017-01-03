package com.novemberain.quartz.mongodb.lock

import com.novemberain.quartz.mongodb.util.Keys
import org.bson.types.ObjectId

class LockBuilder {

    public static Lock lock (@DelegatesTo(LockClosure) Closure closure) {
        Closure runClone = closure.clone()
        def lockClosure = new LockClosure()
        runClone.delegate = lockClosure
        runClone()
        lockClosure.build()
    }

    public static class LockClosure {

        public ObjectId id = ObjectId.get();
        public Keys.LockType lockType = Keys.LockType.t;
        public String keyName = "k1";
        public String keyGroup = "g1";
        public String instanceId = "defaultInstanceId";
        public Date time = new Date();

        public withTime(long milliseconds) {
            time = new Date(milliseconds)
            return this
        }

        private Lock build() {
            return new Lock(id, lockType, keyName, keyGroup, instanceId, time)
        }
    }
}
