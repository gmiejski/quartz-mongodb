package com.novemberain.quartz.mongodb.dao;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.UpdateResult;
import com.novemberain.quartz.mongodb.Constants;
import com.novemberain.quartz.mongodb.lock.Lock;
import com.novemberain.quartz.mongodb.lock.LockConverter;
import com.novemberain.quartz.mongodb.util.Clock;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.TriggerKey;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.novemberain.quartz.mongodb.Constants.LOCK_INSTANCE_ID;
import static com.novemberain.quartz.mongodb.util.Keys.*;
import static com.novemberain.quartz.mongodb.util.Keys.createTriggersLocksFilter;

public class LocksDao {

    private static final Logger log = LoggerFactory.getLogger(LocksDao.class);

    private final MongoCollection<Document> locksCollection;
    private Clock clock;
    public final String instanceId;

    public LocksDao(MongoCollection<Document> locksCollection, Clock clock, String instanceId) {
        this.locksCollection = locksCollection;
        this.clock = clock;
        this.instanceId = instanceId;
    }

    public MongoCollection<Document> getCollection() {
        return locksCollection;
    }

    public void createIndex(boolean clustered) {
        locksCollection.createIndex(
                Projections.include(KEY_GROUP, KEY_NAME, LOCK_TYPE),
                new IndexOptions().unique(true));

        if (!clustered) {
            // Need this to stop table scan when removing all locks
            locksCollection.createIndex(Projections.include(LOCK_INSTANCE_ID));

            // remove all locks for this instance on startup
            locksCollection.deleteMany(Filters.eq(LOCK_INSTANCE_ID, instanceId));
        }
    }

    public void dropIndex() {
        locksCollection.dropIndex("keyName_1_keyGroup_1");
        locksCollection.dropIndex(KEY_AND_GROUP_FIELDS);
    }

    public Lock findJobLock(JobKey job) {
        Bson filter = createJobLockFilter(job);
        return LockConverter.fromDocument(locksCollection.find(filter).first());
    }

    public Lock findTriggerLock(TriggerKey trigger) {
        Bson filter = createTriggerLockFilter(trigger);
        return LockConverter.fromDocument(locksCollection.find(filter).first());
    }

    public List<TriggerKey> findOwnTriggersLocks() {
        final List<TriggerKey> keys = new LinkedList<>();
        final Bson filter = createTriggersLocksFilter(instanceId);
        for (Document doc : locksCollection.find(filter)) {
            keys.add(toTriggerKey(doc));
        }
        return keys;
    }

    public List<Lock> findLocks(String instanceId) {
        final List<Lock> locks = new LinkedList<>();
        Bson filter = createTriggersLocksFilter(instanceId);
        for (Document doc : locksCollection.find(filter)) {
            locks.add(LockConverter.fromDocument(doc));
        }
        return locks;
    }

    public void lockJob(JobDetail job) {
        log.debug("Inserting lock for job {}", job.getKey());
        Document lock = createJobLock(job.getKey(), instanceId, clock.now());
        insertLock(lock);
    }

    public void lockTrigger(TriggerKey key) {
        log.info("Inserting lock for trigger {}", key);
        Document lock = createTriggerLock(key, instanceId, clock.now());
        insertLock(lock);
    }

    /**
     * Lock given trigger iff its <b>lockTime</b> haven't changed.
     *
     * <p>Update is performed using "Update document if current" pattern
     * to update iff document in DB hasn't changed - haven't been relocked
     * by other scheduler.</p>
     *
     * @param key         identifies trigger lock
     * @param lockTime    expected current lockTime
     * @return false when not found or caught an exception
     */
    public boolean relock(TriggerKey key, Date lockTime) {
        UpdateResult updateResult;
        try {
            updateResult = locksCollection.updateOne(
                    createRelockFilter(key, lockTime),
                    createLockUpdateDocument(instanceId, clock.now()));
        } catch (MongoException e) {
            log.error("Relock failed because: " + e.getMessage(), e);
            return false;
        }

        if (updateResult.getModifiedCount() == 1) {
            log.info("Scheduler {} relocked the trigger: {}", instanceId, key);
            return true;
        }
        log.info("Scheduler {} couldn't relock the trigger {} with lock time: {}",
                instanceId, key, lockTime.getTime());
        return false;
    }

    /**
     * Reset lock time on own lock.
     *
     * @throws JobPersistenceException in case of errors from Mongo
     * @param key    trigger whose lock to refresh
     * @return true on successful update
     */
    public boolean updateOwnLock(TriggerKey key) throws JobPersistenceException {
        UpdateResult updateResult;
        try {
            updateResult = locksCollection.updateMany(
                    toFilter(key, instanceId),
                    createLockUpdateDocument(instanceId, clock.now()));
        } catch (MongoException e) {
            log.error("Lock refresh failed because: " + e.getMessage(), e);
            throw new JobPersistenceException("Lock refresh for scheduler: " + instanceId, e);
        }

        if (updateResult.getModifiedCount() == 1) {
            log.info("Scheduler {} refreshed locking time.", instanceId);
            return true;
        }
        log.info("Scheduler {} couldn't refresh locking time", instanceId);
        return false;
    }

    public void remove(Lock lock) {
        locksCollection.deleteMany(LockConverter.toDocument(lock));
    }

    /**
     * Unlock the trigger if it still belongs to the current scheduler.
     *
     * @param trigger    to unlock
     */
    public void unlockTrigger(OperableTrigger trigger) {
        log.info("Removing trigger lock {}.{}", trigger.getKey(), instanceId);
        remove(toFilter(trigger.getKey(), instanceId));
        log.info("Trigger lock {}.{} removed.", trigger.getKey(), instanceId);
    }

    public void unlockJob(JobDetail job) {
        log.debug("Removing lock for job {}", job.getKey());
        remove(createJobLockFilter(job.getKey()));
    }

    public Set<String> findFiredTriggerInstanceIds(){
        Set<String> activeLocksInstancesIds = new HashSet<String>();
        FindIterable<Document> activeLocks = locksCollection.find();
        for (Document document : activeLocks) {
            activeLocksInstancesIds.add(document.getString(Constants.LOCK_INSTANCE_ID));
        }
        return activeLocksInstancesIds;
    }


    private void insertLock(Document lock) {
        locksCollection.insertOne(lock);
    }

    private void remove(Bson filter) {
        locksCollection.deleteMany(filter);
    }
}
