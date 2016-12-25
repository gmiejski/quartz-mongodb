package com.novemberain.quartz.mongodb.cluster;

import com.mongodb.MongoException;
import com.novemberain.quartz.mongodb.dao.LocksDao;
import com.novemberain.quartz.mongodb.dao.SchedulerDao;
import com.novemberain.quartz.mongodb.dao.TriggerDao;
import com.novemberain.quartz.mongodb.lock.Lock;
import org.bson.Document;
import org.quartz.TriggerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The responsibility of this class is to check-in inside Scheduler Cluster and clear dead schedulers and locks.
 */
public class CheckinTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(CheckinTask.class);

    /**
     * This implementation shuts down JVM to not allow to execute the same JOB by two schedulers.
     *
     * If a scheduler cannot register itself due to an exception we stop JVM to prevent
     * concurrent execution of the same jobs together with other nodes that might have found this
     * scheduler as defunct and take over its triggers.
     */
    private static final Runnable DEFAULT_ERROR_HANDLER = new Runnable() {
        @Override
        public void run() {
            //TODO Is there a way to stop only Quartz?
            System.exit(1);
        }
    };

    private final SchedulerDao schedulerDao;
    private final LocksDao locksDao;
    private final TriggerDao triggerDao;
    private final ExpiryCalculator expiryCalculator;

    private Runnable errorhandler = DEFAULT_ERROR_HANDLER;

    public CheckinTask(SchedulerDao schedulerDao, LocksDao locksDao, TriggerDao triggerDao, ExpiryCalculator expiryCalculator) {
        this.schedulerDao = schedulerDao;
        this.locksDao = locksDao;
        this.triggerDao = triggerDao;
        this.expiryCalculator = expiryCalculator;
    }

    // for tests only
    public void setErrorHandler(Runnable errorhandler) {
        this.errorhandler = errorhandler;
    }

    @Override
    public void run() {
        log.info("Node {}:{} checks-in.", schedulerDao.schedulerName, schedulerDao.instanceId);
        try {
            schedulerDao.checkIn();
            recoverDeadSchedulers();
        } catch (MongoException e) {
            log.error("Node " + schedulerDao.instanceId + " could not check-in: " + e.getMessage(), e);
            errorhandler.run();
        }
    }

    /**
     * Finds dead schedulers, removes their locks which doesn't have associated trigger and removes dead scheduler
     */
    private void recoverDeadSchedulers() {
        try {
            List<Scheduler> deadSchedulers = expiryCalculator.findDeadSchedulers();
            removeLocksWithoutTriggers(deadSchedulers);
            removeSchedulers(deadSchedulers);
        } catch (Exception e) {
            log.warn("Error while recovering dead schedulers", e);
        }
    }

    private void removeSchedulers(List<Scheduler> deadSchedulers) {
        for (Scheduler scheduler : deadSchedulers) {
            log.info("Removing scheduler because its dead: {}.", scheduler.getInstanceId());
            schedulerDao.remove(scheduler.getInstanceId(), scheduler.getLastCheckinTime());
        }
    }

    private void removeLocksWithoutTriggers(List<Scheduler> deadSchedulers) {
        List<Lock> activeLocks = new ArrayList<>();
        for (Scheduler scheduler : deadSchedulers) {
            activeLocks.addAll(locksDao.findLocks(scheduler.getInstanceId()));
        }

        for (Lock lock : activeLocks) {
            Document trigger = triggerDao.findTrigger(TriggerKey.triggerKey(lock.getKeyName(), lock.getKeyGroup()));
            if (trigger == null) {
                log.info("Removing lock: {} from dead scheduler. No trigger found for this lock.", lock);
                locksDao.remove(lock);
            }
        }
    }
}
