package com.novemberain.quartz.mongodb.cluster.cleanup;

import com.novemberain.quartz.mongodb.cluster.ClusterTask;
import com.novemberain.quartz.mongodb.cluster.ExpiryCalculator;
import com.novemberain.quartz.mongodb.cluster.Scheduler;
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
 * The responsibility of this class is to check-in inside Scheduler Cluster.
 */
public class CleanupTask implements ClusterTask {

    private static final Logger log = LoggerFactory.getLogger(CleanupTask.class);
    private final SchedulerDao schedulerDao;
    private final LocksDao locksDao;
    private final TriggerDao triggerDao;
    private final ExpiryCalculator expiryCalculator;

    public CleanupTask(SchedulerDao schedulerDao, LocksDao locksDao, TriggerDao triggerDao, ExpiryCalculator expiryCalculator) {
        this.schedulerDao = schedulerDao;
        this.locksDao = locksDao;
        this.triggerDao = triggerDao;
        this.expiryCalculator = expiryCalculator;
    }

    @Override
    public String name() {
        return this.getClass().getName();
    }

    @Override
    public void run() {
        try {
            recoverDeadSchedulers();
        } catch (Exception e) {
            log.warn("Error while recovering dead schedulers", e);
        }
    }

    /**
     * Finds dead schedulers, removes their locks which doesn't have associated trigger and removes dead scheduler
     */
    private void recoverDeadSchedulers() {
        List<Scheduler> deadSchedulers = expiryCalculator.findDeadSchedulers();
        removeLocksWithoutTriggers(deadSchedulers);
        removeSchedulers(deadSchedulers);
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
