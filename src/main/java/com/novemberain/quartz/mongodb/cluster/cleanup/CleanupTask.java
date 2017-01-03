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
import java.util.Set;

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
        List<Scheduler> existingSchedulers = schedulerDao.getAllByCheckinTime();
        List<Scheduler> deadSchedulers = findDeadSchedulers(existingSchedulers);
        deadSchedulers.addAll(findOrphanedLocksInstances(existingSchedulers));

        List<Scheduler> deadSchedulersWithinCluster = filterBySchedulerName(deadSchedulers);

        List<Scheduler> schedulersToDelete = removeLocksWithoutTriggers(deadSchedulersWithinCluster);
        removeSchedulers(schedulersToDelete);
    }

    private List<Scheduler> filterBySchedulerName(List<Scheduler> deadSchedulers) {
        List<Scheduler> schedulersWithSameName = new ArrayList<>();
        for (Scheduler scheduler : deadSchedulers) {
            if (scheduler.getName().equals(schedulerDao.getSchedulerName())) {
                schedulersWithSameName.add(scheduler);
            }
        }
        return schedulersWithSameName;
    }

    private List<Scheduler> findOrphanedLocksInstances(List<Scheduler> existingSchedulers) {
        List<Scheduler> orphanedInstances = new ArrayList<Scheduler>();

        Set<String> allFiredTriggerInstanceNames = locksDao.findFiredTriggerInstanceIds();

        for (Scheduler deadScheduler : existingSchedulers) {
            allFiredTriggerInstanceNames.remove(deadScheduler.getInstanceId());
        }
        allFiredTriggerInstanceNames.remove(schedulerDao.getInstanceId());

        for (String instanceId : allFiredTriggerInstanceNames) {
            orphanedInstances.add(new Scheduler(schedulerDao.getSchedulerName(), instanceId, 0, 0));
            log.warn("Found orphaned locks for instance: " + instanceId);
        }

        return orphanedInstances;
    }

    private void removeSchedulers(List<Scheduler> deadSchedulers) {
        for (Scheduler scheduler : deadSchedulers) {
            log.info("Removing scheduler because its dead: {}.", scheduler.getInstanceId());
            schedulerDao.remove(scheduler.getInstanceId(), scheduler.getLastCheckinTime());
        }
    }

    /**
     * @return dead schedulers without taking this scheduler instance into account
     */
    private List<Scheduler> findDeadSchedulers(List<Scheduler> existingSchedulers) {
        List<Scheduler> deadSchedulers = new ArrayList<>();
        for (Scheduler scheduler : existingSchedulers) {
            if (expiryCalculator.hasDefunctScheduler(scheduler)) {
                deadSchedulers.add(scheduler);
            }
        }
        return deadSchedulers;
    }

    /**
     * Removes lock for given schedulers.
     * If lock are matched with existing triggers then we won't remove the scheduler now - we will wait until its lock will be relocked by other scheduler.
     *
     * @param deadSchedulers for which we have removed all locks.
     * @return really dead schedulers without locks - those which should be removed.
     */
    private List<Scheduler> removeLocksWithoutTriggers(List<Scheduler> deadSchedulers) {
        List<Scheduler> schedulersToRemove = new ArrayList<>();
        for (Scheduler scheduler : deadSchedulers) {
            boolean isSchedulerDead = true;
            for (Lock lock : locksDao.findLocks(scheduler.getInstanceId())) {
                Document trigger = triggerDao.findTrigger(TriggerKey.triggerKey(lock.getKeyName(), lock.getKeyGroup()));
                if (trigger == null) {
                    log.info("Removing lock: {} from dead scheduler. No trigger found for this lock.", lock);
                    locksDao.remove(lock);
                } else {
                    isSchedulerDead = false;
                }
            }
            if (isSchedulerDead) {
                schedulersToRemove.add(scheduler);
            }
        }
        return schedulersToRemove;
    }
}
