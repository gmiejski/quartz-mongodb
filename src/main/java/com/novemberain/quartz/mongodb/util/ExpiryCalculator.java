package com.novemberain.quartz.mongodb.util;

import com.novemberain.quartz.mongodb.cluster.Scheduler;
import com.novemberain.quartz.mongodb.dao.SchedulerDao;
import com.novemberain.quartz.mongodb.lock.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ExpiryCalculator {

    private static final Logger log = LoggerFactory.getLogger(ExpiryCalculator.class);

    private final SchedulerDao schedulerDao;
    private final Clock clock;
    private final long jobTimeoutMillis;
    private final long triggerTimeoutMillis;

    public ExpiryCalculator(SchedulerDao schedulerDao, Clock clock,
                            long jobTimeoutMillis, long triggerTimeoutMillis) {
        this.schedulerDao = schedulerDao;
        this.clock = clock;
        this.jobTimeoutMillis = jobTimeoutMillis;
        this.triggerTimeoutMillis = triggerTimeoutMillis;
    }

    public boolean isJobLockExpired(Lock lock) {
        return isLockExpired(lock, jobTimeoutMillis);
    }

    public boolean isTriggerLockExpired(Lock lock) {
        return isLockExpired(lock, triggerTimeoutMillis) && hasDefunctScheduler(lock.getInstanceId());
    }

    private boolean hasDefunctScheduler(String schedulerId) {
        Scheduler scheduler = schedulerDao.findInstance(schedulerId);
        if (scheduler == null) {
            log.debug("No such scheduler: {}", schedulerId);
            return false;
        }
        return scheduler.isDefunct(clock.millis()) && schedulerDao.isNotSelf(scheduler);
    }

    private boolean isLockExpired(Lock lock, long timeoutMillis) {
        Date lockTime = lock.getDate();
        long elapsedTime = clock.millis() - lockTime.getTime();
        return (elapsedTime > timeoutMillis);
    }
}
