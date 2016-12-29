package com.novemberain.quartz.mongodb.cluster.cleanup;

import java.util.concurrent.TimeUnit;

public class CleanupTimeCalculator {

    static final int MINIMUM_CLEANUP_PERIOD_SECONDS = 60;

    public long calculateCleanupPeriod(long checkinTimeMilliseconds) {
        if (checkinTimeMilliseconds < TimeUnit.SECONDS.toMillis(MINIMUM_CLEANUP_PERIOD_SECONDS)) {
            return TimeUnit.SECONDS.toMillis(MINIMUM_CLEANUP_PERIOD_SECONDS);
        } else {
            return checkinTimeMilliseconds;
        }
    }
}
