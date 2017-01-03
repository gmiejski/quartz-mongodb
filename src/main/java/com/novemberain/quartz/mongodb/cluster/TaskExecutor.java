package com.novemberain.quartz.mongodb.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TaskExecutor {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);
    private static final int INITIAL_DELAY = 0;

    private final ClusterTask task;
    private final long checkingTimeMilliseconds;
    private final String instanceId;

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public TaskExecutor(ClusterTask task, long checkingTimeMilliseconds, String instanceId) {
        this.task = task;
        this.checkingTimeMilliseconds = checkingTimeMilliseconds;
        this.instanceId = instanceId;
    }

    /**
     * Start execution of given task at given rate.
     */
    public void start() {
        log.info("Starting {} task for scheduler instance: {}", task.name(), instanceId);
        executor.scheduleAtFixedRate(task, INITIAL_DELAY, checkingTimeMilliseconds, MILLISECONDS);
    }

    /**
     * Stop execution of scheduled task.
     */
    public void shutdown() {
        log.info("Stopping TaskExecutor for task {} for scheduler instance: {}",task.name(), instanceId);
        executor.shutdown();
    }
}
