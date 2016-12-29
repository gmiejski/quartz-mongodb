package com.novemberain.quartz.mongodb.cluster;

public interface ClusterTask extends Runnable {
    String name();
}
