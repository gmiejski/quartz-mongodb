package com.novemberain.quartz.mongodb.cluster.cleanup

import spock.lang.Specification

class CleanupTimeCalculatorTest extends Specification {

    def "should properly calculate minimum time between cluster cleanup tasks"() {
        expect:
        new CleanupTimeCalculator().calculateCleanupPeriod(checkinTimeMilliseconds) == expectedCleanupPeriod

        where:
        checkinTimeMilliseconds | expectedCleanupPeriod
        1                       | CleanupTimeCalculator.MINIMUM_CLEANUP_PERIOD_SECONDS * 1000L
        61 * 1000               | 61 * 1000
    }
}
