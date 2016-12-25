package com.novemberain.quartz.mongodb.util

import com.novemberain.quartz.mongodb.Clocks
import com.novemberain.quartz.mongodb.cluster.ExpiryCalculator
import com.novemberain.quartz.mongodb.cluster.Scheduler
import com.novemberain.quartz.mongodb.dao.SchedulerDao
import com.novemberain.quartz.mongodb.lock.Lock
import spock.lang.Shared
import spock.lang.Specification

import static com.novemberain.quartz.mongodb.lock.LockBuilder.lock

class ExpiryCalculatorTest extends Specification {

    @Shared def static defaultInstanceId = "test instance"
    @Shared def jobTimeoutMillis = 100l
    @Shared def triggerTimeoutMillis = 10000l

    def 'should tell if job lock has exired'() {
        given:
        def clock = Clocks.constClock(101)
        def calc = createCalc(clock)

        expect: 'Expired lock: 101 - 0 > 100 (timeout)'
        calc.isJobLockExpired(createLock(0))

        and: 'Not expired: 101 - 1/101 <= 100'
        !calc.isJobLockExpired(createLock(1))
        !calc.isJobLockExpired(createLock(101))
    }

    def 'should tell if trigger lock has expired'() {
        given:
        def clock = Clocks.constClock(10001l)

        when: 'Tests for alive scheduler'
        def aliveScheduler = createScheduler(5000) // lastCheckinTime = 5000
        def calc = createCalc(clock, aliveScheduler)

        then: 'Expired lock: 10001 - 0 > 10000 (timeout)'
        !calc.isTriggerLockExpired(createLock(0))

        and: 'Not expired: 101 - 1/10001 <= 10000'
        !calc.isTriggerLockExpired(createLock(1))
        !calc.isTriggerLockExpired(createLock(10001))

        when: 'Tests for dead scheduler'
        def deadScheduler = createScheduler(0) // lastCheckinTime = 0
        calc = createCalc(clock, deadScheduler)

        then: 'Expired lock: 10001 - 0 > 10000 (timeout)'
        calc.isTriggerLockExpired(createLock(0))

        and: 'Not expired: 10001 - 1/10001 <= 10000'
        !calc.isTriggerLockExpired(createLock(1))
        !calc.isTriggerLockExpired(createLock(10001))

        when: 'Tests for no scheduler (acts as dead scheduler)'
        calc = createCalc(clock, null)

        then: 'Expired lock: 10001 - 0 > 10000 (timeout)'
        calc.isTriggerLockExpired(createLock(0))

        and: 'Not expired: 10001 - 1/10001 <= 10000'
        !calc.isTriggerLockExpired(createLock(1))
        !calc.isTriggerLockExpired(createLock(10001))
    }

    def Scheduler createScheduler() {
        createScheduler(100l)
    }

    def createScheduler(long lastCheckinTime) {
        new Scheduler("sname", defaultInstanceId, lastCheckinTime, 100l)
    }

    def SchedulerDao createSchedulerDao(Scheduler scheduler) {
          Mock(SchedulerDao) {
            findInstance(_ as String) >> scheduler
            isNotSelf(scheduler) >> true
        }
    }

    def createCalc(Clock clock) {
        createCalc(clock, createScheduler())
    }

    def createCalc(Clock clock, Scheduler scheduler) {
        new ExpiryCalculator(createSchedulerDao(scheduler), clock,
                jobTimeoutMillis, triggerTimeoutMillis)
    }

    def Lock createLock(long lockTime) {
        lock {
            instanceId : defaultInstanceId
            withTime(lockTime)
        }
    }
}