package com.novemberain.quartz.mongodb.cluster.cleanup

import com.novemberain.quartz.mongodb.cluster.ExpiryCalculator
import com.novemberain.quartz.mongodb.cluster.Scheduler
import com.novemberain.quartz.mongodb.dao.LocksDao
import com.novemberain.quartz.mongodb.dao.SchedulerDao
import com.novemberain.quartz.mongodb.dao.TriggerDao
import com.novemberain.quartz.mongodb.lock.Lock
import org.bson.Document
import org.quartz.TriggerKey
import spock.lang.Shared
import spock.lang.Specification

import static com.novemberain.quartz.mongodb.lock.LockBuilder.lock

class CleanupTaskTest extends Specification {

    @Shared
    def defaultCheckinInterval = 100
    @Shared
    def defaultLastCheckin = 1000

    def schedulerDao = Mock(SchedulerDao)
    def locksDao = Mock(LocksDao)
    def triggerDao = Mock(TriggerDao)
    def expiryCalculator = Mock(ExpiryCalculator)
    def cleanupTask = new CleanupTask(schedulerDao, locksDao, triggerDao, expiryCalculator)

    def 'should find and clear dead schedulers'() {
        when:
        cleanupTask.run()

        then:
        1 * schedulerDao.getAllByCheckinTime() >> [scheduler("i1")]
        1 * expiryCalculator.hasDefunctScheduler({it.instanceId == "i1"} as Scheduler) >> false
        1 * locksDao.findFiredTriggerInstanceIds() >> []
    }

    def 'should continue working even when exceptions happen'() {
        when:
        cleanupTask.run()

        then:
        noExceptionThrown()
        1 * schedulerDao.getAllByCheckinTime() >> { throw new RuntimeException() }
    }

    def "should remove dead schedulers and their locks matching no existing trigger"() {
        given:
        def s1locks = [lock { instanceId = "i1" }, lock { instanceId = "i1" }]
        def s2locks = [lock { instanceId = "i2" }, lock { instanceId = "i2"; keyName = "name4"; keyGroup = "group1" }]

        when:
        cleanupTask.run()

        then:
        1 * schedulerDao.getAllByCheckinTime() >> [scheduler("i1"), scheduler("i2")]
        2 * expiryCalculator.hasDefunctScheduler(_ as Scheduler) >>> [true, true]
        1 * locksDao.findFiredTriggerInstanceIds() >> []
        1 * locksDao.findLocks("i1") >> s1locks
        1 * locksDao.findLocks("i2") >> s2locks
        1 * triggerDao.findTrigger(TriggerKey.triggerKey("name4", "group1")) >> new Document()
        3 * locksDao.remove(_)
        1 * schedulerDao.remove(*_)
    }

    def "should remove locks without existing schedulers"() {
        given: "1 lock without scheduler and one dead scheduler"
        def noSchedulerLocks = [lock { instanceId = "lockNoScheduler1"; keyName = "noSc1"; keyGroup = "group1" }]
        def deadSchedulerLocks = [lock { instanceId = "i1" }, lock { instanceId = "i1"; keyName = "name4"; keyGroup = "group1" }]
        def thisInstanceId = "thisSchedulerId"
        def aliveScheduler = "i3"

        when:
        cleanupTask.run()

        then:
        schedulerDao.instanceId >> thisInstanceId
        1 * schedulerDao.getAllByCheckinTime() >> [scheduler("i1"), scheduler("i2"), scheduler(thisInstanceId), scheduler(aliveScheduler)]
        4 * expiryCalculator.hasDefunctScheduler(_ as Scheduler) >>> [true, false, false, false]

        and: "should find locks of schedulers marked as dead"
        1 * locksDao.findFiredTriggerInstanceIds() >> ["i1", "lockNoScheduler1", thisInstanceId]
        1 * locksDao.findLocks("i1") >> deadSchedulerLocks
        1 * locksDao.findLocks("lockNoScheduler1") >> noSchedulerLocks

        and: "should do nothing with this instance locks"
        0 * locksDao.findLocks(thisInstanceId)

        and: "should not remove lock with associated trigger"
        1 * triggerDao.findTrigger(TriggerKey.triggerKey("name4", "group1")) >> new Document()
        0 * locksDao.remove({it.keyName == "name4"} as Lock)

        and: "should not remove schedulers for which we didn't remove all triggers"
        0 * schedulerDao.remove("i1", _)

        and: "should remove rest of locks and two dead schedulers"
        2 * locksDao.remove(_)
        1 * schedulerDao.remove(*_)
    }

    def Scheduler scheduler(String instanceId) {
        new Scheduler("sname", instanceId, defaultLastCheckin, defaultCheckinInterval)
    }
}
