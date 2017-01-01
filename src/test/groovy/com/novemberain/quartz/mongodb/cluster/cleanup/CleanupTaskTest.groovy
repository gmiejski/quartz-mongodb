package com.novemberain.quartz.mongodb.cluster.cleanup

import com.novemberain.quartz.mongodb.cluster.ExpiryCalculator
import com.novemberain.quartz.mongodb.cluster.Scheduler
import com.novemberain.quartz.mongodb.dao.LocksDao
import com.novemberain.quartz.mongodb.dao.SchedulerDao
import com.novemberain.quartz.mongodb.dao.TriggerDao
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
        1 * expiryCalculator.findDeadSchedulers() >> []
    }

    def 'should continue working even when exceptions happen'() {
        when:
        cleanupTask.run()

        then:
        noExceptionThrown()
        1 * expiryCalculator.findDeadSchedulers() >> { throw new RuntimeException() }
    }

    def "should remove dead schedulers and their locks matching no existing trigger"() {
        given:
        def s1locks = [lock { instanceId = "i1" }, lock { instanceId = "i1" }]
        def s2locks = [lock { instanceId = "i2" }, lock { instanceId = "i2"; keyName = "name4"; keyGroup = "group1" }]

        when:
        cleanupTask.run()

        then:
        1 * expiryCalculator.findDeadSchedulers() >> [scheduler("i1"), scheduler("i2")]
        1 * locksDao.findFiredTriggerInstanceIds() >> []
        1 * locksDao.findLocks("i1") >> s1locks
        1 * locksDao.findLocks("i2") >> s2locks
        1 * triggerDao.findTrigger(TriggerKey.triggerKey("name4", "group1")) >> new Document()
        3 * locksDao.remove(_)
        2 * schedulerDao.remove(*_)
    }

    def "should remove locks without existing schedulers"() {
        given:
        def noSchedulerLocks = [lock { instanceId = "lockNoScheduler1"; keyName = "noSc1"; keyGroup = "group1" }]
        def s1locks = [lock { instanceId = "i1" }, lock { instanceId = "i1"; keyName = "name4"; keyGroup = "group1" }]
        def thisInstanceId = "thisSchedulerId"

        when:
        cleanupTask.run()

        then:
        schedulerDao.instanceId >> thisInstanceId
        1 * expiryCalculator.findDeadSchedulers() >> [scheduler("i1")]
        1 * locksDao.findFiredTriggerInstanceIds() >> ["i1", "lockNoScheduler1", thisInstanceId]
        1 * locksDao.findLocks("lockNoScheduler1") >> noSchedulerLocks
        1 * locksDao.findLocks("i1") >> s1locks
        0 * locksDao.findLocks(thisInstanceId)
        1 * triggerDao.findTrigger(TriggerKey.triggerKey("name4", "group1")) >> new Document()
        2 * locksDao.remove(_)
        2 * schedulerDao.remove(*_)
    }

    def Scheduler scheduler(String instanceId) {
        new Scheduler("sname", instanceId, defaultLastCheckin, defaultCheckinInterval)
    }
}
