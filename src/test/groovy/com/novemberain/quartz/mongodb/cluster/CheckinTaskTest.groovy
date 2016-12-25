package com.novemberain.quartz.mongodb.cluster

import com.mongodb.MongoException
import com.novemberain.quartz.mongodb.dao.LocksDao
import com.novemberain.quartz.mongodb.dao.SchedulerDao
import com.novemberain.quartz.mongodb.dao.TriggerDao
import com.novemberain.quartz.mongodb.lock.Lock
import com.novemberain.quartz.mongodb.lock.LockBuilder
import org.bson.Document
import org.quartz.TriggerKey
import spock.lang.Shared
import spock.lang.Specification

import static com.novemberain.quartz.mongodb.lock.LockBuilder.lock

class CheckinTaskTest extends Specification {

    @Shared
    def defaultCheckinInterval = 100
    @Shared
    def defaultLastCheckin = 1000

    def schedulerDao = Mock(SchedulerDao)
    def locksDao = Mock(LocksDao)
    def triggerDao = Mock(TriggerDao)
    def expiryCalculator = Mock(ExpiryCalculator)
    def checkinTask = new CheckinTask(schedulerDao, locksDao, triggerDao, expiryCalculator)

    def 'should store scheduler data to checkin'() {
        when:
        checkinTask.run()
        checkinTask.run()

        then:
        2 * schedulerDao.checkIn()
        2 * expiryCalculator.findDeadSchedulers() >> []
    }

    def 'should stop scheduler when hit by exception'() {
        given:
        def errorHandler = Mock(Runnable)
        1 * schedulerDao.checkIn() >> {
            throw new MongoException('Checkin Error!')
        }

        checkinTask.setErrorHandler(errorHandler)

        when:
        checkinTask.run()

        then:
        1 * errorHandler.run()
    }

    def "should remove dead schedulers and their locks matching no existing trigger"() {
        given:
        def s1locks = [lock { instanceId = "i1" }, lock { instanceId = "i1" }]
        def s2locks = [lock { instanceId = "i2" }, lock { instanceId = "i2"; keyName = "name4"; keyGroup = "group1" }]

        when:
        checkinTask.run()

        then:
        1 * schedulerDao.checkIn()
        1 * expiryCalculator.findDeadSchedulers() >> [scheduler("i1"), scheduler("i2")]
        1 * locksDao.findLocks("i1") >> s1locks
        1 * locksDao.findLocks("i2") >> s2locks
        1 * triggerDao.findTrigger(TriggerKey.triggerKey("name4", "group1")) >> new Document()
        3 * locksDao.remove(_)
        2 * schedulerDao.remove(*_)
    }

    def Scheduler scheduler(String instanceId) {
        new Scheduler("sname", instanceId, defaultLastCheckin, defaultCheckinInterval)
    }

}