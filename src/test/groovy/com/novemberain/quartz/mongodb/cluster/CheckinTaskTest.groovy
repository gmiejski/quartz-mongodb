package com.novemberain.quartz.mongodb.cluster

import com.mongodb.MongoException
import com.novemberain.quartz.mongodb.cluster.checkin.CheckinTask
import com.novemberain.quartz.mongodb.dao.LocksDao
import com.novemberain.quartz.mongodb.dao.SchedulerDao
import com.novemberain.quartz.mongodb.dao.TriggerDao
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
    def checkinTask = new CheckinTask(schedulerDao)

    def 'should store scheduler data to checkin'() {
        when:
        checkinTask.run()
        checkinTask.run()

        then:
        2 * schedulerDao.checkIn()
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

    def Scheduler scheduler(String instanceId) {
        new Scheduler("sname", instanceId, defaultLastCheckin, defaultCheckinInterval)
    }

}