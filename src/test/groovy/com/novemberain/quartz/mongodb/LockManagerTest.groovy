package com.novemberain.quartz.mongodb

import com.mongodb.MongoWriteException
import com.mongodb.ServerAddress
import com.mongodb.WriteError
import com.novemberain.quartz.mongodb.dao.LocksDao
import com.novemberain.quartz.mongodb.cluster.ExpiryCalculator
import org.bson.BsonDocument
import org.quartz.TriggerKey
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject

import static com.novemberain.quartz.mongodb.lock.LockBuilder.*

class LockManagerTest extends Specification {

    @Shared def tkey = new TriggerKey('n1', 'g1')

    def locksDao = Mock(LocksDao)
    def expiryCalc = Mock(ExpiryCalculator)

    @Subject def manager = new LockManager(locksDao, expiryCalc)

    def newWriteException() {
        new MongoWriteException(
                new WriteError(42, 'Just no!', BsonDocument.parse('{}')),
                new ServerAddress())
    }

    def 'try lock should lock trigger when have no locks'() {
        given:
        1 * locksDao.lockTrigger(tkey)

        expect:
        manager.tryLock(tkey)
    }

    def 'try lock cannot get existing lock for expiration check'() {
        given:
        1 * locksDao.lockTrigger(tkey) >> { throw newWriteException() }

        expect:
        !manager.tryLock(tkey)
    }

    def 'should not relock when lock is not found'() {
        given:
        1 * locksDao.findTriggerLock(tkey) >> null

        expect:
        !manager.relockExpired(tkey)
    }

    def 'should not relock valid lock'() {
        given:
        def existingLock = lock {
            withTime(0)
        }

        when:
        def relocked = manager.relockExpired(tkey)

        then:
        0 * locksDao.lockTrigger(_ as TriggerKey)
        1 * locksDao.findTriggerLock(tkey) >> existingLock
        1 * expiryCalc.isTriggerLockExpired(existingLock) >> false
        !relocked
    }

    def 'should relock expired lock'() {
        given:
        def existingLock = lock {
            withTime(123)
        }

        when:
        def relocked = manager.relockExpired(tkey)

        then:
        0 * locksDao.lockTrigger(_ as TriggerKey)
        1 * locksDao.findTriggerLock(tkey) >> existingLock
        1 * expiryCalc.isTriggerLockExpired(existingLock) >> true
        1 * locksDao.relock(tkey, new Date(123)) >> expectedResult
        relocked == expectedResult

        where:
        expectedResult << [false, true]
    }
}