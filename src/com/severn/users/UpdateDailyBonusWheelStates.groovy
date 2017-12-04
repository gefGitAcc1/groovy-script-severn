import java.util.logging.*

import org.joda.time.DateTime

import com.google.appengine.api.datastore.Entity
import com.severn.datastore.legacy.DatastoreUtil

long dif = 1000L * 60 * 60 * 24
def cnt = 0
long now = System.currentTimeMillis()

def shouldUpdate = { Entity it ->
    def prop = it.getProperty('nextFreeAttempt')
    // def fin  = it.getProperty('finished')
    def res = (null != prop) ? true : false
    res
}
def update = { Entity it ->
    long nextFA = it.getProperty('nextFreeAttempt')
    DateTime dt = new DateTime(nextFA).withTimeAtStartOfDay()
    it.setUnindexedProperty('nextFreeAttempt', dt.getMillis())
//     it.setUnindexedProperty('finished', true)
    it
}
def mc = { Entity it ->
    "DailyBonusWheelState_${it.key.id}".toString()
}

def res = new DatastoreUtil('DailyBonusWheelState').setShouldUpdate(shouldUpdate).setUpdateEntity(update).setMemcacheKeyProvider(mc).execute()
'OK : ' + res + ', cnt ' + cnt