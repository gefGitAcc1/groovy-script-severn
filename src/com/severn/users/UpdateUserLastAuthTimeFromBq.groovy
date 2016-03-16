import java.util.logging.*;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.severn.common.domain.User;
import com.severn.script.utils.BigQueryScriptUtils;
import com.severn.script.utils.DatastoreSciptUtils;

def res = BigQueryScriptUtils.executeQuery('SELECT user_id, max(session_ts) as sts FROM [DWH.raw_sessions] GROUP BY 1')
def mapRes = res.collectEntries { it ->
    [(Long.parseLong(it[0])): Double.parseDouble(it[1]).longValue() * 1000]
}

def shouldUpdate = { 
    !it.getProperty('lastAuthTime') 
}
def updateEntity = {
    def ts = mapRes[it.key.id]
    if (ts) {
        it.setUnindexedProperty('lastAuthTime', ts)
    }
    it
}
def mcKeyProvider = { 
    "User_${it.key.id}".toString() 
}

def info = DatastoreSciptUtils.processEntities('User', shouldUpdate, updateEntity, mcKeyProvider)

'Ok ' + info