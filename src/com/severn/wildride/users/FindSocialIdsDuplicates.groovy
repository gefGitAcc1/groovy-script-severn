import com.google.appengine.api.datastore.*
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.memcache.*

import java.util.logging.*

import groovy.json.*

def ds = DatastoreServiceFactory.getDatastoreService()
def mc = MemcacheServiceFactory.getMemcacheService('default')
def prev = null
def ents = []
def count = 0

def i = ds.prepare(new Query('User').addSort('socialId', SortDirection.ASCENDING)).asIterable(FetchOptions.Builder.withChunkSize(2000))

i.each {
    if (prev) {
        def prevSid = prev.getProperty('socialId')
        def currSid = it.getProperty('socialId')

        if (prevSid && currSid && Objects.equals(prevSid, currSid)) {
            logger.log(Level.FINE, "Found ${prev.getProperty('level')} and ${it.getProperty('level')}. UserId ${it.key.id} : ${prev.key.id}. SocialId ${it.getProperty('socialId')}".toString())
            
            def uas = ds.prepare(new Query('UserAuthentication').setFilter(new FilterPredicate('userId', FilterOperator.EQUAL, prev.key.id))).asList(FetchOptions.Builder.withChunkSize(2000))
            ents << (uas ? it : prev)
            // if (Objects.equals(prev.getProperty('level'), it.getProperty('level'))) {
                
            // } else {
            //     // ents << ( prev.getProperty('level') < it.getProperty('level') ? prev : it )
            // }
        }
    }
    count++
    prev = it
}

logger.log(Level.FINE, "Found ${ents}")

if (ents) {
    ds.put(ents.collect {
        def d = new Entity('UserBackup_UserAuthentication', it.key.id)
        d.setPropertiesFrom(it)
        d
    })

    ds.delete(ents.collect { it.key })
}

'OK. Found ' + ents.size() + '. Proccessed ' + count