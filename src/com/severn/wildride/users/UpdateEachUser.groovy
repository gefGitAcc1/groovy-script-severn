import com.google.appengine.api.datastore.*
import com.google.appengine.api.datastore.FetchOptions.*
import com.google.appengine.api.memcache.*

import java.util.logging.*
import groovy.json.*

Logger logger = Logger.getLogger('com.severn.script')
def ds = DatastoreServiceFactory.getDatastoreService()

FetchOptions fo = FetchOptions.Builder.withChunkSize(2000)
Iterator<Entity> i = ds.prepare(new Query('User')).asIterator(fo)

def ents = []

i.each {
    if (it.getProperty('score')?.longValue() == 0) {
        it.setUnindexedProperty('score', 50000)
        ents << it
        
        if (ents.size() == 5000) {
            update(ents)
            ents.clear()
        }
    }
}

if (!ents.isEmpty()) {
    update(ents)
    ents.clear()
}

'OK'

void update(Collection ents) {
    Logger logger = Logger.getLogger('com.severn.script')
    logger.fine("Updating ${ents.size()} entities")

    DatastoreService ds = DatastoreServiceFactory.getDatastoreService()
    MemcacheService mc  = MemcacheServiceFactory.getMemcacheService('default')

    ds.put(ents)
    mc.deleteAll(ents.collect() {
        def id = it.key.id
        "User_$id".toString()
    });
}