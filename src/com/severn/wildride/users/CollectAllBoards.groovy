import com.google.appengine.api.memcache.*
import com.google.appengine.api.datastore.*
import java.util.logging.*

def ds = DatastoreServiceFactory.getDatastoreService()
def ads = DatastoreServiceFactory.getAsyncDatastoreService()

def preparedQuery = ds.prepare(new Query('User'))

def futures = []
def count = 0
def cursor = null
def done = false

while (!done) {
    def fo = FetchOptions.Builder.withLimit(10000).chunkSize(10000)
    if (cursor) {
        fo.startCursor(Cursor.fromWebSafeString(cursor))
    }
    def entities = preparedQuery.asQueryResultList(fo)
    def ents = []

    entities.each {
        def id = it.getKey().getId()
        Entity ent = new Entity(KeyFactory.createKey('CustomState', 'BoardCollected_' + id))
        ent.setUnindexedProperty('data', new Text('1447106631528'))
        ents << ent
        count++
    }

    cursor = entities.getCursor().toWebSafeString()

    logger.log(Level.FINE, 'Updating batch {0}', cursor)
    futures << ads.put(ents)

    done = null == cursor || entities.isEmpty()
}

logger.log(Level.FINE, 'Done iterating. Waiting for futures...')

futures.each { it.get() }

'OK. ' + count