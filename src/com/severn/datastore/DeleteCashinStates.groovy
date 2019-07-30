import com.google.appengine.api.datastore.DatastoreServiceFactory
import com.google.appengine.api.datastore.Entity
import com.google.appengine.api.datastore.Query
import com.google.appengine.api.datastore.Query.Filter
import com.google.appengine.api.datastore.Query.FilterOperator
import com.google.appengine.api.datastore.Query.FilterPredicate
import com.google.appengine.api.memcache.MemcacheService
import com.google.appengine.api.memcache.MemcacheServiceFactory
import com.google.appengine.tools.cloudstorage.*
import com.severn.datastore.legacy.DatastoreUtil
import com.severn.script.service.GoogleCloudStorageScriptStorageService
import com.google.appengine.api.datastore.*
import java.nio.channels.Channels;

String kind = 'UserProfile'

def ids = [] as ArrayList<Long>

GcsService gcsService = GcsServiceFactory.createGcsService(RetryParams.getDefaultInstance())
GcsFilename fileName = new GcsFilename('prod-test-data', 'reset2.csv')

BufferedReader reader = new BufferedReader(Channels.newReader(gcsService.openReadChannel(fileName, 0), "UTF-8"));

MemcacheService mc = MemcacheServiceFactory.getMemcacheService('default')
DatastoreService ds = DatastoreServiceFactory.getDatastoreService()

try {
    String line
    while (line = reader.readLine()) {
        if (line) {
            ids << Long.parseLong(line.trim())
        }
    }
    
    if (ids && ids.size() > 500) {
        def mcids = ids.collect { id -> "CashInVegasState_$id".toString() }
        def dsids = ids.collect { id -> KeyFactory.createKey('CashInVegasState', id) }
        
        mc.deleteAll(mcids)
        ds.delete(dsids)
        
        ids = []
    }
} finally {
    reader.close()
}

if (ids) {
    def mcids = ids.collect { id -> "CashInVegasState_$id".toString() }
    def dsids = ids.collect { id -> KeyFactory.createKey('CashInVegasState', id) }
    
    mc.deleteAll(mcids)
    ds.delete(dsids)
}

'OK '