import com.google.appengine.api.datastore.DatastoreServiceFactory
import com.google.appengine.api.datastore.Entity
import com.google.appengine.api.datastore.Query
import com.google.appengine.api.datastore.Query.Filter
import com.google.appengine.api.datastore.Query.FilterOperator
import com.google.appengine.api.datastore.Query.FilterPredicate
import com.severn.datastore.legacy.DatastoreUtil

Filter f = new FilterPredicate('segmentableGiftId', FilterOperator.EQUAL, 9642)
Query q = new Query('Gift_marketingCompensation').setFilter(f)


def coll = []

def s = { Entity it ->
    if (it.getProperty('collectsRemain')?.longValue() > 0) {
        coll << it.key
    } 
    false
}

def res = new DatastoreUtil(q).setShouldUpdate(s).execute()

DatastoreServiceFactory.getDatastoreService().delete(coll)
'OK ' + res + ', found ' + coll.size()