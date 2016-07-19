import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.logging.*;

import javax.mail.*;
import javax.mail.internet.*;

import org.springframework.context.ApplicationContext;

import com.google.api.client.extensions.appengine.http.UrlFetchTransport;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.SecurityUtils;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.Bigquery.Jobs.GetQueryResults;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.appengine.api.datastore.*;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.severn.common.bigquery.BigQueryServiceSupport;
import com.severn.common.update.user.EntityUpdaters.EntityAwareEntityFieldUpdater;

Logger logger = Logger.getLogger('com.severn')

def ds = DatastoreServiceFactory.getDatastoreService()
def uids = [5639574185312256,6272386882076672,5737664527466496]
long groupMask = 0b10
def keys = uids.collect { it->
    KeyFactory.createKey('User', it)
}
def users = ds.get(keys)
users.values().each {
    long g = it.getProperty('groups').longValue()
    it.setUnindexedProperty('groups', g | groupMask)
}
ds.put(users.values())

def mc = MemcacheServiceFactory.getMemcacheService('default')
mc.deleteAll(uids.collect { it ->
    "User_${it}".toString()
})
'OK!'