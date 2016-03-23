import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.context.ApplicationContext;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.KeyFactory;
import com.severn.common.dao.DataStorageDAO;
import com.severn.common.services.GoogleServiceFactory;
import com.severn.script.utils.DatastoreSciptUtils;

Logger logger = Logger.getLogger('com.severn')

def keys = []
def shouldUpdate = { it ->
    if (!it?.key?.id) {
        logger.log(Level.FINE, "Bad key ${it.key}")
        keys << it.key
    }
    if (keys && keys.size() == 200) {
        GoogleServiceFactory.getRetryingDatastoreService().delete(keys)
        keys = []
    }
    false
}

DatastoreSciptUtils.processEntities('UserPushData', shouldUpdate, null, null, null)
if (keys) {
    GoogleServiceFactory.getRetryingDatastoreService().delete(keys)
}

'OK'
