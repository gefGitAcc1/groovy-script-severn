import java.util.logging.Level

import org.springframework.util.StringUtils

import com.google.appengine.api.datastore.Entity
import com.severn.datastore.legacy.DatastoreUtil

def ds = new DatastoreUtil('UserInstallation')

def platforms = ['uwp','wp'] as Set<String>
int count = 0

ds.shouldUpdate = { Entity e ->
    def platform = e.getProperty('platform') as String
    if (platforms.contains(platform)) {
        if (StringUtils.isEmpty(e.getProperty('advertiserId'))) {
            count++
        }
    }
    false
}
ds.updateEntity = { null }

def result = ds.execute()

logger.log(Level.INFO, 'Count = {0}', [count])

"OK. Count ${count}. Result ${result}"