import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channels
import java.security.PrivateKey;
import java.util.ArrayList
import java.util.Arrays;
import java.util.List
import java.util.Map
import java.util.logging.*;

import javax.mail.*;
import javax.mail.internet.*;

import org.springframework.context.ApplicationContext
import org.springframework.http.MediaType

import com.google.appengine.api.datastore.DatastoreService
import com.google.appengine.api.datastore.DatastoreServiceFactory
import com.google.appengine.api.datastore.Entity
import com.google.appengine.api.datastore.KeyFactory
import com.google.appengine.tools.cloudstorage.GcsFileOptions
import com.google.appengine.tools.cloudstorage.GcsFilename
import com.google.appengine.tools.cloudstorage.GcsService
import com.severn.auth.service.AuthorizationService
import com.severn.bonuses.service.ExperienceBoosterService
import com.severn.common.dao.UserDAO
import com.severn.common.domain.AuthenticatedUser
import com.severn.common.domain.MapEntity
import com.severn.common.domain.MapEntryStringValue
import com.severn.common.domain.User
import com.severn.common.http.RequestHelper
import com.severn.common.utils.AuthenticationUtils
import com.severn.game.slots.engine.FreeSpinManager
import com.severn.payments.dao.PurchaseDAO
import com.severn.payments.dao.SubscriptionDAO
import com.severn.payments.domain.PurchaseTransaction
import com.severn.payments.domain.SubscriptionInfo
import com.severn.payments.domain.SubscriptionInfosPage
import com.severn.payments.service.SubscriptionsService
import com.severn.script.utils.GcsUtils

import com.severn.datastore.legacy.DatastoreUtil

import static org.codehaus.groovy.runtime.DefaultGroovyMethodsSupport.*;

Logger logger = Logger.getLogger('com.severn')

ApplicationContext ctx = binding.variables.get('applicationContext')
UserDAO userDAO = ctx.getBean('userDAO')

TreeMap tm = [:] as TreeMap
tm << [0d:1]
tm << [750d:2]
tm << [35000d:3]
tm << [100000d:4]
tm << [300000d:5]
tm << [1250000d:6]
tm << [4400000d:7]
tm << [16500000d:8]
tm << [50000000d:9]

GcsFileOptions gcsFileOptions = new GcsFileOptions.Builder().mimeType(MediaType.TEXT_PLAIN_VALUE).build()
GcsService gcsService = GcsUtils.getRetryGcsService()

GcsFilename tiersCsv = new GcsFilename('gambino-slots-events-temp', 'user_tiers.csv')
HashMap<Long, Integer> hm = new HashMap<>(500_000,1f)

BufferedReader reader = new BufferedReader(Channels.newReader(gcsService.openReadChannel(tiersCsv, 0), "UTF-8"))

String line
while (line = reader.readLine()) {
    if (line && !line.startsWith('user_id')) {
        String[] dt = line.split(',')
        hm.put(Long.parseLong(dt[0]), Integer.parseInt(dt[1]))
    }
}


// GcsFilename newFile = new GcsFilename('gambino-slots-events', 'tiers/2017/10/20/fake_insert.log')
GcsFilename newFile = new GcsFilename('gambino-slots-events-temp', 'tiers/2017/10/20/fake_insert.log')

Writer writer = Channels.newWriter(gcsService.createOrReplace(newFile, gcsFileOptions), "UTF-8")

long ts = System.currentTimeMillis() / 1000
def count = 0

def shouldUpdate = { Entity it ->
    long userId = it.key.id
    Double bal = it.getProperty('contributionBalance')
    if (null != bal) {
        Long tier = tm.floorEntry(bal).getValue()
        Long tierTracked = hm.get(userId)

        if (!Objects.equals(tier, tierTracked)) {
            count++
            
            writer.write("${userId},${ts},${ts},${tier-1},completed")
            writer.write('\n')
            writer.write("${userId},${ts},,${tier},")
            writer.write('\n')
        }
         
    }
    false
}

def res = new DatastoreUtil('User').setShouldUpdate(shouldUpdate).setUpdateEntity(null).setMemcacheKeyProvider(null).setCapacity(1).execute()

closeQuietly(writer)
closeQuietly(reader)

"OK! Count ${count}".toString()