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

GcsFileOptions gcsFileOptions = new GcsFileOptions.Builder().mimeType(MediaType.TEXT_PLAIN_VALUE).build()
GcsService gcsService = GcsUtils.getRetryGcsService()

GcsFilename cheatsCsv = new GcsFilename('gambino-slots-events-temp', 'cheat_users.csv')
Writer writer = Channels.newWriter(gcsService.createOrReplace(cheatsCsv, gcsFileOptions), "UTF-8")

long ts = System.currentTimeMillis() / 1000
def count = 0

def shouldUpdate = { Entity it ->
    long userId = it.key.id
    Boolean cheat = it.getProperty('cheatUser')
    if (cheat) {
        count++
        logger.log(Level.INFO, "Cheat user ${it}")
        writer.write("0,${userId},true")
        writer.write('\n')
    }
    false
}

def res = new DatastoreUtil('User').setShouldUpdate(shouldUpdate).setUpdateEntity(null).setMemcacheKeyProvider(null).setCapacity(1).execute()

closeQuietly(writer)

"OK! Count ${count}, processing result ${res}".toString()