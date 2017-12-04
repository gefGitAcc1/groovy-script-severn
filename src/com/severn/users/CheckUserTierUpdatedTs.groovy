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
Long INSTALL_TS = 1477921704000L
def count = 0

def shouldUpdate = { Entity it ->
    long userId = it.key.id
    Long tierStartTs = it.getProperty('tierStartTs')
    Long createdTs   = it.getProperty('createdTime')
    boolean res = false
    if (null == tierStartTs && INSTALL_TS > createdTs) {
        count++
//        res = true
        res = false
    }
    res
}

def update = { Entity it ->
    it.setUnindexedProperty('tierStartTs', 0)
    it
}

def res = new DatastoreUtil('User').setShouldUpdate(shouldUpdate).setUpdateEntity(update).setMemcacheKeyProvider(null).setCapacity(100).execute()

"OK! Count ${count}".toString()