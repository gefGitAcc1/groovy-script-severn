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

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.commons.csv.CSVRecord
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
def counter = 0

GcsFileOptions gcsFileOptions = new GcsFileOptions.Builder().mimeType(MediaType.TEXT_PLAIN_VALUE).build()
GcsService gcsService = GcsUtils.getRetryGcsService()

GcsFilename file  = new GcsFilename('serg-cdn-test', 'slot-simulator-test-1.csv')

Writer writer = Channels.newWriter(gcsService.createOrReplace(file, gcsFileOptions), "UTF-8")

def uuid = UUID.randomUUID().toString()
def random = new Random()

def start = System.currentTimeMillis()
def iterations = 10_000_000

(1..iterations).each { iteration ->
    def row = (1..20).collect { it -> "field${it}" }.join(',')
    writer.write("${uuid},${iteration},${row}\r\n")
}

closeQuietly(writer)

def res = "OK : ${iterations}. Took ${sprintf('%04d', System.currentTimeMillis() - start)}"

println res

res.toString()