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
def count = 0

GcsFileOptions gcsFileOptions = new GcsFileOptions.Builder().mimeType(MediaType.TEXT_PLAIN_VALUE).build()
GcsService gcsService = GcsUtils.getRetryGcsService()

// GcsFilename emailsCsv = new GcsFilename('serg-test', 'emails.csv')
GcsFilename emailsCsv = new GcsFilename('serg-test', 'Cleaned.txt')
GcsFilename guidsCsv  = new GcsFilename('serg-test', 'guids.csv')

BufferedReader reader = new BufferedReader(Channels.newReader(gcsService.openReadChannel(emailsCsv, 0), "UTF-8"))
Writer writer = Channels.newWriter(gcsService.createOrReplace(guidsCsv, gcsFileOptions), "UTF-8")

CSVPrinter csvPrinter = new CSVPrinter(writer, 
    CSVFormat.EXCEL.withDelimiter(',' as char).withHeader("email", "guid"))

// CSVFormat.EXCEL.withDelimiter(',' as char).withHeader()
CSVFormat.EXCEL.withDelimiter(',' as char)
    .parse(reader).iterator().each { CSVRecord record ->
    String email = record.get(0) as String

    String[] pfs = email.split('@')
    String prefix = pfs ? pfs[0] : ''

    csvPrinter.printRecord(email, "${prefix}-${UUID.randomUUID()}-${UUID.randomUUID()}")
}

csvPrinter.flush()

closeQuietly(writer)
closeQuietly(reader)

"OK! Count ${count}".toString()