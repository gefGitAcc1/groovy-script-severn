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

def userIds = [6139421341515770L,4817869970866176L,5078394098155520L,5760942560247800L,6092338265849850L,5654126152122360L,4544263040794620L,5916019475349500L,4833965438402560L,5123002376126460L,5660949670264830L,5232657997758460L,4874905444155390L,5330657988313080L,5291216806608890L,6511460982194170L,5389563850653690L,5248645587795960L,5176463687942140L,4713243628011520L,4862261645541370L,6200993220919290L,6288076333645820L,6353558847881210L]

GcsFileOptions gcsFileOptions = new GcsFileOptions.Builder().mimeType(MediaType.TEXT_PLAIN_VALUE).build()
GcsService gcsService = GcsUtils.getRetryGcsService()

// GcsFilename newFile = new GcsFilename('gambino-slots-events', 'tiers/2017/10/13/fake_insert.log')
GcsFilename newFile = new GcsFilename('gambino-slots-events-temp', 'tiers/2017/10/13/fake_insert.log')

Channels.newReader(openReadChannel.openReadChannel, "")

Writer writer = Channels.newWriter(gcsService.createOrReplace(newFile, gcsFileOptions), "UTF-8")

long ts = System.currentTimeMillis() / 1000
def count = 0

userIds.each { long userId ->
    User user = userDAO.getUser(userId)
    if (user) {
        long tier = tm.floorEntry(user.getContributionBalance()).getValue()
        
        writer.write("${userId},${ts},${ts},${tier-1},completed")
        writer.write('\n')
        writer.write("${userId},${ts},,${tier},")
        writer.write('\n')
        
        count++
    } else {
        writer.write("No user ${userId}")
        writer.write('\n')
    }
}

closeQuietly(writer)

"OK! Count ${count}".toString()