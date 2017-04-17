import java.util.logging.*;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.context.ApplicationContext;

import javax.mail.*;
import javax.mail.internet.*

import com.google.appengine.api.datastore.DatastoreService
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory
import com.google.apphosting.api.ApiProxy;
import com.severn.common.bigquery.BigQueryServiceSupport;
import com.severn.common.domain.User;
import com.severn.common.utils.BigQueryUtil
import com.severn.common.utils.DatastoreUtil;
import com.severn.script.utils.DatastoreSciptUtils;

ApplicationContext ctx = binding.variables.get('applicationContext')
BigQueryServiceSupport bigQueryClient = ctx.getBean('bigQueryServiceSupport')
DateTime dt = DateTime.now(DateTimeZone.UTC)
DateTimeFormatter f = DateTimeFormat.forPattern('yyyy-MM-dd').withZoneUTC()

def MACHINE_STATE = 'MachineState', OTHER_STATES = ['TutorialState', 'BonusState']
def otherStates = []
Map<Key, Entity> loaded = [:]

DatastoreService ds = DatastoreServiceFactory.datastoreService

def dsUtil = new DatastoreUtil(MACHINE_STATE).setTestRun(false)
dsUtil.beforeProcessEntities = { Iterable<Entity> entities ->
    def tutorialKeys = entities.collect { entity -> KeyFactory.createKey('TutorialState', entity.key.name) }
    def bonusKeys = entities.collect { entity -> KeyFactory.createKey('BonusState', entity.key.name) }
    def keys = bonusKeys + tutorialKeys

    loaded = ds.get(keys)
}

dsUtil.shouldUpdate = { Entity entity ->
    def result = loaded.containsKey(KeyFactory.createKey('TutorialState', entity.key.name))
    result |=  loaded.containsKey(KeyFactory.createKey('BonusState', entity.key.name))
    result
}

dsUtil.updateEntity = { Entity entity ->
    def tutorial = loaded.get(KeyFactory.createKey('TutorialState', entity.key.name))
    def bonus    = loaded.get(KeyFactory.createKey('BonusState', entity.key.name))

    if (tutorial) {
        entity.setPropertiesFrom(tutorial)
    }
    if (bonus) {
        entity.setPropertiesFrom(bonus)
    }

    entity
}

def infos = dsUtil.execute()

def result = 'Ok ' + infos
notifyEnded('sergey.shcherbovich@synesis.ru', result)
result

void notifyEnded(def reciever, def message) {
    Properties props = new Properties();
    Session session = Session.getDefaultInstance(props, null);
    
    try {
        MimeMessage msg = new MimeMessage(session);
        msg.setFrom(new InternetAddress("${ApiProxy.getCurrentEnvironment().getAppId().replace('s~', '')}@appspot.gserviceaccount.com", "Script executor module"));
        msg.addRecipient(Message.RecipientType.TO,
         new InternetAddress(reciever, "Dear DEV"));
        msg.setSubject("Script was executed successfully at ${new Date()}");
        msg.setText(message);
        Transport.send(msg);
    
    } catch (AddressException e) {
        Logger.getLogger('com.severn').log(Level.WARNING, "Got", e)
    } catch (MessagingException e) {
        Logger.getLogger('com.severn').log(Level.WARNING, "Got", e)
    }
}