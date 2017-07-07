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

import com.severn.datastore.legacy.*

DatastoreService ds = DatastoreServiceFactory.getDatastoreService()
MemcacheService  mc = MemcacheServiceFactory.getMemcacheService()

def entities = []
def dsUtil = new DatastoreUtil('ContestBooster')

dsUtil.shouldUpdate = { Entity entity ->
    int cnt = entity.getProperty('count')
    if (cnt && (cnt > 1)) {
        entities << entity
//        logger.fine("Found entity : ${entity}");
    }
    false
}

dsUtil.updateEntity = { Entity entity ->
    null
}

def infos = dsUtil.execute()

def newKeys = entities.collect { Entity it -> KeyFactory.createKey('ContestBoosterState', it.key.id) }
def ents = ds.get(newKeys).values()
def olds = entities.collectEntries { Entity it -> [(it.key.id) : it] }

ents.each { Entity it ->
//    logger.fine("Old Entity : ${it}")
    Entity oldEnt = olds[it.key.id]
    def cntOld = oldEnt.getProperty('count')
    def cntNew = it.getProperty('count')
    it.setUnindexedProperty('count', cntOld + cntNew)
    
    olds.remove(it.key.id)
    
//    logger.fine("New Entity : ${it}")
}

def news = olds.values().collect { Entity old ->
    Entity newOne = new Entity(KeyFactory.createKey('ContestBoosterState', old.key.id))
    newOne.setPropertiesFrom(old)
    logger.fine("Totally new ${newOne}");
    newOne
}

def mcKeys = ents.collect { Entity it -> "ContestBooster_${it.key.id}".toString() }
def mcKeysNew = news.collect { Entity it -> "ContestBooster_${it.key.id}".toString() }

//logger.fine("New Entities ${news}");

//ds.put(ents)
ds.put(news)
//mc.deleteAll(mcKeys)
mc.deleteAll(mcKeysNew)

def result = 'Ok ' + olds.size()
//notifyEnded('sergey.shcherbovich@synesis.ru', result)
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