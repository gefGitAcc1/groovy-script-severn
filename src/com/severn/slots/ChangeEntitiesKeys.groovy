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
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory
import com.google.apphosting.api.ApiProxy;
import com.severn.common.bigquery.BigQueryServiceSupport;
import com.severn.common.domain.User;
import com.severn.common.utils.BigQueryUtil;
import com.severn.script.utils.DatastoreSciptUtils;

ApplicationContext ctx = binding.variables.get('applicationContext')
BigQueryServiceSupport bigQueryClient = ctx.getBean('bigQueryServiceSupport')
DateTime dt = DateTime.now(DateTimeZone.UTC)
DateTimeFormatter f = DateTimeFormat.forPattern('yyyy-MM-dd').withZoneUTC()
def infos = []
def c = 0, tot = 0, results = 0

def mapRes = [:]
Logger logger = Logger.getLogger('com.severn')

def QUERY = 'select auth_id, user from ( SELECT device_identifier as auth_id, nth_value(user_id, 1) over (partition by device_identifier order by session_ts desc) as user FROM [DWH.raw_sessions] ) group by 1,2'
Iterator res = BigQueryScriptUtils.executeQuery(QUERY)
res.each { it ->
    try {
        mapRes.put((Long.parseLong(it[0])), (Long.parseLong(it[1])))
        results++
    } catch (Exception e) {
        logger.log(Level.WARNING, "Bad ${it}")
    }
}

logger.log(Level.FINE, "Mapping size ${mapRes.size()}")

DatastoreService ds = DatastoreServiceFactory.getDatastoreService()
int aSize = 2000
def ents = []

def shouldUpdate = {
    Entity entity = it
    String[] parts = entity.key.name.split('_')
    boolean result = null != parts && parts.length == 2
    result = result && (mapRes[Long.parseLong(parts[0])] != null)
    result = result && (entity.getProperty('checked') == null)
    
    if (result) {
        entity.setUnindexedProperty('checked', true)
        ents << entity
    }
    
    if (ents.size() == aSize) {
        ds.put(ents)
//        logger.log(Level.FINE, "Saving ${ents}")
        ents = []
    }
     
    result
}
def updateEntity = {
    Entity entity = it
    
    String[] parts = entity.key.name.split('_')
    Long userId = mapRes[Long.parseLong(parts[0])]
    String newKey = "${userId}_${parts[0]}_${parts[1]}" 
    
    String kind = entity.key.kind + '_new'
    Entity newEntity = new Entity(KeyFactory.createKey(kind, newKey))
    newEntity.setPropertiesFrom(entity)
    newEntity
}

['MachineState', 'BonusState', 'TutorialState'].each { entityKind ->
    def info = DatastoreSciptUtils.processEntities(entityKind, shouldUpdate, updateEntity, null, [capacity: '250', limit: '50000'])
    logger.log(Level.INFO, "Done with ${entityKind} : ${info}")
    infos << info
}

if (ents) {
    ds.put(ents)
}

def result = 'Ok ' + infos + ', rows ' + results
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