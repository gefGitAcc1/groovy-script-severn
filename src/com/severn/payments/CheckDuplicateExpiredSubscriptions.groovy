import java.util.logging.*;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.context.ApplicationContext;

import javax.mail.*;
import javax.mail.internet.*;

import com.google.appengine.api.datastore.DatastoreService
import com.google.appengine.api.datastore.DatastoreServiceFactory
import com.google.appengine.api.datastore.Entity
import com.google.appengine.api.datastore.KeyFactory
import com.google.appengine.api.datastore.Query
import com.google.appengine.api.datastore.Text
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.gson.Gson
import com.severn.common.bigquery.BigQueryServiceSupport;
import com.severn.common.domain.User;
import com.severn.common.utils.BigQueryUtil;
import com.severn.datastore.legacy.DatastoreUtil;
import com.severn.payments.domain.SubscriptionInfo

ApplicationContext ctx = binding.variables.get('applicationContext')
BigQueryServiceSupport bigQueryClient = ctx.getBean('bigQueryServiceSupport')
DateTime dt = DateTime.now(DateTimeZone.UTC)
DateTimeFormatter f = DateTimeFormat.forPattern('yyyy-MM-dd').withZoneUTC()
DatastoreService ds = DatastoreServiceFactory.getDatastoreService()
Gson g = new Gson()

Query q = new Query('ExpiredSubscription')

def i = ds.prepare(q).asIterable()

def mapping = [:] as HashMap
def entitiesToDelete = []
def badsCnt = 0

i.each { Entity e ->
    String json = ((Text)e.getProperty('subscription'))?.value
    SubscriptionInfo si = g.fromJson(json, SubscriptionInfo.class)
    String id = si.getRecieptId()
    if (mapping.containsKey(id)) {
        badsCnt++
        entitiesToDelete << e.key
    } else {
        mapping.put(si.getRecieptId(), si)
    }
}
ds.delete(entitiesToDelete)

def result = 'Ok duplicates ' + badsCnt + ', oks ' + mapping.size()
// notifyEnded('sergey.shcherbovich@synesis.ru', bigQueryClient.getServiceAccountId(), result)
result

void notifyEnded(def reciever, def sender, def message) {
    Properties props = new Properties();
    Session session = Session.getDefaultInstance(props, null);
    
    try {
        MimeMessage msg = new MimeMessage(session);
        msg.setFrom(new InternetAddress(sender, "Script executor module"));
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