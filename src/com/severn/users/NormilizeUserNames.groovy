import com.google.appengine.api.datastore.*
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.memcache.*

import java.util.logging.*

import javax.mail.*
import javax.mail.internet.*

import groovy.json.*

Logger logger = Logger.getLogger('com.severn')
def ds = DatastoreServiceFactory.getDatastoreService()
def mc = MemcacheServiceFactory.getMemcacheService('default')
def badChars = '(\\\n|\\\r)'
def ents = []
def count = 0, bads = 0

def i = ds.prepare(new Query('User')).asIterable(FetchOptions.Builder.withChunkSize(50000))

i.each {
    String name = it.getProperty('name')?.toString()
    count++
    if (name && name.matches(badChars)) {
        bads++
        logger.log(Level.FINE, "Found ${it}")
        it.setProperty('name', name.replaceAll(badChars, ''))
        logger.log(Level.FINE, "New ${it}")
        ents << it
    }
    
    if (ents.size() >= 200) {
        ds.put(ents)
        mc.deleteAll(ents.collect { "User_${it.key.id}".toString() })
        ents.clear()
    }
}

if (ents.size() >= 0) {
    ds.put(ents)
    mc.deleteAll(ents.collect { "User_${it.key.id}".toString() })
    ents.clear()
}

def result = 'OK. Proccessed ' + count + ', found ' + bads
notifyEnded('sergey.shcherbovich@synesis.ru', 'wild-ride-app-viber@appspot.gserviceaccount.com', result)

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