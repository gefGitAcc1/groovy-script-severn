import java.util.logging.*;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.context.ApplicationContext;

import javax.mail.*;
import javax.mail.internet.*;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.severn.common.bigquery.BigQueryServiceSupport;
import com.severn.common.domain.User;
import com.severn.script.utils.BigQueryScriptUtils;
import com.severn.script.utils.DatastoreSciptUtils;

ApplicationContext ctx = binding.variables.get('applicationContext')
BigQueryServiceSupport bigQueryClient = ctx.getBean('bigQueryServiceSupport')
DateTime dt = DateTime.now(DateTimeZone.UTC)
DateTimeFormatter f = DateTimeFormat.forPattern('yyyy-MM-dd').withZoneUTC()
def infos = []
def c = 0, tot = 0, results = 0

def mapRes = [:]
Logger logger = Logger.getLogger('com.severn')

Iterator res = BigQueryScriptUtils.executeQuery("SELECT user_id, max(session_ts) as sts FROM [DWH.raw_sessions] GROUP BY 1")
res.each { it ->
    results++
    mapRes.put((Long.parseLong(it[0])), Double.parseDouble(it[1]).longValue() * 1000)
}

logger.log(Level.FINE, "Mapping size ${mapRes.size()}")

def shouldUpdate = {
    !it.getProperty('lastAuthTime')
}
def updateEntity = {
    def ts = mapRes[it.key.id]
    if (ts) {
        it.setUnindexedProperty('lastAuthTime', ts)
    }
    it
}
def mcKeyProvider = {  "User_${it.key.id}".toString()  }

def info = DatastoreSciptUtils.processEntities('User', shouldUpdate, updateEntity, mcKeyProvider, [capacity: '250', limit: '5000'])

def result = 'Ok ' + info + ', rows ' + results
notifyEnded('sergey.shcherbovich@synesis.ru', bigQueryClient.getServiceAccountId(), result)
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