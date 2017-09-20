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
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.gson.Gson
import com.severn.common.bigquery.BigQueryServiceSupport;
import com.severn.common.domain.User;
import com.severn.common.utils.BigQueryUtil;
import com.severn.datastore.legacy.DatastoreUtil;

ApplicationContext ctx = binding.variables.get('applicationContext')
BigQueryServiceSupport bigQueryClient = ctx.getBean('bigQueryServiceSupport')
DateTime dt = DateTime.now(DateTimeZone.UTC)
DateTimeFormatter f = DateTimeFormat.forPattern('yyyy-MM-dd').withZoneUTC()
def infos = []
def c = 0, tot = 0, results = 0

def mapRes = [:]
Logger logger = Logger.getLogger('com.severn')

String query = """
SELECT
  user_id, real_amount, transaction_ts, rn
FROM
(
  SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY transaction_ts DESC) rn
  FROM [DWH.transactions] 
  WHERE sku_id IN ('coins', 'coins_and_extra', 'highest_balance_coins', 'level_100_plus_coins')
  ORDER BY user_id, transaction_ts DESC
) as ss
WHERE rn <= 10
"""
def res = new BigQueryUtil(query).execute()

Map<Long, List<String>> data = [:]

res.each { it ->
    try {
        long userId = Long.parseLong(it[0].toString())
        List<String> costs = data.get(userId)
        if (!costs) {
            costs = [] as ArrayList
            data.put(userId, costs)
        }
        costs << it[1].toString()

        results++
    } catch (Exception e) {
        logger.log(Level.WARNING, "Bad ${it}")
    }
}

def entities = []

DatastoreService ds = DatastoreServiceFactory.getDatastoreService()
Gson g = new Gson()

data.entrySet().each { Map.Entry it ->
    Long userId = it.key
    List<String> lst = it.value

    try {
        Entity e = ds.get(KeyFactory.createKey('PurchaseStatistics', userId))
        e.setUnindexedProperty('lastCoinsPurchases', g.toJson(lst).replaceAll('\"', ''))
        ds.put(e)
        c++
    } catch (Exception e) {
    }
}

def result = 'Ok users ' + c + ', rows ' + results
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