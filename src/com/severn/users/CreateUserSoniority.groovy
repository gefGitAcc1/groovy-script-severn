import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.*;

import javax.mail.*;
import javax.mail.internet.*;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.springframework.context.ApplicationContext;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.GetQueryResults;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.appengine.api.ThreadManager;
import com.google.appengine.api.datastore.*;
import com.google.appengine.repackaged.org.joda.time.DateTime;
import com.severn.common.bigquery.BigQueryServiceSupport;
import com.severn.common.services.GoogleServiceFactory;
import com.severn.script.utils.BigQueryScriptUtils;

Logger logger = Logger.getLogger('com.severn')

DatastoreService ds = DatastoreServiceFactory.getDatastoreService()
def dataSet = 'DWH', table = 'raw_sessions'
def batchesCount = 100

def usersRaw = BigQueryScriptUtils.executeQuery("SELECT max(user_id) as max_uid, min(user_id) as min_uid FROM [${dataSet}.${table}] WHERE user_id is not null AND platform_id IS NOT NULL AND session_ts IS NOT NULL").next()
long minUid = Long.parseLong(usersRaw[1]), maxUid = Long.parseLong(usersRaw[0]), diff = (maxUid - minUid) / batchesCount
def batches = []
(batchesCount + 1).times { i ->
    def batch = [:]
    batch.from = minUid + i * diff + (i ? 1 : 0)
    batch.to   = minUid + ( i + 1 ) * diff
    batches << batch
}

String querySql = 'SELECT user_id, platform_id, session_ts as ts, date(session_ts) as dt, datediff(current_timestamp(), timestamp(session_ts)) as df ' +
    " FROM [${dataSet}.${table}] " +
    ' WHERE user_id IS NOT NULL AND platform_id IS NOT NULL AND session_ts IS NOT NULL' +
        ' %s ' +
    ' group by 1,2,3,4,5 ' +
    ' order by 1,2,3,4 ';

Long previousUserId = null
String previousPlatform = null
Entity entity = null
def ents = [], c = 0

long sTs = System.currentTimeMillis()
//ExecutorService executorService = Executors.newFixedThreadPool(5, ThreadManager.backgroundThreadFactory())
ExecutorService executorService = Executors.newFixedThreadPool(5, ThreadManager.currentRequestThreadFactory())

logger.log(Level.FINE, "Starting ${batches.size()} jobs")

List<Future<Long>> results = []
batches.each { batch ->
    def f = executorService.submit( new TaskRunner(batch, querySql) )
    results << f
}

logger.log(Level.FINE, "Waiting for results ${results?.size()}")

for (Future<Long> f : results) {
    c += f.get()
}
executorService.shutdown()

logger.log(Level.FINE, "Done")

def result = "Ok. $c. Took ${DurationFormatUtils.formatDuration(System.currentTimeMillis()-sTs, 'HH:mm:ss.S')}".toString()
notifyEnded('sergey.shcherbovich@synesis.ru', 'wild-ride-app-viber@appspot.gserviceaccount.com', result)
result

class TaskRunner implements Callable<Long> {
    Logger logger = Logger.getLogger('com.severn')
    def batch, sqlPattern
    
    TaskRunner(batch, sqlPattern) {
        this.batch = batch
        this.sqlPattern = sqlPattern
    }
    @Override
    public Long call() throws Exception {
        logger.log(Level.FINE, "${Thread.currentThread()} - Kicking off ${batch}")
        def totalRecs = 0, previousUserId = 0l, previousPlatform = 0l, entity = null, ents = [], c = 0l
        Key key = KeyFactory.createKey('ScriptBatchResult', "${batch.from}_${batch.to}".toString())
        DatastoreService ds = GoogleServiceFactory.getRetryingDatastoreService()
        try {
            GoogleServiceFactory.getDatastoreService().get(key)
            logger.log(Level.FINE, "${Thread.currentThread()} - ${batch} was already processed")
            return 0l
        } catch (EntityNotFoundException enfe) {
            def sql = String.format(sqlPattern, "and user_id >= ${batch.from} and user_id <=${batch.to}".toString())

            def raws = BigQueryScriptUtils.executeQuery(sql)

            logger.log(Level.FINE, "${Thread.currentThread()} - Got results")

            raws.each { cells ->
                totalRecs++
                Long currentUserId = Long.parseLong(cells[0])
                String currentPlatform = cells[1]

                if (!previousUserId || !previousPlatform || previousUserId.longValue() != currentUserId.longValue() || !previousPlatform.equals(currentPlatform)) {
                    c++
                    // i.e. new User/Platform
                    entity = new Entity('UserSeniority', "${currentUserId}_${currentPlatform}".toString())
                    entity.setUnindexedProperty('updateTs', System.currentTimeMillis())
                    entity.setUnindexedProperty('createTs', 1000l * Double.parseDouble(cells[2]).longValue())
                    entity.setUnindexedProperty('history', new ArrayList<Long>())

                    ents << entity
                }

                BitSet bs = BitSet.valueOf(extract(entity.getProperty('history')))
                bs.set(Long.parseLong(cells[4]).intValue())
                entity.setUnindexedProperty('history', wrap(bs.toLongArray()))

                previousUserId = currentUserId
                previousPlatform = currentPlatform

                if (ents.size() == 500) {
                    logger.log(Level.FINE, "${Thread.currentThread()} - Flushing ${ents.size()} entities")
                    ds.put(ents)
                    ents = []
                }
            }
            if (ents) {
                ds.put(ents)
            }

            def e = new Entity(key)
            e.setProperty('completed', System.currentTimeMillis())
            e.setUnindexedProperty('doneAt', DateTime.now().toString())
            e.setUnindexedProperty('recsProcessed', totalRecs)
            e.setUnindexedProperty('dataPosted', c)
            ds.put(e)
        }
        logger.log(Level.FINE, "Done ${c} of ${totalRecs}")
        return totalRecs;
    }
    
    long[] extract(List<Long> history) {
        long[] model = new long[history.size()];
        for (int idx = 0; idx < history.size(); idx++) {
            model[idx] = history.get(idx);
        }
        model
    }
    
    List<Long> wrap(long[] model) {
        ArrayList<Long> wrapped = new ArrayList<Long>(model.length)
        for (long m : model) {
            wrapped.add(Long.valueOf(m))
        }
        wrapped
    }
}

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