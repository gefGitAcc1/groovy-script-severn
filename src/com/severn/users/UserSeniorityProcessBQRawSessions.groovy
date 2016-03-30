import java.nio.channels.Channels;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.*;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.joda.time.*;
import org.joda.time.format.*;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;

import javax.mail.*;
import javax.mail.internet.*;

import com.google.appengine.api.ThreadManager;
import com.google.appengine.api.datastore.*;
import com.google.appengine.api.memcache.*;
import com.google.appengine.tools.cloudstorage.*;
import com.severn.common.bigquery.*;
import com.severn.common.services.GoogleServiceFactory;
import com.severn.event.tracking.*;
import com.severn.script.utils.*;
import com.severn.segment.v2.*;

import static org.codehaus.groovy.runtime.DefaultGroovyMethodsSupport.*;

def c = 0, bucket = 'wild-ride-app-viber-temp', filesPrefix = 'raw_session_processed_3' , dataSet = 'TEST', table = 'raw_sessions_test'
Logger localLogger = Logger.getLogger('com.severn')

String SQL = "SELECT * FROM [${dataSet}.${table}] %s ORDER BY user_id, platform_id, session_ts"

def usersRaw = BigQueryScriptUtils.executeQuery("SELECT max(user_id) as max_uid, min(user_id) as min_uid FROM [${dataSet}.${table}] WHERE user_id is not null AND platform_id IS NOT NULL AND session_ts IS NOT NULL").next()
//def zeros = '00000000000000'
def zeros = '0000000000000' 
def sTs = System.currentTimeMillis()
long minUid = Long.valueOf(usersRaw[1]) / Long.valueOf("1${zeros}"), maxUid = Long.valueOf(usersRaw[0]) / Long.valueOf("1${zeros}")

localLogger.log(Level.FINE, "User Ids prefixes from ${minUid} to ${maxUid}")

ExecutorService executorService = Executors.newFixedThreadPool(5, ThreadManager.backgroundThreadFactory());

List<Future<Long>> results = []
(minUid..maxUid).each { aNumber ->
    def f = executorService.submit( new Runner(Long.parseLong("${aNumber}${zeros}"), Long.parseLong("${aNumber + 1}${zeros}"), zeros, SQL, bucket, filesPrefix) )
    results << f
}
for (Future<Long> f : results) {
    c += f.get()
}
executorService.shutdown()

"Ok. $c. Took ${DurationFormatUtils.formatDuration(System.currentTimeMillis()-sTs, 'HH:mm:ss.S')}".toString()

class Runner implements Callable<Long> {
    Logger localLogger = Logger.getLogger('com.severn')
    GcsFileOptions gcsFileOptions = new GcsFileOptions.Builder().mimeType(MediaType.TEXT_PLAIN_VALUE).build()

    long from, to
    def zeros, sql, bucket, filesPrefix

    Runner(long from, long to, def z, def sql, def bucket, def fp) {
        this.from = from
        this.to = to
        this.zeros = z
        this.sql = sql
        this.bucket = bucket
        this.filesPrefix = fp
    }

    @Override
    public Long call() {
        localLogger.log(Level.FINE, "${Thread.currentThread()} : Batch '${from}_${to}' started")
        def k = KeyFactory.createKey('ScriptBatch', "${from}_${to}"), c = 0l
        try {
            GoogleServiceFactory.getDatastoreService().get(k)
            localLogger.log(Level.FINE, "Batch '${from}_${to}' was processed. Skip")
        } catch (EntityNotFoundException e) {
            String start = "${from}", end = "${to}"
            String whereClause = "WHERE user_id >= ${start} AND user_id < ${end} AND user_id IS NOT NULL AND platform_id IS NOT NULL AND session_ts IS NOT NULL"
            String sql = String.format(sql, whereClause)

            localLogger.log(Level.FINE, "Start processing ${sql}")
            long startBatchTs = System.currentTimeMillis(), rawsPerBatch = 0

            Iterator res = BigQueryScriptUtils.executeQuery(sql)

            Writer writer = Channels.newWriter(GcsUtils.getRetryGcsService().createOrReplace(new GcsFilename(bucket, "${filesPrefix}/users_from_${start}_to_${end}.csv"), gcsFileOptions), "UTF-8")

            def raws = [], prevUserId = null, prevPlatform = null, data = null
            res.each { it ->
                rawsPerBatch++
                def raw = [], user_id = it[1]?.toLong(), platform = it[5], ts = it[0]?.toDouble().longValue()
                for (int idx = 0; idx < 30; idx++) {
                    raw << (it[idx] && it[idx].toString().contains('java.lang.Object') ? null : it[idx])
                }
                if (!prevUserId || !prevPlatform || !prevPlatform.equals(platform) || !prevUserId.equals(user_id)) {
                    data = [:]
                    data.createdTs = 0l + ts
                    data.updatedTs = 0l + ts
                    data.model = BitSet.valueOf([1] as long[])
                }
                def diff = dayDiff(ts, data.updatedTs)
                data.updatedTs = ts
                if (diff) {
                    def currentModel = data.model
                    data.model = shiftLeft(currentModel, diff)
                    data.model.set(0)
                }
                long[] arr = data.model.toLongArray()
                for (int idx = 1; idx < 7; idx++) {
                    raw << ( arr.length >= idx ? arr[idx-1] : null )
                }
                raw << data.createdTs

                def rawStr = EventUtils.buildDataString(raw as Object[])
                writer.write(rawStr + '\n')

                prevPlatform = platform
                prevUserId = user_id

                c++
            }
            def entity = new Entity(k)
            entity.setProperty('timestamp', System.currentTimeMillis())
            entity.setUnindexedProperty('created', DateTime.now().toString())
            GoogleServiceFactory.getDatastoreService().put(entity)

            closeQuietly(writer)
            localLogger.log(Level.FINE, "Done with '${from}_${to}'. Processed ${rawsPerBatch} raws. Took ${DurationFormatUtils.formatDuration(System.currentTimeMillis()-startBatchTs, 'HH:mm:ss.S')}")
        }
        return c
    }

    int dayDiff(long ts1, long ts2) {
        if (ts2 > ts1) {
            def ts3 = ts2
            ts2 = ts1
            ts1 = ts3
        }
        Calendar date1 = Calendar.getInstance();
        Calendar date2 = Calendar.getInstance();
        date1.setTime(new Date(ts1 * 1000));
        date2.setTime(new Date(ts2 * 1000));
        return (date1.get(Calendar.YEAR) * 365 + date1.get(Calendar.DAY_OF_YEAR)) - (date2.get(Calendar.YEAR) * 365 + date2.get(Calendar.DAY_OF_YEAR));
    }

    BitSet shiftLeft(BitSet bs, int shift) {
        BitSet result = new BitSet();
        for (int set = -1;;) {
            set = bs.nextSetBit(set + 1);
            if (set == -1) {
                break;
            }
            result.set(set + shift);
        }
        return result;
    }
    
}