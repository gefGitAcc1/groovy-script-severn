import java.nio.channels.Channels;
import java.util.logging.*;

import org.joda.time.*;
import org.joda.time.format.*;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;

import javax.mail.*;
import javax.mail.internet.*;

import com.google.appengine.api.memcache.*;
import com.google.appengine.tools.cloudstorage.*;
import com.severn.common.bigquery.*;
import com.severn.event.tracking.*;
import com.severn.script.utils.*;
import com.severn.segment.v2.*;

import static org.codehaus.groovy.runtime.DefaultGroovyMethodsSupport.*;

def c = 0, bucket = 'wild-ride-stage-1-temp', filesPrefix = 'raw_session_processed_4' 
Logger logger = Logger.getLogger('com.severn')

def SQL = 'SELECT * FROM [DWH.raw_sessions] %s ORDER BY user_id, platform_id, session_ts'
GcsFileOptions gcsFileOptions = new GcsFileOptions.Builder().mimeType(MediaType.TEXT_PLAIN_VALUE).build()
GcsService gcsService = GcsUtils.getRetryGcsService()

def usersRaw = BigQueryScriptUtils.executeQuery('SELECT max(user_id) as max_uid, min(user_id) as min_uid FROM [DWH.raw_sessions]').next()
def zeros = '00000000000000', sTs = System.currentTimeMillis()
long minUid = Long.valueOf(usersRaw[1]) / Long.valueOf("1${zeros}"), maxUid = Long.valueOf(usersRaw[0]) / Long.valueOf("1${zeros}")

logger.log(Level.FINE, "User Ids prefixes from ${minUid} to ${maxUid}")

Writer writer = null

(minUid..maxUid).each { aNumber ->
    String start = "${aNumber}${zeros}", end = "${aNumber + 1}${zeros}"
    String whereClause = "WHERE user_id >= ${start} AND user_id < ${end}"
    String sql = String.format(SQL, whereClause)
    
    closeQuietly(writer)
    writer = Channels.newWriter(gcsService.createOrReplace(new GcsFilename(bucket, "${filesPrefix}/users_from_${start}_to_${end}.csv"), gcsFileOptions), "UTF-8")
    
    logger.log(Level.FINE, "Start processing ${sql}")
    
    Iterator res = BigQueryScriptUtils.executeQuery(sql)
    def raws = [], prevUserId = null, prevPlatform = null, data = null
    res.each { it ->
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
    logger.log(Level.FINE, "Done with ${aNumber}")
}
closeQuietly(writer)

"Ok. $c. Took ${System.currentTimeMillis()/1000 - sTs/1000} sek.".toString()

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