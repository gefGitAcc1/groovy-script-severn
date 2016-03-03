import com.google.appengine.tools.cloudstorage.*
import com.google.appengine.api.log.*
import com.google.appengine.api.log.LogQuery.Version
import org.joda.time.*
import org.joda.time.format.*

import com.google.appengine.api.log.LogService.LogLevel
import org.springframework.http.MediaType
import java.nio.channels.Channels

def bucket = 'wild-ride-app-temp'
def moduleVersions = ['0152', '0153']
def startTime = '2015-12-26 00:00:00'
def endTime = '2016-01-04 09:24:05'
//def endTime = '2015-12-27 00:00:00'
def paths = [
    '/_ah/spi/com.severn.auth.service.endpoint.AuthorizationServiceEndpoint.authorize'
]
def table = 'raw_sessions'
// impls
def fileName = "events_${table}_from_${startTime}_to_${endTime}_${System.currentTimeMillis()}.log"
def format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC()
def startTs = format.parseDateTime(startTime).getMillis() * 1000
def endTs = format.parseDateTime(endTime).getMillis() * 1000
def gcsFileOptions = new GcsFileOptions.Builder().mimeType(MediaType.TEXT_PLAIN_VALUE).build()
def count = 0

GcsService service = GcsServiceFactory.createGcsService();
GcsFilename file = new GcsFilename(bucket, fileName);
def writer = Channels.newWriter(service.createOrReplace(file, gcsFileOptions), "UTF-8")

LogService logService = LogServiceFactory.getLogService();

LogQuery query = new LogQuery()
    .batchSize(100)
    .minLogLevel(LogLevel.INFO)
    .versions(moduleVersions.collect{new Version('default', it)})
    .includeAppLogs(true);
query.startTimeUsec(startTs);
query.endTimeUsec(endTs);
logger.fine("Query startTs=$startTs, endTs=$endTs, version=${query.versions}")

logService.fetch(query).each {
    // count++
    if (paths.contains(it.getResource().split("\\?")[0])) {
        // logger.fine("got ${it.getResource()}")
        it.getAppLogLines().each {
            def allLogMessage = it.getLogMessage()
            if (allLogMessage.startsWith('dataEvent#')) {
                // logger.fine("found $allLogMessage")
                int splitPosition = allLogMessage.indexOf(':');
                String logEntryKey = allLogMessage.substring(0, splitPosition);
                logEntryKey = logEntryKey.trim().substring('dataEvent#'.length());
                
                if (table.equals(logEntryKey)) {
                    
                    String data = allLogMessage.substring(splitPosition + 2); // : + space
                    writer.write(data)
                    count++
                }
            }
        }
    }
}

writer.close()

'DONE. Found ' + count + ' log entries for '