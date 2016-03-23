import java.util.logging.*;

import javax.mail.*;
import javax.mail.internet.*;

import org.springframework.context.ApplicationContext;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.GetQueryResults;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.appengine.api.datastore.*;
import com.severn.common.bigquery.BigQueryServiceSupport;
import com.severn.common.update.user.EntityUpdaters.EntityAwareEntityFieldUpdater;

Logger logger = Logger.getLogger('com.severn')

ApplicationContext ctx = binding.variables.get('applicationContext')
BigQueryServiceSupport bigQueryClient = ctx.getBean('bigQueryServiceSupport')
Bigquery bigquery = bigQueryClient.getBigQuery()
DatastoreService ds = DatastoreServiceFactory.getDatastoreService()

String querySql = 'SELECT user_id, platform_id, session_ts as ts, date(session_ts) as dt, datediff(current_timestamp(), timestamp(session_ts)) as df ' +
    ' FROM [DWH.raw_sessions] ' +
    ' WHERE user_id IS NOT NULL AND platform_id IS NOT NULL AND session_ts IS NOT NULL' +
    ' group by 1,2,3,4,5 ' +
    ' order by 1,2,3,4 ';

QueryResponse query = bigquery.jobs().query(bigQueryClient.getProjectId(),
    new QueryRequest().setQuery(querySql).setTimeoutMs(Integer.MAX_VALUE))
    .execute();
    
logger.log(Level.FINE, 'Query {0}', query)

// Execute it
String pgToken = "start"
GetQueryResults getQueryResults = bigquery.jobs().getQueryResults(query.getJobReference().getProjectId(), query.getJobReference().getJobId())

logger.log(Level.FINE, 'GetQueryResults {0}', getQueryResults)

Long previousUserId = null
String previousPlatform = null
Entity entity = null
def ents = [], c = 0, totalRecs = 0

while (pgToken) {
    GetQueryResultsResponse queryResult = getQueryResults.execute();
    pgToken = queryResult.getPageToken()

    List<TableRow> rows = queryResult.getRows();
//    logger.log(Level.FINE, "Got ${rows.size()}, new token ${pgToken}")

    rows.each {
        totalRecs++
        List<TableCell> cells = it.getF()

        Long currentUserId = Long.parseLong(cells.get(0).getV())
        String currentPlatform = cells.get(1).getV()

        if (!previousUserId || !previousPlatform || previousUserId.longValue() != currentUserId.longValue() || !previousPlatform.equals(currentPlatform)) {
            if (entity) {
                ents << entity
            }
            c++
            // i.e. new User/Platform
            entity = new Entity('UserSeniority', "${currentUserId}_${currentPlatform}".toString())
            entity.setUnindexedProperty('updateTs', System.currentTimeMillis())
            entity.setUnindexedProperty('createTs', 1000l * Double.parseDouble(cells.get(2).getV()).longValue())
            entity.setUnindexedProperty('history', new ArrayList<Long>())
        }

        BitSet bs = BitSet.valueOf(extract(entity.getProperty('history')))
        bs.set(Long.parseLong(cells.get(4).getV()).intValue())
        entity.setUnindexedProperty('history', wrap(bs.toLongArray()))

        previousUserId = currentUserId
        previousPlatform = currentPlatform

        if (ents.size() == 200) {
            ds.put(ents)
            ents = []
        }
    }

    if (pgToken) {
        getQueryResults.setPageToken(pgToken)
    }
}

if (ents) {
    ds.put(ents)
}

def result = "Done. Created $c. Total records $totalRecs".toString()
notifyEnded('sergey.shcherbovich@synesis.ru', bigQueryClient.getServiceAccountId(), result)
result

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