package com.severn.bigquery

import javax.mail.*
import javax.mail.internet.*
import com.google.appengine.tools.cloudstorage.*
import com.google.appengine.api.log.*
import com.google.appengine.api.log.LogQuery.Version;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.*

import org.joda.time.*
import org.joda.time.format.*

import com.severn.common.bigquery.BigQueryServiceSupport;

import org.springframework.http.MediaType

import java.nio.channels.Channels
import java.util.logging.*

def projectId = 'gambino-apps', dataSet = 'TST'
def tableId = 'lost_contest_winners_2017_07_11'
def schema = 'event_ts:timestamp,contest_id:integer,user_id:integer,rank:integer,total_win:integer,collection_ts:timestamp,levelgroup_id:integer,levelgroup_minlevel:integer,levelgroup_maxlevel:integer,user_tier:integer,session_id:string,total_win_usd:float,user_level:integer,calc_strategy:string,team:integer'
def file = [name:'events_spins_from_2017-07-11 00:00:00_to_2017-07-11 17:37:58_1499852735196.log']
def counter = 0

def aBucket = 'gambino-slots-events-temp'

// ctx
def ctx = binding.variables.get("applicationContext")
BigQueryServiceSupport bigQueryClient = ctx.getBean('bigQueryServiceSupport')

def isOk = loadFile(file, bigQueryClient, schema, projectId, tableId, dataSet, aBucket)

def result = 'OK : Processed ' + isOk
//notifyEnded('sergey.shcherbovich@synesis.ru', 'severn-stage-1@appspot.gserviceaccount.com', result)
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
    } catch (MessagingException e) {
    }
}

void moveFile(GcsFilename from, GcsFilename to, GcsService service) {
    def final ATTEMPTS = 3
    for (def aTry = ATTEMPTS - 1; aTry >= 0; aTry--) {
        try {
            service.copy(from, to)
        } catch (IOException e) {
            if (aTry == 0) {
                throw e
            }
            sleep(1000)
        }
    }
    for (def aTry = ATTEMPTS - 1; aTry >= 0; aTry--) {
        try {
            service.delete(from)
        } catch (IOException e) {
            if (aTry == 0) {
                throw e
            }
            sleep(1000)
        }
    }
}

boolean loadFile(def fileName, BigQueryServiceSupport bigQueryClient, def schema, def projectId, def tableId, def dataSet, def bucket) {
    Job job = new Job();
    JobConfiguration config = new JobConfiguration();
    JobConfigurationLoad loadConfig = new JobConfigurationLoad();
    config.setLoad(loadConfig);
    job.setConfiguration(config);
    
    loadConfig.setSourceUris([toSource(fileName, bucket)])
    loadConfig.setSchema(getTableSchema(schema))
    loadConfig.setAllowJaggedRows(true);
    
    TableReference tableRef = new TableReference();
    tableRef.setDatasetId(dataSet);
    tableRef.setTableId(tableId);
    tableRef.setProjectId(projectId);
    loadConfig.setDestinationTable(tableRef);
    loadConfig.setCreateDisposition('CREATE_IF_NEEDED');
    loadConfig.setWriteDisposition('WRITE_APPEND');
    
    Insert insert = bigQueryClient.getBigQuery().jobs().insert(projectId, job);
    insert.setProjectId(projectId);
    def result = insert.execute()
    
    def jRes = '', err = null
    def jResult = null
    while (!'DONE'.equals(jRes)) {
        Thread.sleep(5_000)
        jResult = bigQueryClient.getBigQuery().jobs().get(projectId, result.id.split(':')[1]).execute()
        jRes = jResult.status?.state
        err = jResult.status?.errorResult
    }
    return (!err)
}

def toSource(def name, def bucket) {
    return 'gs://' + bucket + '/' + name.name
//    logger.log(Level.FINE, "Name ${name}");
}

TableSchema getTableSchema(String aSchema) {
    List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
    aSchema.split(',').each {
        def descriptionData = it.split(':')
        TableFieldSchema fieldFoo = new TableFieldSchema();
        fieldFoo.setName(descriptionData[0]);
        fieldFoo.setType(descriptionData[1]);
        fields << fieldFoo
    }
    return new TableSchema().setFields(fields)
}