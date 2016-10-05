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

import com.google.appengine.api.log.LogService.LogLevel
import com.severn.common.bigquery.BigQueryServiceSupport;

import org.springframework.http.MediaType

import java.nio.channels.Channels
import java.util.logging.*

def projectId = 'severn-stage-1', dataSet = 'Restored'
def tableId = 'raw_sessions_prod_till_2016_05_01_00_00_00_inclusive'
def bucket_fails = 'severn-stage-1-temp', bucket_success = 'severn-stage-1-temp-restored', bucket_unsuccess = 'severn-stage-1-temp-restore-failed'
def prefix = 'raw_session_processed' + '/'
def schema = 'session_ts:timestamp,user_id:integer,session_id:string,session_end_ts:timestamp,game_id:integer,platform_id:string,network_id:string,social_network_user_id:string,device_identifier:string,tracker_id:string,campaign_id:string,affiliate_id:string,device_os:string,device_type:string,browser:string,ip:string,screen_resolution:string,ip_country:string,server_id:string,user_name:string,user_level:integer,user_xp:float,user_balance:float,total_number_of_friends:integer,number_of_app_friends:integer,email:string,birth_date:timestamp,ab_test_names:string,ab_test_groups:string,app_version:string,seniority1:integer,seniority2:integer,seniority3:integer,seniority4:integer,seniority5:integer,seniority6:integer,first_installation_ts:timestamp,ad_provider_unique_device_id:string,user_tier:integer'
def counter = 0

GcsService service = GcsServiceFactory.createGcsService();
ListOptions options = new ListOptions.Builder().setPrefix(prefix).setRecursive(true).build();
ListResult files = service.list(bucket_fails, options);
// ctx
def ctx = binding.variables.get("applicationContext")
BigQueryServiceSupport bigQueryClient = ctx.getBean('bigQueryServiceSupport')

files.each {
    counter++
    ListItem li = it
    def result = loadFile(li, bigQueryClient, schema, projectId, tableId, dataSet, bucket_fails)
    if (result) {
        logger.log(Level.FINE, "Success ${li}")
        moveFile(new GcsFilename(bucket_fails, li.name), new GcsFilename(bucket_success, li.name), service);
    } else {
        logger.log(Level.WARNING, "Failed ${li}")
        moveFile(new GcsFilename(bucket_fails, li.name), new GcsFilename(bucket_unsuccess, li.name), service)
    }
}

def result = 'OK : Processed ' + counter
notifyEnded('sergey.shcherbovich@synesis.ru', 'severn-stage-1@appspot.gserviceaccount.com', result)
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