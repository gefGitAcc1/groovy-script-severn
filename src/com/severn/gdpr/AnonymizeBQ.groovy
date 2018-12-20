package com.severn.gdpr

import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.List
import java.util.logging.Level

import org.apache.commons.lang3.StringUtils
import org.springframework.context.ApplicationContext

import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model.QueryRequest
import com.google.api.services.bigquery.model.QueryResponse
import com.google.api.services.bigquery.model.TableList
import com.severn.common.bigquery.BigQueryFactory
import com.severn.common.bigquery.BigQueryServiceSupportUtils
import com.severn.common.dao.UserDAO
import com.severn.common.spring.AppContextProvider

def userIds = [5174950778896384] //[5964688877682688, 5628978463244288, 5457462165504000]
def socialIds = userIds.collect { id -> bean(UserDAO.class).getUser(id)?.socialId }.findAll { socId -> socId }.collect { it -> "'${it}'" }
//socialIds << [130713757786657].collect { it -> "'${it}'" }

logger.log(Level.FINE, "UserIds : ${userIds}")
logger.log(Level.FINE, "SocialIds : ${socialIds}")

def afIds = ['1530191530391-6420915'].collect { it -> "'${it}'" }

def placeHolder = 'GDPR-HIDDEN-V2'
def projectId = 'severn-stage-3', datasetId = 'DWH'

def sqlp = [
    byUserIds: " user_id IN (${StringUtils.join(userIds, ',')}) ",
    bySocialIds: " social_network_user_id IN (${StringUtils.join(socialIds, ',')}) ",
    bySocialIds2: " social_id IN (${StringUtils.join(socialIds, ',')}) ",
    byReceiverIds: " receiver_social_id IN (${StringUtils.join(socialIds, ',')}) ",
    bySenderSocialIds: " sender_social_id IN (${StringUtils.join(socialIds, ',')}) ",
    byInvitedSocialIds: " invited_social_id IN (${StringUtils.join(socialIds, ',')}) ",
    byAfId: " appsflyer_device_id IN (${StringUtils.join(afIds, ',')}) ",
]

["ip", "social_network_user_id", "invited_social_id", "platform_id", "device_identifier", "device_os", "device_type", "browser", "screen_resolution", 
    "appsflyer_device_id", "unique_device_id", "social_id", "user_name", "email", "gender", "ip_country", "platform", "advertiser_id", "sender_social_id", 
    "advertising_id", "android_id", "imei", "idfa", "idfv", "mac", 'device_type', 'os_version', 'device_name', 'device_brand', 'device_model', 
    'receiver_social_id']
.each { String strName ->
    sqlp[strName] = " ${strName} = '${placeHolder}' "
}
["total_number_of_friends", "number_of_app_friends", "birth_date"].each { String strName ->
    sqlp[strName] = " ${strName} = NULL "
}

def tableNamePlaceholder = '::table::'

//[table: '', fields: [], by:"$sqlp.byUserIds"],

def queries = [
    [table: 'appsflyer_installations', fields: ["ip", "advertising_id", "android_id", "imei", "idfa", "idfv", "mac", 'device_type', 'os_version', 'device_name', 'device_brand', 'device_model'], by:"$sqlp.byAfId"],
    [table: 'invitations', fields: ["social_id"], by:"$sqlp.bySocialIds2"],
    [table: 'invitations', fields: ["invited_social_id"], by:"$sqlp.byInvitedSocialIds"],
    [table: 'bonuses', fields: ["ip", "social_network_user_id", "platform_id"], by:"$sqlp.byUserIds"],
    [table: 'client_events', fields: ["platform_id", "social_network_user_id", "device_identifier", "device_os", "device_type", "browser", "screen_resolution", "appsflyer_device_id", "unique_device_id"], by: "$sqlp.byUserIds"],
    [table: 'contest_leaderboard', fields: ["social_id", "user_name"], by:"$sqlp.byUserIds"],
    [table: 'gifts', fields: ["ip", "platform_id", "sender_social_id"], by:"$sqlp.bySenderSocialIds"],
    [table: 'gifts', fields: ["ip", "platform_id", "receiver_social_id"], by:"$sqlp.byReceiverIds"],
    [table: 'jackpot_users', fields: ["social_id", "user_name", "platform_id"], by:"$sqlp.byUserIds"],
    [table: 'jackpot_winner_select', fields: ["social_id", "user_name"], by:"$sqlp.byUserIds"],
    [table: 'jackpot_winners', fields: ["social_id", "user_name", "platform_id"], by:"$sqlp.byUserIds"],
    [table: 'kings', fields: ["social_id"], by:"$sqlp.byUserIds"],
    [table: 'personal_offers', fields: ["platform_id"], by:"$sqlp.byUserIds"],
    [table: 'piggy_bank', fields: ["platform_id"], by:"$sqlp.byUserIds"],
    [table: 'push_notification', fields: ["platform_id"], by:"$sqlp.byUserIds"],
    [table: 'raw_installations', fields: ["platform_id", "social_network_user_id", "device_os", "device_type", "browser", "ip", "screen_resolution", "ip_country", "advertiser_id", "user_name", "email", "gender"], by:"$sqlp.byUserIds"],
    [table: 'raw_sessions', fields: ["platform_id", "social_network_user_id", "device_os", "device_type", "browser", "ip", "screen_resolution", "ip_country", "user_name", "total_number_of_friends", "number_of_app_friends", "email", "birth_date"], by:"$sqlp.byUserIds"],
    [table: 'raw_sessions_end', fields: ["platform_id", "social_network_user_id", "device_os", "device_type", "browser", "ip", "screen_resolution", "ip_country", "user_name", "total_number_of_friends", "number_of_app_friends", "email", "birth_date"], by:"$sqlp.byUserIds"],
    [table: 'raw_sessions_appsflyer', fields: ["platform_id", "social_network_user_id", "device_os", "device_type", "browser", "ip", "screen_resolution", "ip_country", "user_name", "total_number_of_friends", "number_of_app_friends", "email", "birth_date"], by:"$sqlp.byUserIds"],
    [table: 'raw_subscription_info', fields: ["platform"], by:"$sqlp.byUserIds"],
    [table: 'raw_transactions_list', fields: ["platform_id", "social_network_user_id", "device_os", "device_type", "browser", "ip", "ip_country", "screen_resolution"], by:"$sqlp.byUserIds"],
    [table: 'transactions', fields: ["platform_id", "social_network_user_id", "device_os", "device_type", "browser", "ip", "ip_country", "screen_resolution"], by:"$sqlp.byUserIds"],
    [table: 'survey', fields: ["platform_id"], by:"$sqlp.byUserIds"],
    [table: 'spins', fields: ["platform_id", "social_network_user_id", "device_type", "browser", "ip"], by:"$sqlp.byUserIds"],
].collect { it ->
    it.fields.removeAll(['platform', 'platform_id', "screen_resolution", "gender", "ip_country", "birth_date", "appsflyer_device_id"])
    // TODO retain only FB data
    it.fields.retainAll(['social_id','social_network_user_id','sender_social_id','user_name','email','invited_social_id',
        "total_number_of_friends", "number_of_app_friends", "birth_date", 'receiver_social_id', 'gender'])
    it
}.findAll { it ->
    def filt = (it.fields.size() > 0)
    logger.log(Level.FINE, "Processing info for ${it.table} = ${filt}")
    filt
}.collect { it ->
    def fs = it.fields.collect { field ->
        if (sqlp[field]) { 
            sqlp[field]
        } else {
            throw new RuntimeException("Can't find mapping for ${field}")
        } 
    }
    it.query = "UPDATE ${tableNamePlaceholder} SET ${StringUtils.join(fs, ' , ')} WHERE ${it.by}"  
    it 
}

Bigquery bigquery = bean(BigQueryFactory.class).getBigQuery()

def tables = []

String nextToken = null;
com.google.api.services.bigquery.Bigquery.Tables.List listRequest = bigquery.tables().list(projectId, datasetId);
while (true) {
    if (nextToken != null) {
        listRequest.setPageToken(nextToken);
    }
    listRequest.setMaxResults(1000L);
    TableList listResult = listRequest.execute();
    for (TableList.Tables t : listResult.getTables()) {
        String tableName = t.getTableReference().getTableId();
        tables << tableName
    }

    if (listResult.getNextPageToken() == null) {
        break;
    } else {
        nextToken = listResult.getNextPageToken();
    }
}

logger.log(Level.FINE, "Tables ${tables.size()}")
logger.log(Level.FINE, "${new SimpleDateFormat('YYYYMMdd').format(new Date())}")

def mapping = tables.findAll { String it -> 
    !(it.startsWith('appsflyer_installations') && it.endsWith(new SimpleDateFormat('YYYYMMdd').format(new Date()))) 
} groupBy { String tblId -> tblId.split('[0-9]')[0] }

logger.log(Level.FINE, "Mapping ${mapping}")

def count = 0
def errors = 0
queries.each { def queryDef ->
    def tbls = mapping[queryDef.table]
    def qry  = queryDef.query
    if (tbls) {
        tbls.each { String tableName ->
            String query = qry.replace("${tableNamePlaceholder}", "${datasetId}.${tableName}")
            println query
            def res = execute(projectId, query)
            count += res.rows
            errors += res.errors
        }
    }
}

"OK ${count} rows affected. Errors = ${errors}".toString()

def execute(def projectId, def query) {
    def result = 0
    def errors = 0
    Bigquery bigquery = bean(BigQueryFactory.class).getBigQuery()
    QueryRequest queryRequest = new QueryRequest().setQuery(query).setUseLegacySql(false);
    QueryResponse response
    try {
        response = bigquery.jobs().query(projectId, queryRequest).execute();
    } catch (Exception e) {
        logger.log(Level.SEVERE, "Exception on query ${query}", e)
//        throw new RuntimeException("Exception on query ${query}", e)
    }

    if (response.getErrors()) {
        logger.log(Level.SEVERE, "Error : ${response.getErrors()}")
        logger.log(Level.SEVERE, "For query : ${query}")
        errors++
    } else {
        logger.log(Level.INFO, "OK : ${response.getNumDmlAffectedRows()}")
        if (null == response.getNumDmlAffectedRows()) {
            logger.log(Level.FINE, "OK : ${response}")
        } else {
            result += response.getNumDmlAffectedRows()
        }
    }
    [rows: result, errors: errors]
}

Object bean(Class<?> clazz) {
    new BeanR(clazz, binding).getBean()
}

class BeanR<T> {
    Class<T> clazz
    def binding
    
    BeanR(Class<T> clazz, def binding) {
        this.clazz = clazz
        this.binding = binding
    }
    
    T getBean() {
        ApplicationContext ac = binding.variables.get('applicationContext')
        ac.getBeansOfType(clazz).entrySet().iterator().next().value
    }
}
