package com.severn.gdpr

import java.util.List
import java.util.logging.Level

import org.apache.commons.lang3.StringUtils
import org.springframework.context.ApplicationContext

import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model.QueryRequest
import com.google.api.services.bigquery.model.QueryResponse
import com.google.api.services.bigquery.model.TableList
import com.severn.common.bigquery.BigQueryFactory
import com.severn.common.dao.UserDAO
import com.severn.common.spring.AppContextProvider

def userIds = [5181483673190400]
def socialIds = [1695372064109441].collect { it -> "'${it}'" }

def placeHolder = 'GDPR-HIDDEN-V1'

def sqlp = [
    byUserIds: " WHERE user_id IN (${StringUtils.join(userIds, ',')})",
    bySocialIds: " WHERE social_network_user_id IN (${StringUtils.join(socialIds, ',')}) ",
    bySenderSocialIds: " WHERE sender_social_id IN (${StringUtils.join(socialIds, ',')}) ",
]

["ip", "social_network_user_id", "platform_id", "device_identifier", "device_os", "device_type", "browser", "screen_resolution", "appsflyer_device_id", "unique_device_id", "social_id", "user_name", "email", "gender", "ip_country", "platform", "advertiser_id", "sender_social_id"]
.each { String strName ->
    sqlp[strName] = " ${strName} = '${placeHolder}' "
}
["total_number_of_friends", "number_of_app_friends", "birth_date"].each { strName ->
    sqlp[strName] = " ${strName} = NULL "
}

println sqlp
println sqlp.ip

def tableNamePlaceholder = '::table::'

//[table: '', fields: [], by:"$sqlp.byUserIds"],

def queries = [
    [table: 'bonuses', fields: ["ip", "social_network_user_id", "platform_id"], by:"$sqlp.byUserIds"],
    [table: 'client_events', fields: ["platform_id", "social_network_user_id", "device_identifier", "device_os", "device_type", "browser", "screen_resolution", "appsflyer_device_id", "unique_device_id"], by: "$sqlp.byUserIds"],
    [table: 'contest_leaderboard', fields: ["social_id", "user_name"], by:"$sqlp.byUserIds"],
    [table: 'gifts', fields: ["ip", "platform_id", "sender_social_id"], by:"$sqlp.bySenderSocialIds"],
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
    it.fields.removeAll(['platform', 'platform_id'])
    it
}.findAll { it ->
    it.fields.size() > 0
}

.collect { it ->
    def fs = it.fields.collect { field ->
        if (sqlp[field]) { 
            sqlp[field]
        } else {
            throw new RuntimeException("Can't find mapping for ${field}")
        } 
    }
    it.query = "update ${tableNamePlaceholder} set ${StringUtils.join(fs, ' , ')} ${it.by}"  
    it 
}

Bigquery bigquery = bean(BigQueryFactory.class).getBigQuery()
def projectId = 'severn-stage-3', datasetId = 'DWH'

List<TableList.Tables> tables = bigquery
    .tables()
    .list(projectId, datasetId)
    .setMaxResults(100_000)
        .execute().getTables();

def mapping = tables.collect { TableList.Tables tbls -> tbls.id.split(":${datasetId}.")[1] } groupBy { String tblId -> tblId.split('[0-9]')[0] }

def count = 0
def errors = 0
queries.each { def queryDef ->
    def tbls = mapping[queryDef.table]
    def qry  = queryDef.query
    if (tbls) {
        tbls.each { String tableName ->
            String query = qry.replace('::table::', "${datasetId}.${tableName}")
            println query
            def res = execute(projectId, query)
            count += res.rows
            errors + res.errors
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
        throw new RuntimeException("Exception on query ${query}", e)
    }

    if (response.getErrors()) {
        logger.log(Level.SEVERE, "Error : ${response.getErrors()}")
        logger.log(Level.SEVERE, "For query : ${query}")
        errors++
    } else {
        logger.log(Level.INFO, "OK : ${response.getNumDmlAffectedRows()}")
        result += response.getNumDmlAffectedRows()
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