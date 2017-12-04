// New script Script_1509531761919.groovy

import java.io.IOException;
import java.util.logging.*;

import javax.mail.*;
import javax.mail.internet.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.util.List;
import com.google.appengine.api.datastore.Text;
import org.springframework.context.ApplicationContext;

import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableRow;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.QueryResultList;
import com.google.gson.Gson;
import com.google.apphosting.api.ApiProxy;
import com.severn.common.utils.CollectionSerializer;
import com.severn.common.utils.NumberFormatUtils;
import com.severn.game.slots.domain.FreeSpinsGroup;
import com.severn.game.slots.domain.StoredFreeSpinsInfo;
import com.severn.reports.appsflyer.AppsFlyerData;
import com.severn.common.bigquery.BigQueryServiceSupport;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse.InsertErrors;
import com.google.api.services.bigquery.model.ErrorProto;
import java.util.logging.*;


        ApplicationContext ctx = binding.variables.get("applicationContext");
        BigQueryServiceSupport bigQueryClient = (BigQueryServiceSupport) ctx.getBean("bigQueryServiceSupport");
                
        Gson gson = new Gson();
        DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
        Query query = new Query("MachineState");
        QueryResultList<Entity> machineStates = datastore.prepare(query).asQueryResultList(FetchOptions.Builder.withLimit(2000));
        
        int count = 0;
        int countTotal = 0;
        int fails = 0;
        int insId = 1;
        
        long ts = System.currentTimeMillis();
        long startTs = ts;
        List<String> failed = new ArrayList<String>();
        List<TableDataInsertAllRequest.Rows> rowList = new ArrayList<>();
        while (machineStates.size() > 0) {
            
            for (Entity entity : machineStates) {
                if(countTotal % 100000 == 0) {
                    logger.log(Level.INFO, "Total count  : " + countTotal)
                }      
                countTotal++;
                Object freeSpins = entity.getProperty("Freespins");
                String[] parts = entity.getKey().getName().split("_");
                int fsRemains = 0;
                int bonusCount = 0;
                if ((freeSpins == null && entity.getProperty("Bonus") == null && entity.getProperty("Scatter") == null) || parts.length != 3) {
                    continue;
                }
                
                if (entity.getProperty("Bonus") != null || entity.getProperty("Scatter") != null) {
                    bonusCount = 1;
                }
                
                if (freeSpins != null) {
                   String fs = freeSpins.toString();
                    if (freeSpins instanceof Text) {
                        fs = ((Text)freeSpins).getValue();
                    }
                    StoredFreeSpinsInfo storedFreeSpinsInfo = null;
                    try {
                        storedFreeSpinsInfo = gson.fromJson(fs, StoredFreeSpinsInfo.class);
                    } catch (Throwable e) {
                        failed.add(entity.getKey().getName());
                    }
            
                    for(FreeSpinsGroup fsg : storedFreeSpinsInfo.getFreeSpinsGroups()) {
                        fsRemains += fsg.getCount();
                    }
                }
                String userId = parts[0];
                String authId = parts[1];
                String machineId = parts[2];
                
                TableDataInsertAllRequest.Rows rows = new TableDataInsertAllRequest.Rows();
                TableRow row =new TableRow()
                .set("insert_ts",           ts / 1000)
                .set("user_id",             userId)
                .set("auth_id",             authId)
                .set("machine_id",          machineId)
                .set("freespins_remains",   fsRemains)
                .set("bonus_count",   bonusCount);
                
                rows.setInsertId(String.valueOf(insId));
                insId++;
                rows.setJson(row);
                
                rowList.add(rows);
                
                count++;
            }
            
            if (rowList.size() > 500 ) {
                try {
                    TableDataInsertAllRequest tableDataRequest = new TableDataInsertAllRequest().setRows(rowList);
                    bigQueryClient.getBigQuery().tabledata().insertAll("gambino-apps", "DWH", "freespins_remains", tableDataRequest).execute();
                } catch (IOException e) {
                    fails++;
                    errMes =  errMes + "    " + e.getMessage();
                }
                rowList.clear();
            }

            if (machineStates.getCursor() != null) {
                machineStates = datastore.prepare(query).asQueryResultList(FetchOptions.Builder.withLimit(2000).startCursor(machineStates.getCursor()));
                ts = System.currentTimeMillis();
            } else {
                break;
            }

        }
        
        if (rowList.size() > 0 ) {
            try {
                TableDataInsertAllRequest tableDataRequest = new TableDataInsertAllRequest().setRows(rowList);
                bigQueryClient.getBigQuery().tabledata().insertAll("gambino-apps", "DWH", "freespins_remains", tableDataRequest).execute();
            } catch (IOException e) {
                fails++;
                errMes =  errMes + "    " + e.getMessage();
            }
        }        
        
        startTs = (System.currentTimeMillis() - startTs) / 1000;
        String result = "Processed " + count + "    " + countTotal + " users in " + startTs + "sec. There were " + fails + " fails.   " + failed;
        notifyEnded('dmitry.vaserin@synesis.ru', result)
        result
        
        
        void notifyEnded(def reciever, def message) {
            Properties props = new Properties();
            Session session = Session.getDefaultInstance(props, null);
            
            try {
                MimeMessage msg = new MimeMessage(session);
                msg.setFrom(new InternetAddress("${ApiProxy.getCurrentEnvironment().getAppId().replace('s~', '')}@appspot.gserviceaccount.com", "Script executor module"));
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