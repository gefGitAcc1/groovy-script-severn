import java.lang.reflect.Type
import java.nio.channels.Channels
import java.sql.ResultSet
import java.sql.SQLException;
import java.util.Map
import java.util.logging.*;

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVRecord
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.context.*
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.*

import javax.mail.*;
import javax.mail.internet.*
import javax.sql.*

import com.google.appengine.api.datastore.*
import com.google.appengine.api.memcache.*
import com.google.appengine.tools.cloudstorage.GcsFilename
import com.google.appengine.tools.cloudstorage.GcsService
import com.google.apphosting.api.ApiProxy
import com.google.gson.Gson
import com.severn.common.domain.User;
import com.severn.datastore.legacy.DatastoreUtil
import com.severn.script.utils.GcsUtils
import com.google.common.reflect.TypeToken

ApplicationContext ctx = binding.variables.get('applicationContext')
DateTime dt = DateTime.now(DateTimeZone.UTC)
DateTimeFormatter f = DateTimeFormat.forPattern('yyyy-MM-dd').withZoneUTC()
def infos = []

Logger logger = Logger.getLogger('com.severn')

def dataSource = ctx.getBean(DataSource.class)
def jdbcTemplate = new JdbcTemplate(dataSource)

GcsService gcsService = GcsUtils.getRetryGcsService()

// level - xp
GcsFilename file = new GcsFilename('prod-test-data', 'levels.json');
BufferedReader reader = new BufferedReader(Channels.newReader(gcsService.openReadChannel(file, 0), "UTF-8"));
Gson gson = new Gson()

Type type = new TypeToken<Map<Integer,Double>>(){}.getType()
Map<Integer,Double> mapping = gson.fromJson(reader, type)

logger.log(Level.INFO, "Load level mapping : ${mapping.size()}")

// max bets after
GcsFilename betFileAfter = new GcsFilename('prod-test-data', 'Bet_config_0.63_2.csv');
BufferedReader betsFileAfterReader = new BufferedReader(Channels.newReader(gcsService.openReadChannel(betFileAfter, 0), "UTF-8"));

Map<Integer, Double> maxBetsAfter = [:]
CSVFormat.EXCEL.withDelimiter(',' as char).withHeader().parse(betsFileAfterReader).iterator().each { CSVRecord record ->
    int lvlFrom   = record.get(0) as int
    int lvlTo     = record.get(1) as int
    double maxBet = record.get(2) as double
    (lvlFrom..lvlTo).each { int lvl ->
        maxBetsAfter.put(lvl, maxBet)
    }
}
logger.log(Level.INFO, "Max bets after : ${maxBetsAfter.size()}")
//logger.log(Level.FINE, "Max bets after : ${maxBetsAfter}")

// max bets before - maxBetsBefore
GcsFilename betFileBefore = new GcsFilename('prod-test-data', 'bets0.35.csv');
BufferedReader betsFileBeforeReader = new BufferedReader(Channels.newReader(gcsService.openReadChannel(betFileBefore, 0), "UTF-8"));

Map<Integer, Double> maxBetsBefore = [:]
CSVFormat.EXCEL.withDelimiter(',' as char).withHeader().parse(betsFileBeforeReader).iterator().each { CSVRecord record ->
//    logger.log(Level.FINE, "Before row : ${record}")
    int lvlFrom   = record.get(0) as int
    int lvlTo     = record.get(1) as int
    double maxBet = record.get(2) as double
    (lvlFrom..lvlTo).each { int lvl ->
        maxBetsBefore.put(lvl, maxBet)
    }
}
logger.log(Level.INFO, "Max bets before : ${maxBetsBefore.size()}")
//logger.log(Level.FINE, "Max bets before : ${maxBetsBefore}")

def bads = []
def bets = []
def testCount = 0, betsCount = 0 

def shouldUpdate = { Entity entity ->
    boolean result = checkLevel(entity, mapping, true)
    if (result) {
        bads << entity
        testCount++
    }
    Integer level = entity.getProperty('level')?.intValue()
    if (level >= 9400) {
//        logger.log(Level.FINE, "Checking : ${entity}")
        betsCount++
        double balance = entity.getProperty('balance')?.doubleValue()
        double betBefore = maxBetsBefore.get(level)
        double betAfter  = maxBetsAfter.get(level)
        double newBalance = balance / betBefore * betAfter
        bets << "Id ${entity.key.id} (Lvl ${entity.getProperty('level')}) Balance : ${balance} (bet ${betBefore}) -> ${newBalance} (bet ${betAfter})" 
//        result = true
    }
    // result
    false
}

def updateEntity = { Entity entity ->
    Integer level = entity.getProperty('level')?.intValue()
    if (checkLevel(entity, mapping)) {

        level++
    
        Double xp = mapping[level]
        entity.setUnindexedProperty('experience', xp)
        entity.setProperty('level', level)
    }

    if (level >= 9400) {
        
    }

    entity
}
def mcKeyProvider = { Entity entity -> "User_${entity.key.id}".toString()  }

def info = new DatastoreUtil('User').setShouldUpdate(shouldUpdate).setUpdateEntity(updateEntity).setMemcacheKeyProvider(mcKeyProvider).execute()
//def info = ''
def users = bads.collect { Entity e ->
    "Id ${e.key.id} + Level ${e.getProperty('level')} + XP ${e.getProperty('experience')}".toString()
}.join('\n')
def bts = bets.join('\n')

def result = "Done : ${info}\nCount = ${testCount}\nUsers\n\n${users}\n\nBets (${betsCount})\n\n${bts}".toString()
//notifyEnded('sergey.shcherbovich@synesis.ru', result)
result

boolean checkLevel(Entity entity, Map<Integer, Double> levelXp, boolean notify = false) {
    Integer level     = entity.getProperty('level').intValue()
    Integer nextLevel = level + 1
    Double xp = entity.getProperty('experience')
    if (notify && !levelXp.containsKey(nextLevel)) {
        logger.warning("User is out of economy ${entity}")
    }
    Double nextXp    = levelXp.get(nextLevel)
    Double currentXp = levelXp.get(level)

    return levelXp.containsKey(nextLevel) && (xp >= nextXp || xp < currentXp)
}

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