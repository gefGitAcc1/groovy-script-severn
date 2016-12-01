import java.sql.ResultSet
import java.sql.SQLException
import java.util.Map.Entry;
import java.util.logging.*

import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import org.springframework.context.*
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.*

import javax.mail.*;
import javax.mail.internet.*
import javax.sql.*

import com.google.appengine.api.datastore.*
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.google.appengine.api.datastore.Query.FilterOperator
import com.google.appengine.api.memcache.*
import com.google.apphosting.api.ApiProxy

import com.severn.common.domain.User
import com.severn.script.utils.DatastoreSciptUtils
import com.severn.common.utils.DatastoreUtil

ApplicationContext ctx = binding.variables.get('applicationContext')
DateTime dt = DateTime.now(DateTimeZone.UTC)
DateTimeFormatter f = DateTimeFormat.forPattern('yyyy-MM-dd').withZoneUTC()
def infos = []

Logger logger = Logger.getLogger('com.severn')

def dataSource = ctx.getBean(DataSource.class)
def jdbcTemplate = new JdbcTemplate(dataSource)
// XXX : segment id
def segmentId = '4', counter = 0

TreeMap<Double, Integer> mapping = jdbcTemplate.query("SELECT lc.levelNumber, lc.startExperience  FROM level_config lc  WHERE lc.segmentId = $segmentId", 
    new ResultSetExtractor() {
            public TreeMap<Double, Integer> extractData(ResultSet rs) throws SQLException, DataAccessException {
                TreeMap<Double, Integer> m = new TreeMap<>();
                while (rs.next()) {
                    Integer lvl = rs.getInt("levelNumber")
                    Double xp = rs.getDouble("startExperience")
                    if (xp != null && lvl) {
                        m.put(xp, lvl)
                        // m[xp] = lvl
                    }
                }
                return m;
            }
        });

TreeMap<Double, Integer> levelUpMapping = jdbcTemplate.query("SELECT lc.levelNumber, lc.balanceBonus  FROM level_config lc  WHERE lc.segmentId = $segmentId",
    new ResultSetExtractor() {
            public TreeMap<Double, Integer> extractData(ResultSet rs) throws SQLException, DataAccessException {
                TreeMap<Double, Integer> m = new TreeMap<>();
                while (rs.next()) {
                    Integer lvl = rs.getInt("levelNumber")
                    Double balanceBonus = rs.getDouble("balanceBonus")
                    m[lvl] = balanceBonus
                }
                return m;
            }
        });

logger.log(Level.INFO, "Load level mapping : ${mapping.size()}")

def shouldUpdate = {
    Entity entity = it
    Integer level = entity.getProperty('level').intValue()
    Double xp = entity.getProperty('experience')
    Entry<Double, Integer> e = xp != null ? mapping.floorEntry(xp) : null
    if (!e) {
        logger.warning("User is out of economy ${entity}")
    }
    boolean result = e && (!level.equals(e.getValue())) && level >= 2000
    if (result) {
        counter++
    }
    // TODO change to result
//    result
     false
}

def updateEntity = {
    Entity entity = it
    Double xp = entity.getProperty('experience')
    Integer level = entity.getProperty('level')
    Integer newLevel = mapping.floorEntry(xp).getValue()
    
    Double balance = entity.getProperty('balance')
    Double contributionBalance = entity.getProperty('contributionBalance')
    Double newContributionBalance = (newLevel > level) ? (contributionBalance + (10 * (newLevel - level))) : contributionBalance

    for (int i = level + 1; i <= newLevel; i++) {
        double balanceBonus = levelUpMapping[i]
        if (balanceBonus) {
            balance += balanceBonus
        } 
    }

    entity.setUnindexedProperty('contributionBalance', newContributionBalance)
    entity.setUnindexedProperty("balance", balance)
    entity.setProperty('level', newLevel)
    entity
}
def mcKeyProvider = { "User_${it.key.id}".toString() }

def query = new Query("User").setFilter(new FilterPredicate('level', FilterOperator.GREATER_THAN_OR_EQUAL, 2000))
def dsUtil = new DatastoreUtil(query).setShouldUpdate(shouldUpdate).setUpdateEntity(updateEntity).setMemcacheKeyProvider(mcKeyProvider)
def info = dsUtil.execute()//DatastoreSciptUtils.processEntities('User', shouldUpdate, updateEntity, mcKeyProvider, [capacity: '250', limit: '50000'])

def result = 'Ok ' + info + ', counts ' + counter
notifyEnded('sergey.shcherbovich@synesis.ru', result)
result

void notifyEnded(def reciever, def message) {
    try {
        MimeMessage msg = new MimeMessage(Session.getDefaultInstance(new Properties(), null));
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