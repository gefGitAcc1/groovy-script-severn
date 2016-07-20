import java.sql.ResultSet
import java.sql.SQLException;
import java.util.logging.*;

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
import com.google.apphosting.api.ApiProxy

import com.severn.common.domain.User;
import com.severn.script.utils.BigQueryScriptUtils;
import com.severn.script.utils.DatastoreSciptUtils;

ApplicationContext ctx = binding.variables.get('applicationContext')
DateTime dt = DateTime.now(DateTimeZone.UTC)
DateTimeFormatter f = DateTimeFormat.forPattern('yyyy-MM-dd').withZoneUTC()
def infos = []

Logger logger = Logger.getLogger('com.severn')

def dataSource = ctx.getBean(DataSource.class)
def jdbcTemplate = new JdbcTemplate(dataSource)
// XXX : segment id
def segmentId = '4'

def mapping = jdbcTemplate.query("SELECT lc.levelNumber, lc.startExperience  FROM level_config lc  WHERE lc.segmentId = $segmentId", 
    new ResultSetExtractor() {
            public Map extractData(ResultSet rs) throws SQLException, DataAccessException {
                def m =[:]
                while (rs.next()) {
                    Integer lvl = rs.getInt("levelNumber")
                    Double xp = rs.getDouble("startExperience")
                    m[lvl] = xp
                }
                return m;
            }
        });

logger.log(Level.INFO, "Load level mapping : ${mapping.size()}")

def shouldUpdate = {
    Entity entity = it
    Integer level = entity.getProperty('level').intValue()
    Integer nextLevel = level + 1
    Double xp = entity.getProperty('experience')
    boolean result = xp >= mapping[nextLevel] || xp < mapping[level]
    result
}

def updateEntity = {
    Entity entity = it
    Integer level = entity.getProperty('level')?.intValue()

    level++

    Double xp = mapping[level]
    entity.setUnindexedProperty('experience', xp)
    entity.setProperty('level', level)
    entity
}
def mcKeyProvider = {  "User_${it.key.id}".toString()  }

def info = DatastoreSciptUtils.processEntities('User', shouldUpdate, updateEntity, mcKeyProvider, [capacity: '250', limit: '50000'])

def result = 'Ok ' + info
notifyEnded('sergey.shcherbovich@synesis.ru', result)
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