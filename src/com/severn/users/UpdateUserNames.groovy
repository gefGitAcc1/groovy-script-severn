import java.util.logging.*

import org.joda.time.DateTime
import org.springframework.context.ApplicationContext
import org.springframework.jdbc.core.JdbcTemplate

import com.google.appengine.api.datastore.Entity
import com.severn.datastore.legacy.DatastoreUtil

long now = System.currentTimeMillis()

ApplicationContext ctx = binding.variables.get('applicationContext')
def ds = ctx.getBean('configDataSource')
JdbcTemplate jdbcTemplate = new JdbcTemplate(ds)

//Set<String> fNames = ['Happy','Lucky','Good','Awesome','Cool','VIP','Diamond','Pretty','Golden','Joyful','Great','Luxury','Rich'] as HashSet<String>
//Set<String> lNames = ['Winner','Spinner','Player','Gamer','Gambler','Competitor','Fan',''] as HashSet<String>

Set<String> fNames = jdbcTemplate.queryForList('select first_name fn from default_first_name').collect{ row -> row.fn } as HashSet<String>
Set<String> lNames = jdbcTemplate.queryForList('select last_name ln from default_last_name').collect{ row -> row.ln } as HashSet<String>

def shouldUpdate = { Entity it ->
    def name = it.getProperty('name')
    def dispName = it.getProperty('displayName')

    def res = (null == dispName) ? !autoGen(name, fNames, lNames) : false

    res
}

def update = { Entity it ->
    def name = it.getProperty('name')
    def dispName = name ? (autoGen(name, fNames, lNames) ? name : reduceName(name)) : 'Unknown'
    it.setUnindexedProperty('displayName', dispName)

    it
}

def mc = { Entity it ->
    "User_${it.key.id}".toString()
}

def res = new DatastoreUtil('User').setShouldUpdate(shouldUpdate).setUpdateEntity(update).setMemcacheKeyProvider(mc).execute()
'OK : ' + res + ', took ' + (System.currentTimeMillis() - now) + ' ms.'

def autoGen(def name, def fNames, def lNames) {
    def splitted = name ? name.split(' ') : []
    if (splitted && 2 == splitted.length) {
        def fName = splitted[0], lName = splitted[1]
        if (fNames.contains(fName) && lNames.contains(lName)) {
            return true
        }
    }

    return false
}

def reduceName (def name) {
    if (name == null) return null;
    name = name.trim();
    int firstSpace = name.indexOf(" ");
    int lastSpace = name.lastIndexOf(" ");
    if (firstSpace > 0) {
        name = name.substring(0, firstSpace) + " " + name.substring(lastSpace + 1, lastSpace + 2) + ".";
    }
    return name;
}