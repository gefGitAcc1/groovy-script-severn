import com.google.appengine.api.datastore.*
import com.google.appengine.api.memcache.*

import java.util.logging.*
import groovy.json.*

def ds = DatastoreServiceFactory.getDatastoreService()
def mc = MemcacheServiceFactory.getMemcacheService('default')
def ents = []

def addGems = 900
def addCoins = 30000
def userId = 6291842857435136l

def mc_keys = []
mc_keys << 'User_' + userId << 'CustomState_UserState_' + userId << 'CustomState_UserReward_' + userId

def entity = ds.get(KeyFactory.createKey('User', userId))

logger.log(Level.FINE, 'User entity before {0}', entity)

// Update User Entity
def lives = Math.max(entity.getProperty('lives'), 5)
entity.setUnindexedProperty("lives", lives)

def lvl = entity.getProperty('level') + 1
entity.setProperty("level", lvl);
entity.setUnindexedProperty("experience", 1d);

def coins = entity.getProperty('balance')
entity.setUnindexedProperty("maxBalance", coins + addCoins );
entity.setUnindexedProperty("balance", coins + addCoins );

def gems = entity.getProperty('gems')
entity.setUnindexedProperty("gems", gems + addGems);

logger.log(Level.FINE, 'User entity after {0}', entity)

ents << entity

// PROFILE UPDATE
def dataEntity = ds.get(KeyFactory.createKey('CustomState', "UserState_$userId"))

logger.log(Level.FINE, 'UserData entity before {0}', dataEntity)

def json = dataEntity.getProperty('data').value

logger.log(Level.FINE, "JSON : $json")

def slurper = new JsonSlurper()
def result = new JsonSlurper().parseText(json)

logger.log(Level.FINE, "Parsed : $result")

result.Gems = result.Gems + addGems
result.Lives = lives
result.Coins = result.Coins + addCoins
result.Level = result.Level + 1
result.Experience = 1

def newJson = JsonOutput.toJson(result)

logger.log(Level.FINE, "NEW JSON : $newJson")

newJson = JsonOutput.prettyPrint(newJson)

logger.log(Level.FINE, "NEW JSON (pretty) : $newJson")

dataEntity.setUnindexedProperty('data', new Text(newJson))

logger.log(Level.FINE, 'UserData entity after {0}', dataEntity)

ents << dataEntity

mc.deleteAll(mc_keys)
ds.put(ents)

'OK'