import com.google.appengine.api.datastore.*
import com.google.appengine.api.memcache.*

import java.util.logging.*
import groovy.json.*

def ds = DatastoreServiceFactory.getDatastoreService()
def mc = MemcacheServiceFactory.getMemcacheService('default')
def ents = []

def addGems = 0
def addCoins = 0
def userId = 6237013304934400l

def mc_keys = []
mc_keys << 'User_' + userId << 'CustomState_UserState_' + userId << 'CustomState_UserReward_' + userId

def entity = ds.get(KeyFactory.createKey('User', userId))

// Update User Entity
def lives = Math.max(entity.getProperty('lives'), 5)
entity.setUnindexedProperty("lives", lives)

def lvl = entity.getProperty('level') // + 1
entity.setProperty("level", lvl);
entity.setUnindexedProperty("experience", 100000d);

def coins = entity.getProperty('balance')
entity.setUnindexedProperty("maxBalance", coins + addCoins );
entity.setUnindexedProperty("balance", coins + addCoins );

def gems = entity.getProperty('gems')
entity.setUnindexedProperty("gems", gems + addGems);
ents << entity

// PROFILE UPDATE
def dataEntity = ds.get(KeyFactory.createKey('CustomState', "UserState_$userId"))
def json = dataEntity.getProperty('data').value

logger.log(Level.FINE, "JSON : $json")

def slurper = new JsonSlurper()
def result = new JsonSlurper().parseText(json)

logger.log(Level.FINE, "Parsed : $result")

result.Gems = result.Gems + addGems
result.Lives = lives
result.Coins = result.Coins + addCoins
result.Level = 98// result.Level + 1
result.Experience = 100000

def newJson = JsonOutput.toJson(result)

logger.log(Level.FINE, "NEW JSON : $newJson")
 
dataEntity.setUnindexedProperty('data', new Text(newJson))
ents << dataEntity

mc.deleteAll(mc_keys)
ds.put(ents)

'OK'