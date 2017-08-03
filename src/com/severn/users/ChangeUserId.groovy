import org.springframework.context.ApplicationContext

import com.google.appengine.api.datastore.DatastoreService
import com.google.appengine.api.datastore.DatastoreServiceFactory
import com.google.appengine.api.datastore.Entity
import com.google.appengine.api.datastore.EntityNotFoundException
import com.google.appengine.api.datastore.Key
import com.google.appengine.api.datastore.KeyFactory
import com.google.appengine.api.datastore.Query
import com.google.appengine.api.datastore.Query.FilterOperator
import com.google.appengine.api.datastore.Query.FilterPredicate
import com.severn.common.services.MemCacheService

long oldUserId = 5370523045855232L, newUserId = 1111111111111111L

ApplicationContext ctx = binding.variables.get('applicationContext')

MemCacheService mc = ctx.getBean('memCacheService')

DatastoreService ds = DatastoreServiceFactory.getDatastoreService()

String[] entities = ['User', 'UserSocialInfo', 'DailyBonusWheelState', 'ContestBoosterState']
def entToMcPrefix = []

entities.each { String entName ->
    Key oldKey = KeyFactory.createKey(entName, oldUserId)
    Key newKey = KeyFactory.createKey(entName, newUserId)

    try {
        Entity oldEntity = ds.get(oldKey)
        Entity newEntity = new Entity(newKey)

        newEntity.setPropertiesFrom(oldEntity)

        String mcKey = entToMcPrefix['entName'] ? entToMcPrefix['entName'] + oldUserId : entName + '_' + oldUserId
        if (mcKey) {
            mc.delete(mcKey)
        }

        ds.delete(oldKey)
        ds.put(newEntity)
    } catch (EntityNotFoundException oldPropsEntity) {
        // do nothing
    }
}

// auths
Query query = new Query('UserAuthentication')
query.setFilter(new FilterPredicate('userId', FilterOperator.EQUAL, oldUserId))

def i = ds.prepare(query).asIterable()
List<Entity> ents = [] as List<Entity>

i.each { Entity oldPropsEntity ->
    oldPropsEntity.setProperty('userId', newUserId)
    ents << oldPropsEntity
}
ds.put(ents)

// Custom props
Key oldUserKey = KeyFactory.createKey('User', oldUserId)
Key oldPropsKey = KeyFactory.createKey(oldUserKey, 'UserCustomProps', 'Default')
Entity oldPropsEntity = ds.get(oldPropsKey)

Key newUserKey = KeyFactory.createKey('User', newUserId)
Key newPropsKey = KeyFactory.createKey(newUserKey, 'UserCustomProps', 'Default')
Entity newPropsEntity = new Entity(newPropsKey)
newPropsEntity.setPropertiesFrom(oldPropsEntity)

ds.delete(oldPropsKey)
ds.put(newPropsEntity)

mc.delete('User_UserCustomProps_Default_' + oldUserId)

"Ok".toString()