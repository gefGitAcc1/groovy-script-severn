import com.google.appengine.api.datastore.Entity
import com.severn.bonuses.dao.UserBonusDAO
import com.severn.common.dao.UserDAO
import com.severn.common.domain.User
import com.severn.common.domain.UserFriendsInfo
import com.severn.common.spring.AppContextProvider
import com.severn.datastore.legacy.DatastoreUtil
import com.severn.repositories.BaseRepository

UserBonusDAO ubDAO = AppContextProvider.getApplicationContext().getBeansOfType(UserBonusDAO.class).values().iterator().next()
UserDAO userDAO = AppContextProvider.getApplicationContext().getBeansOfType(UserDAO.class).values().iterator().next()
BaseRepository<UserFriendsInfo, Long> uInfoRepo = AppContextProvider.getApplicationContext().getBean('userFriendsInfoRepository') 

def m = [:]
def usrs = []

def sh = { Entity e ->
    long userId = e.key.id
    Long level = e.getProperty('level')?.longValue()
    String socId = e.getProperty('socialId')
    Long lastPlayed = e.getProperty('lastAuthTime')?.longValue()
    Long createdTime = e.getProperty('createdTime')?.longValue()

    if (level && socId && level <= 30 && lastPlayed > 1509494400000L /*1522540800000L*/) {

        List<Long> fIds = uInfoRepo.findOne(userId)?.friends// userDAO.getUserSocialFriends(userId)
        if (fIds) {
            List<User> fs = userDAO.getUsers(fIds)
            fs.each { User f ->
                if (createdTime > f.getCreatedTime()) {
                    usrs << userId

//                    ubDAO.saveFriendJoinedGameReward(f.userId, userId)
                }
            }
        }
    }

    false
}

def dsUtil = new DatastoreUtil('User').setShouldUpdate(sh).setUpdateEntity(null).setMemcacheKeyProvider(null)
def info = dsUtil.execute()

"Done ${usrs.size()}. Process data : ${info}".toString()
