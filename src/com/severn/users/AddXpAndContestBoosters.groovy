import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.security.PrivateKey;
import java.util.ArrayList
import java.util.Arrays;
import java.util.List
import java.util.logging.*;

import javax.mail.*;
import javax.mail.internet.*;

import org.springframework.context.ApplicationContext

import com.google.appengine.api.datastore.DatastoreService
import com.google.appengine.api.datastore.DatastoreServiceFactory
import com.google.appengine.api.datastore.Entity
import com.google.appengine.api.datastore.KeyFactory
import com.severn.auth.service.AuthorizationService
import com.severn.bonuses.dao.ExperienceBoosterDAO
import com.severn.bonuses.service.ExperienceBoosterService
import com.severn.common.dao.UserCustomStateDAO
import com.severn.common.dao.UserDAO
import com.severn.common.domain.AuthenticatedUser
import com.severn.common.domain.ExperienceBoosterState
import com.severn.common.domain.MapEntryStringValue
import com.severn.common.domain.User
import com.severn.common.http.RequestHelper
import com.severn.common.services.MemCacheService
import com.severn.common.utils.AuthenticationUtils
import com.severn.contests.dao.ContestBoosterDAO
import com.severn.contests.domain.ContestBoosterState
import com.severn.game.slots.dao.MachineStateDAOAsyncImpl
import com.severn.game.slots.engine.FreeSpinManager
import com.severn.user.service.UserServiceImpl

Logger logger = Logger.getLogger('com.severn')

ApplicationContext ctx = binding.variables.get('applicationContext')

MemCacheService mc = ctx.getBean('memCacheService')
UserDAO userDAO = ctx.getBean('userDAO')
ExperienceBoosterDAO experienceBoosterDAO = ctx.getBean('experienceBoosterDAO')
ContestBoosterDAO contestBoosterDAO = ctx.getBean('contestBoosterDAO')

long id = 5752957537091584L

ContestBoosterState st = contestBoosterDAO.get(id)
if (!st) {
    st = new ContestBoosterState()
    st.id = id
}
st.count = 44
contestBoosterDAO.save(st)

UserCustomStateDAO userCustomStateDAO = userDAO

ExperienceBoosterState state = userCustomStateDAO.getState(id, 'experienceBooster', ExperienceBoosterState.class)
state.setBostersQueue([new MapEntryStringValue('big', '5')])
userCustomStateDAO.saveState(id, 'experienceBooster', state)
userCustomStateDAO.flush()
experienceBoosterDAO.saveState(id, state)

//mc.delete(UserServiceImpl.TOP_PLAYERS_MC_KEY)
//mc.delete(UserServiceImpl.TOP_PLAYERS_MIN_VALUE_MC_KEY)

"OK! ContBoosters = ${st.count}".toString()