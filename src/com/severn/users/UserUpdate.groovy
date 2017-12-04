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
import com.severn.game.slots.engine.FreeSpinManager
import com.severn.user.service.UserServiceImpl

Logger logger = Logger.getLogger('com.severn')

ApplicationContext ctx = binding.variables.get('applicationContext')

UserDAO userDAO = ctx.getBean('userDAO')

long id = 5910326099836928L

User user = userDAO.getUser(id)

if (user) {
    user.disabled = false
    userDAO.saveUser(user)
}

"OK! User = ${user}".toString()