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
import com.severn.bonuses.service.ExperienceBoosterService
import com.severn.common.domain.AuthenticatedUser
import com.severn.common.domain.MapEntryStringValue
import com.severn.common.http.RequestHelper
import com.severn.common.utils.AuthenticationUtils
import com.severn.game.slots.dao.MachineStateDAOAsyncImpl
import com.severn.game.slots.engine.FreeSpinManager

Logger logger = Logger.getLogger('com.severn')

ApplicationContext ctx = binding.variables.get('applicationContext')
FreeSpinManager freeSpinManager = ctx.getBean('freeSpinManager')

AuthorizationService authService = ctx.getBean('authorizationService')

MachineStateDAOAsyncImpl machineStateDAO = ctx.getBean('machineStateDAO')

long authId = 5379796336902144L
long userId = 5029873540136960L
int fs = 1000
double bet = 44_000_000_000
int lines = 27

AuthenticatedUser authUser = authService.authenticate(authId)
AuthenticationUtils.setCurrentUser(authUser)

freeSpinManager.createFreeSpins(authId, 64L, 2, fs, bet, lines, -1L)

machineStateDAO.flush()

'OK!'