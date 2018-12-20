package com.severn.users

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

long id1 = 5215493609750528L
long id2 = 6681465891323904L

User user1 = userDAO.getUser(id1)
User user2 = userDAO.getUser(id2)

if (user1 && user2) {
    def sId = user1.socialId
    
    user1.socialId = user2.socialId
    user2.socialId = sId
    
    userDAO.saveUser(user1)
    userDAO.saveUser(user2)
}

"OK! User1 = ${user1}, User2 = ${user2}".toString()