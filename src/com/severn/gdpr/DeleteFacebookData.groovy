package com.severn.gdpr

import org.springframework.context.ApplicationContext

import com.google.appengine.api.memcache.MemcacheServiceFactory
import com.severn.auth.config.AuthConfigDAO
import com.severn.common.dao.AuthorizationDAO
import com.severn.common.dao.UserDAO
import com.severn.common.domain.User

def userId = 6440846836629504L

ApplicationContext ctx = binding.variables.get('applicationContext')
UserDAO userDAO = ctx.getBean('userDAO')
AuthConfigDAO authConfig = ctx.getBeansOfType(AuthConfigDAO.class).entrySet().iterator().next().value
AuthorizationDAO authorizationDAO = ctx.getBeansOfType(AuthorizationDAO.class).entrySet().iterator().next().value

User user = userDAO.getUser(userId)

user.forceNotSocial = true
user.name = authConfig.generateName()

userDAO.saveUser(user)
authorizationDAO.deleteSocialUserInfo(userId)

MemcacheServiceFactory.getMemcacheService().delete('level_TOP_PLAYERS_MC_KEY')
MemcacheServiceFactory.getMemcacheService('default').delete('level_TOP_PLAYERS_MC_KEY')

"OK ${user}".toString()