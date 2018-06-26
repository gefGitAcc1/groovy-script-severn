package com.severn.gdpr

import java.util.Arrays
import java.util.logging.Level
import java.util.logging.Logger

import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

import org.apache.commons.codec.binary.Base64
import org.apache.commons.codec.binary.StringUtils
import org.joda.time.LocalDateTime
import org.springframework.context.ApplicationContext

import com.google.appengine.api.NamespaceManager
import com.google.appengine.api.datastore.DatastoreService
import com.google.appengine.api.datastore.DatastoreServiceFactory
import com.google.appengine.api.datastore.Entity
import com.google.appengine.api.datastore.EntityNotFoundException
import com.google.appengine.api.datastore.Key
import com.google.appengine.api.datastore.KeyFactory
import com.google.appengine.api.memcache.MemcacheServiceFactory
import com.severn.auth.config.AuthConfigDAO
import com.severn.common.dao.AuthorizationDAO
import com.severn.common.dao.UserDAO
import com.severn.common.domain.DeviceAuthorizationInfo
import com.severn.common.domain.User
import com.severn.common.domain.UserAuthentication

ApplicationContext ctx = binding.variables.get('applicationContext')
UserDAO userDAO = ctx.getBean('userDAO')
AuthorizationDAO authorizationDAO = ctx.getBean('authorizationDAO')

Logger logger = Logger.getLogger('com.severn')
def userId = 6717908650557440L

User user = userDAO.getUser(userId)

def result
if (user) {
    String guid = "gdpr-${UUID.randomUUID()}-${UUID.randomUUID()}"

    DatastoreService ds = DatastoreServiceFactory.getDatastoreService()
    Key key = KeyFactory.createKey('UserForgotten', userId)

    try {
        ds.get(key)
        throw new RuntimeException('User is forgotten')
    } catch (EntityNotFoundException e) { }

    // backups
    baldurEntity(ds, 'User', userId)
    baldurEntity(ds, 'UserSocialInfo', userId)
    // backups end

    Entity e = new Entity(key)
    e.setProperty('ts', LocalDateTime.now().toString())
    e.setProperty('guid', guid)

    if (user.socialId) {
        user.socialId = guid
        user.forceNotSocial = true
        user.name = ctx.getBeansOfType(AuthConfigDAO.class).entrySet().iterator().next().value.generateName()
    }

    List<UserAuthentication> auths = authorizationDAO.getUserAuthenticationsByUserIds([userId] as List)

    logger.log(Level.INFO, "User ${user}")
    logger.log(Level.INFO, "Entity ${e}")

    auths.each { UserAuthentication it ->
        baldurEntity(ds, 'UserInstallation', it.authId)
        baldurEntity(ds, 'UserAuthentication', it.authId)

        it.deviceId = guid
        it.authorizationKey = guid

        authorizationDAO.saveUserAuthentication(it)
        authorizationDAO.saveDeviceAuthorizationInfo(it, new DeviceAuthorizationInfo())
    }
    ds.put(e)
    userDAO.saveUser(user)
    baldurEntity(ds, 'UserForgotten', userId)
    
    MemcacheServiceFactory.getMemcacheService().delete('level_TOP_PLAYERS_MC_KEY')
    MemcacheServiceFactory.getMemcacheService('default').delete('level_TOP_PLAYERS_MC_KEY')
    
    result = "OK ${guid}".toString()
} else {
    throw new RuntimeException('User not found')
}

result

class Bean<T> {
    Class<T> clazz
    def binding
    
    Bean(Class<T> clazz, def binding) {
        this.clazz = clazz
        this.binding = binding
    }
    
    T getBean() {
        ApplicationContext ac = binding.variables.get('applicationContext')
        ac.getBeansOfType(clazz).entrySet().iterator().next().value
    }
}

void baldurEntity(DatastoreService ds, String kind, long id) {
    String oldNamespace = NamespaceManager.get()
    try {
        Entity existingEntity = ds.get(KeyFactory.createKey(kind, id))
        NamespaceManager.set('Baldur')
        Entity newEntity = new Entity(KeyFactory.createKey(kind, id))
        existingEntity.getProperties().entrySet().each { entry ->
            newEntity.setProperty(entry.key, entry.value)
        }
        DatastoreServiceFactory.getDatastoreService().put(newEntity)
    } catch (EntityNotFoundException enfe) {
        logger.log(Level.WARNING, "Exception on gettig ${kind}:${id}", enfe)
    } finally {
        NamespaceManager.set(oldNamespace)
    }
}

class Ciph {
    private static final String ALGORITHM = "AES/CBC/PKCS5Padding";
    
    public static String encrypt(String encryptionKey, String text) throws Exception {
        byte[] key = StringUtils.getBytesUtf8(encryptionKey);
        byte[] toEncrypt = StringUtils.getBytesUtf8(text);
    
        byte[] encrypted = process(fitKey(key), toEncrypt, Cipher.ENCRYPT_MODE);
        byte[] encoded = Base64.encodeBase64(encrypted);
    
        return StringUtils.newStringUtf8(encoded);
    }
    
    public static String decrypt(String encryptionKey, String encrypted) throws Exception {
        byte[] key = StringUtils.getBytesUtf8(encryptionKey);
        byte[] toDecrypt = Base64.decodeBase64(StringUtils.getBytesUtf8(encrypted));
    
        byte[] decrypted = process(fitKey(key), toDecrypt, Cipher.DECRYPT_MODE);
    
        return StringUtils.newStringUtf8(decrypted);
    }
    
    private static byte[] process(byte[] key, byte[] message, int mode) throws Exception{
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        cipher.init(mode, new SecretKeySpec(key, "AES"), new IvParameterSpec(key));
        return cipher.doFinal(message);
    }
    
    private static byte[] fitKey(byte[] key) {
        return Arrays.copyOf(key, 16);
//        return key;
    }
}