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

import com.google.appengine.api.datastore.DatastoreService
import com.google.appengine.api.datastore.DatastoreServiceFactory
import com.google.appengine.api.datastore.Entity
import com.google.appengine.api.datastore.EntityNotFoundException
import com.google.appengine.api.datastore.Key
import com.google.appengine.api.datastore.KeyFactory
import com.severn.common.dao.AuthorizationDAO
import com.severn.common.dao.UserDAO
import com.severn.common.domain.DeviceAuthorizationInfo
import com.severn.common.domain.User
import com.severn.common.domain.UserAuthentication

ApplicationContext ctx = binding.variables.get('applicationContext')
UserDAO userDAO = ctx.getBean('userDAO')
AuthorizationDAO authorizationDAO = ctx.getBean('authorizationDAO')

Logger logger = Logger.getLogger('com.severn')
def userId = 1111111111111111L

User user = userDAO.getUser(userId)

def result
if (user) {
    String guid = "gdpr-${UUID.randomUUID()}"
    
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService()
    Key key = KeyFactory.createKey('UserForgotten', userId)
    
    try {
        ds.get(key)
        throw new RuntimeException('User is forgotten')
    } catch (EntityNotFoundException e) {
    }
    
    Entity e = new Entity(key)
    e.setProperty('ts', LocalDateTime.now().toString())
    e.setProperty('socialId', user.socialId)
    e.setProperty('guid', guid)
    
    if (user.socialId) {
        user.socialId = "gdpr-${user.socialId}-${UUID.randomUUID()}"
        user.forceNotSocial = true
    }
    
    logger.log(Level.INFO, "User ${user}")
    
    List<UserAuthentication> auths = authorizationDAO.getUserAuthenticationsByUserIds([userId] as List)
    List<UserAuthentication> toSave = auths.collect{ UserAuthentication it ->
        def encDevId = it.deviceId ? Ciph.encrypt(guid, it.deviceId) : it.deviceId
        logger.log(Level.FINE, "${it.deviceId} -> ${encDevId}")
        it.deviceId = encDevId
        
        def encAuthorizationKey = Ciph.encrypt(guid, it.authorizationKey)
        logger.log(Level.FINE, "${it.authorizationKey} -> ${encAuthorizationKey}")
        it.authorizationKey = encAuthorizationKey
        
        logger.log(Level.INFO, "UserAuthentication ${it}")
        
        it
    }
    
    logger.log(Level.INFO, "Entity ${e}")
    toSave.each { UserAuthentication it ->
//        authorizationDAO.saveUserAuthentication(it)
//        authorizationDAO.saveDeviceAuthorizationInfo(it, new DeviceAuthorizationInfo())
    }
//    ds.put(e)
//    userDAO.saveUser(user)
    result = "OK ${guid}".toString()
} else {
    throw new RuntimeException('User not found')
}

result

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