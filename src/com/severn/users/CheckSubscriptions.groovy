import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.security.PrivateKey;
import java.util.ArrayList
import java.util.Arrays;
import java.util.List
import java.util.Map
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
import com.severn.common.dao.UserDAO
import com.severn.common.domain.AuthenticatedUser
import com.severn.common.domain.MapEntity
import com.severn.common.domain.MapEntryStringValue
import com.severn.common.domain.User
import com.severn.common.http.RequestHelper
import com.severn.common.utils.AuthenticationUtils
import com.severn.game.slots.engine.FreeSpinManager
import com.severn.payments.dao.PurchaseDAO
import com.severn.payments.dao.SubscriptionDAO
import com.severn.payments.domain.PurchaseTransaction
import com.severn.payments.domain.SubscriptionInfo
import com.severn.payments.domain.SubscriptionInfosPage
import com.severn.payments.service.SubscriptionsService

Logger logger = Logger.getLogger('com.severn')

ApplicationContext ctx = binding.variables.get('applicationContext')

SubscriptionDAO dao = ctx.getBean('subscriptionDAO')
SubscriptionInfosPage page = dao.listSubscriptions(null, 1_000)
PurchaseDAO pd = ctx.getBean('purchaseDAO')

UserDAO userDAO = ctx.getBean('userDAO')

def count = 0

for (Map.Entry<Long, List<SubscriptionInfo>> e : page.getSubscriptionInfos().entrySet()) {
    Long userId = e.getKey()
    User u = userDAO.getUser(userId);
    if (u == null || u.isCheatUser()) {
        logger.info("Bad user : ${u}")
        for (SubscriptionInfo si : e.getValue()) {
            if (si.getTransactionId()) {
                si.setTransactionId(null)
                dao.saveSubscription(userId, si)
                count++
            }
//            Long txId = si.getTransactionId()
//            PurchaseTransaction tx = pd.getTransactionById(txId)
//            Long stubId = -100L
//            if (tx && !tx.getAuthId().equals(stubId)) {
//                logger.info("Bad tx : ${tx}")
//                tx.setAuthId(stubId)
//                pd.updatePurchaseTransaction(tx)
//                count++
//            }
        }
    }
}

"OK! Count ${count}".toString()