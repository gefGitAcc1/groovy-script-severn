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
import com.severn.bonuses.service.ExperienceBoosterService
import com.severn.common.domain.MapEntryStringValue
import com.severn.common.http.RequestHelper

Logger logger = Logger.getLogger('com.severn')

def uids = [6003651926556672]

ApplicationContext ctx = binding.variables.get('applicationContext')
ExperienceBoosterService experienceBoosterService = ctx.getBean('experienceBoosterService')

uids.each { long uid ->
    logger.log(Level.FINE, "State is {0}", experienceBoosterService.getUserExperienceBoosterState(uid))

    List<MapEntryStringValue> params = new ArrayList<MapEntryStringValue>();
    params.add(new MapEntryStringValue("durationMs", "19800000"))
    params.add(new MapEntryStringValue("multiplier", "2"))
    params.add(new MapEntryStringValue("count", "1"))

    experienceBoosterService.applyUserExperienceBooster(uid, params)
    RequestHelper.afterRequest()
}

'OK!'