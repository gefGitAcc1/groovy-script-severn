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
import com.google.appengine.api.datastore.Text
import com.severn.auth.service.AuthorizationService
import com.severn.bonuses.service.ExperienceBoosterService
import com.severn.common.domain.AuthenticatedUser
import com.severn.common.domain.MapEntryStringValue
import com.severn.common.http.RequestHelper
import com.severn.common.utils.AuthenticationUtils
import com.severn.datastore.legacy.DatastoreUtil
import com.severn.game.slots.dao.MachineStateDAOAsyncImpl
import com.severn.game.slots.engine.FreeSpinManager

import javassist.expr.Instanceof

Logger logger = Logger.getLogger('com.severn')

ApplicationContext ctx = binding.variables.get('applicationContext')
FreeSpinManager freeSpinManager = ctx.getBean('freeSpinManager')

AuthorizationService authService = ctx.getBean('authorizationService')

MachineStateDAOAsyncImpl machineStateDAO = ctx.getBean('machineStateDAO')
String IMG = 'https://storage.googleapis.com/gambino-slots/MediaLibrary/{platform}/{locale}/{res}/marketing/compensations/version_1_19_loyalty_card_feature_content_0{type}';

def shouldUpdate = { Entity e ->
    def imgLoc = e.getProperty("imageLocation")
    
    if (imgLoc instanceof Text) {
        imgLoc = ((Text)imgLoc).getValue()
    }
    
    IMG.equals(imgLoc) 
}

def update = { Entity e ->
    e.setProperty("receiver", "delete_comp_loyV2")
    e
}

def res = new DatastoreUtil("Gift_balanceCompensation").setShouldUpdate(shouldUpdate).setUpdateEntity(update).execute()

'OK! Res : ' + res