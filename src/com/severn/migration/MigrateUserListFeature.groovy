package com.severn.migration

import java.util.logging.*

import org.joda.time.DateTimeZone
import org.joda.time.LocalDateTime

import com.google.appengine.api.datastore.*

def count = 0

['UserListFeature_inAppMessage','UserListFeature_marketingCompensation','UserListFeature_contests'].each { kind ->
    DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService()
    Query query = new Query(kind)

    def oldKeys = [], newEntities = []

    datastoreService.prepare(query).asIterable().each { e ->
        oldKeys << e.key

        if (e.hasProperty('assignedUserIds')) {
            e.getProperty('assignedUserIds').each { userId ->
                def newE = new Entity(e.key.kind, "${e.key.id}_${userId}")

                newE.setUnindexedProperty('updateTimestamp', e.getProperty('updateTimestamp'))
                newE.setUnindexedProperty('segmentId', e.getProperty('segmentId'))
                newE.setUnindexedProperty('expireTimestamp', e.getProperty('expireTimestamp'))

                newE.setProperty('active', e.getProperty('active'))
                newE.setProperty('userId', userId)
                newE.setProperty('featureId', e.key.id)

                newEntities << newE

                count++
            }
        }
    }

//    datastoreService.delete(oldKeys)
    datastoreService.put(newEntities)
}

'OK : ' + count