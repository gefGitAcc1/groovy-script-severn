import java.util.logging.*

import org.joda.time.DateTime

import com.google.appengine.api.datastore.Entity
import com.google.appengine.api.datastore.Text
import com.google.appengine.repackaged.com.google.common.base.Equivalence.Equals
import com.google.gson.Gson
import com.severn.datastore.legacy.DatastoreUtil
import com.severn.game.slots.domain.FreeSpinsGroup
import com.severn.game.slots.domain.StoredFreeSpinsInfo

Gson gson = new Gson()
int count = 0

def shouldUpdate = { Entity it ->
    String[] parts = it.key.name.split('_')
    Object obj = it.getProperty('Freespins')
    String fs = obj instanceof Text ? ((Text)obj).getValue() : obj as String
    boolean res = 3 == parts.length && '64'.equals(parts[2]) && null != fs && !'null'.equals(fs.toString())
    if (res) {
        res = false
//        println "fs is ${fs}"
        StoredFreeSpinsInfo sfsi = gson.fromJson(fs, StoredFreeSpinsInfo.class)
        for (FreeSpinsGroup fsg : sfsi.getFreeSpinsGroups()) {
            if (fsg.count > 1000) {
                count++
                res = true
            }
        }
    }
    if (res) {
        println "selected ${fs}"
    }
    res
}
def update = { Entity it ->
    println "userId = ${it.key.name.split('_')[0]}"
    Object obj = it.getProperty('Freespins')
    String fs = obj instanceof Text ? ((Text)obj).getValue() : obj as String
    StoredFreeSpinsInfo sfsi = gson.fromJson(fs, StoredFreeSpinsInfo.class)
    for (FreeSpinsGroup fsg : sfsi.getFreeSpinsGroups()) {
        if (fsg.count > 1000) {
            fsg.count = 1000
        }
    }
    it.setUnindexedProperty('Freespins', gson.toJson(sfsi))
    it
}

def mcProvider = { Entity it ->
    "MachineStateR_${it.key.name}".toString()
}

def res = new DatastoreUtil('MachineState').setShouldUpdate(shouldUpdate).setUpdateEntity(update).setMemcacheKeyProvider(mcProvider).setCapacity(1).execute()
'OK : ' + res + ', count ' + count