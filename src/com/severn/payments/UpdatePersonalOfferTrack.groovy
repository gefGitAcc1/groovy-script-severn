import java.util.logging.*

import org.joda.time.DateTime

import com.google.appengine.api.datastore.Entity
import com.severn.datastore.legacy.DatastoreUtil

long dif = 1000L * 60 * 60 * 24
def cnt = 0
long now = System.currentTimeMillis()
String pos = '[{\"id\" : 24146,\"type\" : \"coins_and_extra\",\"startValue\" : 170000.0,\"discountInPercent\" : 900.0,\"endValue\" : 1700000.0,\"price\" : 19.99,\"providerProductId\" : \"https://storage.googleapis.com/gambino-slots/browser/products/product.gambino.19.99.html\",\"platform\" : \"1\",\"contributionPoints\" : 1041.0,\"extraPurchaseItems\" : [{\"type\" : \"contest_booster\",\"parameters\" : [{\"key\" : \"count\",\"value\" : \"2\"}]}, {\"type\" : \"experience_booster\",\"parameters\" : [{\"key\" : \"durationMs\",\"value\" : \"28800000\"}, {\"key\" : \"count\",\"value\" : \"1\"}, {\"key\" : \"multiplier\",\"value\" : \"2\"}]}],\"default\" : true}, {\"id\" : 24147,\"type\" : \"coins_and_extra\",\"startValue\" : 170000.0,\"discountInPercent\" : 900.0,\"endValue\" : 1700000.0,\"price\" : 7.99,\"providerProductId\" : \"https://storage.googleapis.com/gambino-slots/browser/products/product.gambino.7.99.html\",\"platform\" : \"1\",\"contributionPoints\" : 393.0,\"extraPurchaseItems\" : [{\"type\" : \"contest_booster\",\"parameters\" : [{\"key\" : \"count\",\"value\" : \"2\"}]}],\"default\" : false}, {\"id\" : 24148,\"type\" : \"coins_and_extra\",\"startValue\" : 170000.0,\"discountInPercent\" : 900.0,\"endValue\" : 1700000.0,\"price\" : 17.99,\"providerProductId\" : \"https://storage.googleapis.com/gambino-slots/browser/products/product.gambino.17.99.html\",\"platform\" : \"1\",\"contributionPoints\" : 928.0,\"extraPurchaseItems\" : [{\"type\" : \"experience_booster\",\"parameters\" : [{\"key\" : \"durationMs\",\"value\" : \"28800000\"}, {\"key\" : \"count\",\"value\" : \"1\"}, {\"key\" : \"multiplier\",\"value\" : \"2\"}]}],\"default\" : false}]'

//Set<Long> userIds = [5990992372039680,4508783257583616,4610735228321792,4820546303295488,5124410246692864,5459535908044800,6154727552188416,6177865052717056,4952072120696832,5037296236625920,5521904570990592,5799313539072000,6244582617513984,6369200317661184,6090398716395520,6708093189095424,4827443181912064,6624148512571392,6070538202513408,6412002109423616,6114160018980864,5684830367907840,4576867414704128,4746628509466624,4621890134999040,4915703915216896,6280994936586240,4519468230246400,5074007572348928,5151898559578112,5476379696037888,5504139097276416,5546821679054848,6005679909765120,6635301282775040,6719553926070272,6732824261427200,5352017923932160,5999576213880832,6225627334574080,5176429250084864,4781278222614528,5171155086868480,4712823941758976,4766149676892160,5051455449858048,6737463265984512,5181049356156928,5532949163278336,6055881869885440,6064812579618816,6379382766043136,6100870457458688,4636696007147520,6743382868099072,4844961340588032,6493940143357952,5686836275970048,4738445887930368,4830764062474240,4998379591958528,5505022585470976,5735647168430080,6110778021642240,6191961686933504,4779842317844480,5757417202647040,5885424818978816,5020653416611840,6235654039011328,5130889045475328,5869612513099776,6382023663943680,6608337154605056,4565939092193280,6076815643770880] as Set<Long>
Set<Long> userIds = [6624148512571392] as Set<Long>

def shouldUpdate = { Entity it ->
    String[] parts = it.key.name?.split('_')
    def res = false
    if (parts && parts.length == 2) {
        res = userIds.contains(Long.parseLong(parts[1]))
    }
    res = res && it.getProperty('personalOfferId')?.equals(1203L)
    res
}
def update = { Entity it ->
    it.setUnindexedProperty('paymentOptionS', pos)
    it
}
def mc = null

def res = new DatastoreUtil('PersonalOfferTrack').setShouldUpdate(shouldUpdate).setUpdateEntity(update).setMemcacheKeyProvider(mc).execute()
'OK : ' + res + ', cnt ' + cnt