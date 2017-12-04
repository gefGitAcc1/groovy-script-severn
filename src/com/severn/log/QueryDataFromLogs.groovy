import com.google.appengine.tools.cloudstorage.*
import com.google.appengine.api.log.*
import com.google.appengine.api.log.LogQuery.Version
import org.joda.time.*
import org.joda.time.format.*

import com.google.appengine.api.log.LogService.LogLevel
import org.springframework.http.MediaType
import java.nio.channels.Channels

def bucket = 'gambino-slots-events-temp'
def moduleVersions = ['043']
def moduleNames = ['slots','default']
def startTime = '2017-07-11 00:00:00'
def endTime = '2017-07-11 17:37:58'
def tables = ['spins'] as Set<String>

HashSet<String> paths = pathes()
// impls
def format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC()
def startTs = format.parseDateTime(startTime).getMillis() * 1000
def endTs = format.parseDateTime(endTime).getMillis() * 1000

GcsService service = GcsServiceFactory.createGcsService()

def writers = tables.collectEntries { table ->
    def fileName = "events_${table}_from_${startTime}_to_${endTime}_${System.currentTimeMillis()}.log"
    def gcsFileOptions = new GcsFileOptions.Builder().mimeType(MediaType.TEXT_PLAIN_VALUE).build()
    GcsFilename file = new GcsFilename(bucket, fileName);
    def writer = Channels.newWriter(service.createOrReplace(file, gcsFileOptions), "UTF-8")

    [(table): writer] 
}

def count = 0

LogService logService = LogServiceFactory.getLogService();

def versions = []
moduleVersions.each { moduleVersion ->
    moduleNames.each { moduleName ->
        versions << new Version(moduleName, moduleVersion)
    }
}

LogQuery query = new LogQuery()
    .batchSize(100)
    .minLogLevel(LogLevel.INFO)
    .versions(versions)
    .includeAppLogs(true);
query.startTimeUsec(startTs);
query.endTimeUsec(endTs);

logger.fine("Query startTs=$startTs, endTs=$endTs, version=${query.versions}")

logService.fetch(query).each {
    // count++
    if (paths.contains(it.getResource().split("\\?")[0])) {
        // logger.fine("got ${it.getResource()}")
        it.getAppLogLines().each {
            def allLogMessage = it.getLogMessage()
            if (allLogMessage.startsWith('dataEvent#')) {
                // logger.fine("found $allLogMessage")
                int splitPosition = allLogMessage.indexOf(':');
                String logEntryKey = allLogMessage.substring(0, splitPosition);
                logEntryKey = logEntryKey.trim().substring('dataEvent#'.length());

                if (tables.contains(logEntryKey)) {
                    def writer = writers[logEntryKey]
                    String data = allLogMessage.substring(splitPosition + 2); // : + space
                    writer.write(data)
                    count++
                }
            }
        }
    }
}

writers.values().each { writer ->
    writer.close()
}

'DONE. Found ' + count + ' log entries for '

def config() {
    [:]
}

HashSet<String> pathes() {
    return [
        '/_ah/spi/com.severn.game.service.enpoints.SlotsServiceEndpoint.spin',
        '/_ah/spi/com.severn.game.service.enpoints.SlotsServiceEndpoint.pickBonus',
        '/_ah/spi/com.severn.game.service.enpoints.SlotsServiceEndpoint.pickScatter',
        '/_ah/spi/com.severn.game.service.enpoints.SlotsServiceEndpoint.startMachine',
        '/_ah/spi/com.severn.game.service.enpoints.SlotsServiceEndpoint.startModularMachine',
        '/_ah/spi/com.severn.game.service.enpoints.SlotsServiceEndpoint.modularSpin',
        '/_ah/spi/com.severn.game.service.enpoints.SlotsServiceEndpoint.tutorialSpin',
        '/sendCompensation',
        '/kingOfSlotWorker',
        '/jackpotRewardWorker',
        '/_ah/spi/com.severn.payments.endpoint.PaymentServiceEndpoint.finishPurchase',
        '/_ah/spi/com.severn.payments.endpoint.PaymentServiceEndpoint.startPurchase',
        '/_ah/spi/com.severn.payments.endpoint.PaymentServiceEndpoint.trackPersonalOffer',
        '/_ah/spi/com.severn.payments.endpoint.PaymentServiceEndpoint.cancelPurchase',
        '/_ah/spi/com.severn.payments.endpoint.PaymentServiceEndpoint.collectContinuesPurchase',
        '/_ah/spi/com.severn.payments.endpoint.PaymentServiceEndpoint.getContinuesPurchaseCard',
        '/_ah/spi/com.severn.payments.endpoint.PaymentServiceEndpoint.increaseContinuesPurchaseGoal',
        '/_ah/spi/com.severn.auth.service.endpoint.AuthorizationServiceEndpoint.authorizeDevice',
        '/_ah/spi/com.severn.auth.service.endpoint.AuthorizationServiceEndpoint.authorizeWeb',
        '/_ah/spi/com.severn.auth.service.endpoint.AuthorizationServiceEndpoint.authorizeByKey',
        '/_ah/spi/com.severn.auth.service.endpoint.AuthorizationServiceEndpoint.authorize',
        '/_ah/spi/com.severn.auth.service.endpoint.AuthorizationServiceEndpoint.trackInstallSource',
        '/_ah/spi/com.severn.gifts.endpoint.GiftServiceEndpoint.sendFreeCoinsGift',
        '/_ah/spi/com.severn.gifts.endpoint.GiftServiceEndpoint.collectFreeCoinsGiftById',
        '/_ah/spi/com.severn.gifts.endpoint.GiftServiceEndpoint.collectGiftsById',
        '/_ah/spi/com.severn.gifts.endpoint.GiftServiceEndpoint.collectCompensationsById',
        '/_ah/spi/com.severn.gifts.endpoint.GiftServiceEndpoint.collectSocialGift',
        '/_ah/spi/com.severn.gifts.endpoint.GiftServiceEndpoint.sendGift',
        '/_ah/spi/com.severn.gifts.endpoint.GiftServiceEndpoint.sendCoinsGift',
        '/_ah/spi/com.severn.gifts.endpoint.GiftServiceEndpoint.collectAndSendBackGifts',
        '/_ah/spi/com.severn.bonuses.endpoint.BonusServiceEndpoint.playDailyBonusGame',
        '/_ah/spi/com.severn.bonuses.endpoint.BonusServiceEndpoint.collectProgressiveBonus',
        '/_ah/spi/com.severn.bonuses.endpoint.BonusServiceEndpoint.collectSocialBonus',
        '/_ah/spi/com.severn.bonuses.endpoint.BonusServiceEndpoint.collectInvitationBonus',
        '/_ah/spi/com.severn.bonuses.endpoint.BonusServiceEndpoint.collectSharingBonus',
        '/_ah/spi/com.severn.bonuses.endpoint.BonusServiceEndpoint.collectFriendJoinedGameReward',
        '/_ah/spi/com.severn.bonuses.endpoint.BonusServiceEndpoint.playDailyBonusWheel',
        '/_ah/spi/com.severn.contests.endpoint.ContestsServiceEndpoint.getContestResult',
        '/_ah/spi/com.severn.contests.endpoint.ContestsServiceEndpoint.getContestHistory',
        '/_ah/spi/com.severn.challenge.endpoint.ChallengeServiceEndpoint.getChallengeConfig',
        '/_ah/spi/com.severn.challenge.endpoint.ChallengeServiceEndpoint.getChallengeProgress',
        '/_ah/spi/com.severn.challenge.endpoint.ChallengeServiceEndpoint.rewardChallenge',
        '/_ah/spi/com.severn.challenge.endpoint.ChallengeServiceEndpoint.rewardChallengeTask',
        '/_ah/spi/com.severn.challenge.endpoint.ChallengeServiceEndpoint.trackClientTask',
        '/_ah/spi/com.severn.challenge.endpoint.ChallengeServiceEndpoint.skipCurrentTask',
        '/_ah/spi/com.severn.challenge.v2.endpoint.ChallengeServiceEndpointV2.getChallengeConfig',
        '/_ah/spi/com.severn.challenge.v2.endpoint.ChallengeServiceEndpointV2.getChallengeProgress',
        '/_ah/spi/com.severn.challenge.v2.endpoint.ChallengeServiceEndpointV2.rewardChallenge',
        '/_ah/spi/com.severn.challenge.v2.endpoint.ChallengeServiceEndpointV2.rewardChallengeTask',
        '/_ah/spi/com.severn.challenge.v2.endpoint.ChallengeServiceEndpointV2.trackClientTask',
        '/_ah/spi/com.severn.challenge.v2.endpoint.ChallengeServiceEndpointV2.skipCurrentTask',
        '/_ah/spi/com.severn.challenge.v2.endpoint.ChallengeServiceEndpointV2.startNewChallenge',
        '/_ah/spi/com.severn.inapp.endpoint.InAppServiceEndpoint.collectAdViewed',
        '/_ah/spi/com.severn.user.endpoint.UserServiceEndpoint.trackInvitedUsers',
        '/_ah/spi/com.severn.user.endpoint.UserServiceEndpoint.surveyStart',
        '/_ah/spi/com.severn.user.endpoint.UserServiceEndpoint.surveyAnswer',
        '/_ah/spi/com.severn.user.endpoint.UserServiceEndpoint.surveyStartRating',
        '/expiredSessionsWorker',
        '/http-events',
        '/contestRewarder',
        '/payments/fbPaymentCallback',
        '/publicGift',
        '/multiPushBroadcast',
        '/massUpdate',
        '/subscriptionsValidationWorker'
    ] as HashSet<String>
}