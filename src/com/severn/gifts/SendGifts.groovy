import java.util.logging.*

import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.springframework.context.*

import com.severn.gifts.dao.impl.*
import com.severn.gifts.domain.Gift
import com.severn.gifts.domain.ListGiftsParams;
import com.severn.script.utils.GcsUtils;

ApplicationContext ctx = binding.variables.get('applicationContext')
BaseGiftDAO giftDao = ctx.getBean('coinsGiftDAO')
def reciever = '1686418115004362'; // nastya
//def reciever = '1797518277167446'; // me

def expire = '2017-03-18T11:37:34.562Z', count = 0

LocalDateTime currentDateTimeInUserTimeZone = LocalDateTime.now()

(1..300).each { it ->
    Gift gift = new Gift();
    gift.setValue(1)
    gift.setSenderName("Seryezha_$it")
    gift.setSender(String.valueOf(it))
    gift.setReceiver(reciever)
    gift.setGiftType('coins')
    gift.setSentDateTimeInUserTimeZone(currentDateTimeInUserTimeZone.toString())
    gift.setExpireDateTimeUTC(expire)

    giftDao.saveGift(gift)
    count++
}

'OK : ' + count