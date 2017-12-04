import java.nio.charset.Charset

import org.springframework.util.StreamUtils

import com.google.gson.Gson

File file = new File('d:/temp/friends.json')
def json = StreamUtils.copyToString(new FileInputStream(file), Charset.forName('UTF-8'))

Frnds friends = new Gson().fromJson(json, Frnds.class)

String str = friends.items.collect { it.socialId }.toString()
println str

"OK ${str}".toString()

class Frnds {
    Frnd[] items
}

class Frnd {
    String socialId
}