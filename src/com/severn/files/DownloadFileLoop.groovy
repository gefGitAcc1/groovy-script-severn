package com.severn.files

import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.client.HttpClients
import org.springframework.util.StreamUtils

import com.google.common.io.Resources

def times = 1_000_000
def metricEach = 50
def newClientForEachDownload = true

def protocol = 'http'
def url = 'static.stage1.gambinoslot.com'
//def url = 'storage.googleapis.com/stage-1'

//def resources = (1..13).collect { it ->
////    "android/en/1080/1/game${it}/game${it}.zip"
//    "android/en/1080/1/game${it}/slot/paytable.zip"
//}

//println "Got resources: ${resources}"

def resources = [
//    'test1/kot.jpg',
//    'test1/kot-rare-get-cache.jpg?attempt=1',
//    'android/en/1080/1/game1/game1.zip',
//    'fucking_cursor.png',
//    'browser/assets/en/low/games/game9/newgame/5969fa55fcbbb02a5c94b80bcad45ba9.atlas.rgba8888.webp',
//    'android/en/1080/1/game1/slot/machine.zip',
//    'android/en/1080/1/game1/slot/bg_mobile.zip',
//    'android/en/1080/1/game1/slot/end_freespins.zip',
    'android/en/1080/1/game1/slot/logo.zip',
//    'android/en/1080/1/game1/slot/machine.zip',
//    'android/en/1080/1/game1/slot/paytable.zip',
//    'android/en/1080/1/game1/slot/start_freespins.zip'
]

RequestConfig requestConfig = RequestConfig.custom()
    .setConnectionRequestTimeout(15_000)
    .setConnectTimeout(15_000)
    .setSocketTimeout(15_000)
        .build();

long now = System.currentTimeMillis(), nowInt = 0

CloseableHttpClient client
if (!newClientForEachDownload) {
    client = HttpClients.createDefault()
}
try {
    (1..times).each { it ->
        if (newClientForEachDownload) {
            client = HttpClients.createDefault()
        }

        String file = resources[it % resources.size]
        String resource = "${protocol}://${url}/${file}"

        HttpGet get = new HttpGet(resource)
        get.config = requestConfig

        try {
            CloseableHttpResponse response = client.execute(get)
    
            if (response.statusLine.statusCode != 200) {
                println "Resource ${resource}. Response ${response}"
            }
        
            String fname = file.split('(\\/|\\?)')[1]
            File f = new File("f:/temp/file_stream_test/${fname}")
            f.createNewFile()
    
            def os = new FileOutputStream(f)
            StreamUtils.copy(response.entity.content, os)
    
            os.close()
            response.close()
        } catch (IOException ioe) {
            println "Got exception ${ioe}"
        }
    
        if (it % 17 == 0) {
            println "Downloading ${it}/${times}. ${resource}"
        }

        if ((it-1) % metricEach == 0) {
            if (nowInt) {
                println "DOWNLOADED last ${metricEach} for ${System.currentTimeMillis() - nowInt} ms. ${(System.currentTimeMillis() - nowInt)/metricEach} ms/file"
            }
            nowInt = System.currentTimeMillis()
        }

        if (newClientForEachDownload) {
            client.close()
        }

        Thread.sleep(500)
    }
} finally {
    if (null != client) {
        client.close()
    }
}

println "Done in ${System.currentTimeMillis() - now} ms."

'OK'