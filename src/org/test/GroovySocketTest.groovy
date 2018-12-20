package org.test

import com.severn.common.client.SocketClientImpl

SocketClientImpl client = new SocketClientImpl('130.211.189.110', 9014)

long now = System.currentTimeMillis()
def name = "User ${UUID.randomUUID()}"
println name

(1..10).each { it ->
    def s = "user:{\"c\":1,\"l\":1,\"t\":1,\"u\":1,\"sc\":${now + it},\"n\":\"${name}\",\"s\":\"123123abc1\",\"ut\":3,\"fm\":1,\"bc\":1}"
    println 'sending : ' + s
    client.send(s)

    Thread.sleep(5 * 60 * 1000L)
}

//client.close()
'OK'

int sleep(int time) {
    int res = 0;
    (1..time).each { it ->
        res += it
    }
    res
}