println config().class

'OK'

Cfg config() {
    return [k:'v', k1:'v1']
}

class Cfg {
    String k, k1;
}