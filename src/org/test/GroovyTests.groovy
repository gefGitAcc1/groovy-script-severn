import java.text.SimpleDateFormat

double d1 = 1.1081933274379854E19 * 2.46E+12 / 2.5E+12
double d2 = 1.1081933274379854E19 / 2.5E+12 * 2.46E+12

println d1
println d2

def table = ['bonuses', 'bonuses20']
//prln("Replace by ${replacer}", 'piyy')

println table.groupBy { it -> it.split('[0-9]')[0] }

println "Formatting ${new SimpleDateFormat('YYYYMMdd').format(new Date())}"

println "${1000L * 60 * 60 * 24 * 7}"

'OK'

void prln(def nstring, def replacer) {
    println nstring
}

Cfg config() {
    return [k:'v', k1:'v1']
}

class Cfg {
    String k, k1;
}