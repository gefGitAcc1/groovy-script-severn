import java.util.logging.*

import com.google.appengine.tools.cloudstorage.*
import com.severn.script.utils.GcsUtils;

def FROM_BUCKET = 'gambino-slots-events-restore'
// def TO_BUCKET   = 'wild-ride-stage-1-temp'
def TABLE       = 'spins' + '/'
// def TABLE       = 'gifts' + '/'
def count = 0
def res = [:]
Logger logger = Logger.getLogger('com.severn')

GcsService service = GcsUtils.getRetryGcsService()
// ListOptions options = new ListOptions.Builder().setPrefix(TABLE).setRecursive(true).build();
ListOptions options = new ListOptions.Builder().setRecursive(true).build();
ListResult files = service.list(FROM_BUCKET, options);

files.each { ListItem it ->
    def name = it.name
    def bucket = name.split('/')[0]
    
    def aCount = res[bucket] ? res[bucket] : 0
    res[bucket] = aCount + 1
    
    count++
}

'OK : ' + res + ', total ' + count