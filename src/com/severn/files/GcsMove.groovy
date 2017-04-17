import java.util.logging.*

import com.google.appengine.tools.cloudstorage.*
import com.severn.script.utils.GcsUtils;

// def FROM_BUCKET = 'gambino-slots-events'
// def TO_BUCKET   = 'gambino-slots-events-restore' 
def FROM_BUCKET = 'gambino-slots-events-restore' 
def TO_BUCKET   = 'gambino-slots-events' 
def TABLE       = ['spins' + '/' + '2017/03', 'tiers' + '/' + '2017/03', 'raw_transactions_list' + '/' + '2017/03']

def ignore_prefixes = [] //['spins', 'gifts']
def total_count = 0, limit = 200, ts = System.currentTimeMillis()
def res = [:]
Logger logger = Logger.getLogger('com.severn')

TABLE.each { table ->
    def count = 0
    GcsService service = GcsUtils.getRetryGcsService()
    ListOptions options = new ListOptions.Builder().setPrefix(table).setRecursive(true).build();
    ListResult files = service.list(FROM_BUCKET, options);

    while (files.hasNext()) {
        ListItem it = files.next()
        
        if (ignore_prefixes) {
            for (String prefix : ignore_prefixes) {
                if (it.name.startsWith(prefix)) {
                    continue;
                }
            }
        }
        
        GcsFilename initialFile = new GcsFilename(FROM_BUCKET, it.getName());
        GcsFilename newFile = new GcsFilename(TO_BUCKET, it.getName());
        logger.fine("About ot copy from $initialFile to $newFile")
        service.copy(initialFile, newFile);
        service.delete(initialFile);
        
        count++    
        total_count++
        
        if (limit && count >= limit) {
            break
        }
    }
    res[table] = count
}

'OK : ' + res + '. Tool ' + (System.currentTimeMillis() - ts) + ' ms.'