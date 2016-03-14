import java.util.logging.*
import com.google.appengine.tools.cloudstorage.*

def FROM_BUCKET = 'wild-ride-app-viber-temp-restored-events'
def TO_BUCKET   = 'wild-ride-viber-app-events-success-archive-1'
def TABLE       = 'raw_sessions_end' + '/'// + '/2016/01/1'
def count = 0
Logger logger = Logger.getLogger('com.severn')

GcsService service = GcsServiceFactory.createGcsService();
ListOptions options = new ListOptions.Builder().setPrefix(TABLE).setRecursive(true).build();
ListResult files = service.list(FROM_BUCKET, options);

files.each {
    GcsFilename initialFile = new GcsFilename(FROM_BUCKET, it.getName());
    GcsFilename newFile = new GcsFilename(TO_BUCKET, it.getName());
    logger.fine("About ot copy from $initialFile to $newFile")
    service.copy(initialFile, newFile);
    service.delete(initialFile);
    count++
}

'OK : ' + count