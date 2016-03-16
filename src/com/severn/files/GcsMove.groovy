import java.util.logging.*

import com.google.appengine.tools.cloudstorage.*
import com.severn.script.utils.GcsUtils;

def FROM_BUCKET = 'wild-ride-events-fail-archive-1'
def TO_BUCKET   = 'wild-ride-stage-1-temp'
def TABLE       = 'push_notification' + '/' + '2016/0'
def count = 0
Logger logger = Logger.getLogger('com.severn')

GcsService service = GcsUtils.getRetryGcsService()
ListOptions options = new ListOptions.Builder().setPrefix(TABLE).setRecursive(true).build();
ListResult files = service.list(FROM_BUCKET, options);

def willCopy = { ListItem it ->
    def name = it.getName()
    name.endsWith('.restored')
}

files.each { ListItem it ->
    if (willCopy(it)) {
        GcsFilename initialFile = new GcsFilename(FROM_BUCKET, it.getName());
        GcsFilename newFile = new GcsFilename(TO_BUCKET, it.getName());
        logger.fine("About ot copy from $initialFile to $newFile")
        service.copy(initialFile, newFile);
        service.delete(initialFile);
        count++
    }
}

'OK : ' + count