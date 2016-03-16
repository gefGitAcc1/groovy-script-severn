import java.io.BufferedReader;
import java.io.Writer;
import java.nio.channels.Channels;
import java.util.logging.*

import org.springframework.http.MediaType;

import static org.codehaus.groovy.runtime.DefaultGroovyMethodsSupport.*;

import com.google.appengine.tools.cloudstorage.*
import com.severn.script.utils.GcsUtils;

def FROM_BUCKET = 'wild-ride-app-temp-wc'
def TO_BUCKET   = 'wild-ride-app-temp'
def TABLE       = 'push_notification' + '/' + '2016/03'

def count = 0, NEW_LINE = '\n'
Logger logger = Logger.getLogger('com.severn')
GcsFileOptions gcsFileOptions = new GcsFileOptions.Builder().mimeType(MediaType.TEXT_PLAIN_VALUE).build()

GcsService gcsService = GcsUtils.getRetryGcsService()
ListOptions options = new ListOptions.Builder().setPrefix(TABLE).setRecursive(true).build();
ListResult files = gcsService.list(FROM_BUCKET, options);

def process = { String line ->
    def newLine = line.replaceAll(',null,', ',,').replaceAll('(,null|null,)', ',')
    newLine
}

files.each { ListItem it ->
    GcsFilename initialFile = new GcsFilename(FROM_BUCKET, it.getName());
    GcsFilename newFile = new GcsFilename(TO_BUCKET, it.getName() + '.restored');
    
    BufferedReader reader = new BufferedReader(Channels.newReader(gcsService.openReadChannel(initialFile, 0), "UTF-8"));
    Writer writer = Channels.newWriter(gcsService.createOrReplace(newFile, gcsFileOptions), "UTF-8");
    try {
        def line
        while (line = reader.readLine()) {
            def newLine = process(line)
            writer.write(newLine + NEW_LINE)
        }
    } finally {
        closeQuietly(reader)
        closeQuietly(writer)
    }
    gcsService.copy(initialFile, new GcsFilename(TO_BUCKET, it.getName()))
    gcsService.delete(initialFile)
    count++
}

'OK : ' + count