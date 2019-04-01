import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.FileInputStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;


public class ImageReadWrite {

    public static void main(String[] args) {
        try {
            /* get connected */
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://Master:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            /* get the list of files under target directory */
            String directoryName = "/home/sigrid/Downloads/non_cancer_subset00";
            File directory = new File(directoryName);
            String[] fileList = directory.list();
            /* build and open the unified file */
            String unifiedFileName = "hw1/ImageCluster/UnifiedFile";
            Path unifiedFile = new Path(unifiedFileName);
            FSDataOutputStream os = fs.create(unifiedFile);
            /* build and open the index file */
            String indexFileName = "/home/sigrid/Downloads/index";
            File indexFile = new File(indexFileName);
            BufferedWriter bfwt = new BufferedWriter(new FileWriter(indexFile));
            /* count bytes */
            long byteCount = 0;
            for (String fileName : fileList) {
                File file = new File(directoryName + "/" + fileName);
                /* read the content of the file */
                byte[] buff = readFile(file);
                /* write the content to the unified file */
                os.write(buff, 0, buff.length);
                /* update the index file */
                writeFile(bfwt, fileName, buff.length, byteCount);
                byteCount += buff.length;
            }
            System.out.println("Done");
            os.close();
            bfwt.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static byte[] readFile(File file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        byte[] buff = new byte[fis.available()];
        fis.read(buff);
        fis.close();
        //System.out.println("Read file: " + file.getName());
        return buff;
    }

    public static void writeFile(BufferedWriter bfwt, String fileName, int fileSize, long location) throws IOException {
        bfwt.write(fileName + "\t" + String.valueOf(fileSize) + "\t" + String.valueOf(location) + "\n");
    }
}
