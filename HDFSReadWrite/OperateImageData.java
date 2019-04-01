import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.RandomAccessFile;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.File;
import java.math.BigInteger;


public class OperateImageData {
    public static void main(String[] args) {
        try {
        	String fileName = "normal1.ndpi.16.5702_35104.2048x2048.tiff";
            /* insert image info */
            insertImage(fileName);
            /* get image info */
            readImage(fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void insertImage(String imageName) throws IOException {
    	/* get connected */
        FileSystem fs = connect();
        /* get image info */
        String imagePath = "/home/sigrid/Downloads/" + imageName;
        FileInputStream fis = new FileInputStream(new File(imagePath));
        byte[] buff = new byte[fis.available()];
        fis.read(buff);
        fis.close();
        /* reach the end of the unified file */
        String unifiedFileName = "hw1/ImageCluster/UnifiedFile";
        Path unifiedFile = new Path(unifiedFileName);
        FSDataOutputStream os = fs.append(unifiedFile);
        /* write into unified file */
        os.write(buff, 0, buff.length);
        os.close();
        fs.close();
        /* reach the end of the index file */
        String indexFileName = "/home/sigrid/Downloads/index";
        File indexFile = new File(indexFileName);
        BigInteger location = getSize(indexFile);
        /* update the index file */
        File image = new File(imagePath);
        long imageSize = image.length();
        BufferedWriter writer = new BufferedWriter(new FileWriter(indexFile, true));
        writer.write(imageName + "\t" + String.valueOf(imageSize) + "\t" + String.valueOf(location) + "\n");
        writer.close();
    }

    public static void readImage(String imageName) throws IOException {
    	/* search in the index file */
        String indexFileName = "/home/sigrid/Downloads/index";
        BufferedReader bfrd = new BufferedReader(new InputStreamReader(new FileInputStream(indexFileName)));
        /* get target file info */
        String line = "";
        long[] size = {0, 0};
        while ((line = bfrd.readLine()) != null) {
            String[] list = line.split("\t");
            String name = list[0];
            if (name.equals(imageName)) {
                size[0] = Long.parseLong(list[1]);
                size[1] = Long.parseLong(list[2]);
                break;
            }
        }
        bfrd.close();
        /* if no such file exists */
        if (size[0] == 0) {
            System.out.println("No file named " + imageName + " exists. ");
            return;
        }
        /* get connected */
        FileSystem fs = connect();
        /* search in unified file and get image info */
        String unifiedFileName = "hw1/ImageCluster/UnifiedFile";
        Path unifiedFile = new Path(unifiedFileName);
        FSDataInputStream in = fs.open(unifiedFile);
        in.seek(size[1]);
        byte[] buff = new byte[(int)size[0]];
        in.read(size[1], buff, 0, (int)size[0]);
        in.close();
        fs.close();
        /* write image info to local file */
        String localFileName = "/home/sigrid/Downloads/local.tiff";
        FileOutputStream fos = new FileOutputStream(new File(localFileName));
        fos.write(buff);
        fos.close();
    }

    public static BigInteger getSize(File file) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        long len = raf.length();
        long pos = len - 1;
        while (pos > 0) {
            pos--;
            raf.seek(pos);
            if (raf.readByte() == '\n') {
                break;
            }
        }
        if (pos == 0) {
            raf.seek(0);
        }
        byte[] bytes = new byte[(int) (len - pos)];
        raf.read(bytes);
        raf.close();
        String line = new String(bytes);
        String[] list = line.trim().split("\t");
        BigInteger a = new BigInteger(list[1]);
        BigInteger b = new BigInteger(list[2]);
        BigInteger size = a.add(b);
        return size;
    }

    public static FileSystem connect() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://Master:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.support.append", "true");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        FileSystem fs = FileSystem.get(conf);
        return fs;
    }
}
