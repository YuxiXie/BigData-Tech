import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.IOException;
import java.util.Random;


public class FileCluster {
    public static void main(String [] args) {
        try {
            /* connect */
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://Master:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            /* build new files in target directory */
            String directoryName = "hw1/FileCluster/";
            int fileNum = 1000;
            for (int i = 1; i <= fileNum; i++) {
                String fileName = directoryName + "file" + String.valueOf(i);
                Path file = new Path(fileName);
                createFile(fs, file);
            }
            fs.close();
        } catch(Exception e) {
            /* print error info if fail */
            e.printStackTrace();
        }
    }

    public static void createFile(FileSystem fs, Path file) throws IOException {
        /* generate different contents for different files */
        String content = "This is the file named " + file.getName() + ". \n";
        content += "The following is something shown randomly: \n";
        String[] sentenceStock = {"In question generation, we can condition our encoder on two different sources of information (compared to the single source in neural machine translation (NMT)): a document that the question should be about and an answer that should fit the generated question.",
                                   "When formulating questions based on documents, it is common to refer to phrases and entities that appear directly in the text.",
                                   "Our model operates on QA datasets where the answer is extractive; thus, we encode the answer A using the annotation vectors corresponding to the answer word positions in the document.",
                                   "At every time-step t, the model computes a soft-alignment score over the document to decide which words are more relevant to the question being generated.",
                                   "As is standard in NMT, during decoding we use a beam search (Graves, 2012) to maximize (approximately) the conditional probability of an output sequence."};
        Random r = new Random();
        int index = r.nextInt(5);
        content += sentenceStock[index] + "\n";
        /* write files */
        byte[] buff = content.getBytes();
        FSDataOutputStream os = fs.create(file);
        os.write(buff, 0, buff.length);
        System.out.println("Create: " + file.getName());
        os.close();
    }
}
