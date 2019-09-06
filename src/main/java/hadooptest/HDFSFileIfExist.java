package hadooptest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;

import java.io.*;

/**
 * @author: zhangyachong1
 * @date: 2019-04-19
 * @description:
 */
public class HDFSFileIfExist {
    static {
//        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.7");
//        System.setProperty("HADOOP_USER_NAME", "mart_ebs");
        System.setProperty("HADOOP_USER_NAME", "zhangyachong");
    }
    public static void main(String[] args) throws IOException {
//        testExist("/user/zhangyachong/v20190318.pdf");
        testCreateFile();
//          testGetFile();
//            deleteFile();
//        chown();
    }
    public static void testExist(final String fileName) {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://ebsdi-dev-master-19369.hadoop.jd.com:8020");
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            if(fs.exists(new Path(fileName))){
                System.out.println("文件存在");
            }else{
                System.out.println("文件不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void testCreateFile () {
        FileSystem fs = null;
        FSDataOutputStream os = null;
        try {
            Configuration conf = new Configuration();
//            conf.set("fs.defaultFS","hdfs://localhost:9000");
            conf.set("fs.defaultFS","hdfs://ebsdi-dev-master-19369.hadoop.jd.com:8020");
            conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
            fs = FileSystem.get(conf);
            // 要写入的内容
            byte[] buff = "Hello world".getBytes();
            //要写入的文件名
            String filename = "/user/zhangyachong/input/010230-99999-1951.gz";
            Path src = new Path("D:/010230-99999-1951.gz");
            Path dst = new Path(filename);
            fs.copyFromLocalFile(src, dst);
            fs.delete(new Path("output"),true);
//            os = fs.create(new Path(filename));
//            os.write(buff,0,buff.length);
//            System.out.println("Create:"+ filename);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                os.close();
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void testGetFile () {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS","hdfs://ebsdi-dev-master-19369.hadoop.jd.com:8020");
//            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            Path file = new Path("/user/zhangyachong/test");
            FSDataInputStream getIt = fs.open(file);
            BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
            //读取文件一行
            String content = d.readLine();
            System.out.println(content);
            //关闭文件
            d.close();
            //关闭hdfs
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void deleteFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://ebsdi-dev-master-19369.hadoop.jd.com:8020");
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/zhangyachong/oozie");
        fs.delete(path, true);
    }

    public static void chown() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://ebsdi-dev-master-19369.hadoop.jd.com:8020");
        conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/zhangyachong");
        FileStatus status = fs.getFileStatus(path);
        fs.setOwner(path,"zhangyachong", "supergroup");
    }
}
