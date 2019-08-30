package hadooptest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * @author: zhangyachong1
 * @date: 2019-08-28
 * @description:
 */
public class FileCopyWithProgress {
    public static void main(String[] args) throws Exception{
        String localSrc = "C:\\Users\\zhangyachong\\Desktop\\hadoop重点章节.txt";
        String dst = "hdfs://localhost:9000/user/zhangyachong/hadoop重点章节.txt";
        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
