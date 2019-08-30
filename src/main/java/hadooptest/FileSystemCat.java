package hadooptest;


import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * @author: zhangyachong1
 * @date: 2019-08-28
 * @description:
 */
public class FileSystemCat {
    public static void main(String[] args) {
        String uri = "hdfs://localhost:9000/user/zhangyachong/test";
        Configuration conf = new Configuration();
        InputStream in = null;
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            FileSystem check = new RawLocalFileSystem();
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (IOException e) {
            IOUtils.closeStream(in);
            e.printStackTrace();
        }
    }
}
