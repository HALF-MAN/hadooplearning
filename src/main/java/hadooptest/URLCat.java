package hadooptest;

import com.sun.jdi.PathSearchingVirtualMachine;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

/**
 * @author: zhangyachong1
 * @date: 2019-08-28
 * @description:
 */
public class URLCat {
    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws Exception{
        InputStream in = null;
        String uri = "hdfs://localhost:9000/user/zhangyachong/test";
        try {
            in = new URL(uri).openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
