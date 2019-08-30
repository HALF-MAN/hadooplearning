package hadooptest;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;



/**
 * @author: zhangyachong1
 * @date: 2019-08-29
 * @description:
 */
public class StreamCompressor {
    public static void main(String[] args) throws Exception {
        CompressionCodec codec = new GzipCodec();
        CompressionOutputStream out = codec.createOutputStream(System.out);
        IOUtils.copyBytes(System.in, out, 4096, false);
        out.finish();
    }
}
