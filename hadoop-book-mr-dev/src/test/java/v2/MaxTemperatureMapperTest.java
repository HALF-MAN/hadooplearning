package v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * @author: zhangyachong1
 * @date: 2019-09-03
 * @description:
 */
public class MaxTemperatureMapperTest {
    static {
        System.load("D:/hadoop-3.1.2/bin/hadoop.dll");
    }
    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "file:///");
        conf.set("fs.defaultFS","hdfs://ebsdi-dev-master-19369.hadoop.jd.com:8020");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        Path input = new Path("input");
        Path output = new Path("output");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true);

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);

        int exitCode = driver.run(new String[] {
           input.toString(), output.toString()
        });
        System.out.println(exitCode == 0);
    }
}
