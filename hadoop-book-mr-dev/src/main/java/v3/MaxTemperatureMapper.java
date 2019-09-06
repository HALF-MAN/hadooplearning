package v3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import v2.NcdcRecordParser;

import java.io.IOException;

/**
 * @author: zhangyachong1
 * @date: 2019-09-02
 * @description:
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private NcdcRecordParser parser = new NcdcRecordParser();
    enum Temperature {
        OVER_100
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()) {
            int airTemperature = parser.getAirTemperature();
            if (airTemperature > 1000) {
                System.err.println("Temperature over 100 degree for in put: "+ value);
                context.setStatus("Detected possibly corrupt record:see logs.");
                context.getCounter(Temperature.OVER_100).increment(1);
            }
            context.write(new Text(parser.getYear()), new IntWritable(parser.getAirTemperature()));
        }
    }
}
