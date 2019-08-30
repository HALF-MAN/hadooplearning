package hadooptest;

import com.sun.org.apache.regexp.internal.RE;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Waitable;

import javax.sound.midi.Soundbank;
import java.io.*;

/**
 * @author: zhangyachong1
 * @date: 2019-08-29
 * @description:
 */
public class WritableTest {
    public static void main(String[] args) throws IOException {
//        IntWritable writable = new IntWritable();
//        writable.set(163);
//        IntWritable newWritable = new IntWritable();
//        deserialize(newWritable, serialize(writable));
//        System.out.println(StringUtils.byteToHexString(serialize(writable)));
//        System.out.println(newWritable.get());

        //利用比较器直接比较数据流中的记录
//        IntWritable w1 = new IntWritable(163);
//        IntWritable w2 = new IntWritable(67);
//        RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);
//        byte[] b1 = serialize(w1);
//        byte[] b2 = serialize(w2);
//        System.out.println(b1.length);
//        System.out.println(b2.length);
//        System.out.println(comparator.compare(b1, 0 , b1.length, b2, 0, b2.length));
//        text();
//        stringTextComparion();
//        getLengthAndgetBytes();
//        bytesWritable();
        mapWritable();
    }
    public static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(out);
        writable.write(dataOut);
        dataOut.close();
        return out.toByteArray();
    }
    public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream dataIn = new DataInputStream(in);
        writable.readFields(dataIn);
        dataIn.close();
        return bytes;
    }
    public static void text() {
        Text t = new Text("hadoop");
        System.out.println(t.getLength());
        System.out.println(t.getBytes().length);
        System.out.println(t.charAt(1));
        System.out.println(t.charAt(100));
        System.out.println(t.find("do"));
        System.out.println(t.find("do", 3));
    }
    public static void stringTextComparion() {
        String s = "\u0041\u00DF\u6771\uD801\udc00";
        String a = "我爱中国";
        System.out.println(s.length());
        System.out.println(s.getBytes().length);
        System.out.println(s.indexOf("\u0041"));
        System.out.println(s.indexOf("\uD801\udc00"));
        System.out.println(s.charAt(0) == '\u0041');
        System.out.println(s.codePointAt(3)== 0x10400);
        System.out.println(s.charAt(3));
        System.out.println(s.codePointAt(3));

        Text t = new Text("\u0041\u00DF\u6771\uD801\udc00");
        System.out.println("Text:"+ t.getLength());
        System.out.println("Text:"+ t.find("\u0041"));
        System.out.println("Text:"+ t.find("\uD801\udc00"));

        System.out.println("Text:"+ t.charAt(0));
        System.out.println("Text:"+ t.charAt(1));
        System.out.println("Text:"+ t.charAt(3));
        System.out.println("Text:"+ (t.charAt(3) == 0x6771));

    }

    public static void getLengthAndgetBytes() {

        Text t = new Text("hadoop");
        t.set("pig");
        System.out.println(t);
        System.out.println(t.getLength());
        System.out.println(t.getBytes().length);
    }

    public static void bytesWritable() throws IOException {
        BytesWritable b = new BytesWritable(new byte[] {3, 5});
        byte[] bytes = serialize(b);
        System.out.println(StringUtils.byteToHexString(bytes).equals("000000020305"));

        b.setCapacity(11);
        System.out.println(b.getLength());
        System.out.println(b.getBytes().length);
        System.out.println(b.getSize());
    }
    public static void NullWritable() {
        NullWritable.get();
    }

    public static void mapWritable() throws IOException {
        MapWritable src = new MapWritable();
        src.put(new IntWritable(1), new Text("cat"));
        src.put(new VIntWritable(2), new LongWritable(163));

        MapWritable dest = new MapWritable();
        WritableUtils.cloneInto(dest, src);
        System.out.println(new Text("cat").equals(dest.get(new IntWritable(1))));
    }
}
