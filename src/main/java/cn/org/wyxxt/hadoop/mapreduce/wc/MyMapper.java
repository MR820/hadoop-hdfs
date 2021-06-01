package cn.org.wyxxt.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author xingzhiwei
 * @createBy IntelliJ IDEA
 * @time 2021/6/1 上午9:59
 * @email jsjxzw@163.com
 */

/**
 * public class TokenCounterMapper extends Mapper&lt;Object, Text, Text, IntWritable&gt;{
 *
 * <p>private final static IntWritable one = new IntWritable(1); private Text word = new Text();
 *
 * <p>public void map(Object key, Text value, Context context) throws IOException,
 * InterruptedException { StringTokenizer itr = new StringTokenizer(value.toString()); while
 * (itr.hasMoreTokens()) { word.set(itr.nextToken()); context.write(word, one); } } }
 */
public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    // hadoop 框架中，它是一个分布式 数据 ： 序列化、反序列化
    // hadoop 有自己的一套可以序列化、反序列化类型
    // 或者自己开发类型，必须实现序列化、反序列化接口，实现比较器接口
    // 排序 -> 比较 这个世界上有2种顺序， 字典序、数值顺序

    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    //hello hadoop 1
    //hello hadoop 2
    //TextInputFormat
    //key 是每一行字符串自己第一个字节面向源文件的偏移量
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
