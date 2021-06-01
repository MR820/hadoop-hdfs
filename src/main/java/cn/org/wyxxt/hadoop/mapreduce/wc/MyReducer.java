package cn.org.wyxxt.hadoop.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author xingzhiwei
 * @createBy IntelliJ IDEA
 * @time 2021/6/1 上午9:59
 * @email jsjxzw@163.com
 */

/**
 * public class IntSumReducer&lt;Key&gt; extends Reducer&lt;Key,IntWritable,
 * Key,IntWritable&gt; {
 * private IntWritable result = new IntWritable();
 * <p>
 * public void reduce(Key key, Iterable&lt;IntWritable&gt; values,
 * Context context) throws IOException, InterruptedException {
 * int sum = 0;
 * for (IntWritable val : values) {
 * sum += val.get();
 * }
 * result.set(sum);
 * context.write(key, result);
 * }
 * }
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    private IntWritable result = new IntWritable();

    // 相同的key为一组，这一组数据调用一次reduce
    // hello 1
    // hello 1
    // hello 1
    // hello 1
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
