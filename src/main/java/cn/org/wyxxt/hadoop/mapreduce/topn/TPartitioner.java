package cn.org.wyxxt.hadoop.mapreduce.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author xingzhiwei
 * @createBy IntelliJ IDEA
 * @time 2021/6/1 下午2:06
 * @email jsjxzw@163.com
 */
public class TPartitioner extends Partitioner<TKey, IntWritable> {
    @Override
    public int getPartition(TKey key, IntWritable value, int numPartitions) {
        // 1、不能太复杂。。。
        // partitioner 按年，月分区 -》 分区 > 分组   可按 年分区～！！！
        // 分区器的潜台词：满足 相同的key获得相同的分区号就可以～！
        return key.getYear() % numPartitions; // 数据倾斜
    }
}
