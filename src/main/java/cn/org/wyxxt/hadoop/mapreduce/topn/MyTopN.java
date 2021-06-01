package cn.org.wyxxt.hadoop.mapreduce.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author xingzhiwei
 * @createBy IntelliJ IDEA
 * @time 2021/6/1 上午11:58
 * @email jsjxzw@163.com
 */
public class MyTopN {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);


        conf.set("mapreduce.framework.name", "local");
        conf.set("mapreduce.app-submission.cross-platform", "true");


        String[] other = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf);
        job.setJarByClass(MyTopN.class);
        job.setJobName("TopN");

        // 初学者，关注的是client端的代码梳理：因为把这块写明白了，其实你也就真的知道这个作业的开发原理

        // maptask
        // input
        TextInputFormat.addInputPath(job, new Path(other[0]));
        Path outPath = new Path(other[1]);
        // 如果输出路径存在，则应该报错。不要删除数据，这里为了方便
        if (outPath.getFileSystem(conf).exists(outPath)) outPath.getFileSystem(conf).delete(outPath, true);
        TextOutputFormat.setOutputPath(job, outPath);

        // key
        // map
        job.setMapperClass(TMapper.class);
        job.setMapOutputKeyClass(TKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        // partitioner 按年，月分区 -》 分区 > 分组   可按 年分区～！！！
        // 分区器的潜台词：满足 相同的key获得相同的分区号就可以～！
        job.setPartitionerClass(TPartitioner.class);

        // sortComparator 年，月，温度 且温度倒叙
        job.setSortComparatorClass(TSortComparator.class);

        // combine
//        job.setCombinerClass();


        // reducetask
        // shuffle(不用管)
        // groupingComparator
        job.setGroupingComparatorClass(TGroupingComparator.class);
        // reduce
        job.setReducerClass(TReducer.class);


        job.waitForCompletion(true);
    }
}
