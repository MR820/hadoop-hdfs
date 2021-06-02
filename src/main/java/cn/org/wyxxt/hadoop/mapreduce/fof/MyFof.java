package cn.org.wyxxt.hadoop.mapreduce.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author xingzhiwei
 * @createBy IntelliJ IDEA
 * @time 2021/6/2 上午10:52
 * @email jsjxzw@163.com
 */
public class MyFof {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration(true);


        conf.set("mapreduce.app-submission.cross-platform", "true");


        String[] other = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf);
        job.setJarByClass(MyFof.class);
        job.setJobName("fof");

        job.setJar("/Users/xingzhiwei/java/hadoop/target/hadoop-1.0-SNAPSHOT.jar");

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
        job.setMapperClass(FMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        // reducetask
        // shuffle(不用管)
        // reduce
//        job.setNumReduceTasks(0);
        job.setReducerClass(FReducer.class);


        job.waitForCompletion(true);
    }
}
