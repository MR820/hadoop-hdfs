package cn.org.wyxxt.hadoop.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author xingzhiwei
 * @createBy IntelliJ IDEA
 * @time 2021/6/1 上午9:24
 * @email jsjxzw@163.com
 */
public class MyWordCount {


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(true);
    Job job = Job.getInstance(conf);
    // 必须写的
    job.setJarByClass(MyWordCount.class);

    job.setJobName("xzw");

    Path infile = new Path("/data/wc/input");
    TextInputFormat.addInputPath(job, infile);

    Path outfile = new Path("/data/wc/output");
    if (outfile.getFileSystem(conf).exists(outfile)) {
      outfile.getFileSystem(conf).delete(outfile, true);
    }
    TextOutputFormat.setOutputPath(job, outfile);

    job.setMapperClass(MyMapper.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(MyReducer.class);

    // Submit the job, then poll for progress until the job is complete
    job.waitForCompletion(true);


  }

}
