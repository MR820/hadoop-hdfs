package cn.org.wyxxt.hadoop.mapreduce.topn;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author xingzhiwei
 * @createBy IntelliJ IDEA
 * @time 2021/6/1 下午2:06
 * @email jsjxzw@163.com
 */
public class TMapper extends Mapper<LongWritable, Text, TKey, IntWritable> {

    // 应为map可能被调起多次，定义在外边减少gc。同时，你要知道，源码中看到了，
    // map输出的key，value，是会发生序列化，变成字节数组进入buffer
    TKey mkey = new TKey();
    IntWritable mval = new IntWritable();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    Calendar cal = Calendar.getInstance();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 开发习惯，不要过于自信
        //value 2019-6-1 22:22:22   1   31
        String[] strs = StringUtils.split(value.toString(), '\t');
        //2019-6-1 22:22:22  / 1  / 31

        try {
            Date date = sdf.parse(strs[0]);
            cal.setTime(date);
            mkey.setYear(cal.get(Calendar.YEAR));
            mkey.setMonth(cal.get(Calendar.MONDAY) + 1);
            mkey.setDay(cal.get(Calendar.DAY_OF_MONTH));
            int wd = Integer.parseInt(strs[2]);
            mkey.setWd(wd);
            mval.set(wd);

            context.write(mkey, mval);

        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
