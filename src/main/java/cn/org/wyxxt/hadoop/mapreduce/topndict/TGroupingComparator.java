package cn.org.wyxxt.hadoop.mapreduce.topndict;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author xingzhiwei
 * @createBy IntelliJ IDEA
 * @time 2021/6/1 下午2:11
 * @email jsjxzw@163.com
 */
public class TGroupingComparator extends WritableComparator {

    public TGroupingComparator() {
        super(TKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TKey k1 = (TKey) a;
        TKey k2 = (TKey) b;
        // 按年 月 分组
        int c1 = Integer.compare(k1.getYear(), k2.getYear());
        if (c1 == 0) {
            return Integer.compare(k1.getMonth(), k2.getMonth());
        }
        return c1;
    }
}
