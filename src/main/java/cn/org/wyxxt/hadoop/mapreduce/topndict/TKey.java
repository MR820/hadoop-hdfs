package cn.org.wyxxt.hadoop.mapreduce.topndict;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author xingzhiwei
 * @createBy IntelliJ IDEA
 * @time 2021/6/1 下午2:06
 * @email jsjxzw@163.com
 */
// 自定义类型必须实现接口：序列化/反序列化  比较器
public class TKey implements WritableComparable<TKey> {
    private int year;
    private int month;
    private int day;
    private int wd;


    private String location;

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getWd() {
        return wd;
    }

    public void setWd(int wd) {
        this.wd = wd;
    }

    @Override
    public int compareTo(TKey that) {

        // 我们为了让这个案例体现api开发，所以下边的逻辑是一种通用的逻辑：按照时间正序，
        // 但是我们目前业务需要的是 年，月，温度，且温度倒叙。所以一会还得开发一个sortComparator。。。

        int c1 = Integer.compare(this.year, that.getYear());

        if (c1 == 0) {
            int c2 = Integer.compare(this.month, that.getMonth());
            if (c2 == 0) {
                return Integer.compare(this.day, that.getDay());
            }
            return c2;
        }
        return c1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(wd);
        out.writeUTF(location);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.wd = in.readInt();
        this.location = in.readUTF();
    }
}
