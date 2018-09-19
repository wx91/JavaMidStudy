package com.wangx.tq;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class TQMapper extends Mapper<LongWritable, Text, Weather, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Weather, IntWritable>.Context context)
			throws IOException, InterruptedException {
		try {
			// 拆分出日期和温度
			String[] strs = StringUtils.split(value.toString(), '\t');
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Calendar cal = Calendar.getInstance();
			cal.setTime(sdf.parse(strs[0]));
			Weather w = new Weather();
			w.setYear(cal.get(Calendar.YEAR));
			w.setMonth(cal.get(Calendar.MONTH) + 1);
			w.setDay(cal.get(Calendar.DAY_OF_MONTH));

			String wdstr = strs[1];
			int wd = Integer.parseInt(wdstr.substring(0, wdstr.lastIndexOf("c")));
			w.setWd(wd);
			context.write(w, new IntWritable(wd));
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
