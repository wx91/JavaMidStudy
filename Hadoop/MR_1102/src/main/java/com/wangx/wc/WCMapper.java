package com.wangx.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

/**
 * 文本分析的例子，其中入参的Key为文本的行号，入参的value为文本数据，
 * 出参的key为单词，出参的value为统计数
 *
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String str = value.toString();
		String[] strs = StringUtils.split(str,' ');
		for (String s : strs) {
			context.write(new Text(s), new IntWritable(1));
		}
	}

}
