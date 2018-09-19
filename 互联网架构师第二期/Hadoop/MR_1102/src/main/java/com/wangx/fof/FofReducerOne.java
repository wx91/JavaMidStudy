package com.wangx.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class FofReducerOne extends Reducer<Text, IntWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable,
			Reducer<Text, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		// 亲密度
		int sum = 0;
		boolean flag = true;
		for (IntWritable i : iterable) {
			if (i.get() == 0) {
				flag = false;
				break;
			}
			sum += i.get();
		}
		if (flag) {
			String msg = text.toString() + "-" + sum;
			context.write(new Text(msg), NullWritable.get());
		}
	}

}
