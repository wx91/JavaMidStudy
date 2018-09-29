package com.wangx.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCTableReduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
	public static final byte[] CF = "cf".getBytes();
	public static final byte[] COUNT = "count".getBytes();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> iterable,
			Reducer<Text, IntWritable, ImmutableBytesWritable, Mutation>.Context context)
			throws IOException, InterruptedException {
		int i = 0;
		for (IntWritable val : iterable) {
			i += val.get();
		}
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.addColumn(CF, COUNT, Bytes.toBytes(i));
		context.write(null, put);
	}
}
