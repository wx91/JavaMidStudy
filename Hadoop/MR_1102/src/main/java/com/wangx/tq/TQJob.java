package com.wangx.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TQJob {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node2:8020");
		conf.set("yarn.resourcemanager.hostname", "node3");

		Job job = Job.getInstance(conf);
		job.setJarByClass(TQJob.class);

		job.setMapperClass(TQMapper.class);
		job.setMapOutputKeyClass(Weather.class);
		job.setOutputValueClass(IntWritable.class);

		job.setReducerClass(TQReducer.class);

		job.setPartitionerClass(TQPartition.class);
		job.setSortComparatorClass(TQSort.class);
		job.setGroupingComparatorClass(TQGroup.class);

		job.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job, new Path("/weather/input/tq"));

		Path outpath = new Path("/weather/output/");
		FileSystem fs = FileSystem.get(conf);

		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		}

		FileOutputFormat.setOutputPath(job, outpath);
		boolean flag = job.waitForCompletion(true);
		if (flag) {
			System.out.println("Job Sucess");
		}

	}
}
