package com.wangx.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCJob {
	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "root");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:8020");
		conf.set("hadoop.home.dir", "E:\\developer\\hadoop-2.8.5");
		conf.set("yarn.resourcemanager.hostname", "node3");

		Job job = Job.getInstance(conf);
		job.setJarByClass(WCJob.class);

		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setReducerClass(WCReducer.class);

		FileInputFormat.addInputPath(job, new Path("/wc/input/wc.txt"));

		Path outpath = new Path("/wc/output");
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
