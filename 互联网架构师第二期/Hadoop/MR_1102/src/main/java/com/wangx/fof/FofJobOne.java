package com.wangx.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FofJobOne {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node2:8020");
		conf.set("yarn.resourcemanager.hostname", "node3");

		Job job = Job.getInstance(conf);
		job.setJarByClass(Fof.class);

		job.setMapperClass(FofMapperOne.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setReducerClass(FofReducerOne.class);

		FileInputFormat.addInputPath(job, new Path("/fof/input/"));

		Path outpath = new Path("/fof/output1/");
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
