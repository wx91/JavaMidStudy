package com.wangx.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.wangx.tq.TQGroup;
import com.wangx.tq.TQJob;
import com.wangx.tq.TQMapper;
import com.wangx.tq.TQPartition;
import com.wangx.tq.TQReducer;
import com.wangx.tq.TQSort;
import com.wangx.tq.Weather;

public class FofJobTwo {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:8020");
		conf.set("yarn.resourcemanager.hostname", "node3");

		Job job = Job.getInstance(conf);
		job.setJarByClass(FofJobTwo.class);

		job.setMapperClass(FofMapperTwo.class);
		job.setMapOutputKeyClass(Friend.class);
		job.setOutputValueClass(IntWritable.class);

		job.setReducerClass(FofReducerTwo.class);
		job.setCombinerClass(FofReducerTwo.class);

		job.setSortComparatorClass(FofSort.class);
		job.setGroupingComparatorClass(FofGroup.class);


		FileInputFormat.addInputPath(job, new Path("/fof/output1"));

		Path outpath = new Path("/fof/output2/");
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
