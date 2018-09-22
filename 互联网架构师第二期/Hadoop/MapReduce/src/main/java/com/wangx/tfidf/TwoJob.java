package com.wangx.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoJob {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://node2:8020");
		config.set("yarn.resourcemanager.hostname", "node3");
		try {
			FileSystem fs = FileSystem.get(config);

			Job job = Job.getInstance(config);
			job.setJarByClass(TwoJob.class);
			job.setJobName("weibo2");
			//设置Map任务的输出key类型，value类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setMapperClass(TwoMapper.class);
			job.setCombinerClass(TwoReduce.class);
			job.setReducerClass(TwoReduce.class);
			
			// MR运行时的输入数据从hdfs的那个目录中获取
			FileInputFormat.addInputPath(job, new Path("/user/tfidf/output/weibo1"));
			FileOutputFormat.setOutputPath(job,new Path("/user/tfidf/output/weibo2"));
			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("weibo1 job success");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
