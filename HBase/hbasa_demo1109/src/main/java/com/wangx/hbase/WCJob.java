package com.wangx.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.w3c.dom.Text;

public class WCJob {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node2:8020");
		conf.set("yarn.resourcemanager.hostname", "node3");
		conf.set("hbase.zookeeper.quorum", "node1,node2,node3");

		Job job = Job.getInstance(conf);
		job.setJarByClass(WCJob.class);

		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("/wc/input/wc"));

		// hbase reducer
		TableMapReduceUtil.initTableReducerJob("wc", // reducer 输出到哪一个表
				WCTableReduce.class, // reducer class
				job);
		boolean flag = job.waitForCompletion(true);
		if (flag) {
			System.out.println("Job Sucess");
		}

	}

}
