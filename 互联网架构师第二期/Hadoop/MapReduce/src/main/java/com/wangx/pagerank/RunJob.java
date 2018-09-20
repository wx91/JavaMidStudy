package com.wangx.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {
	public static void main(String[] args) {

		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://node2:8020");
		config.set("yarn.resourcemanager.hostname", "node3");

		// 收敛值
		double d = 0.001;
		int i = 0;
		while (true) {
			i++;
			try {
				// 记录计算次数
				config.setInt("runCount'", i);

				FileSystem fs = FileSystem.get(config);
				Job job = Job.getInstance(config);

				job.setJarByClass(RunJob.class);
				job.setJobName("pr" + i);

				job.setMapperClass(PageRankMapper.class);
				job.setReducerClass(PageRankReduce.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				job.setInputFormatClass(KeyValueTextInputFormat.class);

				Path inputPath = new Path("/user/pagerank/input/pagerank.txt");
				if (i > 1) {
					inputPath = new Path("/user/pagerank/output/pr" + (i - 1));
				}
				FileInputFormat.addInputPath(job, inputPath);

				Path outPath = new Path("/user/pagerank/output/pr" + i);
				if (fs.exists(outPath)) {
					fs.delete(outPath, true);
				}
				FileOutputFormat.setOutputPath(job, outPath);
				boolean f = job.waitForCompletion(true);
				if (f) {
					System.out.println("Success Job.");
					long sum = job.getCounters().findCounter(MyCounter.my).getValue();
					System.out.println("SUM:" + sum);
					double avgd = sum / 4000.0;
					if (avgd < d) {
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
