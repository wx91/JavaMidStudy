package com.wangx.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LastJob {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://node2:8020");
		config.set("yarn.resourcemanager.hostname", "node3");
		try {
			FileSystem fs = FileSystem.get(config);

			Job job = Job.getInstance(config);
			job.setJarByClass(LastJob.class);
			job.setJobName("weibo3");
			// 把微博总数加载到内存中
			job.addCacheFile(new Path("/user/tfidf/ouput/weibo1/part-r-00003").toUri());

			// 把df加载到内存
			job.addCacheFile(new Path("/user/tfidf/output/weibo2/part-r-00000").toUri());

			// 设置map任务的输出key类型，value类型\
			job.setMapperClass(LastMapper.class);
			job.setReducerClass(LastReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// MR运行时输入数据从hdfs的那个沐目录获取
			FileInputFormat.addInputPath(job, new Path("/user/tfidf/output/weibo1"));
			Path outpath = new Path("/user/tfidf/output/weibo3");
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);
			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("weibo1 job success");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
