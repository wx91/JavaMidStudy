package com.wangx.itemcf;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 按照推荐得分降序排列，每个用户列出10个推荐物品
 *
 */
public class Step6 {
	private final static Text K = new Text();
	private final static Text V = new Text();

	public static boolean run(Configuration config, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);

			job.setJobName("step6");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step6_Mapper.class);
			job.setReducerClass(Step6_Reducer.class);
			job.setSortComparatorClass(NumSort.class);
			job.setGroupingComparatorClass(UserGroup.class);

			job.setMapOutputKeyClass(PairWritable.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(job,
					new Path[] { new Path(paths.get("Step6Input1")), new Path(paths.get("Step4Input2")) });

			Path outpath = new Path(paths.get("Step6Output"));
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			boolean f = job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	static class Step6_Mapper extends Mapper<LongWritable, Text, PairWritable, Text> {
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, PairWritable, Text>.Context context)
				throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());
			String u = tokens[0];
			String item = tokens[1];
			String num = tokens[2];
			PairWritable k = new PairWritable();
			k.setUid(u);
			k.setNum(Double.parseDouble(num));
			V.set(item + ":" + num);
			context.write(k, V);
		}
	}

	static class Step6_Reducer extends Reducer<PairWritable, Text, Text, Text> {
		@Override
		protected void reduce(PairWritable key, Iterable<Text> iterable,
				Reducer<PairWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			int i = 0;
			StringBuffer sb = new StringBuffer();
			for (Text v : iterable) {
				if (i == 10) {
					break;
				}
				sb.append(v.toString() + ",");
				i++;
			}
			K.set(key.getUid());
			V.set(sb.toString());
			context.write(K, V);
		}
	}

}
