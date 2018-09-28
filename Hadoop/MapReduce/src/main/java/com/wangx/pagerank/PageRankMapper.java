package com.wangx.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		int runCount = context.getConfiguration().getInt("runCount", 1);
		// A B C D
		String page = key.toString();
		Node node = null;
		if (runCount == 1) {// 第一次计算 初始值PR值为1.0
			node = Node.fromMR("1.0" + "\t" + value.toString());
		} else {
			node = Node.fromMR(value.toString());
		}
		// A:1.0 B D
		// 将计算前的数据输出reduce计算做差值
		context.write(new Text(page), new Text(node.toString()));
		if (node.containsAdjacentNodes()) {
			double outValue = node.getPageRank()/node.getAdjacentNodeNames().length;
			for (int i = 0; i < node.getAdjacentNodeNames().length; i++) {
				String outPage = node.getAdjacentNodeNames()[i];
				// B:.0.5 D:0.5
				context.write(new Text(outPage), new Text(outValue+""));
			}
		}
	}
}
