package com.wangx.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReduce extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> iterable, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		double sum = 0.0;
		Node sourceNode = null;
		for (Text i : iterable) {
			Node node = Node.fromMR(i.toString());
			// A:1.0 B D
			if (node.containsAdjacentNodes()) {
				// 计算之前的数据 A:1.0 B D
				sourceNode = node;
			} else {
				// B:0.5 D:0.5
				sum = sum + node.getPageRank();
			}
		}
		// 计算新的PR值 4为页面总数
		double newPR = (0.15 / 4) + (0.85 * sum);
		System.out.println("******************* new pagerank value is " + newPR);

		// 把新的pr值和计算之前的pr值比较
		double d = newPR - sourceNode.getPageRank();
		int j = (int) (d * 1000.0);
		j = Math.abs(j);
		System.out.println(j + "____________");
		// 累加
		context.getCounter(MyCounter.my).increment(j);

		sourceNode.setPageRank(newPR);
		context.write(key, new Text(sourceNode.toString()));
	}

}
