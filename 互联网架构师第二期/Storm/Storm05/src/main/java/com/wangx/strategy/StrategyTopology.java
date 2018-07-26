package com.wangx.strategy;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class StrategyTopology {

	public static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();
		// 设定数据源
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sub"), // 声明输入的域字段为“sub”
				4, // 设置批处理大小为4
					// 设置数据源 测试数据
				new Values("java"), new Values("python"), new Values("php"), new Values("c++"), new Values("ruby"));
		// 指定是否循环
		spout.setCycle(true);
		// 指定输入源spout
		Stream inputStream = topology.newStream("spout", spout);
		inputStream
				// 随机分组：shuffle
				// .shuffle()
				// 分区分组
				// .partitionBy(new Fields("sub"))
				// 全局分组
				// .global()
				// 广播分组
				// .broadcast()
				.each(new Fields("sub"), new WriteFunction(), new Fields()).parallelismHint(4);
		// 利用这种方式，我们返回一个StormTopology对象就行提交。
		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		Config conf = new  Config();
		conf.setNumWorkers(2);
		conf.setMaxSpoutPending(20);
		if (args.length==0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("trident-strategy", conf, buildTopology());
			Thread.sleep(5000);
			cluster.shutdown();
		}else {
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}

}
