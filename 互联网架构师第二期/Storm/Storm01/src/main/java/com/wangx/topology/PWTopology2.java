package com.wangx.topology;

import com.wangx.bolt.PrintBolt;
import com.wangx.bolt.WriteBolt;
import com.wangx.spout.PWSpout;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class PWTopology2 {
	public static void main(String[] args) throws Exception {

		Config cfg = new Config();
		cfg.setNumWorkers(2);// 设置使用俩个工作进程
		cfg.setDebug(false);
		TopologyBuilder builder = new TopologyBuilder();
		// 设置sqout的并行度和任务数（产生2个执行器和俩个任务）
		builder.setSpout("spout", new PWSpout(), 2);// .setNumTasks(2);
		// 设置bolt的并行度和任务数:（产生2个执行器和4个任务）
		builder.setBolt("print-bolt", new PrintBolt(), 2).shuffleGrouping("spout").setNumTasks(4);
		// 设置bolt的并行度和任务数:（产生6个执行器和6个任务）
		builder.setBolt("write-bolt", new WriteBolt(), 6).shuffleGrouping("print-bolt");

		// 1 本地模式
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("top2", cfg, builder.createTopology());
//		Thread.sleep(10000);
//		cluster.killTopology("top2");
//		cluster.shutdown();

		// 2 集群模式
		StormSubmitter.submitTopology("top2", cfg, builder.createTopology());

	}
}
