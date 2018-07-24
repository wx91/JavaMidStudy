package com.wangx.drpc1;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class BaseDRPCTopology {

	public static class ExclaimBolt extends BaseBasicBolt {

		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String input = tuple.getString(1);
			System.out.println("=========" + tuple.getValue(0));
			collector.emit(new Values(tuple.getValue(0), input + "!"));
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "result"));
		}
	}

	public static void main(String[] args) throws Exception {
		// 创建drpc实例
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
		// 添加bolt
		builder.addBolt(new ExclaimBolt(), 3);
		Config config = new Config();
		if (args == null || args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("drpc-demo", config, builder.createLocalTopology(drpc));
			for (String word : new String[] { "hello", "goodbye" }) {
				System.out.println("Result for\"" + word + "\":" + drpc.execute("exclamation", word) + "");
			}
			cluster.shutdown();
			drpc.shutdown();
		} else {
			config.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], config, builder.createRemoteTopology());
		}

	}

}
