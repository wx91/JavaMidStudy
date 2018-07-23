package com.wangx.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.wangx.bolt.WordCountBolt;
import com.wangx.bolt.WordReportBolt;
import com.wangx.bolt.WordSplitBolt;
import com.wangx.spout.WordSpout;
import com.wangx.util.Utils;

public class WordTopology {

	// 定义常量
	private static final String WORD_SPOUT_ID = "word-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String REPORT_BOLT_ID = "report-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";

	public static void main(String[] args) {
		// 实例化对象
		WordSpout spout = new WordSpout();
		WordSplitBolt splitBolt = new WordSplitBolt();
		WordCountBolt countBolt = new WordCountBolt();
		WordReportBolt reportBolt = new WordReportBolt();

		// 构建拓扑
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(WORD_SPOUT_ID, spout);
		// WordSpout --> wordSplitBolt
		builder.setBolt(SPLIT_BOLT_ID, splitBolt, 5).shuffleGrouping(WORD_SPOUT_ID);

		// wordSplitBolt --> WordCountBolt
		builder.setBolt(COUNT_BOLT_ID, countBolt, 5).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));

		// WordCountBolt -- > Word ReportBot
		builder.setBolt(REPORT_BOLT_ID, reportBolt, 10).globalGrouping(COUNT_BOLT_ID);

		// 本地配置
		Config config = new Config();
		config.setDebug(false);
		LocalCluster cluster = new LocalCluster();

		cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
		Utils.waitForSeconds(10);
		cluster.killTopology(TOPOLOGY_NAME);
		cluster.shutdown();

	}

}
