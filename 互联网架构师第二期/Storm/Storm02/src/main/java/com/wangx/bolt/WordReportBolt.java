package com.wangx.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class WordReportBolt extends BaseRichBolt {

	private HashMap<String, Long> counts = null;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.counts = new HashMap<String, Long>();
	}

	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = tuple.getLongByField("count");
		this.counts.put(word, count);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public void cleanup() {
		System.out.println("-----------------Final Counts-------------------");
		List<String> keys = new ArrayList<String>();
		keys.addAll(this.counts.keySet());
		for (String key : keys) {
			System.out.println(key + ":" + this.counts.get(key));
		}
	}
}
