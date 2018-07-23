package com.wangx.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SpliterBolt extends BaseRichBolt {

	private OutputCollector collector;

	private boolean flag = false;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple tuple) {
		try {
			String subjects = tuple.getStringByField("subjects");

//			if (!flag && subjects.equals("flume,activiti")) {
//				flag = true;
//				int a = 1 / 0;
//			}

			String[] words = subjects.split(",");
			for (String word : words) {
				collector.emit(tuple, new Values(word));
			}
			collector.ack(tuple);
		} catch (Exception e) {
			e.printStackTrace();
			collector.fail(tuple);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
