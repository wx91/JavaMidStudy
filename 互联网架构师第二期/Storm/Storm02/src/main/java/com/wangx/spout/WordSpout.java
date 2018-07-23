package com.wangx.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.wangx.util.Utils;

public class WordSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	private int index = 0;

	private String[] sentences = { "my dog has fleas", "i like cold beverages", "the dog ate my homework",
			"don't have a cow man", "i don't think i like fleas" };

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void nextTuple() {
		this.collector.emit(new Values(sentences[index]));
		index++;
		if (index >= sentences.length) {
			index = 0;
		}
		Utils.waitForSeconds(1);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
