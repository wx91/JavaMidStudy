package com.wangx.wordcount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SubjectsSpout implements IBatchSpout {

	private int batchSize;

	private HashMap<Long, List<List<Object>>> batchesMap = new HashMap<Long, List<List<Object>>>();

	public SubjectsSpout(int batchSize) {
		this.batchSize = batchSize;
	}

	private static final Map<Integer, String> DATA_MAP = new HashMap<Integer, String>();

	static {
		DATA_MAP.put(0, "java java php ruby c++");
		DATA_MAP.put(1, "java python python python c++");
		DATA_MAP.put(2, "java java java java ruby");
		DATA_MAP.put(3, "c++ java ruby php c++");
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

	}

	public void emitBatch(long batchId, TridentCollector collector) {
		List<List<Object>> batches = new ArrayList<List<Object>>();
		for (int i = 0; i < this.batchSize; i++) {
			batches.add(new Values(DATA_MAP.get(i)));
		}
		System.out.println("batchId:" + batchId);
		this.batchesMap.put(batchId, batches);
		for (List<Object> list : batches) {
			collector.emit(list);
		}
	}

	public void nextTuple() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("subjects"));
	}

	public void open(Map conf, TopologyContext context) {

	}

	public void ack(long batchId) {
		System.out.println("batchId:" + batchId);
		this.batchesMap.remove(batchId);
	}

	public void close() {

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public Fields getOutputFields() {
		return new Fields("subjects");
	}

}