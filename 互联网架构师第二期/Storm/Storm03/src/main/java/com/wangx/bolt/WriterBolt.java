package com.wangx.bolt;

import java.io.FileWriter;
import java.util.Map;

import org.apache.storm.shade.org.apache.http.impl.conn.Wire;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WriterBolt extends BaseRichBolt {

	private boolean flag = false;

	private FileWriter writer;

	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			writer = new FileWriter("E://message.txt");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple tuple) {
		String word = tuple.getString(0);
		try {
//			if (!flag && word.equals("hadoop")) {
//				flag = true;
//				int a = 1 / 0;
//			}

			writer.write(word);
			writer.write("\r\n");
			writer.flush();
		} catch (Exception e) {
			e.printStackTrace();
			collector.fail(tuple);
		}
		collector.emit(tuple, new Values(word));
		collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
