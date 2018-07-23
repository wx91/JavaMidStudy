package com.wangx.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class MessageSpout extends BaseRichSpout {

	private int index;

	private SpoutOutputCollector collector;

	private String[] subjects = new String[] { "groovg,oeacnbase", "openfire,restful", "flume,activiti", "hadoop,hbase",
			"spark,sqoop" };

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void nextTuple() {
		if (index < subjects.length) {
			String sub = subjects[index];
			// 发送消息参数1为数值，参数2为msgId
			collector.emit(new Values(sub), index);
			index++;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("subjects"));
	}

	public void ack(Object msgId) {
		System.out.println("【消息发送成功！！！】(msgId=" + msgId + ")");
	}

	public void fail(Object msgId) {
		System.out.println("【消息发送失败！！！】(msgId=" + msgId + ")");
		System.out.println("【消息进行中】");
		collector.emit(new Values(subjects[(Integer) msgId]), msgId);
		System.out.println("【重发成功！！！】");
	}

}
