package com.wangx.example;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import kafka.Kafka;

public class KakfaTestConsumer {

	public static final String topic = "test";

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zookeeper.connect", "192.168.217.130:2181");
		// group 代表一个消费组
		props.put("group.id", "group");
		// zk连接超时
		props.put("zookeeper.session.timeout.ms", "4000");
		props.put("zookeeper.sync.timeo.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset,reset", "smallest");
		// 序列化
		props.put("serializer,class", "kafka.serializer.StringEncoder");

		ConsumerConfig config = new ConsumerConfig(props);
	}

}
