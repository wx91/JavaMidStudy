package com.wangx.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaTestProducer {

	public static final String topic = "test";

	public static void main(String[] args) throws Exception {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "192.168.217.130:2181");
		properties.put("serialzer.class", "");
		properties.put("metadata.broker.list", "192.168.217.130:9092");
		properties.put("reqeust.required.acks", "1");
		Producer<Integer, String> producer = new KafkaProducer<Integer, String>(properties);
		for (int i = 0; i < 10; i++) {
			producer.send(new ProducerRecord<Integer, String>(topic, "hello kafka" + 1));
			System.out.println("send message : " + "hello kafka" + i);
		}
		producer.close();
	}

}
