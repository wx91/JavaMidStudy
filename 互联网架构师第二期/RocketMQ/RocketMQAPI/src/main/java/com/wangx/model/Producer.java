package com.wangx.model;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class Producer {
	public static void main(String[] args) throws Exception {
		String group_name = "message_producer";
		DefaultMQProducer producer = new DefaultMQProducer(group_name);
		producer.setNamesrvAddr("192.168.217.130:9876");
		producer.start();
		for (int i = 0; i < 100; i++) {
			try {
				Message msg = new Message("Topic1", "Tag1", ("信息内容" + i).getBytes());
				SendResult sendResult = producer.send(msg);
				System.out.println(sendResult);
			} catch (Exception e) {
				e.printStackTrace();
				Thread.sleep(1000);
			}
		}
		producer.shutdown();
	}

}
