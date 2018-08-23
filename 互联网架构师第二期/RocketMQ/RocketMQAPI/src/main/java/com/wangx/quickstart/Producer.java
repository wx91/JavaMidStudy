package com.wangx.quickstart;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer {
	public static void main(String[] args) throws Exception {
		DefaultMQProducer producer = new DefaultMQProducer("quickstart_producer");
		producer.setNamesrvAddr("192.168.217.130:9876");
		producer.start();
		for (int i = 0; i < 100; i++) {
			try {// topic tag body
				Message msg = new Message("TopicQuickStart", "TagA", ("Hello RocketMQ " + i).getBytes());
				SendResult sendResult = producer.send(msg);
				// SendResult sendResult = producer.send(msg,1000);
				System.out.println(sendResult);
			} catch (Exception e) {
				e.printStackTrace();
				Thread.sleep(1000);
			}
		}
		producer.shutdown();
	}

}
