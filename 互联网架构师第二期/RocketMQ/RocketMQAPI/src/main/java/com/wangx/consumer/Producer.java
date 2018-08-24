package com.wangx.consumer;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

public class Producer {
	public static void main(String[] args) throws MQClientException, InterruptedException {
		String group_name = "pull_producer";
		DefaultMQProducer producer = new DefaultMQProducer(group_name);
		producer.setNamesrvAddr("192.168.217.130:9876;192.168.217.131:9876");
		producer.start();
		for (int i = 0; i < 5; i++) {
			try {
				Message msg = new Message("TopicPull", "TagA", ("Hello RocketMQ " + i).getBytes());
				SendResult sendResult = producer.send(msg);
				System.out.println(sendResult);
				Thread.sleep(1000);
			} catch (Exception e) {
				e.printStackTrace();
				Thread.sleep(3000);
			}
		}
		producer.shutdown();
	}

}
