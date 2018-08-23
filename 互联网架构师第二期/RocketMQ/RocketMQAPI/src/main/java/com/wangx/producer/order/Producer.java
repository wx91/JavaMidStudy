package com.wangx.producer.order;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;

/**
 * Producer,发送顺序消息 RocketMQ的顺序消息需要满足2点：
 *  1.Producer端保证发送消息有序，且发送到同一队列。
 * 2.Consumer端保证消费同一队列。
 * 同一客户端消费
 */
public class Producer {
	public static void main(String[] args) throws Exception {
		try {
			String group_name = "order_producer";
			DefaultMQProducer producer = new DefaultMQProducer(group_name);
			producer.setNamesrvAddr("192.168.217.130:9876;192.168.217.131:9876");
			producer.start();
			// String[] tags = new String[] { "TagA", "TagC", "TagD" };

			Date date = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String dateStr = sdf.format(date);
			for (int i = 1; i <= 5; i++) {
				// 时间戳
				String body = dateStr + "order_1 " + i;
				// 参数 topic tag message
				Message msg = new Message("TopicTest", "order_1", "KEY" + i, body.getBytes());
				// 发送数据：如果使用顺序消费，则必须自己实现MessageQueueSelector,保证消息进入同一个队列中区
				SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
					public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
						Integer id = (Integer) arg;
						return mqs.get(id);
					}
				}, 0);// 0是队列的下标
				System.out.println(sendResult + ", body：" + body);
			}
			for (int i = 1; i <= 5; i++) {
				// 时间戳
				String body = dateStr + "order2 " + i;
				// 参数 topic tag message
				Message msg = new Message("TopicTest", "order_2", "KEY" + i, body.getBytes());
				// 发送数据：如果使用顺序消费，则必须自己实现MessageQueueSelector,保证消息进入同一个队列中区
				SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
					public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
						Integer id = (Integer) arg;
						return mqs.get(id);
					}
				}, 1);// 0是队列的下标
				System.out.println(sendResult + ", body：" + body);
			}
			for (int i = 1; i <= 5; i++) {
				// 时间戳
				String body = dateStr + "order3 " + i;
				// 参数 topic tag message
				Message msg = new Message("TopicTest", "order_3", "KEY" + i, body.getBytes());
				// 发送数据：如果使用顺序消费，则必须自己实现MessageQueueSelector,保证消息进入同一个队列中区
				SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
					public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
						Integer id = (Integer) arg;
						return mqs.get(id);
					}
				}, 2);// 0是队列的下标
				System.out.println(sendResult + ", body：" + body);
			}
			producer.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.in.read();
	}
}
