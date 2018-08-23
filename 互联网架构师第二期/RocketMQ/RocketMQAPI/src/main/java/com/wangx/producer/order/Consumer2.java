package com.wangx.producer.order;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

public class Consumer2 {

	public Consumer2() throws Exception {
		String group_name = "order_consumer";
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
		consumer.setNamesrvAddr("192.168.217.130:9876;192.168.217.131:9876");
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		consumer.subscribe("TopicTest", "*");

		consumer.registerMessageListener(new Listener());
		consumer.start();
		System.out.println("C2 Started.");
	}

	class Listener implements MessageListenerOrderly {
		private Random random = new Random();

		public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
			// 设置自动提交
			context.setAutoCommit(true);
			System.out.println(Thread.currentThread().getName() + "Receive New Message");
			for (MessageExt msg : msgs) {
				System.out.println(msg + ", content" + new String(msg.getBody()));
			}
			try {
				// 模拟业务逻辑处理中...
				TimeUnit.SECONDS.sleep(random.nextInt());
			} catch (Exception e) {
				e.printStackTrace();
			}
			return ConsumeOrderlyStatus.SUCCESS;
		}
	}

	public static void main(String[] args) throws Exception {
		Consumer2 c = new Consumer2();
	}

}
