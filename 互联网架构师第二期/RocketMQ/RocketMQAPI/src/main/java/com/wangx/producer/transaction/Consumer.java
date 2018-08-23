package com.wangx.producer.transaction;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;

public class Consumer {

	public Consumer() {
		try {
			String group_name = "transaction_consumer";
			DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
			consumer.setNamesrvAddr("192.168.217.130:9876;192.168.217.131:9876");
			// consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
			consumer.subscribe("TopicTransaction", "*");
			consumer.registerMessageListener(new Listener());
			consumer.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	class Listener implements MessageListenerConcurrently {
		public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
			try {
				for (MessageExt msg : msgs) {
					String topic = msg.getTopic();
					String msgBody = new String(msg.getBody(), "UTF-8");
					String tags = msg.getTags();
					System.out.println("接受消息：" + "topic：" + topic + "tags：" + tags + "msg：" + msgBody);
					// String orignMsgId =
					// msg.getProperties().get(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
				}
			} catch (Exception e) {
				e.printStackTrace();
				return ConsumeConcurrentlyStatus.RECONSUME_LATER;
			}
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		}
	}

	public static void main(String[] args) throws Exception {
		Consumer c = new Consumer();
		System.out.println("transaction consumer start...");
	}

}
