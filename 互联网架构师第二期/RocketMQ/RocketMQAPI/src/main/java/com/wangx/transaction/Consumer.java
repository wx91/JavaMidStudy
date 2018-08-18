package com.wangx.transaction;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;

import com.sun.corba.se.pept.transport.EventHandler;

import sun.launcher.resources.launcher;

public class Consumer {
	public static void main(String[] args) throws Exception {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("quickstart_consumer");
		consumer.setNamesrvAddr("192.168.217.130:9876");
		/**
		 * 设置Consuner第一次启动是从头部开始消费还是队列尾部开始消费 如果非第一次启动， 那么按照上次消费的位置进行消费
		 */
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		consumer.subscribe("TopicQuickStart", "*");
		consumer.registerMessageListener(new MessageListenerConcurrently() {
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				// System.out.println(Thread.currentThread().getName() + "Receive New Message："
				// + msgs);
				try {
					for (MessageExt msg : msgs) {
						String topic = msg.getTopic();
						String msgBody = new String(msg.getBody(), "UTF-8");
						String tags = msg.getTags();
						System.out.println("接受消息：" + "topic：" + topic + "tags：" + tags + "msg：" + msgBody);
						// String orignMsgId
						// msg.getProperties().get(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
					}
				} catch (Exception e) {
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});
		consumer.start();
		System.out.println("Consumer Started.");
	}

}
