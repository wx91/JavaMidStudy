package com.wangx.model;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

public class Consumer1 {
	public static void main(String[] args) {
		try {
			String group_name = "message_consumer";
			DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
			consumer.setNamesrvAddr("192.168.217.130:9876");
			consumer.subscribe("Topic1", "Tag1||Tag2||Tag3");
			// 广播模式需要钱启动Consumer
			 consumer.setMessageModel(MessageModel.CLUSTERING);
			consumer.registerMessageListener(new MessageListenerConcurrently() {
				public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
						ConsumeConcurrentlyContext context) {
					try {
						for (MessageExt msg : msgs) {
							String topic = msg.getTopic();
							String msgBody = new String(msg.getBody(), "UTF-8");
							String tags = msg.getTags();
							System.out.println("接受消息：" + "topic：" + topic + "tags：" + tags + "msg：" + msgBody);
							// String orignMsgId = msg.getProperties().get(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
						}

					} catch (Exception e) {
						e.printStackTrace();
						return ConsumeConcurrentlyStatus.RECONSUME_LATER;
					}
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				}
			});
			consumer.start();
			System.out.println("Consumer1 start");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
