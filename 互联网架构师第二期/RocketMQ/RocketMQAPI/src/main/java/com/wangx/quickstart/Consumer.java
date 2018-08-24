package com.wangx.quickstart;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

public class Consumer {
	public static void main(String[] args) throws Exception {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("quickstart_consumer");
		consumer.setNamesrvAddr("192.168.217.130:9876");
		/**
		 * 设置Consuner第一次启动是从头部开始消费还是队列尾部开始消费 如果非第一次启动， 那么按照上次消费的位置进行消费
		 */
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		//消费线程最小数量：默认10
		consumer.setConsumeThreadMin(10);
		//消费线程池最大数量：默认20
		consumer.setConsumeThreadMax(20);
		//拉消息本地队列缓存消息最大数：默认1000
		consumer.setPullThresholdForQueue(1000);
		//批量消费，一次消费多少条消息：默认1条
		consumer.setConsumeMessageBatchMaxSize(1);
		//批量拉消息，一次最多拉多少条，默认32
		consumer.setPullBatchSize(32);
		//消息拉取线程每隔多久拉取一次，默认为0
		consumer.setPullInterval(0);
		
		consumer.subscribe("TopicQuickStart", "*");
		consumer.registerMessageListener(new MessageListenerConcurrently() {
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				// System.out.println(Thread.currentThread().getName() + "Receive New Message："
				// + msgs);
				MessageExt message = msgs.get(0);
				try {
					for (MessageExt msg : msgs) {
						String topic = msg.getTopic();
						String msgBody = new String(msg.getBody(), "UTF-8");
						String tags = msg.getTags();
						System.out.println("接受消息：" + "topic：" + topic + "tags：" + tags + "msg：" + msgBody);
						// String orignMsgId
						// msg.getProperties().get(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
						if ("Hello RocketMQ 4".equals(msgBody)) {
							System.out.println("失败消息开始");
							System.out.println(msg);
							System.out.println(msgBody);
							System.out.println("失败消息结束");
							// 异常
							int a = 1 / 0;
						}
					}
				} catch (Exception e) {
					if (message.getReconsumeTimes() == 2) {
						System.out.println("重试2次以后，还是没有成功，记录日志操作");
						return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
					}
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});
		consumer.start();
		System.out.println("Consumer Started.");
	}

}
