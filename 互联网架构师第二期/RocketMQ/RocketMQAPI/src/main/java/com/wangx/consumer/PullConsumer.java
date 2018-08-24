package com.wangx.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

public class PullConsumer {

	// Map<key,value> key为指定的队列，value为这个队列拉取数据的最后位置
	private static final Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();

	public static void main(String[] args) throws Exception {
		String group_name = "pull_consumer";
		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(group_name);
		consumer.setNamesrvAddr("192.168.217.130:9876;192.168.217.131:9876");
		consumer.start();
		// 从TopicTest这个主题去获取所有的队列
		Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest");
		// 遍历每一个队列，继续拉取数据
		for (MessageQueue mq : mqs) {
			System.out.println("Consumer from the queue " + mq);
			SINGLE_MQ: while (true) {
				try {
					// 从queue中获取数据，从什么位置开始拉取数据，单次最多拉取32条记录
					PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
					System.out.println(pullResult);
					System.out.println(pullResult.getPullStatus());
					System.out.println();
					pullMessageQueueOffset(mq, pullResult.getNextBeginOffset());
					switch (pullResult.getPullStatus()) {
					case FOUND:
						List<MessageExt> list = pullResult.getMsgFoundList();
						for (MessageExt msg : list) {
							System.out.println(new String(msg.getBody()));
						}
						break;
					case NO_MATCHED_MSG:
						break;
					case NO_NEW_MSG:
						System.out.println("没有新的数据啦...");
					case OFFSET_ILLEGAL:
						break;
					default:
						break;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static void pullMessageQueueOffset(MessageQueue mq, long offset) {
		offsetTable.put(mq, offset);
	}

	private static long getMessageQueueOffset(MessageQueue mq) {
		Long offset = offsetTable.get(mq);
		if (offset != null) {
			return offset;
		}
		return 0;
	}

}
