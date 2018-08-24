package com.wangx.consumer;

import java.util.List;

import com.alibaba.rocketmq.client.consumer.MQPullConsumer;
import com.alibaba.rocketmq.client.consumer.MQPullConsumerScheduleService;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullTaskCallback;
import com.alibaba.rocketmq.client.consumer.PullTaskContext;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

public class PullScheduldeService {
	public static void main(String[] args) throws MQClientException {
		String group_name = "schedule_consumer";
		final MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService(group_name);
		scheduleService.getDefaultMQPullConsumer().setNamesrvAddr("192.168.217.130:9876;192.168.217.131:9876");
		scheduleService.setMessageModel(MessageModel.CLUSTERING);
		scheduleService.registerPullTaskCallback("TopicPull", new PullTaskCallback() {
			public void doPullTask(MessageQueue mq, PullTaskContext context) {
				MQPullConsumer consumer = context.getPullConsumer();
				try {
					// 获取从那里拉取
					long offset = consumer.fetchConsumeOffset(mq, false);
					if (offset < 0) {
						offset = 0;
					}
					PullResult pullResult = consumer.pull(mq, "*", offset, 32);
					switch (pullResult.getPullStatus()) {
					case FOUND:
						List<MessageExt> list = pullResult.getMsgFoundList();
						for (MessageExt msg : list) {
							// 消费数据
							System.out.println(new String(msg.getBody()));
						}
						break;
					case NO_MATCHED_MSG:
						break;
					case NO_NEW_MSG:
						break;
					case OFFSET_ILLEGAL:
						break;
					default:
						break;
					}
					// 存储Offset，客户端每隔5s会定时刷新到Broker
					consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
					// 设置100ms后重新拉取
					context.setPullNextDelayTimeMillis(1000);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		scheduleService.start();
	}
}
