package com.wangx.filter;

import java.io.UnsupportedEncodingException;
import java.util.List;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.MessageExt;

public class Consumer {
	public static void main(String[] args) throws MQClientException {
		String group_name = "filter_consumer";
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group_name);
		consumer.setNamesrvAddr("192.168.217.130:9876");
		// 使用Java代码，做服务器消息过滤
		String filterCode = MixAll.file2String("E:\\com\\wangx\\filter\\MessageFilterImpl.java");
		consumer.subscribe("TopicFilter7", "com.wangx.filter.MessageFilterImpl", filterCode);
//		consumer.subscribe("TopicFilter7", MessageFilterImpl.class.getCanonicalName());
		consumer.registerMessageListener(new MessageListenerConcurrently() {
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				MessageExt msg = msgs.get(0);
				try {
					System.out.println("收到信息：" + new String(msg.getBody(), "UTF-8"));
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});
		consumer.start();
		System.out.println("Consumer Started.");
	}

}
