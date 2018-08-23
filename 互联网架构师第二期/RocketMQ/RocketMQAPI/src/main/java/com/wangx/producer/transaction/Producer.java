package com.wangx.producer.transaction;

import java.util.concurrent.TimeUnit;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

public class Producer {

	public static void main(String[] args) throws MQClientException, InterruptedException {
		/**
		 * 一个应用创建一个Producer，由应用来维护此对象，可以设置为全局对象或者单例
		 * 注意：producerGroupName需要由应用来保证唯一，一类Producer集合的名称，这类Producer通常发送一类消息，且发送逻辑一致
		 * ProducerGroup这个概念发送普通的消息时，作用不大，但是发送分布式消息时，比较关键， 因为服务器会回查这个Group下的任意一个Producer
		 */
		String group_name = "transaction_producer";
		final TransactionMQProducer producer = new TransactionMQProducer(group_name);
		// nameserver服务
		producer.setNamesrvAddr("192.168.217.130:9876;192.168.217.131:9876");
		// 事务回查最小并发数
		producer.setCheckThreadPoolMinSize(5);
		// 事务回查最大并发数
		producer.setCheckThreadPoolMaxSize(20);
		// 队列数
		producer.setCheckRequestHoldMax(2000);
		/**
		 * Producer对象在使用之前需要调用start初始化，初始化一次即可 注意：切记不可以在每次发送消息时，都调用start方法
		 */
		producer.start();
		// 服务器回调Producer，检查本地事务成功还是失败
		producer.setTransactionCheckListener(new TransactionCheckListener() {
			public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
				System.out.println("state -- " + new String(msg.getBody()));
				return LocalTransactionState.COMMIT_MESSAGE;
			}
		});
		/**
		 * 下面这段代码表明一个Producer对象可以发送多个topic，多个tag信息
		 * 注意：send方法是同步调用，只有不抛异常标识成功，但是发送成功也可能会有多种状态
		 * 例如消息写入Master成功，但是Slave不成功，这种情况消息属于成功，但是对于个别应用如果消息可靠性要求很高
		 * 需要对这种情况处理，另外，可能会存在发送失败的情况，失败重试由应用处理
		 */
		TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl();
		for (int i = 1; i <= 1; i++) {
			try {
				Message msg = new Message("TopicTransaction", "Transaction" + i, "key",
						("Hello RocketMQ" + i).getBytes());
				SendResult sendResult = producer.sendMessageInTransaction(msg, tranExecuter, "tq");

			} catch (Exception e) {
				e.printStackTrace();
			}
			TimeUnit.MILLISECONDS.sleep(1000);
		}
		/**
		 * 应用退出时，要调用shutdown来清理支援，关闭网络连接，从MetaQ服务器上注销自己
		 * 
		 */
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				producer.shutdown();
			}
		}));
	}

}
