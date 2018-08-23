package com.wangx.producer.transaction;

import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.common.message.Message;

public class TransactionExecuterImpl implements LocalTransactionExecuter {

	private AtomicInteger transactionIndex = new AtomicInteger(1);

	public LocalTransactionState executeLocalTransactionBranch(Message msg, Object arg) {
		System.out.println("msg = " + new String(msg.getBody()));
		System.out.println("arg = " + arg);
		String tag = msg.getTags();
		if (tag.equals("Transaction3")) {
			// 这里有一个分阶段提交任务概念
			System.out.println("这里处理业务逻辑，比如操作数据库，失败情况下就ROLLBACK");
			return LocalTransactionState.ROLLBACK_MESSAGE;
		}
		return LocalTransactionState.COMMIT_MESSAGE;
		// return LocalTransactionState.UNKNOW;
	}

}
