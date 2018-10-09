package com.wangx.zklock;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLock {
	private static final Logger LOG = LoggerFactory.getLogger(TestLock.class);

	private static final int THREAD_NUM = 10;

	public static CountDownLatch threadSemaphore = new CountDownLatch(THREAD_NUM);

	public static void main(String[] args) {
		for (int i = 0; i < THREAD_NUM; i++) {
			final int threadId = i;
			new Thread() {
				public void run() {
					try {
						new LockService().doService(new DoTemplate() {
							public void dodo() {
								LOG.info("我要修改一个文件...." + threadId);
							}
						});
					} catch (Exception e) {
						LOG.error("【第" + threadId + "个线程】抛出异常");
						e.printStackTrace();
					}
				}
			}.start();
		}
		try {
			threadSemaphore.await();
			LOG.info("所有线程运行结束！");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
