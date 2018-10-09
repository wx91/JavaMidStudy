package com.wangx.zklock;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockWatcher implements Watcher {
	private static final Logger LOG = LoggerFactory.getLogger(LockWatcher.class);
	private DistributedLock distributedLock;
	private DoTemplate doTemplate;

	public LockWatcher(DistributedLock distributedLock, DoTemplate doTemplate) {
		this.distributedLock = distributedLock;
		this.doTemplate = doTemplate;
	}

	public void process(WatchedEvent event) {
		if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(distributedLock.getWaitPath())) {
			LOG.info(Thread.currentThread().getName() + "收到情报，排在我前面的家伙已挂，我是不是可以出山了?");
			try {
				if (distributedLock.checkMinPath()) {
					dosomethig();
					distributedLock.unlock();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void dosomethig() {
		LOG.info(Thread.currentThread().getName() + "获取锁成功，赶紧干活！");
		doTemplate.dodo();
		TestLock.threadSemaphore.countDown();
	}

}
