package com.wangx.zklock;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockService {
	private static final Logger LOG = LoggerFactory.getLogger(LockService.class);
	private static final String CONNECTION_STRING = "192.168.8.11:2181";
	private static final int THREAD_NUM = 10;
	public static CountDownLatch threadSemaphore = new CountDownLatch(THREAD_NUM);
	private static final String GROUP_PATH = "/disLock";
	private static final String SUB_PATH = "/disLocls/sub";
	private static final int SESSION_TIMEOUT = 10000;
	AbstrackZookeeper az = new AbstrackZookeeper();

	public void doService(DoTemplate doTemplate) {
		try {
			ZooKeeper zk = az.connection(CONNECTION_STRING, SESSION_TIMEOUT);
			DistributedLock dc = new DistributedLock(zk);
			LockWatcher lw = new LockWatcher(dc, doTemplate);
			dc.setWatcher(lw);
			// group_path不存在的话，由一个线程创建即可
			dc.createPath(GROUP_PATH, "该节点由线程" + Thread.currentThread().getName() + "创建");
			boolean rs = dc.getLock();
			if (rs == true) {
				lw.dosomethig();
				dc.unlock();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}

}
