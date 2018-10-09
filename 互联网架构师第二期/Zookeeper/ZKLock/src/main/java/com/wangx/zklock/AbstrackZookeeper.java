package com.wangx.zklock;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

public class AbstrackZookeeper implements Watcher {
	protected ZooKeeper zooKeeper;
	protected CountDownLatch countDownLatch = new CountDownLatch(1);

	public ZooKeeper connection(String hosts, int session_timeout) throws IOException, InterruptedException {
		zooKeeper = new ZooKeeper(hosts, session_timeout, this);
		countDownLatch.await();
		System.out.println("AbstrackZookeeper.connection()");
		return zooKeeper;
	}

	public void process(WatchedEvent event) {
		if (event.getState() == KeeperState.SyncConnected) {
			countDownLatch.countDown();
		}
	}

	public void close() throws InterruptedException {
		zooKeeper.close();
	}
}
