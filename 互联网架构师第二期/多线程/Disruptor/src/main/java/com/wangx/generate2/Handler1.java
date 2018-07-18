package com.wangx.generate2;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WorkHandler;
import com.wangx.generate1.Trade;

public class Handler1 implements EventHandler<Trade>,WorkHandler<Trade>{

	public void onEvent(Trade event) throws Exception {
		System.out.println("handle1:set name");
		event.setName("h1");
		Thread.sleep(1000);
	}

	public void onEvent(Trade event, long sequence, boolean endOfBatch) throws Exception {
		this.onEvent(event);
	}

}
