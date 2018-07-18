package com.wangx.generate2;

import com.lmax.disruptor.EventHandler;
import com.wangx.generate1.Trade;

public class Handler2 implements EventHandler<Trade>{

	public void onEvent(Trade event, long sequence, boolean endOfBatch) throws Exception {
		System.out.println("handler 2 : set price");
		event.setPrice(17.0);
		Thread.sleep(1000);
	}
	
}
