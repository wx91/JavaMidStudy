package com.wangx.base;

import java.nio.ByteBuffer;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
/**
 * Disruptor 3.0提供了lambda式的API。这样可以把一些复杂的操作放在Ring Buffer，
 * 所以在Disruptor3.0以后的版本最好使用Event Publisher或者Event Translator来发布事件
 *
 */
public class LongEventProducerWithTranslator {
	
	private final RingBuffer<LongEvent> ringBuffer;
	
	public LongEventProducerWithTranslator(RingBuffer<LongEvent> ringBuffer) {
		this.ringBuffer = ringBuffer;
	}
	

	private static final EventTranslatorOneArg<LongEvent, ByteBuffer> TRANSLATOR = 
			new EventTranslatorOneArg<LongEvent, ByteBuffer>() {
				public void translateTo(LongEvent event, long sequence, ByteBuffer buffer) {
					event.setValue(buffer.getLong(0));
				}
			};
	
	public void onData(ByteBuffer buffer) {
	}
}
