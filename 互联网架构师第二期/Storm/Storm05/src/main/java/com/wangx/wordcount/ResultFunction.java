package com.wangx.wordcount;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class ResultFunction extends BaseFunction {

	public void execute(TridentTuple tuple, TridentCollector collector) {
		// 获取tuple输入的内容
		String sub = tuple.getStringByField("sub");
		Long count = tuple.getLongByField("count");
		System.out.println(sub + ":" + count);
	}

}
