package com.wangx.wordcount;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class SplitFunction extends BaseFunction {

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String subjects = tuple.getStringByField("subjects");
		//获取tuple输入内容
		//逻辑处理，然后发射给下一个组件
		for(String sub : subjects.split(" ")){
			collector.emit(new Values(sub));
		}
	}

}
