package com.wangx.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TridentFilter {

	public static class CheckEvenSumFilter extends BaseFilter {

		public boolean isKeep(TridentTuple tuple) {
			int num1 = tuple.getInteger(0);
			int num2 = tuple.getInteger(1);
			int sum = num1 + num2;
			if (sum % 2 == 0) {
				return true;
			}
			return false;
		}
	}
	// 继承BaseFunction类，重写execute方法
	public static class Result extends BaseFunction {

		public void execute(TridentTuple tuple, TridentCollector collector) {
			// 获取tuple输入内容
			System.out.println();
			Integer a = tuple.getIntegerByField("a");
			Integer b = tuple.getIntegerByField("b");
			Integer c = tuple.getIntegerByField("c");
			Integer d = tuple.getIntegerByField("d");
			System.out.println("a=" + a + "b=" + b + "c=" + c + "d=" + d);
		}

	}
	public static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();
		// 设定数据源
		FixedBatchSpout spout = new FixedBatchSpout(
				new Fields("a", "b", "c", "d"),//声明输入的域字段为"a","b","c","d"
				4, //设置批处理的次数为1
				//设置数据源内容，测试数据
				new Values(1, 4, 7, 10),
				new Values(1, 1, 3, 11),
				new Values(2, 2, 7, 11), 
				new Values(2, 5, 7, 2));
		spout.setCycle(false);
		//指定输入源spout
		Stream inputStream = topology.newStream("spout", spout);
		/**
		 *  要实现spout - bolt的模式在trident里是使用each来做的
		 *  each 方法参数
		 *  1.输入数据源参数名称："a","b","c","d"
		 *  2.需要流转执行的function对象(也就是bolt对象):new SumFunciton()
		 *  3.指定function对象里的输出参数名称：sum
		 */
		inputStream.each(new Fields("a","b","c","d"),  new CheckEvenSumFilter())
		/**
		 * 继续使用each调用下一个function(bolt)
		 * 第一个输入参数为："a","b","c","d","sum"
		 * 第二个参数为：new Result() 也就是执行函数，第三个函数为没有输出
		 */
		.each(new Fields("a","b","c","d"),  new Result(), new Fields());
		return topology.build();
	}

	public static void main(String[] args) throws Exception{
		Config conf = new Config();
		// 设置batch最大处理
		conf.setNumWorkers(2);
		conf.setMaxSpoutPending(20);
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("trident-function", conf, buildTopology());
			Thread.sleep(10000);
			cluster.shutdown();
		} else {
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}
}
