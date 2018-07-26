package com.wangx.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


public class WordCountTopology {

	public static StormTopology buildTopology() {
		TridentTopology topology = new TridentTopology();
		//设定数据源
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("subjects"),4,
				new Values("java java php ruby c++"),
				new Values("java python python python c++"),
				new Values("java java java java ruby"),
				new Values("c++ java ruby php java"));
		spout.setCycle(false);
		Stream inputStream = topology.newStream("spout", spout);
		inputStream.shuffle()
		.each(new Fields("subjects"), new SplitFunction(),new Fields("sub"))
		//进行分组操作：参数为分组字段subject，比较类似于我们之前结束到的FieldGroup
		.groupBy(new Fields("sub"))
		//对分组之后的结果进行聚合操作：参数1为聚合方法为count函数，输出字段名称为count
		.aggregate(new Count(),new Field("count"))
		//继续使用each调用下一个function（bolt）输入参数为subject和count，第二个参数为new Result 也就是执行函数 第三个参数为没有输入
		.each(new Field("sub","count"),new ResultFunction(),new Field())
		.parallelismHint(1);
		return topology.build();
	}
	public static void main(String[] args) throws Exception {
		Config conf = new  Config();
		conf.setNumWorkers(2);
		conf.setMaxSpoutPending(20);
		if (args.length==0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("trident-strategy", conf, buildTopology());
			Thread.sleep(5000);
			cluster.shutdown();
		}else {
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}
}
