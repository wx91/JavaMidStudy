package com.wangx.itemcf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class StartRun {
	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://node2:8020");
		config.set("yarn.resourcemanager.hostname", "node3");
		// 所有MR的输入和输入目录定义在map集合中
		Map<String, String> paths = new HashMap<String, String>();
		paths.put("Step1Input", "/user/itemcf/input/(sample)sam_tianchi_2014_rec_tmall_log.csv");
		paths.put("Step1Output", "/user/itemcf/output/step1");
		paths.put("Step2Input", paths.get("Step1Output"));
		paths.put("Step2Output", "/user/itemcf/output/step2");
		paths.put("Step3Input", paths.get("Step2Output"));
		paths.put("Step3Output", "/user/itemcf/output/step3");
		paths.put("Step4Input1", paths.get("Step2Output"));
		paths.put("Step4Input2", paths.get("Step3Output"));
		paths.put("Step4Output", "/user/itemcf/output/step4");
		paths.put("Step5Input", paths.get("Step4Output"));
		paths.put("Step5Output", "/user/itemcf/output/step5");
		paths.put("Step6Input", paths.get("Step5Output"));
		paths.put("Step6Output", "/user/itemcf/output/step6");
		
		Step1.run(config, paths);
		Step2.run(config, paths);
		Step3.run(config, paths);
		Step4.run(config, paths);
		Step5.run(config, paths);
		Step6.run(config, paths);
	}

	public static Map<String, Integer> R = new HashMap<String, Integer>();

	static {
		R.put("click", 1);
		R.put("collect", 2);
		R.put("cart", 3);
		R.put("alipay", 4);
	}

}
