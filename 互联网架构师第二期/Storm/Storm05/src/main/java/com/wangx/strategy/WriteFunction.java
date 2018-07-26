package com.wangx.strategy;

import java.io.FileWriter;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class WriteFunction extends BaseFunction {

	private FileWriter writer;

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String text = tuple.getStringByField("sub");
		try {
			if (writer == null) {
				if (System.getProperty("os.name").equals("Windows 10")) {
					writer = new FileWriter("E:\\test\\" + this);
				} else if (System.getProperty("os.name").equals("Windows 8.1")) {
					writer = new FileWriter("E:\\test\\" + this);
				} else if (System.getProperty("os.name").equals("Windows 7")) {
					writer = new FileWriter("E:\\test\\" + this);
				} else if (System.getProperty("os.name").equals("Linux")) {
					writer = new FileWriter("/usr/local/temp/" + this);
				}

			}
			writer.write(text);
			writer.write("\n");
			writer.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
