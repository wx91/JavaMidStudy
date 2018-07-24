package com.wangx.drpc1;

import org.apache.storm.utils.DRPCClient;

public class DrpcExclam {
	public static void main(String[] args) throws Exception {
		DRPCClient client = new DRPCClient(null, "192.168.217.130", 3772);
		for (String word : new String[] { "a", "b", "c" }) {
			System.out.println(client.execute("exclamation", word));
		}
	}
}
