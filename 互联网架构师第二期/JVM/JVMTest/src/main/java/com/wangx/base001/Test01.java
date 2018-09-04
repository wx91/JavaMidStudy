package com.wangx.base001;

public class Test01 {
	public static void main(String[] args) {
		// -XX:+UserCompressedOops -XX:UseLargePagesIndividuakAllocation

		// 1：
		// -XX:+PrintGC -Xms5m -Xms20m -XX:+UseSerialGC -XX:+PrintGCDetails

		// 查看GC信息
		System.out.println("Max memory:" + Runtime.getRuntime().maxMemory());
		System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory());
		System.out.println("Total memory:" + Runtime.getRuntime().totalMemory());

		byte[] b1 = new byte[1 * 1024 * 1024];
		System.out.println("分配了1M");
		System.out.println("Max memory:" + Runtime.getRuntime().maxMemory());
		System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory());
		System.out.println("Total memory:" + Runtime.getRuntime().totalMemory());

		byte[] b2 = new byte[1 * 1024 * 1024];
		System.out.println("分配了4M");
		System.out.println("Max memory:" + Runtime.getRuntime().maxMemory());
		System.out.println("Free Memory:" + Runtime.getRuntime().freeMemory());
		System.out.println("Total memory:" + Runtime.getRuntime().totalMemory());

	}

}
