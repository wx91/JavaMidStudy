package com.wangx.dubbo.provider;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SampleProvider {
	public static void main(String[] args) throws Exception {
		String contextPath = "sample-provider.xml";
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(contextPath);
		context.start();
		// 为保证服务一直开着，利用输入流阻塞来模拟
		System.in.read();
	}

}
