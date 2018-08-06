package com.wangx.dubbo.consumer;

import java.util.List;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.wangx.dubbo.entity.User;
import com.wangx.dubbo.sample.SampleService;

public class SampleConsumer {
	public static void main(String[] args) throws Exception {
		String contextPath = "sample-consumer.xml";
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(contextPath);
		context.start();

		SampleService sampleService = (SampleService) context.getBean("sampleService");
		String hello = sampleService.sayHello("tom");
		System.out.println(hello);

		List<User> list = sampleService.getUsers();
		if (list != null && list.size() > 0) {
			for (int i = 0; i < list.size(); i++) {
				System.out.println(list.get(i));
			}
		}
		System.in.read();
	}
}
