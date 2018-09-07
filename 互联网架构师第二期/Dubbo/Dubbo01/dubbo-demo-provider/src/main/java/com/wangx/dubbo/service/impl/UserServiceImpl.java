package com.wangx.dubbo.service.impl;

import org.springframework.beans.factory.annotation.Autowired;

import com.wangx.dubbo.service.SampleService;
import com.wangx.dubbo.service.UserService;

public class UserServiceImpl implements UserService {

	@Autowired
	private SampleService sampleService;

	public String queryUser() throws Exception {
		// 这里测试服务之间的依赖关系
		System.out.println(sampleService.sayHello("jack"));
		return "userService exec";
	}

}
