package com.wangx.dubbo.sample;

import java.util.List;

import com.wangx.dubbo.entity.User;

public interface SampleService {

	String sayHello(String name);

	List<User> getUsers();
}
