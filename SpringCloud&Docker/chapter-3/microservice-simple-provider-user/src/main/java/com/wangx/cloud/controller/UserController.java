package com.wangx.cloud.controller;

import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import com.wangx.cloud.entity.User;
import com.wangx.cloud.repository.UserRepository;

@RestController
public class UserController {

	@Autowired
	private UserRepository userRepository;

	@GetMapping("/{id}")
	public User findById(@PathVariable Long id) {
		Optional<User> findOne = this.userRepository.findById(id);
		if (findOne.isPresent()) {
			return findOne.get();
		} else {
			return null;
		}
	}

}
