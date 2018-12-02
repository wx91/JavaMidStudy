package com.wangx.cloud.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.wangx.cloud.entity.User;

public interface UserRepository extends JpaRepository<User, Long> {

}
