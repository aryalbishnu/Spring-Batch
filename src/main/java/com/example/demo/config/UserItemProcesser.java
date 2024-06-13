package com.example.demo.config;

import org.springframework.batch.item.ItemProcessor;

import com.example.demo.model.User;

public class UserItemProcesser implements ItemProcessor<User,User>{

  @Override
  public User process(User user) throws Exception {
    
    return user;
  }

}
