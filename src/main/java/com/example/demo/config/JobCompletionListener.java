package com.example.demo.config;

import java.time.LocalDateTime;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public class JobCompletionListener implements JobExecutionListener {

  @Override
  public void beforeJob(JobExecution jobExecution) {
    LocalDateTime startTime = LocalDateTime.now();
    System.out.println("Job started at: " + startTime);
    
  }

  @Override
  public void afterJob(JobExecution jobExecution) {
    LocalDateTime endTime = LocalDateTime.now();
    System.out.println("Job finished at: " + endTime);
    
  }

 
}