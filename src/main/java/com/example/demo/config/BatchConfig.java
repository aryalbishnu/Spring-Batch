package com.example.demo.config;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import com.example.demo.model.User;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

@Autowired
  private DataSource dataSource;
  
  @Autowired
  private JobBuilderFactory jobBuilderFactory;
  
  @Autowired
  private StepBuilderFactory stepBuilderFactory;
  
  @Bean
  public FlatFileItemReader<User>reader(){
   FlatFileItemReader<User>reader= new FlatFileItemReader<>();
   reader.setResource(new ClassPathResource("book.csv"));
   
   reader.setLineMapper(getLineMapper());
   reader.setLinesToSkip(1);
   return reader;
    
  }

  private LineMapper<User> getLineMapper() {
     DefaultLineMapper<User>lineMapper= new DefaultLineMapper<>();
     
     DelimitedLineTokenizer lineTokenizer= new DelimitedLineTokenizer();
     lineTokenizer.setNames(new String[] {"id", "name", "email","password"});
     lineTokenizer.setIncludedFields(new int[] {0,1,2,3});
     
     BeanWrapperFieldSetMapper<User>filedSetter= new BeanWrapperFieldSetMapper<>();
     filedSetter.setTargetType(User.class);
     
    lineMapper.setLineTokenizer(lineTokenizer);
     lineMapper.setFieldSetMapper(filedSetter);
    return lineMapper;
  }
  
  @Bean
  public UserItemProcesser processor() {
    return new UserItemProcesser();
    
  }
  
  @Bean 
  public JdbcBatchItemWriter<User>writer(){
    JdbcBatchItemWriter<User>writer= new JdbcBatchItemWriter<>();
    writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<User>());
    writer.setSql("insert into useraa(id, name, email, password) values (:id, :name, :email, :password)");
    writer.setDataSource(this.dataSource);
    return writer; 
  }
  
  @Bean 
  public JdbcBatchItemWriter<User>testWriter(){
    JdbcBatchItemWriter<User>writer= new JdbcBatchItemWriter<>();
    writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<User>());
    writer.setSql("insert into test(id, name, email) values (:id, :name, :email)");
    writer.setDataSource(this.dataSource);
    return writer; 
  }
  
  @Bean
  public Job importUserJob() {
    return this.jobBuilderFactory.get("USER-IMPORT-JOB")
               .incrementer(new RunIdIncrementer())
               .listener(jobCompletionListener())
               .flow(step1())
               .end()
               .build() ;
  }
  
  @Bean
  public JobCompletionListener jobCompletionListener() {
      return new JobCompletionListener();
  }
  
  @Bean
  public CompositeItemWriter<User> compositeWriter() {
      List<ItemWriter<? super User>> writers = new ArrayList<>();
      writers.add(writer());
      writers.add(testWriter());

      CompositeItemWriter<User> compositeWriter = new CompositeItemWriter<>();
      compositeWriter.setDelegates(writers);
      return compositeWriter;
  }
  
@Bean
  public  Step step1() {
   return  this.stepBuilderFactory.get("step1")
    .<User, User>chunk(10)
    .reader(reader())
    .processor(processor())
    .writer(compositeWriter())
    .build();

  }
}
