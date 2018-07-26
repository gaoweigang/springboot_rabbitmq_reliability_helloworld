package com.gwg.demo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.gwg.demo.mq.consumer.ConsumerExample;
import com.gwg.demo.mq.message.UserMessage;
import com.gwg.demo.mq.producer.ProducerExample;

/**
 * 生产数据，然后启动springboot应用进行消费
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RabbitMQTest {
	
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQTest.class);
	
	@Autowired
	private ProducerExample producerExample;
	
	@Autowired
    private ConsumerExample consumerExample;
	
	@Test
	public void produce(){
		for(int i = 0; i < 10; i++){
			UserMessage userMessage = new UserMessage(1000+i, "gaoweigang"+i);
			producerExample.send(userMessage);

		}
	}
	/**
	 * basicGet是主动拉取消息，并不常用
	 * 而实现推模式推荐的方式是继承DefaultConsumer基类，常用
	 */
	@Test
	public void consume() throws InterruptedException{
		for(int i = 0; i <50; i++){
			logger.info("consume ............");
			consumerExample.consume();
		}
	}
	

}
