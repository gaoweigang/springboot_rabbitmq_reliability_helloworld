package com.gwg.demo.mq.config;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.gwg.demo.mq.common.MQAccessBuilder;
import com.gwg.demo.mq.common.MessageConsumer;
import com.gwg.demo.mq.common.MessageProducer;
import com.gwg.demo.mq.consumer.process.impl.UserMessageProcess;

/**
 * 
 * 生成ConnectionFactory
 *
 */
@Configuration
public class RabbitMQConfig {

	private static Logger logger = LoggerFactory.getLogger(RabbitMQConfig.class);

	// 测试 调试环境
	@Value("${rabbitmq.host}")
	private String host;
	@Value("${rabbitmq.username}")
	private String username;
	@Value("${rabbitmq.password}")
	private String password;
	@Value("${rabbitmq.port}")
	private Integer port;
	@Value("${rabbitmq.virtual-host}")
	private String virtualHost;//虚拟主机 

	//用户消息队列
	@Value("${rabbitmq.direct.exchange}")
	private String userExchangeName;
	@Value("${rabbitmq.queue}")
	private String userQueueName;
	@Value("${rabbitmq.routing}")
	private String userRouting;
	

	@Bean
	public ConnectionFactory connectionFactory() {
		logger.info("connectionFactory, host:{}, port:{}, username:{}, password:{}", host, port, username, password);
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory(host, port);

		connectionFactory.setUsername(username);
		connectionFactory.setPassword(password);
		connectionFactory.setVirtualHost(virtualHost);//设置虚拟主机
		// 设置消息手动确认模式
		connectionFactory.setPublisherConfirms(true); // enable confirm mode
		connectionFactory.setPublisherReturns(true);  // enable return mode
		// connectionFactory.getRabbitConnectionFactory().setAutomaticRecoveryEnabled(true);

		return connectionFactory;
	}

	/***************** messsage consumer ***************************************************/
	@Bean("userMessageConsumer")
	public MessageConsumer userMessageConsumer() throws IOException {
		logger.info("messageConsumer, exchange:{},  queue:{}, routing:{}", userExchangeName, userQueueName, userRouting);
		MQAccessBuilder mqAccessBuilder = new MQAccessBuilder(connectionFactory());
		return mqAccessBuilder.buildMessageConsumer(userExchangeName, userQueueName, userRouting, new UserMessageProcess(), "direct");

	}

	/***************** message producer*****************************************************/
	@Bean("userMessageProducer")
	public MessageProducer userMessageProducer() throws IOException {
		logger.info("messageSender, exchange:{}, queue:{} , routing:{}", userExchangeName, userQueueName, userRouting);
		MQAccessBuilder mqAccessBuilder = new MQAccessBuilder(connectionFactory());
		return mqAccessBuilder.buildMessageSender(userExchangeName, userQueueName, userRouting);
	}
	
	
	
	/******************message listener************************************************************/
	/**
	 * 监听器配置
	 */
	@Bean
	public RabbitListenerContainerFactory<?> rabbitListenerContainerFactory(){
		SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory = new SimpleRabbitListenerContainerFactory();
		rabbitListenerContainerFactory.setConnectionFactory(connectionFactory());
		rabbitListenerContainerFactory.setConcurrentConsumers(1);
		rabbitListenerContainerFactory.setMaxConcurrentConsumers(10);
		return rabbitListenerContainerFactory;
	}
	

}
