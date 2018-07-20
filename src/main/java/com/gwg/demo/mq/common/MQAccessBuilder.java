package com.gwg.demo.mq.common;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;

import com.alibaba.fastjson.JSON;
import com.gwg.demo.mq.common.Constants;
import com.gwg.demo.mq.common.DetailRes;
import com.gwg.demo.mq.common.MessageWithTime;
import com.gwg.demo.mq.consumer.process.MessageProcess;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
public class MQAccessBuilder {
	
	private static final Logger logger = LoggerFactory.getLogger(MQAccessBuilder.class);
	
	private ConnectionFactory connectionFactory;

	public MQAccessBuilder(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public MessageProducer buildMessageSender(final String exchange, final String routingKey, final String queue)
			throws IOException {
		return buildMessageSender(exchange, routingKey, queue, "direct");
	}

	public MessageProducer buildTopicMessageSender(final String exchange, final String routingKey) throws IOException {
		return buildMessageSender(exchange, routingKey, null, "topic");
	}

	/***************************通过发送确定的方式确保数据发送broker**********************************************************************/
	// 1 构造template, exchange, routingkey等
	// 2 设置message序列化方法
	// 3 设置发送确认,确认消息有没有发送到broker代理服务器上
	// 4 构造sender方法
	public MessageProducer buildMessageSender(final String exchange, final String routingKey, final String queue,
			final String type) throws IOException {
		logger.info("buildMessageSender 创建新连接 start .....");
		Connection connection = connectionFactory.createConnection();
		// 1
		if (type.equals("direct")) {
			logger.info("buildMessageSender 构造交换器和队列 ，并将交换器和队列绑定 start .....");
			buildQueue(exchange, routingKey, queue, connection, "direct");
		} else if (type.equals("topic")) {
			buildTopic(exchange, connection);
		}

		final RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);

		rabbitTemplate.setMandatory(true);
		rabbitTemplate.setExchange(exchange);
		rabbitTemplate.setRoutingKey(routingKey);
		// 2
		logger.info("设置message序列化方法 start....");
		rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter());
		RetryCache retryCache = new RetryCache();

		//3.
		logger.info("设置发送确认,确认消息有没有路由到exchange,不管有没有路由到exchange上，都会回调该方法.....");
		rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
			logger.info("发送消息确认, correlationData:{}, ack:{},cause:{}", JSON.toJSON(correlationData), ack, cause);
			if (!ack) {
				logger.info("send message failed: " + cause + correlationData.toString());
			} else {
				logger.info("producer在收到确认消息后，删除本地缓存,correlationData:{}", JSON.toJSON(correlationData));
				//消息路由到exhange成功，删除本地缓存，我们发送的每条消息都会与correlationData相关，而correlationData中的id是我们自己指定的
				retryCache.del(Long.valueOf(correlationData.getId()));
			}
		});
		
		//3.1
		logger.info("确定消息有没有路由到queue,只有在消息从exchange路由到queue失败时候，才会调用该方法....");
		rabbitTemplate.setMandatory(true);
		rabbitTemplate.setReturnCallback((message, replyCode, replyText, tmpExchange, tmpRoutingKey) -> {
			logger.info("ReturnCallback start ....");
			try {
				Thread.sleep(Constants.ONE_SECOND);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			logger.info("send message failed: " + replyCode + " " + replyText);
			//消息路由到queue失败，重发
			rabbitTemplate.send(message);
		});

		// 4
		return new MessageProducer() {
			//初始化块
			{
				retryCache.setSender(this);
			}

			@Override
			public DetailRes send(Object message) {
				long id = retryCache.generateId();
				long time = System.currentTimeMillis();

				return send(new MessageWithTime(id, time, message));
			}

			@Override
			public DetailRes send(MessageWithTime messageWithTime) {
				try {
					logger.info("在发送消息之前，先将消息进行本地缓存....");
					retryCache.add(messageWithTime);
					//将消息与CorrelationData关联，并发送
					rabbitTemplate.correlationConvertAndSend(messageWithTime.getMessage(),
							new CorrelationData(String.valueOf(messageWithTime.getId())));
				} catch (Exception e) {
					logger.error("将消息丢mq失败，异常：{}", e.getMessage());
					return new DetailRes(false, "");
				}

				return new DetailRes(true, "");
			}
		};
	}

	/*************************** Consumer采用消费失败消息重新投递*********************************************/
	public <T> MessageConsumer buildMessageConsumer(String exchange, String routingKey, final String queue,
			final MessageProcess<T> messageProcess) throws IOException {
		return buildMessageConsumer(exchange, routingKey, queue, messageProcess, "direct");
	}

	public <T> MessageConsumer buildTopicMessageConsumer(String exchange, String routingKey, final String queue,
			final MessageProcess<T> messageProcess) throws IOException {
		return buildMessageConsumer(exchange, routingKey, queue, messageProcess, "topic");
	}

	// 1 创建连接和channel
	// 2 设置message序列化方法
	// 3 consume
	public <T> MessageConsumer buildMessageConsumer(String exchange, String routingKey, final String queue,
			final MessageProcess<T> messageProcess, String type) throws IOException {
		final Connection connection = connectionFactory.createConnection();

		//1
		buildQueue(exchange, routingKey, queue, connection, type);

		// 2
		final MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();
		final MessageConverter messageConverter = new Jackson2JsonMessageConverter();

		// 3 匿名类
		return new MessageConsumer() {
			Channel channel = connection.createChannel(false);

			// 1 通过basicGet获取原始数据
			// 2 将原始数据转换为特定类型的包
			// 3 处理数据
			// 4 手动发送ack确认
			@Override
			public DetailRes consume() {
				try {
					// 1
					logger.info("通过basicGet获取原始数据 start........");
					GetResponse response = channel.basicGet(queue, false);
                    logger.info("原始数据格式：{}", JSON.toJSON(response));
					while (response == null) {
						logger.info("如果没有获取到原始数据，则睡眠一秒之后再尝试获取");
						response = channel.basicGet(queue, false);
						Thread.sleep(Constants.ONE_SECOND);
					}
                    logger.info("原始数据：body:{}, props:{}, MessageCount:{}, Envelope：{}", response.getBody(), response.getProps(), response.getMessageCount(), response.getEnvelope());
					Message message = new Message(response.getBody(), messagePropertiesConverter
							.toMessageProperties(response.getProps(), response.getEnvelope(), "UTF-8"));

					// 2
					@SuppressWarnings("unchecked")
					T messageBean = (T) messageConverter.fromMessage(message);

					// 3
					DetailRes detailRes;

					try {
						logger.info("process 开始处理消息 start......,消息内容：{}", JSON.toJSON(messageBean));
						detailRes = messageProcess.process(messageBean);
					} catch (Exception e) {
						log.error("exception", e);
						detailRes = new DetailRes(false, "process exception: " + e);
					}

					// 4 只有在消息处理成功后发送ack确认，或失败后发送nack使信息重新投递，使用springboot来改写测试信息重新投递
					if (detailRes.isSuccess()) {
						logger.info("ack确认：{}", response.getEnvelope().getDeliveryTag());
						/**
					     * 确认收到的一个或几个消息。由包含接收消息确认的com.rabbitmq.client.AMQP.Basic.GetOk
					     * 或com.rabbitmq.client.AMQP.Basic.Deliver方法提供deliveryTag。
					     * 参数：deliveryTag 来自com.rabbitmq.client.AMQP.Basic.GetOk或com.rabbitmq.client.AMQP.Basic.Deliver的标签
					     * 参数：multiple=true 确认所有信息，包括提供的传送标签;
					     *       multiple=false  只确认所提供的传送标签。
					     */
						channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
					} else {
						// 避免过多失败log
						//Thread.sleep(Constants.ONE_SECOND);
						logger.info("process message failed: {}, 投递的标识:{}", detailRes.getErrMsg(), response.getEnvelope().getDeliveryTag());
						 /**
					     * 拒绝接受的一个或几个消息。由包含拒绝消息的com.rabbitmq.client.AMQP.Basic.GetOk或com.rabbitmq.client.AMQP.Basic.Deliver方法 
					     * 提供投递标识。
					     * deliveryTag：投递标识,收到com.rabbitmq.client.AMQP.Basic.GetOk或com.rabbitmq.client.AMQP.Basic.Deliver的标签
					     * multiple = true:将一次性拒绝所有小于deliveryTag的消息;
					     * multiple = false: 仅仅拒绝提供的 投递标识。
					     * requeue = true：被拒绝的是否重新入队列，而不是丢弃/死信
					     */
						channel.basicNack(response.getEnvelope().getDeliveryTag(), true, true);
					}

					return detailRes;
				} catch (InterruptedException e) {
					log.error("exception", e);
					return new DetailRes(false, "interrupted exception " + e.toString());
				} catch (ShutdownSignalException | ConsumerCancelledException | IOException e) {
					log.error("exception", e);

					try {
						channel.close();
					} catch (IOException | TimeoutException ex) {
						log.error("exception", ex);
					}

					channel = connection.createChannel(false);

					return new DetailRes(false, "shutdown or cancelled exception " + e.toString());
				} catch (Exception e) {
					e.printStackTrace();
					log.info("exception : ", e);

					try {
						channel.close();
					} catch (IOException | TimeoutException ex) {
						ex.printStackTrace();
					}

					channel = connection.createChannel(false);

					return new DetailRes(false, "exception " + e.toString());
				}
			}
		};
	}
    //创建交换器和队列
	private void buildQueue(String exchange, String routingKey, final String queue, Connection connection, String type)
			throws IOException {
		logger.info("buildQueue, exchange:{}, routingKey:{}, queue:{}, connection:{}, type:{}",exchange ,routingKey, queue, connection, type);
		/*
		 * 创建一个新的通道，使用内部分配的通道号。 transactional true :通道是否应该支持事物
		 */
		logger.info("createChannel ..........");
		Channel channel = connection.createChannel(false);

		if (type.equals("direct")) {
			/*
			 * 1.交换器名称
			 * 2.交换器类型 
			 * 3.durable = true : 声明一个持久的交换器(交换器在服务器重新启动后仍然有效)
			 * 4.autoDelete = true :当交换器不再使用的的时候 服务器应该删除交换器 
			 * 5.参数 ： 交换器的其他属性(构造参数)
			 */
			logger.info("direct exchangeDeclare ..........");
			channel.exchangeDeclare(exchange, "direct", true, false, null);
		} else if (type.equals("topic")) {
			logger.info("topic exchangeDeclare ..........");
			channel.exchangeDeclare(exchange, "topic", true, false, null);
		}
		logger.info("queueDeclare ..........");
		channel.queueDeclare(queue, true, false, false, null);
		logger.info("queueBind ..........");
		channel.queueBind(queue, exchange, routingKey);

		try {
			channel.close();
		} catch (TimeoutException e) {
			log.info("close channel time out ", e);
		}
	}

	private void buildTopic(String exchange, Connection connection) throws IOException {
		Channel channel = connection.createChannel(false);
		channel.exchangeDeclare(exchange, "topic", true, false, null);
	}

	// for test
	public int getMessageCount(final String queue) throws IOException {
		Connection connection = connectionFactory.createConnection();
		final Channel channel = connection.createChannel(false);
		final AMQP.Queue.DeclareOk declareOk = channel.queueDeclarePassive(queue);

		return declareOk.getMessageCount();
	}
}
