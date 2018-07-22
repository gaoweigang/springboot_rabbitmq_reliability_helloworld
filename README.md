#rabbitmq可靠发送的自动重试机制
https://www.jianshu.com/p/6579e48d18ae
#一.RabbitMQ可靠发送###########################################################################





#一.RabbitMQ可靠消费###########################################################################
#如何保证可靠性消费？
所谓事务性处理，是指对一个消息的处理必须严格可控，必须满足原子性，只有两种可能的处理结果：
(1) 处理成功，从队列中删除消息
(2) 处理失败(网络问题，程序问题，服务挂了)，将消息重新放回队列
为了做到这点，我们使用rabbitmq的手动ack模式，这个后面细说。
具体处理办法：
1.首先需要设置消息确认模式为手动
/**
	 * 监听器配置
	 */
	@Bean
	public RabbitListenerContainerFactory<?> rabbitListenerContainerFactory(){
		SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory = new SimpleRabbitListenerContainerFactory();
		rabbitListenerContainerFactory.setConnectionFactory(connectionFactory());
		rabbitListenerContainerFactory.setConcurrentConsumers(1);
		rabbitListenerContainerFactory.setMaxConcurrentConsumers(10);
		rabbitListenerContainerFactory.setAcknowledgeMode(AcknowledgeMode.MANUAL);//设置消息确认模式为手动
		return rabbitListenerContainerFactory;
	}
2.失败后发送nack使信息重新投递
channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);//前提需要设置手动确认，否则无效




#三.RabbitMQ消息丢失与重复消费问题