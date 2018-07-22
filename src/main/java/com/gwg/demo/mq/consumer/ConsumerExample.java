package com.gwg.demo.mq.consumer;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;
import com.gwg.demo.mq.common.DetailRes;
import com.gwg.demo.mq.common.MessageConsumer;

/**
 * Created 
 */
@Component
public class ConsumerExample {

	/*
	 * @Autowired默认按类型进行注入，如果需要按名称进行注入，需要使用@Qualifier注解
	 */
    @Autowired
    private MessageConsumer messageConsumer;

	@RabbitListener(queues = "${rabbitmq.queue}")
    public void consume() {
        DetailRes result = messageConsumer.consume();
        System.out.println("返回结果"+JSON.toJSON(result));
    }

}
