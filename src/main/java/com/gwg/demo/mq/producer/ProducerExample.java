package com.gwg.demo.mq.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gwg.demo.mq.common.DetailRes;
import com.gwg.demo.mq.common.MessageProducer;
import com.gwg.demo.mq.message.UserMessage;

/**
 * Created
 */
@Component
public class ProducerExample {
	
	private static final Logger logger = LoggerFactory.getLogger(ProducerExample.class);

	@Autowired
    private MessageProducer messageProducer;

    public DetailRes send(UserMessage userMessage) {
    	logger.info("消息生产者  发送数据 start ......");
        return messageProducer.send(userMessage);
    }
}
