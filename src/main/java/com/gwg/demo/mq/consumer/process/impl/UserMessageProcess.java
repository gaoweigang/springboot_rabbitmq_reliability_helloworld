package com.gwg.demo.mq.consumer.process.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.gwg.demo.mq.common.DetailRes;
import com.gwg.demo.mq.consumer.process.MessageProcess;
import com.gwg.demo.mq.message.UserMessage;

/**
 * 用户消息处理逻辑
 */
public class UserMessageProcess implements MessageProcess<UserMessage> {
	
	private static final Logger logger = LoggerFactory.getLogger(UserMessageProcess.class);

    @Override
    public DetailRes process(UserMessage userMessage) {
    	logger.info("user 消息内容:{}", JSON.toJSON(userMessage));

        
        //return new DetailRes(true, "");//消息被正确消费
        return new DetailRes(false, "");//消息消费异常
    }
}
