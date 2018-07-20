package com.gwg.demo.mq.common;

import com.gwg.demo.mq.common.DetailRes;
import com.gwg.demo.mq.common.MessageWithTime;

/**
 * 
 */
public interface MessageProducer {
    DetailRes send(Object message);

    DetailRes send(MessageWithTime messageWithTime);
}
