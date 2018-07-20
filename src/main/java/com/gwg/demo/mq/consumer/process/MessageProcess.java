package com.gwg.demo.mq.consumer.process;

import com.gwg.demo.mq.common.DetailRes;

/**
 * Created
 */
public interface MessageProcess<T> {
    DetailRes process(T message);
}
