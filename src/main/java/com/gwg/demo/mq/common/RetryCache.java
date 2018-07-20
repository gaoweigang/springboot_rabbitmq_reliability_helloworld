package com.gwg.demo.mq.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gwg.demo.mq.common.Constants;
import com.gwg.demo.mq.common.DetailRes;
import com.gwg.demo.mq.common.MessageWithTime;

import lombok.extern.slf4j.Slf4j;

/**
 * Created
 */
@Slf4j
public class RetryCache {
	
	private static final Logger logger = LoggerFactory.getLogger(RetryCache.class);
    private MessageProducer sender;
    private boolean stop = false;
    private Map<Long, MessageWithTime> map = new ConcurrentHashMap<Long, MessageWithTime>();
    private AtomicLong id = new AtomicLong();

    public void setSender(MessageProducer sender) {
    	logger.info("setSender .....");
        this.sender = sender;
        startRetry();
    }

    public long generateId() {
        return id.incrementAndGet();
    }

    public void add(MessageWithTime messageWithTime) {
        map.putIfAbsent(messageWithTime.getId(), messageWithTime);
    }

    public void del(long id) {
        map.remove(id);
    }
    //对于消费失败的消息重试机制
    private void startRetry() {
    	logger.info("startRetry .....");
        new Thread(() ->{
            while (!stop) {
                try {
                    Thread.sleep(Constants.RETRY_TIME_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                long now = System.currentTimeMillis();

                for (Map.Entry<Long, MessageWithTime> entry : map.entrySet()) {
                    MessageWithTime messageWithTime = entry.getValue();

                    if (null != messageWithTime) {
                        if (messageWithTime.getTime() + 3 * Constants.VALID_TIME < now) {
                            log.info("send message {} failed after 3 min ", messageWithTime);
                            del(entry.getKey());
                        } else if (messageWithTime.getTime() + Constants.VALID_TIME < now) {
                            DetailRes res = sender.send(messageWithTime);

                            if (!res.isSuccess()) {
                                log.info("retry send message failed {} errMsg {}", messageWithTime, res.getErrMsg());
                            }
                        }
                    }
                }
            }
        }).start();
    }
}
