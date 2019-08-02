package com.chenyiwenglish.hamq.async;

import com.chenyiwenglish.hamq.MessageProcessor;
import com.chenyiwenglish.hamq.impl.HighAvailabilityMessageQueueConsumer;
import com.chenyiwenglish.hamq.spring.boot.autoconfigure.annotation.MessageQueueConsumer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Component
public class TestMessageProcessor implements MessageProcessor, InitializingBean {

    @MessageQueueConsumer(queueId = "test", name = "highAvailabilityMessageQueueConsumer")
    private HighAvailabilityMessageQueueConsumer highAvailabilityMessageQueueConsumer;

    @Override
    public void afterPropertiesSet() throws Exception {
        highAvailabilityMessageQueueConsumer.registerProcessor(this);
    }

    @Override
    public void process(String message) {
        System.out.println(message);
    }
}
