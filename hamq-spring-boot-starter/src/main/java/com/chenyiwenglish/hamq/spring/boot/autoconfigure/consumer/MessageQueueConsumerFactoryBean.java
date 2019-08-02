package com.chenyiwenglish.hamq.spring.boot.autoconfigure.consumer;

import java.lang.annotation.Annotation;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import com.chenyiwenglish.hamq.impl.HighAvailabilityMessageQueueConsumer;
import com.chenyiwenglish.hamq.mapper.MessageQueueInfoMapper;
import com.chenyiwenglish.hamq.mapper.MessageQueueMapper;
import com.chenyiwenglish.hamq.model.MessageQueueInfo;
import com.chenyiwenglish.hamq.spring.boot.autoconfigure.HamqConfigService;

import lombok.Data;

@Data
public class MessageQueueConsumerFactoryBean implements FactoryBean, InitializingBean, DisposableBean {

    private HamqConfigService hamqConfigService;

    private Class<?> messageQueueConsumerInterface;

    private Annotation messageQueueAnnotation;

    private HamqConfigService.MessageQueueConfiguration configuration;

    private MessageQueueInfoMapper messageQueueInfoMapper;

    private MessageQueueMapper messageQueueMapper;

    private Object singletonInstance = null;

    public MessageQueueConsumerFactoryBean() {
    }

    public MessageQueueConsumerFactoryBean(Class<?> messageQueueConsumerInterface, Annotation messageQueueAnnotation) {
        this.messageQueueConsumerInterface = messageQueueConsumerInterface;
        this.messageQueueAnnotation = messageQueueAnnotation;
    }

    @Override
    public void destroy() throws Exception {
        if (singletonInstance != null) {
            ((HighAvailabilityMessageQueueConsumer) singletonInstance).destroy();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (singletonInstance == null) {
            configuration = hamqConfigService.getMessageQueueConfiguration(messageQueueAnnotation);
            singletonInstance = newInstance();
            ((HighAvailabilityMessageQueueConsumer) singletonInstance).init();
        }
    }

    private HighAvailabilityMessageQueueConsumer newInstance() {
        HighAvailabilityMessageQueueConsumer consumer = new HighAvailabilityMessageQueueConsumer();
        consumer.setMessageQueueInfoMapper(messageQueueInfoMapper);
        consumer.setMessageQueueMapper(messageQueueMapper);
        consumer.setConfiguration(MessageQueueInfo.builder()
                .queueId(configuration.getQueueId())
                .maxRetryCount(configuration.getMaxRetryCount())
                .status(configuration.getStatus())
                .fadingType(configuration.getFadingType()).build());
        consumer.setIdc(configuration.getIdc());
        return consumer;
    }

    @Override
    public synchronized Object getObject() throws Exception {
        return singletonInstance;
    }

    @Override
    public Class<?> getObjectType() {
        return messageQueueConsumerInterface;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
