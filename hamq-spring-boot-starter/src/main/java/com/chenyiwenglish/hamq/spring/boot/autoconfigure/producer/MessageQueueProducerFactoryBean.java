package com.chenyiwenglish.hamq.spring.boot.autoconfigure.producer;

import com.chenyiwenglish.hamq.MessageIdGenerator;
import com.chenyiwenglish.hamq.impl.HighAvailabilityMessageQueueProducer;
import com.chenyiwenglish.hamq.mapper.MessageQueueInfoMapper;
import com.chenyiwenglish.hamq.mapper.MessageQueueMapper;
import com.chenyiwenglish.hamq.model.MessageQueueInfo;
import com.chenyiwenglish.hamq.spring.boot.autoconfigure.HamqConfigService;
import lombok.Data;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.lang.annotation.Annotation;

@Data
public class MessageQueueProducerFactoryBean implements FactoryBean, InitializingBean, DisposableBean {

    private HamqConfigService hamqConfigService;

    private Class<?> messageQueueProducerInterface;

    private Annotation messageQueueAnnotation;

    private HamqConfigService.MessageQueueConfiguration configuration;

    private MessageQueueInfoMapper messageQueueInfoMapper;

    private MessageQueueMapper messageQueueMapper;

    private MessageIdGenerator messageIdGenerator;

    private Object singletonInstance = null;

    public MessageQueueProducerFactoryBean() {
    }

    public MessageQueueProducerFactoryBean(Class<?> messageQueueProducerInterface, Annotation messageQueueAnnotation) {
        this.messageQueueProducerInterface = messageQueueProducerInterface;
        this.messageQueueAnnotation = messageQueueAnnotation;
    }

    @Override
    public void destroy() throws Exception {
        if (singletonInstance != null) {
            ((HighAvailabilityMessageQueueProducer) singletonInstance).destroy();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (singletonInstance == null) {
            configuration = hamqConfigService.getMessageQueueConfiguration(messageQueueAnnotation);
            singletonInstance = newInstance();
            ((HighAvailabilityMessageQueueProducer) singletonInstance).init();
        }
    }

    private HighAvailabilityMessageQueueProducer newInstance() {
        HighAvailabilityMessageQueueProducer producer = new HighAvailabilityMessageQueueProducer();
        producer.setMessageQueueInfoMapper(messageQueueInfoMapper);
        producer.setMessageQueueMapper(messageQueueMapper);
        producer.setMessageIdGenerator(messageIdGenerator);
        producer.setConfiguration(MessageQueueInfo.builder()
                .queueId(configuration.getQueueId())
                .maxRetryCount(configuration.getMaxRetryCount())
                .status(configuration.getStatus())
                .fadingType(configuration.getFadingType()).build());
        producer.setIdc(configuration.getIdc());
        return producer;
    }

    @Override
    public synchronized Object getObject() throws Exception {
        return singletonInstance;
    }

    @Override
    public Class<?> getObjectType() {
        return messageQueueProducerInterface;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
