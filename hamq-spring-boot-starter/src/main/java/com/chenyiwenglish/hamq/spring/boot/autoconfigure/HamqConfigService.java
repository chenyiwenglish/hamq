package com.chenyiwenglish.hamq.spring.boot.autoconfigure;

import java.lang.annotation.Annotation;
import java.util.Map;

import com.chenyiwenglish.hamq.spring.boot.autoconfigure.annotation.MessageQueueConsumer;
import com.chenyiwenglish.hamq.spring.boot.autoconfigure.annotation.MessageQueueProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;

import lombok.Data;

public class HamqConfigService {
    public static final Logger logger = LoggerFactory.getLogger(HamqConfigService.class);

    private final HamqProperties properties;

    public HamqConfigService(HamqProperties properties, Environment environment) {
        this.properties = properties;

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        boolean enable = ClassUtils.isPresent(Binder.class.getName(), classLoader)
                && environment instanceof ConfigurableEnvironment;

        if (enable) {
            messageQueue = new MessageQueueConfigurationBinder(environment, properties).bind();
        } else {
            String reason = environment instanceof ConfigurableEnvironment
                    ? "Spring Boot version lower than 2.0.0" : "environment is not configurable";
            logger.warn("hamq configuration not support, because {}", reason);
        }
    }

    private Map<String, MessageQueueConfiguration> messageQueue;

    public MessageQueueConfiguration getMessageQueueConfiguration(Annotation annotation) {
        MessageQueueConfiguration configuration = new MessageQueueConfiguration();
        String queueId = getQueueId(annotation);
        configuration.setQueueId(getQueueId(queueId));
        configuration.setMaxRetryCount(getMaxRetryCount(queueId));
        configuration.setFadingType(getFadingType(queueId));
        configuration.setStatus(getStatus(queueId));
        configuration.setIdc(properties.getIdc());
        configuration.setAtomType(properties.getAtomType());
        return configuration;
    }

    private String getQueueId(Annotation annotation) {
        if (annotation instanceof MessageQueueConsumer) {
            return ((MessageQueueConsumer) annotation).queueId();
        }
        if (annotation instanceof MessageQueueProducer) {
            return ((MessageQueueProducer) annotation).queueId();
        }
        throw new HamqAutoConfigurationException("hamq configuration error, annotation incorrect");
    }

    private Long getQueueId(String queueId) {
        MessageQueueConfiguration conf = messageQueue.get(queueId);
        if (conf != null && conf.getQueueId() != null) {
            return conf.getQueueId();
        }
        throw new HamqAutoConfigurationException("hamq configuration error, queueId not found in the configuration");
    }

    private Integer getMaxRetryCount(String queueId) {
        MessageQueueConfiguration conf = messageQueue.get(queueId);
        return conf != null && conf.getMaxRetryCount() != null ? conf.getMaxRetryCount()
                : properties.getDefaultMaxRetryCount();
    }

    private Integer getFadingType(String queueId) {
        MessageQueueConfiguration conf = messageQueue.get(queueId);
        return conf != null && conf.getFadingType() != null ? conf.getFadingType() : properties.getDefaultFadingType();
    }

    private Integer getStatus(String queueId) {
        MessageQueueConfiguration conf = messageQueue.get(queueId);
        return conf != null && conf.getStatus() != null ? conf.getStatus() : properties.getDefaultStatus();
    }

    @Data
    public static class MessageQueueConfiguration {
        private Long queueId;
        private Integer maxRetryCount;
        private Integer fadingType;
        private Integer status;
        private String idc;
        private String atomType;
    }
}
