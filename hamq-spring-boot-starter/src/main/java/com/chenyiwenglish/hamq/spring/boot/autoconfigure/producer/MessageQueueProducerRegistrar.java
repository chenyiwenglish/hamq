package com.chenyiwenglish.hamq.spring.boot.autoconfigure.producer;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.chenyiwenglish.hamq.spring.boot.autoconfigure.annotation.MessageQueueProducer;
import com.chenyiwenglish.hamq.spring.boot.autoconfigure.common.AbstractRegistrar;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanNameGenerator;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.filter.AbstractTypeHierarchyTraversingFilter;

import lombok.Builder;
import lombok.Data;

public class MessageQueueProducerRegistrar extends AbstractRegistrar implements ImportBeanDefinitionRegistrar {
    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        BeanNameGenerator beanNameGenerator = new MessageQueueProducerBeanNameGenerator();

        Collection<BeanDefinition> candidates = getCandidates(resourceLoader);

        Map<String, MessageQueueProducerReference> references = new HashMap<>();
        for (BeanDefinition candidate : candidates) {
            Class<?> beanClazz = getClass(candidate.getBeanClassName());
            for (Field field : FieldUtils.getAllFieldsList(beanClazz)) {
                MessageQueueProducer messageQueueProducer = field.getAnnotation(MessageQueueProducer.class);
                if (messageQueueProducer != null) {
                    MessageQueueProducerReference reference = MessageQueueProducerReference.builder()
                            .messageQueueProducer(field.getAnnotation(MessageQueueProducer.class))
                            .clazz(field.getType()).build();
                    references.put(messageQueueProducer.queueId(), reference);
                }
            }
        }

        for (Map.Entry<String, MessageQueueProducerReference> entry : references.entrySet()) {
            MessageQueueProducerReference reference = entry.getValue();
            BeanDefinition bd = BeanDefinitionBuilder.rootBeanDefinition(MessageQueueProducerFactoryBean.class)
                    .addPropertyReference("hamqConfigService", "hamqConfigService")
                    .addPropertyReference("messageQueueInfoMapper", "messageQueueInfoMapper")
                    .addPropertyReference("messageQueueMapper", "messageQueueMapper")
                    .addPropertyReference("messageIdGenerator", "messageIdGenerator")
                    .addConstructorArgValue(reference.getClazz())
                    .addConstructorArgValue(reference.getMessageQueueProducer())
                    .getBeanDefinition();
            String name = StringUtils.isNotBlank(reference.getMessageQueueProducer().name()) ?
                    reference.getMessageQueueProducer().name() : beanNameGenerator.generateBeanName(bd, registry);
            bd.setAttribute("factoryBeanObjectType", reference.getClazz().getName());
            registry.registerBeanDefinition(name, bd);
        }
    }

    private Collection<BeanDefinition> getCandidates(ResourceLoader resourceLoader) {
        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false, environment);
        scanner.addIncludeFilter(new AbstractTypeHierarchyTraversingFilter(true, false) {
            @Override
            protected boolean matchClassName(String className) {
                try {
                    Class<?> clazz = Class.forName(className);
                    List<Field> fields = FieldUtils.getAllFieldsList(clazz);
                    return fields
                            .stream()
                            .anyMatch(f -> f.getAnnotation(MessageQueueProducer.class) != null);
                } catch (ClassNotFoundException e) {
                    throw new BeanInitializationException("class not found when match class name", e);
                }
            }
        });

        scanner.setResourceLoader(resourceLoader);
        return getBasePackages()
                .stream()
                .flatMap(basePackage -> scanner.findCandidateComponents(basePackage).stream())
                .collect(Collectors.toSet());
    }

    @Data
    @Builder
    private static class MessageQueueProducerReference {
        private MessageQueueProducer messageQueueProducer;
        private Class<?> clazz;
    }
}
