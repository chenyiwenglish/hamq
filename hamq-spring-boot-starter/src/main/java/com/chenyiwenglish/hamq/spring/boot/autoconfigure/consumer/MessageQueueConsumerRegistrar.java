package com.chenyiwenglish.hamq.spring.boot.autoconfigure.consumer;

import com.chenyiwenglish.hamq.spring.boot.autoconfigure.annotation.MessageQueueConsumer;
import com.chenyiwenglish.hamq.spring.boot.autoconfigure.common.AbstractRegistrar;
import lombok.Builder;
import lombok.Data;
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

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MessageQueueConsumerRegistrar extends AbstractRegistrar implements ImportBeanDefinitionRegistrar {
    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        BeanNameGenerator beanNameGenerator = new MessageQueueConsumerBeanNameGenerator();

        Collection<BeanDefinition> candidates = getCandidates(resourceLoader);

        Map<String, MessageQueueConsumerReference> references = new HashMap<>();
        for (BeanDefinition candidate : candidates) {
            Class<?> beanClazz = getClass(candidate.getBeanClassName());
            for (Field field : FieldUtils.getAllFieldsList(beanClazz)) {
                MessageQueueConsumer messageQueueConsumer = field.getAnnotation(MessageQueueConsumer.class);
                if (messageQueueConsumer != null) {
                    MessageQueueConsumerReference reference = MessageQueueConsumerReference.builder()
                            .messageQueueProducer(field.getAnnotation(MessageQueueConsumer.class))
                            .clazz(field.getType()).build();
                    references.put(messageQueueConsumer.queueId(), reference);
                }
            }
        }

        for (Map.Entry<String, MessageQueueConsumerReference> entry : references.entrySet()) {
            MessageQueueConsumerReference reference = entry.getValue();
            BeanDefinition bd = BeanDefinitionBuilder.rootBeanDefinition(MessageQueueConsumerFactoryBean.class)
                    .addPropertyReference("hamqConfigService", "hamqConfigService")
                    .addPropertyReference("messageQueueInfoMapper", "messageQueueInfoMapper")
                    .addPropertyReference("messageQueueMapper", "messageQueueMapper")
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
                            .anyMatch(f -> f.getAnnotation(MessageQueueConsumer.class) != null);
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
    private static class MessageQueueConsumerReference {
        private MessageQueueConsumer messageQueueProducer;
        private Class<?> clazz;
    }
}
