package com.chenyiwenglish.hamq.spring.boot.autoconfigure;

import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.Environment;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class MessageQueueConfigurationBinder {
    private static final String PREFIX = HamqProperties.PREFIX + ".queue";
    private static final Pattern PATH_SPLITTER_PATTERN = Pattern.compile("[.]");

    private static final Set<String> SUFFIX = new HashSet<String>() {
        {
            add("queue-id");
            add("fading-type");
            add("max-retry-count");
            add("status");
        }
    };

    private final Environment environment;

    private final HamqProperties properties;

    private Map<String, HamqConfigService.MessageQueueConfiguration> messageQueueConfig;

    public MessageQueueConfigurationBinder(Environment environment, HamqProperties properties) {
        this.environment = environment;
        this.properties = properties;
    }

    public Map<String, HamqConfigService.MessageQueueConfiguration> bind() {
        Comparator<String> comparator = Comparator.comparing(s -> {
            int cnt = PATH_SPLITTER_PATTERN.matcher(s).groupCount();
            return s.contains("queue-id") ? cnt - 1 : cnt;
        });
        comparator = comparator.reversed().thenComparing(Comparator.naturalOrder());
        messageQueueConfig = new TreeMap<>(comparator);

        Binder binder = Binder.get(environment);
        binder.bind(PREFIX, Bindable.mapOf(String.class, String.class))
                .orElseGet(Collections::emptyMap)
                .forEach(this::setMessageQueueConfig);
        return messageQueueConfig;
    }

    private void setMessageQueueConfig(String key, String value) {
        SUFFIX.stream()
                .filter(key::endsWith)
                .forEach(name -> {
                    String messgaeQueueKey = key.substring(0, key.length() - name.length() - 1);
                    if (!messageQueueConfig.containsKey(messgaeQueueKey)) {
                        messageQueueConfig.put(messgaeQueueKey, new HamqConfigService.MessageQueueConfiguration());
                    }
                    setValue(messageQueueConfig.get(messgaeQueueKey), name, value);
                });
    }

    private void setValue(HamqConfigService.MessageQueueConfiguration serviceConfiguration, String name, String value) {
        switch (name) {
            case "queue-id":
                serviceConfiguration.setQueueId(Long.parseLong(value));
                break;
            case "fading-type":
                serviceConfiguration.setFadingType(Integer.parseInt(value));
                break;
            case "max-retry-count":
                serviceConfiguration.setMaxRetryCount(Integer.parseInt(value));
                break;
            case "status":
                serviceConfiguration.setStatus(Integer.parseInt(value));
                break;
            default:
                throw new IllegalArgumentException("illegal argument: " + name);
        }
    }
}
