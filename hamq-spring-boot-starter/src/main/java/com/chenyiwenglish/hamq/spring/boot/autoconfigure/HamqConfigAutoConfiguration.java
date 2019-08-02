package com.chenyiwenglish.hamq.spring.boot.autoconfigure;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(
        prefix = "chenyiwenglish.hamq.autoconfigure",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true
)
@Configuration
@EnableConfigurationProperties(HamqProperties.class)
public class HamqConfigAutoConfiguration {
}
