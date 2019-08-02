package com.chenyiwenglish.hamq.spring.boot.autoconfigure;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = HamqProperties.PREFIX)
@Data
public class HamqProperties {
    static final String PREFIX = "chenyiwenglish.hamq";

    private Integer defaultMaxRetryCount = 3;

    private Integer defaultFadingType = 0;

    private Integer defaultStatus = 0;

    private String idc;
}
