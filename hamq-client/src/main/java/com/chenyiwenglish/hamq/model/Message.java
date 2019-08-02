package com.chenyiwenglish.hamq.model;

import lombok.Data;

@Data
public class Message {
    private Long messageId;
    private String extraInfo;
    private Long queueId;
    private Integer retryCount;
    private String messageBody;
    private Long consumeTime;
}
