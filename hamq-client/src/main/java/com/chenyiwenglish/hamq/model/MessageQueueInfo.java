package com.chenyiwenglish.hamq.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class MessageQueueInfo {
    private Long queueId;
    private Integer maxRetryCount;
    private Integer fadingType;
    private Integer status;
}
