package com.chenyiwenglish.hamq.mapper;

import com.chenyiwenglish.hamq.model.Message;
import org.apache.ibatis.annotations.Param;

public interface MessageQueueMapper {
    Message selectByQueueIdAndIdcAndConsumeTimeOldest(@Param("queueId") Long queueId,
                                                      @Param("idc") String idc,
                                                      @Param("consumeTime") Long consumeTime);

    int insert(@Param("message") Message message,
               @Param("idc") String idc);

    int lockByMessageId(@Param("messageId") Long messageId);

    int deleteByMessageIdAndQueueId(@Param("messageId") Long messageId,
                                    @Param("queueId") Long queueId);
}
