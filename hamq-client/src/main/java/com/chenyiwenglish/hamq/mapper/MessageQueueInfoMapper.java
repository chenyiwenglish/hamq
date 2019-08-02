package com.chenyiwenglish.hamq.mapper;

import com.chenyiwenglish.hamq.model.MessageQueueInfo;
import org.apache.ibatis.annotations.Param;

public interface MessageQueueInfoMapper {
    MessageQueueInfo selectByQueueId(@Param("queueId") Long queueId);
}
