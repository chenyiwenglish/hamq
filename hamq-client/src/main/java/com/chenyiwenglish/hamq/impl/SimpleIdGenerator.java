package com.chenyiwenglish.hamq.impl;

import com.chenyiwenglish.hamq.MessageIdGenerator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleIdGenerator implements MessageIdGenerator {

    public Long generateId() {
        return 1L;
    }

}

