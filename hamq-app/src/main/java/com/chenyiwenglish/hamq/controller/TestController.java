package com.chenyiwenglish.hamq.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.chenyiwenglish.hamq.impl.HighAvailabilityMessageQueueProducer;
import com.chenyiwenglish.hamq.spring.boot.autoconfigure.annotation.MessageQueueProducer;

@Controller
@RequestMapping("/hamq")
public class TestController {

    @MessageQueueProducer(queueId = "test", name = "highAvailabilityMessageQueueProducer")
    private HighAvailabilityMessageQueueProducer highAvailabilityMessageQueueProducer;

    @RequestMapping("/test")
    @ResponseBody
    public String test() {
        highAvailabilityMessageQueueProducer.produce("hello, world");
        return "success";
    }

}
