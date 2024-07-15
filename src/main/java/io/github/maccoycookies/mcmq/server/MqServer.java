package io.github.maccoycookies.mcmq.server;

import io.github.maccoycookies.mcmq.client.McMessage;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author Maccoy
 * @date 2024/7/12 22:33
 * Description Mq Server
 */
@RestController
@RequestMapping("/mcmq")
public class MqServer {

    // send
    @RequestMapping("/send")
    public Result<String> send(@RequestParam("topic") String topic, @RequestBody McMessage<String> message) {
        return Result.ok("" + MessageQueue.send(topic, message));
    }
    // receive
    @RequestMapping("receive")
    public Result<McMessage<?>> receive(@RequestParam("topic") String topic, @RequestParam("consumerId") String consumerId) {
        return Result.msg(MessageQueue.receive(topic, consumerId));
    }

    @RequestMapping("batch")
    public Result<List<McMessage<?>>> batch(@RequestParam("topic") String topic, @RequestParam("consumerId") String consumerId,
                                            @RequestParam(value = "size", required = false, defaultValue = "1000") Integer size) {
        return Result.msg(MessageQueue.batch(topic, consumerId, size));
    }

    // ack
    @RequestMapping("ack")
    public Result<String> ack(@RequestParam("topic") String topic,
                              @RequestParam("consumerId") String consumerId,
                              @RequestParam("offset") Integer offset) {
        return Result.ok("" + MessageQueue.ack(topic, consumerId, offset));
    }

    // subscribe
    @RequestMapping("/subscribe")
    public Result<String> subscribe(@RequestParam("topic") String topic, @RequestParam("consumerId") String consumerId) {
        MessageQueue.sub(new MessageSubscription(topic, consumerId, 0));
        return Result.ok();
    }

    // unsubscribe
    @RequestMapping("/unsubscribe")
    public Result<String> unsubscribe(@RequestParam("topic") String topic, @RequestParam("consumerId") String consumerId) {
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, 0));
        return Result.ok();
    }

}
