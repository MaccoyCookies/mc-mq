package io.github.maccoycookies.mcmq.server;

import io.github.maccoycookies.mcmq.client.McMessage;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @author Maccoy
 * @date 2024/7/12 22:33
 * Description Mq Server
 */
@Controller
@RequestMapping("/mcmq")
public class MqServer {

    // send
    @RequestMapping("/send")
    public Result<String> send(@RequestParam("topic") String topic, @RequestParam("consumerId") String consumerId,
                       @RequestBody McMessage<String> message) {
        return Result.ok("" + MessageQueue.send(topic, consumerId, message));
    }
    // receive
    @RequestMapping("receive")
    public Result<McMessage<?>> receive(@RequestParam("topic") String topic, @RequestParam("consumerId") String consumerId) {
        return Result.msg(MessageQueue.receive(topic, consumerId));
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
        MessageQueue.sub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }

    // unsubscribe
    @RequestMapping("/unsubscribe")
    public Result<String> unsubscribe(@RequestParam("topic") String topic, @RequestParam("consumerId") String consumerId) {
        MessageQueue.unsub(new MessageSubscription(topic, consumerId, -1));
        return Result.ok();
    }

}
