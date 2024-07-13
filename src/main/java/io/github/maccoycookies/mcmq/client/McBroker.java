package io.github.maccoycookies.mcmq.client;

import cn.kimmking.utils.HttpUtils;
import cn.kimmking.utils.ThreadUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.github.maccoycookies.mcmq.server.Result;
import lombok.Getter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * broker for topics
 */
public class McBroker {


    @Getter
    public static final McBroker Default = new McBroker();

    private final static String brokerUrl = "http://localhost:8765/mcmq";

    private final MultiValueMap<String, McConsumer<?>> consumerMap = new LinkedMultiValueMap<>();

    static {
        init();
    }

    public static void init() {
        ThreadUtils.getDefault().init(1);
        ThreadUtils.getDefault().schedule(() -> {
            for (Map.Entry<String, List<McConsumer<?>>> entry : getDefault().consumerMap.entrySet()) {
                String topic = entry.getKey();
                for (McConsumer<?> consumer : entry.getValue()) {
                    McMessage<?> message = consumer.receive(topic);
                    if (message == null) continue;
                    try {
                        consumer.getListener().onMessage(message);
                        consumer.ack(topic, message);
                    } catch (Exception exception) {
                        // TODO
                    }
                }
            }
        }, 100, 100);
    }

    public McProducer createProducer() {
        return new McProducer(this);
    }

    public McConsumer<?> createConsumer(String topic) {
        McConsumer<?> consumer = new McConsumer<>(this);
        consumer.subscribe(topic);
        return consumer;
    }

    public boolean send(String topic, McMessage message) {
        System.out.println("===> send req topic/msg = " + topic + "/" + message);
        Result<String> result = HttpUtils.httpPost(JSON.toJSONString(message), brokerUrl + "/send?topic=" + topic, new TypeReference<Result<String>>(){});
        System.out.println("===> send rsp: " + result);
        return result.getCode() == 1;
    }

    public <T> McMessage<T> receive(String topic, String consumerId) {
        System.out.println("===> receive req: " + topic + "/" + consumerId);
        Result<McMessage<String>> result = HttpUtils.httpGet(brokerUrl + "/receive?topic=" + topic + "&consumerId=" + consumerId, new TypeReference<Result<McMessage<String>>>(){});
        System.out.println("===> receive rsp: " + result);
        return (McMessage<T>) result.getData();
    }

    public void subscribe(String topic, String consumerId) {
        System.out.println("===> subscribe req: " + topic + "/" + consumerId);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/subscribe?topic=" + topic + "&consumerId=" + consumerId, new TypeReference<Result<String>>(){});
        System.out.println("===> subscribe rsp: " + result);
    }

    public void unsubscribe(String topic, String consumerId) {
        System.out.println("===> unsubscribe req: " + topic + "/" + consumerId);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/unsubscribe?topic=" + topic + "&consumerId=" + consumerId, new TypeReference<Result<String>>(){});
        System.out.println("===> unsubscribe rsp: " + result);
    }

    public boolean ack(String topic, String consumerId, Integer offset) {
        System.out.println("===> ack req topic/consumerId/offset = " + topic + "/" + consumerId + "/" + offset);
        Result<String> result = HttpUtils.httpGet(brokerUrl + "/ack?topic=" + topic + "&consumerId=" + consumerId + "&offset=" + offset, new TypeReference<Result<String>>(){});
        System.out.println("===> ack rsp: " + result);
        return result.getCode() == 1;
    }

    public void addListener(String topic, McConsumer<?> consumer) {
        consumerMap.add(topic, consumer);
    }
}
