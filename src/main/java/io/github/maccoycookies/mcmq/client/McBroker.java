package io.github.maccoycookies.mcmq.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * broker for topics
 */
public class McBroker {

    Map<String, McMq> mqMapping = new ConcurrentHashMap<>(64);

    public McMq find(String topic) {
        return mqMapping.get(topic);
    }

    public McMq createTopic(String topic) {
        return mqMapping.putIfAbsent(topic, new McMq(topic));
    }

    public McProducer createProducer() {
        return new McProducer(this);
    }

    public McConsumer<?> createConsumer(String topic) {
        McConsumer<?> consumer = new McConsumer<>(this);
        consumer.subscribe(topic);
        return consumer;
    }
}
