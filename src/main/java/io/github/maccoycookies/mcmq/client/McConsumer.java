package io.github.maccoycookies.mcmq.client;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * message consumer
 */
public class McConsumer<T> {

    private String id;

    McBroker broker;

    static AtomicInteger idgen = new AtomicInteger(0);

    public McConsumer(McBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public McMessage<T> receive(String topic) {
        return broker.receive(topic, id);
    }

    public void subscribe(String topic) {
        broker.subscribe(topic, id);
    }

    public void unsubscribe(String topic) {
        broker.unsubscribe(topic, id);
    }

    public boolean ack(String topic, Integer offset) {
        return broker.ack(topic, id, offset);
    }

    public boolean ack(String topic, McMessage<?> message) {
        int offset = Integer.parseInt(message.getHeaders().get("X-offset"));
        return ack(topic, offset);
    }

    public void listen(String topic, McListener<T> listener) {
        this.listener = listener;
        broker.addListener(topic, this);
    }

    @Getter
    private McListener listener;

}
