package io.github.maccoycookies.mcmq.client;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * message consumer
 */
public class McConsumer<T> {

    private String id;

    McBroker broker;

    String topic;

    McMq mcMq;

    static AtomicInteger idgen = new AtomicInteger(0);

    public McConsumer(McBroker broker) {
        this.broker = broker;
        this.id = "CID" + idgen.getAndIncrement();
    }

    public void subscribe(String topic) {
        this.topic = topic;
        this.mcMq = broker.find(topic);

    }

    public McMessage<T> poll(long timeout) {
        return mcMq.poll(timeout);
    }

    public void listen(McListener<T> listener) {
        mcMq.addListener(listener);
    }
}
