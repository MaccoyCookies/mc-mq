package io.github.maccoycookies.mcmq.core;

/**
 * message consumer
 */
public class McConsumer<T> {

    McBroker broker;

    String topic;

    McMq mcMq;

    public McConsumer(McBroker broker) {
        this.broker = broker;
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
