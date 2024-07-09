package io.github.maccoycookies.mcmq.core;

import lombok.AllArgsConstructor;

/**
 * message queue producer
 */
@AllArgsConstructor
public class McProducer {

    McBroker broker;

    public boolean send(String topic, McMessage message) {
        McMq mcMq = broker.find(topic);
        if (mcMq == null) throw new RuntimeException("topic not found");
        return mcMq.send(message);
    }

}
