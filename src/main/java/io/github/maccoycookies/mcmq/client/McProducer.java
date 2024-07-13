package io.github.maccoycookies.mcmq.client;

import lombok.AllArgsConstructor;

/**
 * message queue producer
 */
@AllArgsConstructor
public class McProducer {

    McBroker broker;

    public boolean send(String topic, McMessage message) {
        return broker.send(topic, message);
    }

}
