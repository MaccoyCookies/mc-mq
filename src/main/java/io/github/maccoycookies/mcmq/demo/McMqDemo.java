package io.github.maccoycookies.mcmq.demo;

import io.github.maccoycookies.mcmq.client.McBroker;
import io.github.maccoycookies.mcmq.client.McConsumer;
import io.github.maccoycookies.mcmq.client.McMessage;
import io.github.maccoycookies.mcmq.client.McProducer;

public class McMqDemo {

    public static void main(String[] args) throws Exception {

        long ids = 0;

        String topic = "mc.order";
        McBroker broker = new McBroker();
        broker.createTopic(topic);

        McProducer producer = broker.createProducer();
        McConsumer<?> consumer = broker.createConsumer(topic);
        consumer.listen(message -> {
            System.out.println("onMessage => " + message);
        });

        for (int i = 0; i < 10; i++) {
            McOrder order = new McOrder(ids, "Mc-Item-" + ids, 100.0 * ids);
            producer.send(topic, new McMessage<>(ids++, order, null));
        }

        for (int i = 0; i < 10; i++) {
            McMessage<McOrder> message = (McMessage<McOrder>) consumer.poll(1000);
            System.out.println(message);
        }

        while (true) {
            char c = (char) System.in.read();
            if (c == 'q' || c == 'e') {
                break;
            }
            if (c == 'p') {
                McOrder order = new McOrder(ids, "Mc-Item-" + ids, 100.0 * ids);
                producer.send(topic, new McMessage<>(ids++, order, null));
                System.out.println("send ok => " + order);
            }
            if (c == 'c') {
                McMessage<McOrder> message = (McMessage<McOrder>) consumer.poll(1000);
                System.out.println("poll ok => " + message);
            }

            if (c == 'a') {
                for (int i = 0; i < 10; i++) {
                    McOrder order = new McOrder(ids, "Mc-Item-" + ids, 100.0 * ids);
                    producer.send(topic, new McMessage<>(ids++, order, null));
                }
                System.out.println("send 10 orders ...");
            }

        }

    }

}
