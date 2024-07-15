package io.github.maccoycookies.mcmq.server;

import io.github.maccoycookies.mcmq.client.McMessage;
import io.github.maccoycookies.mcmq.store.Indexer;
import io.github.maccoycookies.mcmq.store.Store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Maccoy
 * @date 2024/7/12 22:36
 * Description queues
 */
public class MessageQueue {

    private static final Map<String, MessageQueue> queues = new HashMap<>();
    private static final String TEST_TOPIC = "io.github.maccoycookies.test";

    static {
        queues.put(TEST_TOPIC, new MessageQueue(TEST_TOPIC));
        queues.put("a", new MessageQueue("a"));
    }

    private Map<String, MessageSubscription> subscriptionMap = new HashMap<>();
    private String topic;
    private Store store = null;

    public MessageQueue(String topic) {
        this.topic = topic;
        this.store = new Store(topic);
    }


    public int send(McMessage<String> message) {
        // if (index >= queue.length) {
        //     return -1;
        // }
        int offset = store.pos();
        message.getHeaders().put("X-offset", String.valueOf(offset));
        store.write(message);
        return offset;
    }

    public McMessage<?> recv(int offset) {
        return store.read(offset);
    }

    private void subscribe(MessageSubscription subscription) {
        subscriptionMap.putIfAbsent(subscription.getConsumerId(), subscription);
    }

    private void unsubscribe(MessageSubscription subscription) {
        subscriptionMap.remove(subscription.getConsumerId());
    }

    public static void sub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println("===> sub: topic = " + subscription.getTopic());
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        System.out.println("===> unsub: topic = " + subscription.getTopic());
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.unsubscribe(subscription);
    }


    public static int send(String topic, McMessage<String> message) {
        MessageQueue messageQueue = queues.get(topic);
        System.out.println("===> send: topic/msg = " + topic + "/" + message);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        return messageQueue.send(message);
    }

    public static McMessage<?> receive(String topic, String consumerId, int ind) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (!messageQueue.subscriptionMap.containsKey(consumerId)) {
            throw new RuntimeException("subscription not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        return messageQueue.recv(ind);
    }

    /**
     * 使用此方法 需要手工更新 subscription的offset
     */
    public static McMessage<?> receive(String topic, String consumerId) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (!messageQueue.subscriptionMap.containsKey(consumerId)) {
            throw new RuntimeException("subscription not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        MessageSubscription messageSubscription = messageQueue.subscriptionMap.get(consumerId);
        // Indexer.Entry entry = Indexer.getEntry(topic, messageSubscription.getOffset());
        McMessage<?> msg = messageQueue.recv(messageSubscription.getOffset());
        System.out.println("===> receive: topic/cid/offset1/offset2 = " + topic + "/" + consumerId + "/" + messageSubscription.getOffset());
        System.out.println("===> receive: msg = " + msg);
        return msg;
    }

    public static List<McMessage<?>> batch(String topic, String consumerId, Integer size) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (!messageQueue.subscriptionMap.containsKey(consumerId)) {
            throw new RuntimeException("subscription not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        MessageSubscription messageSubscription = messageQueue.subscriptionMap.get(consumerId);
        List<McMessage<?>> msgs = new ArrayList<>();
        int cur = 0;
        do {
            McMessage<?> msg = messageQueue.recv(messageSubscription.getOffset());
            Indexer.Entry entry = Indexer.getEntry(topic, messageSubscription.getOffset());
            cur += entry.getLength();
            if (msg == null) break;
            msgs.add(msg);
        } while (msgs.size() < size);
        System.out.println("===> batch: topic/cid/size = " + topic + "/" + consumerId + "/" + msgs.size());
        System.out.println("===> batch: msg = " + msgs);
        return msgs;
    }

    public static int ack(String topic, String consumerId, Integer offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (!messageQueue.subscriptionMap.containsKey(consumerId)) {
            throw new RuntimeException("subscription not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        MessageSubscription messageSubscription = messageQueue.subscriptionMap.get(consumerId);
        if (offset < messageSubscription.getOffset() || offset > Store.LEN) {
            return -1;
        }
        Indexer.Entry entry = Indexer.getEntry(topic, offset);
        System.out.println("===> ack: topic/cid/offset = " + topic + "/" + consumerId + "/" + offset);
        messageSubscription.setOffset(offset + entry.getLength());
        return offset;
    }
}
