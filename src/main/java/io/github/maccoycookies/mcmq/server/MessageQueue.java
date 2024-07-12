package io.github.maccoycookies.mcmq.server;

import io.github.maccoycookies.mcmq.client.McMessage;

import java.util.HashMap;
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
    }

    private Map<String, MessageSubscription> subscriptionMap = new HashMap<>();
    private String topic;
    private McMessage<?>[] queue = new McMessage[1024 * 10];
    private int index = 0;

    public MessageQueue(String topic) {
        this.topic = topic;
    }

    public int send(McMessage<?> message) {
        if (index >= queue.length) {
            return -1;
        }
        queue[index++] = message;
        return index;
    }

    public McMessage<?> recv(int ind) {
        if (ind <= index) {
            return queue[ind];
        }
        return null;
    }

    private void subscribe(MessageSubscription subscription) {
        subscriptionMap.putIfAbsent(subscription.getConsumerId(), subscription);
    }

    private void unsubscribe(MessageSubscription subscription) {
        subscriptionMap.remove(subscription.getConsumerId());
    }

    public static void sub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.subscribe(subscription);
    }

    public static void unsub(MessageSubscription subscription) {
        MessageQueue messageQueue = queues.get(subscription.getTopic());
        if (messageQueue == null) throw new RuntimeException("topic not found");
        messageQueue.unsubscribe(subscription);
    }


    public static int send(String topic, String consumerId, McMessage<String> message) {
        MessageQueue messageQueue = queues.get(topic);
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
        return messageQueue.recv(messageSubscription.getOffset());
    }

    public static int ack(String topic, String consumerId, Integer offset) {
        MessageQueue messageQueue = queues.get(topic);
        if (messageQueue == null) throw new RuntimeException("topic not found");
        if (!messageQueue.subscriptionMap.containsKey(consumerId)) {
            throw new RuntimeException("subscription not found for topic/consumerId = " + topic + "/" + consumerId);
        }
        MessageSubscription messageSubscription = messageQueue.subscriptionMap.get(consumerId);
        if (offset <= messageSubscription.getOffset() || offset > messageQueue.index) {
            return -1;
        }
        messageSubscription.setOffset(offset);
        return offset;
    }
}
