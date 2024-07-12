package io.github.maccoycookies.mcmq.client;

import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Mq for topic
 */

public class McMq {

    public McMq(String topic) {
        this.topic = topic;
    }

    private String topic;

    private LinkedBlockingQueue<McMessage> queue = new LinkedBlockingQueue<>();
    private List<McListener> listeners = new ArrayList<>();

    public boolean send(McMessage message) {
        boolean offered = queue.offer(message);
        for (McListener listener : listeners) {
            listener.onMessage(message);
        }
        return offered;
    }

    // 拉模式获取消息
    @SneakyThrows
    public <T> McMessage<T> poll(long timeout) {
        return queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    public <T> void addListener(McListener<T> listener) {
        listeners.add(listener);
    }
}
