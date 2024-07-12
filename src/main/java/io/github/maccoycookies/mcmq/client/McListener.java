package io.github.maccoycookies.mcmq.client;

public interface McListener<T> {

    void onMessage(McMessage<T> message);

}
