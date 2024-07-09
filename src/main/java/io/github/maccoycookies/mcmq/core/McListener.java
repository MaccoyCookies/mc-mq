package io.github.maccoycookies.mcmq.core;

public interface McListener<T> {

    void onMessage(McMessage<T> message);

}
