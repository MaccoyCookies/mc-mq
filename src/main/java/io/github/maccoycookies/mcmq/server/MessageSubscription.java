package io.github.maccoycookies.mcmq.server;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Maccoy
 * @date 2024/7/12 22:41
 * Description Message Subscription
 */
@Data
@AllArgsConstructor
public class MessageSubscription {

    private String topic;

    private String consumerId;

    private int offset = -1;



}
