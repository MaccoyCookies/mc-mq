package io.github.maccoycookies.mcmq.server;

import io.github.maccoycookies.mcmq.client.McMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Maccoy
 * @date 2024/7/12 22:47
 * Description Result for MqServer
 */
@Data
@AllArgsConstructor
public class Result<T> {

    /**
     * 1 -> success
     * 0 -> fail
     */
    private int code;

    private T data;

    public static Result<String> ok() {
        return new Result<String>(1, "OK");
    }

    public static Result<String> ok(String msg) {
        return new Result<String>(1, msg);
    }

    public static Result<McMessage<?>> msg(String msg) {
        return new Result<>(1, McMessage.create(msg, null));
    }

    public static Result<McMessage<?>> msg(McMessage<?> message) {
        return new Result<>(1, message);
    }
}
