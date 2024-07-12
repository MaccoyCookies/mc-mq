package io.github.maccoycookies.mcmq.client;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * mc message model
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class McMessage<T> {

    // private String topic;

    static AtomicLong idgen = new AtomicLong(0);

    private Long id;

    private T body;

    /**
     * 系统属性
     */
    private Map<String, String> headers;

    /**
     * 业务属性
     */
    // private Map<String, String> properties;

    public static long getId() {
        return idgen.getAndIncrement();
    }

    public static McMessage<?> create(String body, Map<String, String> header) {
        return new McMessage<>(getId(), body, header);
    }

}
