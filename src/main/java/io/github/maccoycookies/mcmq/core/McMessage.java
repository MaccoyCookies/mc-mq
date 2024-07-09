package io.github.maccoycookies.mcmq.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * mc message model
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class McMessage<T> {

    // private String topic;

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


}
