package io.github.maccoycookies.mcmq.demo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class McOrder {

    private long id;
    private String item;
    private double price;

}
