package io.github.maccoycookies.mcmq.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.github.maccoycookies.mcmq.client.McMessage;
import lombok.Getter;
import lombok.SneakyThrows;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * message store
 */
public class Store {

    private final String topic;

    public static final int LEN = 1024 * 1000;

    public Store(String topic) {
        this.topic = topic;
        init();
    }


    @Getter
    MappedByteBuffer mappedByteBuffer = null;

    @SneakyThrows
    public void init() {
        File file = new File(this.topic + ".dat");
        if (!file.exists()) {
            file.createNewFile();
        }
        Path path = Paths.get(file.getAbsolutePath());
        FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, LEN);

        // TODO 读取索引
        // 判断是否有数据
        // 读前10位，转成int，看是不是大于0，往后翻len的长度，就是下一条记录
        // 重复上一步，一直到0为止，找到数据结尾
        // 找到数据结尾
        // mappedByteBuffer.position(init_position);

        // TODO 如果总数据 > 10M, 使用多个数据文件的list来管理持久化数据
        // 需要创建第二个数据文件，怎么来管理多个数据文件
    }

    public int write(McMessage<String> message) {
        int position = mappedByteBuffer.position();
        System.out.println("write pos ===> " + position);
        String jsonMessage = JSON.toJSONString(message);
        int length = jsonMessage.getBytes(StandardCharsets.UTF_8).length;
        String formatLength = String.format("%10d", length);
        jsonMessage = formatLength + jsonMessage;
        Indexer.addEntry(this.topic, position, length + 10);
        mappedByteBuffer.put(Charset.forName("UTF-8").encode(jsonMessage));
        return position;
    }

    public int pos() {
        return mappedByteBuffer.position();
    }

    public McMessage<String> read(int offset) {
        ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
        Indexer.Entry entry = Indexer.getEntry(this.topic, offset);
        readOnlyBuffer.position(entry.getOffset());
        byte[] bytes = new byte[entry.getLength()];
        readOnlyBuffer.get(bytes, 0, entry.getLength());
        String res = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("read only ===> " + res);
        return JSON.parseObject(res, new TypeReference<McMessage<String>>() {
        });
    }
}
