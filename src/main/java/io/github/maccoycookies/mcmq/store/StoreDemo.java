package io.github.maccoycookies.mcmq.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.github.maccoycookies.mcmq.client.McMessage;

import java.io.BufferedInputStream;
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
import java.util.Scanner;

/**
 * mmap store
 */
public class StoreDemo {

    public static void main(String[] args) throws Exception {
        String content = """
                this is a good file.
                that is a new line for store.
                """;
        File file = new File("test.dat");
        if (!file.exists()) {
            file.createNewFile();
        }
        Path path = Paths.get(file.getAbsolutePath());
        try (FileChannel channel = (FileChannel) Files.newByteChannel(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            MappedByteBuffer mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 1024);
            for (int i = 0; i < 10; i++) {
                System.out.println(i + "->" + mappedByteBuffer.position());
                McMessage<String> message = McMessage.create(i + ":" + content, null);
                String messageJson = JSON.toJSONString(message);
                Indexer.addEntry("test", mappedByteBuffer.position(), messageJson.getBytes(StandardCharsets.UTF_8).length);
                mappedByteBuffer.put(Charset.forName("utf-8").encode(messageJson));
            }

            ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                if (line.equals("q")) break;
                System.out.println(" IN = " + line);
                int offset = Integer.parseInt(line);
                Indexer.Entry entry = Indexer.getEntry("test", offset);
                readOnlyBuffer.position(entry.getOffset());
                byte[] bytes = new byte[entry.getLength()];
                readOnlyBuffer.get(bytes, 0, entry.getLength());
                String res = new String(bytes, StandardCharsets.UTF_8);
                System.out.println("read only ===> " + res);
                McMessage<String> message = JSON.parseObject(res, new TypeReference<McMessage<String>>(){});
                System.out.println("read only message.body ===> " + message.getBody());
            }
        }
    }

}
