package com.example.redissonstream;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.TrimStrategy;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author fht
 * @since 2022-08-08 20:45
 */
public class RedissonStreamTest {

    public static void main(String[] args) {
        Random random = new Random();

        Config config = new Config();
        config
            .useSingleServer()
            .setAddress("redis://localhost:6379")
            .setDatabase(4);
        config.setCodec(StringCodec.INSTANCE);
        RedissonClient redissonClient = Redisson.create(config);

//        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(6);
//        executorService.scheduleAtFixedRate(() -> {
        while (true) {
            Map<Object, Object> content = new HashMap<>();
            content.put("time", System.currentTimeMillis() + "-test");
            StreamAddArgs<Object, Object> streamAddArgs = StreamAddArgs.entries(content);
            streamAddArgs.trim(TrimStrategy.MAXLEN, 1000);
            StreamMessageId recordId = redissonClient.getStream("appt:business-group-user:create").add(streamAddArgs);
            System.out.println("recordId: " + recordId);
            try {
                Thread.sleep(random.nextInt(5000));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
//        }, 0, 1, TimeUnit.SECONDS);
    }

}
