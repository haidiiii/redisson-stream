package com.example.redissonstream;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.util.Assert;

import java.time.Duration;

/**
 * @author fht
 * @since 2022-08-08 20:00
 */
@Configuration
public class RedisStreamConfiguration {

    @Bean
    public StreamOperations<String, String, String> streamOperations(StringRedisTemplate stringRedisTemplate) {
        return stringRedisTemplate.opsForStream();
    }

    @Bean
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer(StringRedisTemplate stringRedisTemplate) {
        RedisConnectionFactory redisConnectionFactory = stringRedisTemplate.getConnectionFactory();
        StreamOperations<String, String, String> ops = stringRedisTemplate.opsForStream();

        String stream = "test";
        String groupName = "groupName";
        prepareChannelAndGroup(ops, stream, groupName);

        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> streamMessageListenerContainerOptions =
            StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                .builder()
                .batchSize(10)
                .pollTimeout(Duration.ofSeconds(30))
                .serializer(new StringRedisSerializer())
                .build();

        StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer = StreamMessageListenerContainer.create(redisConnectionFactory, streamMessageListenerContainerOptions);
        StreamMessageListenerContainer.ConsumerStreamReadRequest<String> request = StreamMessageListenerContainer.StreamReadRequest.builder(StreamOffset.create("test", ReadOffset.lastConsumed())).consumer(Consumer.from(groupName, "consumerName")).autoAcknowledge(false).cancelOnError((t) -> false).build();
        streamMessageListenerContainer.register(request, message -> System.out.println("receive: " + message.getId() + "," + message.getValue()));
        streamMessageListenerContainer.start();
        return streamMessageListenerContainer;
    }

    public static void prepareChannelAndGroup(StreamOperations<String, String, String> ops, String stream, String group) {
        String status = "OK";
        try {
            StreamInfo.XInfoGroups groups = ops.groups(stream);
            if (groups.stream().noneMatch(xInfoGroup -> group.equals(xInfoGroup.groupName()))) {
                status = ops.createGroup(stream, group);
            }
        } catch (Exception exception) {
            RecordId initialRecord = ops.add(ObjectRecord.create(stream, "Initial Record"));
            Assert.notNull(initialRecord, "Cannot initialize stream with key '" + stream + "'");
            status = ops.createGroup(stream, ReadOffset.from(initialRecord), group);
        } finally {
            Assert.isTrue("OK".equals(status), "Cannot create group with name '" + group + "'");
        }
    }

}
