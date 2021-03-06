package com.patinousward.demo.rabbitmq.broad;

import com.patinousward.demo.rabbitmq.RabbitMQConnectionFactory;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class ConsumerBroad01 {
    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitMQConnectionFactory.newConnection();
        final Channel channel = connection.createChannel();
        DefaultConsumer defaultConsumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String callbackId = properties.getMessageId();
                if (!"01".equals(callbackId)) {

                    channel.basicReject(envelope.getDeliveryTag(), false);
                } else {
                    // TODO: 2021/2/18  没在缓存中获取到，1.不是这个client的 2.client可能重启了 3.缓存中过期了（执行时间比较长的情况）

                    // TODO: 2021/2/18  如果不是next状态，说明顺序有问题，重新丢回队列中即可
                    System.out.println("consumer01:" + new String(body, StandardCharsets.UTF_8));
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };
        channel.basicConsume("maxcompute-spark-B", false, "01", defaultConsumer);
        System.out.println("----------");
    }
}
