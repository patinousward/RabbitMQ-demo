package com.patinousward.demo.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Consumer01 {
    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitMQConnectionFactory.newConnection();
        final Channel channel = connection.createChannel();
        DefaultConsumer defaultConsumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String callbackId = properties.getMessageId();
                if (!"01".equals(callbackId)) {

                    channel.basicReject(envelope.getDeliveryTag(), true);
                } else {
                    // TODO: 2021/2/18  没在缓存中获取到，1.不是这个client的 2.client可能重启了 3.缓存中过期了（执行时间比较长的情况）

                    // TODO: 2021/2/18  如果不是next状态，说明顺序有问题，重新丢回队列中即可

                    // TODO: 2021/2/18 通过expiration 的设置进行判断，如果设置时间过期，说明直接丢弃消息即可（因为可能已经没有reuest参数了，只能丢掉）
                    System.out.println(properties.getExpiration());
                    System.out.println("consumer01:" + new String(body, StandardCharsets.UTF_8));
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };
        channel.basicConsume("maxcompute-spark", false, "01", defaultConsumer);
        // TODO: 2021/2/18 不能直接close，否则没消费到就关闭了
        System.out.println("----------");
    }
}
