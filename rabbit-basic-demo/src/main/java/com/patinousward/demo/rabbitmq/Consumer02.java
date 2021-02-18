package com.patinousward.demo.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Consumer02 {
    public static void main(String[] args) throws IOException, TimeoutException {
        Connection connection = RabbitMQConnectionFactory.newConnection();
        final Channel channel = connection.createChannel();
        DefaultConsumer defaultConsumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println(new String(body, StandardCharsets.UTF_8));
                String callbackId = (String) properties.getMessageId();
                if (!"02".equals(callbackId)) {
                    // TODO: 2021/2/18 加上消费时间的判断，如果长时间没有，就直接ack消费掉
                    System.out.println("consumer01: reject" + new String(body, StandardCharsets.UTF_8));
                    channel.basicReject(envelope.getDeliveryTag(), true);
                } else {
                    System.out.println("consumer02:" + new String(body, StandardCharsets.UTF_8));
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };
        channel.basicConsume("maxcompute-spark", false, "01", defaultConsumer);
    }
}
