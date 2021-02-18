package com.patinousward.demo.rabbitmq.broad;

import com.patinousward.demo.rabbitmq.RabbitMQConnectionFactory;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class ProducerBroad {

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        Connection connection = RabbitMQConnectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare("exchangeNameB", "fanout", true);
        channel.queueDeclare("maxcompute-spark-B", true, false, false, null);
        channel.queueBind("maxcompute-spark-B", "exchangeNameB", "");

        // TODO: 2021/2/18 广播应该 是申明多个队列，不合适
        channel.confirmSelect();
        for (int i = 0; i < 200; i++) {
            String s = UUID.randomUUID().toString();
            System.out.println(i);
            AMQP.BasicProperties basicProperties = new AMQP.BasicProperties();
            AMQP.BasicProperties build = basicProperties.builder().messageId("01").build();
            channel.basicPublish("exchangeNameB", "", build, String.valueOf(i).getBytes());
            boolean b = channel.waitForConfirms();
            if (b) {
                System.out.println("success");
            } else {
                // TODO: 2021/2/18 抛出异常

                System.out.println("failed");
            }
        }
        channel.close();
        connection.close();
    }
}
