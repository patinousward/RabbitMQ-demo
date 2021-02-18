package com.patinousward.demo.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class Producer01 {

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        Connection connection = RabbitMQConnectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare("exchangeName", "direct", true);
        channel.queueDeclare("maxcompute-spark", true, false, false, null);
        channel.queueBind("maxcompute-spark", "exchangeName", "roudKey");
        channel.confirmSelect();
        for (int i = 0; i < 200; i++) {
            String s = UUID.randomUUID().toString();
            System.out.println(i);
            AMQP.BasicProperties basicProperties = new AMQP.BasicProperties();
            AMQP.BasicProperties build = basicProperties.builder().messageId("01").headers(new HashMap<>()).build();
            channel.basicPublish("exchangeName", "roudKey", build, String.valueOf(i).getBytes());
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
