package ru.otus;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Arrays;

public class Consumer {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(
                Utils.createConsumerConfig(b -> b.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"))
        );
        consumer.subscribe(Arrays.asList(Utils.TOPIC1, Utils.TOPIC2));

        while (true) {
            var result = consumer.poll(Duration.ofSeconds(5));

            System.out.println(MessageFormat.format("Read {0}", result.count()));

            for (var record : result) {
                System.out.println(MessageFormat.format("Message {0}.{1}: {2} -> {3}",
                        record.topic(), record.partition(),
                        record.key(), record.value())
                );
            }
        }
    }
}
