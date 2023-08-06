package ru.otus;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
    public static void main(String[] args) throws Exception {
        var producer = new KafkaProducer<String, String>(Utils.createProducerConfig(b -> {
            b.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-id");
        }));
        producer.initTransactions();

        producer.beginTransaction();
        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>(Utils.TOPIC1, Integer.toString(i), i + " - committed - " + Math.random()));
            producer.send(new ProducerRecord<>(Utils.TOPIC2, Integer.toString(i), i + " - committed - " + Math.random()));
        }
        producer.commitTransaction();

        producer.beginTransaction();
        for (int i = 0; i < 2; i++) {
            producer.send(new ProducerRecord<>(Utils.TOPIC1, null, i + " - uncommitted - " + Math.random()));
            producer.send(new ProducerRecord<>(Utils.TOPIC2, null, i + " - uncommitted - " + Math.random()));
        }
        Thread.sleep(500);
        producer.abortTransaction();

        producer.close();
    }
}
