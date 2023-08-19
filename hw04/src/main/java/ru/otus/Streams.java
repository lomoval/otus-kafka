package ru.otus;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.sql.Time;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Windowed;
import ru.otus.model.Event;
import ru.otus.model.JsonDeserializer;
import ru.otus.model.JsonSerializer;
import java.time.Duration;
import java.util.UUID;

public class Streams {
    static final String TOPIC = "hw04-events";

    public static void main(String[] args) throws Exception {
        var builder = new StreamsBuilder();

        Serde<String> keySerde = Serdes.String();
        Serde<Event> mesSerde =  new Serdes.WrapperSerde<>(new JsonSerializer<>(), new JsonDeserializer<>(Event.class));

        var winLength = Duration.ofMinutes(5);
        KTable<Windowed<String>, Long> counts = builder
                .stream(TOPIC, Consumed.with(keySerde, mesSerde)                )
                .groupBy((key, event) -> key, Grouped.with(keySerde, mesSerde))
                .windowedBy(TimeWindows.of(winLength))
                .count();

        KStream<String, Long> countStream = counts.toStream().map((window, count) -> {
                DateFormat dateFormatOne = new SimpleDateFormat("HH:mm");
                dateFormatOne.setTimeZone(TimeZone.getTimeZone("UTC"));
                String info = dateFormatOne.format(Time.from(window.window().startTime())) + "-" +
                        dateFormatOne.format(Time.from(window.window().endTime())) +
                        " - count " + window.key();
                return KeyValue.pair(info, count);
        });
        countStream.foreach((info, v) -> {
            Utils.log.info("{}: {}", info, v);
        });

        var topology = builder.build();

        Utils.log.warn("{}", topology.describe());
        try (
                var kafkaStreams = new KafkaStreams(topology, Utils.createStreamsConfig(b -> {
                    b.put(StreamsConfig.APPLICATION_ID_CONFIG, "hw04-" + UUID.randomUUID().hashCode());
                    b.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
                    b.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
                }));
        ) {
            Utils.log.info("App Started");
            kafkaStreams.start();
            new CountDownLatch(1).await();
        } catch (Exception e){
            Utils.log.error(e.getMessage());
        }
    }
}
