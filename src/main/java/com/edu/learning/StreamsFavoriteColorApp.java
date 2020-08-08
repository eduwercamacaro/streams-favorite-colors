package com.edu.learning;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class StreamsFavoriteColorApp {

    private static final Set<String> ALLOWED_COLORS = new HashSet<>(Arrays.asList("BLUE", "RED", "GREEN"));


    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-colors-app");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> streamSource = streamsBuilder.stream("users-color");
        KTable<String, Long> count = streamSource.filter((key, value) -> value != null)
                .mapValues((readOnlyKey, value) -> value.toUpperCase())
                .filter((key, value) -> ALLOWED_COLORS.contains(value))
                .selectKey((key, value) -> value)
                .groupByKey()
                .count(Named.as("colors-count"));
        count.toStream().to("favorite-colors");
        Topology build = streamsBuilder.build();

        System.out.println(build.describe());

        final KafkaStreams streams = new KafkaStreams(build, properties);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);


    }

}
