package com.praveen.kafka.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

import static com.oracle.jrockit.jfr.ContentType.Bytes;
import static org.apache.kafka.streams.kstream.Materialized.as;

//input:
//message: {<user_id>:<favorite_color>}
//output: {<color>:<number_of_people_for_whom_this_is_favorite_color>}
public class FavoriteColor {

    public static void main(String[] args) {
        Properties properties = createConfigProperties();
        FavoriteColor favoriteColorApp = new FavoriteColor();
        KafkaStreams streams = new KafkaStreams(favoriteColorApp.createTopology(), properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private Topology createTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KTable<String, String> table = streamsBuilder.table("favorite-color-input-topic");
                table.groupBy((user,color) -> new KeyValue<>(color, color))
                .count(Materialized.<String, Long, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("favorite-color-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                .toStream()
                .to("favorite-color-count-output-topic", Produced.with(Serdes.String(), Serdes.Long()));
        return streamsBuilder.build();
    }

    private static Properties createConfigProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "favoriteColor");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
